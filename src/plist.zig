const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.dbg;
const Appender = mod_utils.Appender;

const mod_gram = @import("gram.zig");

const mod_mmap = @import("mmap.zig");
const SwappingAllocator = mod_mmap.SwappingAllocator;
const SwappingList = mod_mmap.SwappingList;
const Pager = mod_mmap.Pager;
const Ptr = mod_mmap.SwappingAllocator.Ptr;

pub fn SwappingPostingListUnmanaged(comptime I: type, comptime gram_len: u4) type {
    if (gram_len == 0) {
        @compileError("gram_len is 0");
    }
    return struct {
        const Self = @This();
        const Gram = mod_gram.Gram(@as(u4, gram_len));
        const GramPos = mod_gram.GramPos(@as(u4, gram_len));

        const map_len = std.math.pow(usize, std.math.maxInt(u8) + 1, gram_len);

        pager: Pager,
        // map: []?SwappingList(I), // = [_]?SwappingList(I){null} ** map_len,
        map: std.AutoHashMapUnmanaged(Gram, SwappingList(I)),
        cache: std.AutoHashMapUnmanaged(GramPos, void) = .{},

        pub fn init(pager: Pager) Self {
            return Self {
                .pager = pager,
                .map = std.AutoHashMapUnmanaged(Gram, SwappingList(I)){},
                .cache = std.AutoHashMapUnmanaged(GramPos, void){},
                // .map = try ha7r.alloc(?SwappingList(I), map_len),
            };
        }

        pub fn deinit(self: *Self, ha7r: Allocator) void {
            var it = self.map.valueIterator();
            while (it.next()) |list| {
                list.deinit();
            }
            self.map.deinit(ha7r);
            self.cache.deinit(ha7r);
        }

        pub fn insert(self: *Self, a7r: Allocator, id: I, name: []const u8, delimiters: []const u8) !void {
            self.cache.clearRetainingCapacity();

            var appender = blk: {
                const curry = struct {
                    map: *std.AutoHashMapUnmanaged(GramPos, void),
                    a7r: Allocator,

                    fn append(this: *@This(), item: GramPos) !void {
                        try this.map.put(this.a7r, item, {});
                    }
                };
                break :blk Appender(GramPos).new(&curry{ .map = &self.cache, .a7r = a7r }, curry.append);
            };
            try mod_gram.grammer(@as(u4, gram_len), name, true, delimiters, appender);

            var it = self.cache.keyIterator();
            while (it.next()) |gpos| {
                var list = blk: {
                    const entry = try self.map.getOrPut(a7r, gpos.gram);
                    if (entry.found_existing) {
                        break :blk entry.value_ptr;
                    } else {
                        entry.value_ptr.* = SwappingList(I).init(a7r, self.pager);
                        break :blk entry.value_ptr;
                    }
                };
                // try list.append(allocator, .{ .id = id, .pos = gpos.pos });
                try list.append(id);
            }
        }

        pub fn str_matcher(allocator: Allocator, pager: Pager) StrMatcher {
            return StrMatcher.init(allocator, pager);
        }

        pub const StrMatcher = struct {
            // allocator: Allocator,
            out_vec: std.ArrayList(I),
            check: std.AutoHashMap(I, void),
            grams: std.ArrayList(GramPos),
            pager: Pager,
            
            pub fn init(allocator: Allocator, pager: Pager) StrMatcher {
                return StrMatcher{
                    .check = std.AutoHashMap(I, void).init(allocator),
                    .out_vec = std.ArrayList(I).init(allocator),
                    .grams = std.ArrayList(GramPos).init(allocator),
                    .pager = pager
                };
            }

            pub fn deinit(self: *StrMatcher) void {
                self.out_vec.deinit();
                self.check.deinit();
                self.grams.deinit();
            }
            // const Clause = union(enum){
            //     const Op = union(enum){
            //         And: Clause,
            //         Or: Clause,
            //         // Not: Clause,
            //     };
            //     const Match = struct {
            //         grams: []const Gram,
            //     };
            //     op: Op,
            //     match: Match,
            //
            // };
            
            pub const Error = error { TooShort } || Allocator.Error || mod_mmap.Pager.SwapInError;

            /// Returned slice is invalid by next usage of this func.
            /// FIXME: optimize
            pub fn str_match(
                self: *StrMatcher, 
                plist: *const Self,
                string: []const u8, 
                delimiters: []const u8,
            ) Error![]const I {
                if (string.len < gram_len) return Error.TooShort;

                self.check.clearRetainingCapacity();
                self.out_vec.clearRetainingCapacity();
                self.grams.clearRetainingCapacity();

                try mod_gram.grammer(
                    gram_len, 
                    string, 
                    false, 
                    delimiters, 
                    Appender(GramPos).new(&self.grams, std.ArrayList(GramPos).append)
                );
                var is_init = false;
                for (self.grams.items) |gpos| {
                    const gram = gpos.gram;
                    // if we've seen the gram before
                    if (plist.map.get(gram)) |list| {
                        // if this isn't our first gram
                        if (is_init) {
                            self.check.clearRetainingCapacity();
                            for (self.out_vec.items) |id| {
                                try self.check.put(id, {});
                            }

                            self.out_vec.clearRetainingCapacity();

                            var it = list.iteratorWithCache(self.pager);
                            defer it.close();
                            while (try it.next()) |ptr| {
                                const id = ptr.*;
                                // reduce the previous list of eligible
                                // matches according the the new list
                                if (self.check.contains(id)) {
                                    try self.out_vec.append(id);
                                }
                            }
                            if (self.out_vec.items.len == 0 ) {
                                // no items contain that gram
                                return &[_]I{};
                            }
                        } else {
                            // alll items satisfying first gram are elgiible
                            var it = list.iteratorWithCache(self.pager);
                            defer it.close();
                            while (try it.next()) |ptr| {
                                const id = ptr.*;
                                try self.out_vec.append(id);
                            }
                            is_init = true;
                        }
                    } else {
                        // no items contain that gram
                        return &[_]I{};
                    }
                }
                return self.out_vec.items;
            }
        };
    };
}

test "SwappingPlist.str_match" {
    const TriPList = SwappingPostingListUnmanaged(u64, 3);
        // const exp_uni = @as(TriPList.StrMatcher.Error![]const u64, case.expected);
    const Expected = union(enum){
        ok: []const u64,
        err: TriPList.StrMatcher.Error,
    };
    comptime var table = .{
        .{ 
            .name = "single_gram", 
            .items = ([_][]const u8{ "Bilbo Baggins", "Frodo Baggins", "Bagend", "Thorin Oakenshield" })[0..], 
            .query = "Bag", 
            .expected = Expected { .ok = &.{ 0, 1, 2 } },
        },
        .{ 
            .name = "single_gram.not_found", 
            .items = ([_][]const u8{ "Bilbo Baggins", "Frodo Baggins", "Bagend", "Thorin Oakenshield" })[0..], 
            .query = "Gab", 
            .expected = Expected { .ok = &.{ } }
        },
        .{ 
            .name = "multi_gram", 
            .items = ([_][]const u8{ "Bilbo Baggins", "Frodo Baggins", "Bagend" })[0..], 
            .query = "Bagend", 
            .expected = Expected { .ok = &.{2} }
        },
        .{ 
            .name = "multi_gram.not_found", 
            .items = ([_][]const u8{ "Bilbo Baggins", "Frodo Baggins", "Knife Party" })[0..], 
            .query = "Bagend", 
            .expected = Expected { .ok = &.{ } }
        },
        .{ 
            .name = "boundary_actual.1", 
            .items = ([_][]const u8{ "Gandalf", "Sauron", "Galandriel" })[0..], 
            .query = "Ga", 
            // .expected = &.{0, 2} 
            .expected = Expected { .err = TriPList.StrMatcher.Error.TooShort },
        },
        .{ 
            .name = "boundary_actual.2", 
            .items = ([_][]const u8{ "Gandalf", "Sauron", "Galandriel" })[0..], 
            .query = "Sau", 
            .expected = Expected { .ok = &.{1} }
        },
        .{ 
            .name = "boundary_delimter", 
            .items = ([_][]const u8{ " Gandalf", " Sauron", " Lady\nGalandriel" })[0..], 
            .query = "Ga", 
            // .expected = &.{0, 2} 
            .expected = Expected { .err = TriPList.StrMatcher.Error.TooShort },
        },
    };
    inline for (table) |case| {
        var ha7r = std.testing.allocator;

        var mmap_pager = try mod_mmap.MmapPager.init(ha7r, "/tmp/SwappingPlist.str_match", .{});
        defer mmap_pager.deinit();

        var lru = try mod_mmap.LRUSwapCache.init(ha7r, mmap_pager.pager(), 1);
        defer lru.deinit();

        var pager = lru.pager();

        var plist = TriPList.init(pager);
        defer plist.deinit(ha7r);

        var matcher = TriPList.str_matcher(ha7r, pager);
        defer matcher.deinit();

        for (case.items) |name, id| {
            try plist.insert(std.testing.allocator, @as(u64, id), name, std.ascii.spaces[0..]);
        }

        var res = matcher.str_match(&plist, case.query, std.ascii.spaces[0..]);
        switch (case.expected) {
            .ok => |expected|{
                var matches = try res;
                std.testing.expectEqualSlices(u64, expected, matches) catch |err| {
                    std.debug.print("{s}\n{any}\n!=\n{any}\n", .{ case.name, expected, matches });
                    var it = plist.map.iterator();
                    while (it.next()) |pair| {
                        var gram_items = try ha7r.alloc(usize, pair.value_ptr.len);
                        defer ha7r.free(gram_items);
                        var ii: usize = 0;
                        var it2 = pair.value_ptr.iterator();
                        defer it2.close();
                        while (try it2.next()) |id| {
                            gram_items[ii] = id.*;
                            ii += 1;
                        }
                        std.debug.print("gram {s} => {any}\n", .{ pair.key_ptr.*, gram_items });
                    }
                    for (matcher.grams.items) |gram| {
                        std.debug.print("search grams: {s}\n", .{ gram.gram });
                    }
                    return err;
                };
            },
            .err => |e_err| {
                try std.testing.expectError(e_err, res);
            }
        }
    }
}

pub fn PostingListUnmanaged(comptime I: type, comptime gram_len: u4) type {
    if (gram_len == 0) {
        @compileError("gram_len is 0");
    }
    return struct {
        const Self = @This();
        const Gram = mod_gram.Gram(gram_len);
        const GramPos = mod_gram.GramPos(gram_len);
        // const GramRef = struct {
        //     id: I,
        //     pos: usize,
        // };

        map: std.AutoHashMapUnmanaged(Gram, std.ArrayListUnmanaged(I)),
        cache: std.AutoHashMapUnmanaged(GramPos, void),

        pub fn init() Self {
            return Self{
                .map = std.AutoHashMapUnmanaged(Gram, std.ArrayListUnmanaged(I)){},
                .cache = std.AutoHashMapUnmanaged(GramPos, void){},
            };
        }

        pub fn deinit(self: *Self, allocator: Allocator) void {
            self.cache.deinit(allocator);
            var it = self.map.valueIterator();
            while (it.next()) |list| {
                list.deinit(allocator);
            }
            self.map.deinit(allocator);
        }

        pub fn insert(self: *Self, allocator: Allocator, id: I, name: []const u8, delimiters: []const u8) !void {
            self.cache.clearRetainingCapacity();

            var appender = blk: {
                const curry = struct {
                    map: *std.AutoHashMapUnmanaged(GramPos, void),
                    allocator: Allocator,

                    fn append(this: *@This(), item: GramPos) !void {
                        try this.map.put(this.allocator, item, {});
                    }
                };
                break :blk Appender(GramPos).new(&curry{ .map = &self.cache, .allocator = allocator }, curry.append);
            };
            try mod_gram.grammer(gram_len, name, true, delimiters, appender);

            var it = self.cache.keyIterator();
            while (it.next()) |gpos| {
                var list = blk: {
                    const entry = try self.map.getOrPut(allocator, gpos.gram);
                    if (entry.found_existing) {
                        break :blk entry.value_ptr;
                    } else {
                        entry.value_ptr.* = std.ArrayListUnmanaged(I){};
                        break :blk entry.value_ptr;
                    }
                };
                // try list.append(allocator, .{ .id = id, .pos = gpos.pos });
                try list.append(allocator, id);
            }
        }

        pub fn str_matcher(allocator: Allocator) StrMatcher {
            return StrMatcher.init(allocator);
        }

        pub const StrMatcher = struct {
            // allocator: Allocator,
            out_vec: std.ArrayList(I),
            check: std.AutoHashMap(I, void),
            grams: std.ArrayList(GramPos),
            
            pub fn init(allocator: Allocator) StrMatcher {
                return StrMatcher{
                    .check = std.AutoHashMap(I, void).init(allocator),
                    .out_vec = std.ArrayList(I).init(allocator),
                    .grams = std.ArrayList(GramPos).init(allocator),
                };
            }

            pub fn deinit(self: *StrMatcher) void {
                self.out_vec.deinit();
                self.check.deinit();
                self.grams.deinit();
            }
            // const Clause = union(enum){
            //     const Op = union(enum){
            //         And: Clause,
            //         Or: Clause,
            //         // Not: Clause,
            //     };
            //     const Match = struct {
            //         grams: []const Gram,
            //     };
            //     op: Op,
            //     match: Match,
            //
            // };
            
            pub const Error = error { TooShort } || Allocator.Error;

            /// Returned slice is invalid by next usage of this func.
            /// FIXME: optimize
            pub fn str_match(
                self: *StrMatcher, 
                plist: *const Self,
                string: []const u8, 
                delimiters: []const u8,
            ) Error![]const I {
                if (string.len < gram_len) return Error.TooShort;

                self.check.clearRetainingCapacity();
                self.out_vec.clearRetainingCapacity();
                self.grams.clearRetainingCapacity();

                try mod_gram.grammer(
                    gram_len, 
                    string, 
                    false, 
                    delimiters, 
                    Appender(GramPos).new(&self.grams, std.ArrayList(GramPos).append)
                );
                var is_init = false;
                for (self.grams.items) |gpos| {
                    const gram = gpos.gram;
                    // if we've seen the gram before
                    if (plist.map.get(gram)) |list| {
                        // if this isn't our first gram
                        if (is_init) {
                            self.check.clearRetainingCapacity();
                            for (self.out_vec.items) |id| {
                                try self.check.put(id, {});
                            }

                            self.out_vec.clearRetainingCapacity();
                            for (list.items) |id| {
                                // reduce the previous list of eligible
                                // matches according the the new list
                                if (self.check.contains(id)) {
                                    try self.out_vec.append(id);
                                }
                            }
                            if (self.out_vec.items.len == 0 ) {
                                // no items contain that gram
                                return &[_]I{};
                            }
                        } else {
                            // alll items satisfying first gram are elgiible
                            for (list.items) |id| {
                                try self.out_vec.append(id);
                            }
                            is_init = true;
                        }
                    } else {
                        // no items contain that gram
                        return &[_]I{};
                    }
                }
                return self.out_vec.items;
            }
        };
    };
}

test "plist.str_match" {
    const TriPList = PostingListUnmanaged(u64, 3);
        // const exp_uni = @as(TriPList.StrMatcher.Error![]const u64, case.expected);
    const Expected = union(enum){
        ok: []const u64,
        err: TriPList.StrMatcher.Error,
    };
    comptime var table = .{
        .{ 
            .name = "single_gram", 
            .items = ([_][]const u8{ "Bilbo Baggins", "Frodo Baggins", "Bagend", "Thorin Oakenshield" })[0..], 
            .query = "Bag", 
            .expected = Expected { .ok = &.{ 0, 1, 2 } },
        },
        .{ 
            .name = "single_gram.not_found", 
            .items = ([_][]const u8{ "Bilbo Baggins", "Frodo Baggins", "Bagend", "Thorin Oakenshield" })[0..], 
            .query = "Gab", 
            .expected = Expected { .ok = &.{ } }
        },
        .{ 
            .name = "multi_gram", 
            .items = ([_][]const u8{ "Bilbo Baggins", "Frodo Baggins", "Bagend" })[0..], 
            .query = "Bagend", 
            .expected = Expected { .ok = &.{2} }
        },
        .{ 
            .name = "multi_gram.not_found", 
            .items = ([_][]const u8{ "Bilbo Baggins", "Frodo Baggins", "Knife Party" })[0..], 
            .query = "Bagend", 
            .expected = Expected { .ok = &.{ } }
        },
        .{ 
            .name = "boundary_actual.1", 
            .items = ([_][]const u8{ "Gandalf", "Sauron", "Galandriel" })[0..], 
            .query = "Ga", 
            // .expected = &.{0, 2} 
            .expected = Expected { .err = TriPList.StrMatcher.Error.TooShort },
        },
        .{ 
            .name = "boundary_actual.2", 
            .items = ([_][]const u8{ "Gandalf", "Sauron", "Galandriel" })[0..], 
            .query = "Sau", 
            .expected = Expected { .ok = &.{1} }
        },
        .{ 
            .name = "boundary_delimter", 
            .items = ([_][]const u8{ " Gandalf", " Sauron", " Lady\nGalandriel" })[0..], 
            .query = "Ga", 
            // .expected = &.{0, 2} 
            .expected = Expected { .err = TriPList.StrMatcher.Error.TooShort },
        },
    };
    var allocator = std.testing.allocator;
    inline for (table) |case| {
        var plist = TriPList.init();
        defer plist.deinit(allocator);

        var matcher = TriPList.str_matcher(allocator);
        defer matcher.deinit();

        for (case.items) |name, id| {
            try plist.insert(std.testing.allocator, @as(u64, id), name, std.ascii.spaces[0..]);
        }

        var res = matcher.str_match(&plist, case.query, std.ascii.spaces[0..]);
        switch (case.expected) {
            .ok => |expected|{
                var matches = try res;
                std.testing.expectEqualSlices(u64, expected, matches) catch |err| {
                    std.debug.print("{s}\n{any}\n!=\n{any}\n", .{ case.name, expected, matches });
                    var it = plist.map.iterator();
                    while (it.next()) |pair| {
                        std.debug.print("gram {s} => {any}\n", .{ pair.key_ptr.*, pair.value_ptr.items });
                    }
                    for (matcher.grams.items) |gram| {
                        std.debug.print("search grams: {s}\n", .{ gram.gram });
                    }
                    return err;
                };
            },
            .err => |e_err| {
                try std.testing.expectError(e_err, res);
            }
        }
    }
}
