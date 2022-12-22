const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.dbg;
const Appender = mod_utils.Appender;

const mod_gram = @import("gram.zig");

const mod_mmap = @import("mmap.zig");
const SwapAllocator = mod_mmap.SwapAllocator;
const SwapList = mod_mmap.SwapList;
const Pager = mod_mmap.Pager;
const Ptr = mod_mmap.SwapAllocator.Ptr;

pub fn SwapPostingList(comptime I: type, comptime gram_len: u4) type {
    if (gram_len == 0) {
        @compileError("gram_len is 0");
    }
    return struct {
        const Self = @This();
        const Gram = mod_gram.Gram(@as(u4, gram_len));
        const GramPos = mod_gram.GramPos(@as(u4, gram_len));

        const map_len = std.math.pow(usize, std.math.maxInt(u8) + 1, gram_len);

        // ha7r: Allocator,
        // sa7r: SwapAllocator,
        // pager: Pager,
        // map: []?SwapList(I), // = [_]?SwapList(I){null} ** map_len,
        map: std.AutoHashMapUnmanaged(Gram, SwapList(I)) = .{},
        cache: std.AutoHashMapUnmanaged(GramPos, void) = .{},

        // pub fn init(
        //     ha7r:Allocator, sa7r: SwapAllocator, pager: Pager
        // ) Self {
        //     return Self {
        //         // .ha7r = ha7r,
        //         // .sa7r = sa7r,
        //         // .pager = pager,
        //         // .map = std.AutoHashMapUnmanaged(Gram, SwapList(I)){},
        //         // .cache = std.AutoHashMapUnmanaged(GramPos, void){},
        //         // .map = try ha7r.alloc(?SwapList(I), map_len),
        //     };
        // }

        pub fn deinit(
            self: *Self, ha7r:Allocator, sa7r: SwapAllocator, pager: Pager
        ) void {
            var it = self.map.valueIterator();
            while (it.next()) |list| {
                list.deinit(ha7r, sa7r, pager);
            }
            self.map.deinit(ha7r);
            self.cache.deinit(ha7r);
        }

        pub fn insert(
            self: *Self, 
            ha7r:Allocator, 
            sa7r: SwapAllocator, 
            pager: Pager,
            id: I, 
            name: []const u8, 
            delimiters: []const u8
        ) !void {
            self.cache.clearRetainingCapacity();

            try mod_gram.grammer(
                @as(u4, gram_len), 
                name, true, 
                delimiters, 
                Appender(GramPos).new(
                    &Appender(GramPos).Curry.UnmanagedSet{ .set = &self.cache, .a7r = ha7r }, 
                    Appender(GramPos).Curry.UnmanagedSet.put
                )
            );

            var it = self.cache.keyIterator();
            while (it.next()) |gpos| {
                var list = blk: {
                    const entry = try self.map.getOrPut(ha7r, gpos.gram);
                    if (entry.found_existing) {
                        break :blk entry.value_ptr;
                    } else {
                        entry.value_ptr.* = SwapList(I).init(pager.pageSize());
                        break :blk entry.value_ptr;
                    }
                };
                // try list.append(allocator, .{ .id = id, .pos = gpos.pos });
                try list.append(ha7r, sa7r, pager, id);
            }
        }

        /// Returned slice is only valid until next modification of `self`.
        pub fn gramItems(
            self: *const Self,
            gram: Gram,
            sa7r: SwapAllocator,
            pager: Pager,
            appender: Appender(I)
        ) !void {
            if (self.map.get(gram)) |list| {
                var it = list.iterator(sa7r, pager);
                defer it.close();
                while (try it.next()) |ptr| {
                    const id = ptr.*;
                    try appender.append(id);
                }
            }
        }

        pub const StrMatcher = struct {
            out_vec: std.ArrayListUnmanaged(I) = .{},
            check: std.AutoHashMapUnmanaged(I, void) = .{},
            grams: std.ArrayListUnmanaged(GramPos) = .{},
            // sa7r: SwapAllocator,
            // pager: Pager,
            // ha7r: Allocator,
            
            // pub fn init(ha7r: Allocator, sa7r: SwapAllocator, pager: Pager) StrMatcher {
            //     return StrMatcher{
            //         // .ha7r = ha7r,
            //         // .pager = pager,
            //         // .sa7r = sa7r,
            //     };
            // }

            pub fn deinit(
                self: *StrMatcher, ha7r: Allocator, // sa7r: SwapAllocator, pager: Pager
            ) void {
                self.out_vec.deinit(ha7r);
                self.check.deinit(ha7r);
                self.grams.deinit(ha7r);
            }
            
            pub const Error = error { TooShort } || 
                    Allocator.Error || 
                    mod_mmap.Pager.SwapInError;

            /// Returned slice is invalid by next usage of this func.
            /// FIXME: optimize
            pub fn strMatch(
                self: *StrMatcher, 
                ha7r: Allocator, 
                sa7r: SwapAllocator, 
                pager: Pager,
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
                    Appender(GramPos).new(
                        &Appender(GramPos).Curry.UnamanagedList { .a7r = ha7r, .list = &self.grams }, 
                        Appender(GramPos).Curry.UnamanagedList.append
                    )
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
                                try self.check.put(ha7r, id, {});
                            }

                            self.out_vec.clearRetainingCapacity();

                            var it = list.iterator(sa7r, pager);
                            defer it.close();
                            while (try it.next()) |ptr| {
                                const id = ptr.*;
                                // reduce the previous list of eligible
                                // matches according the the new list
                                if (self.check.contains(id)) {
                                    try self.out_vec.append(ha7r, id);
                                }
                            }
                            if (self.out_vec.items.len == 0 ) {
                                // no items contain that gram
                                return &[_]I{};
                            }
                        } else {
                            // alll items satisfying first gram are elgiible
                            var it = list.iterator(sa7r, pager);
                            defer it.close();
                            while (try it.next()) |ptr| {
                                const id = ptr.*;
                                try self.out_vec.append(ha7r, id);
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

test "SwapPlist.strMatch" {
    const TriPList = SwapPostingList(u64, 3);
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

        var mmap_pager = try mod_mmap.MmapPager.init(ha7r, "/tmp/SwapPlist.strMatch", .{});
        defer mmap_pager.deinit();

        var lru = try mod_mmap.LRUSwapCache.init(ha7r, mmap_pager.pager(), 1);
        defer lru.deinit();
        var pager = lru.pager();

        var msa7r = mod_mmap.PagingSwapAllocator(.{}).init(ha7r, pager);
        defer msa7r.deinit();
        var sa7r = msa7r.allocator();

        var plist = TriPList{};
        defer plist.deinit(ha7r, sa7r, pager);

        var matcher = TriPList.StrMatcher{};
        defer matcher.deinit(ha7r);

        for (case.items) |name, id| {
            try plist.insert(
               ha7r, sa7r, pager, @as(u64, id), name, std.ascii.whitespace[0..]
            );
        }

        var res = matcher.strMatch(ha7r, sa7r, pager, &plist, case.query, std.ascii.whitespace[0..]);
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
                        var it2 = pair.value_ptr.iterator(sa7r, pager);
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

            try mod_gram.grammer(
                gram_len, 
                name, 
                true, 
                delimiters, 
                Appender(GramPos).new(
                    &Appender(GramPos).Curry.UnmanagedSet { .set = &self.cache, .a7r = allocator}, 
                    Appender(GramPos).Curry.UnmanagedSet.put
                )
            );

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

        /// Returned slice is only valid until next modification of `self`.
        pub fn gramItems(
            self: *const Self,
            gram: Gram,
        ) []const I {
            if (self.map.get(gram)) |list| {
                return list.items;
            } else {
                return &[_]I{};
            }
        }

        pub fn strMatcher(allocator: Allocator) StrMatcher {
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
            pub fn strMatch(
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

test "plist.strMatch" {
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

        var matcher = TriPList.strMatcher(allocator);
        defer matcher.deinit();

        for (case.items) |name, id| {
            try plist.insert(std.testing.allocator, @as(u64, id), name, std.ascii.whitespace[0..]);
        }

        var res = matcher.strMatch(&plist, case.query, std.ascii.whitespace[0..]);
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
