const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

pub const mod_utils = @import("utils.zig");
pub const println = mod_utils.println;
pub const dbg = mod_utils.dbg;

pub const mod_gram = @import("gram.zig");
pub const mod_treewalking = @import("treewalking.zig");
pub const mod_mmap = @import("mmap.zig");

const FsEntry = mod_treewalking.FsEntry;
const Tree = mod_treewalking.Tree;
const PlasticTree = mod_treewalking.PlasticTree;

pub const log_level = std.log.Level.debug;

pub fn main() !void {
    // try in_mem();
    try swapping();
}

fn swapping () !void {
    const Index = mod_index.SwappingIndex;
    const SwappingAllocator = mod_mmap.SwappingAllocator;

    // var fixed_a7r = std.heap.FixedBufferAllocator.init(mmap_mem);
    // var a7r = fixed_a7r.threadSafeAllocator();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){
        // .backing_allocator = fixed_a7r.allocator(),
    };
    defer _ = gpa.deinit();

    var a7r = gpa.allocator();
    
    var mmap_pager = try mod_mmap.MmapPager.init(a7r, "/tmp/comb.db", .{});
    defer mmap_pager.deinit();

    // var lru = try mod_mmap.LRUSwapCache.init(a7r, mmap_pager.pager(), (16 * 1024 * 1024) / std.mem.page_size);
    var lru = try mod_mmap.LRUSwapCache.init(a7r, mmap_pager.pager(), 1);
    defer lru.deinit();

    var pager = lru.pager();

    var ma7r = mod_mmap.MmapSwappingAllocator(.{}).init(a7r, pager);
    defer ma7r.deinit();
    var sa7r = ma7r.allocator();

    var index = Index.init(a7r, pager, sa7r);
    defer index.deinit();

    var name_arena = std.heap.ArenaAllocator.init(a7r);
    defer name_arena.deinit();

    var timer = try std.time.Timer.start();
    {
        std.log.info("Walking tree from /", . {});

        var arena = std.heap.ArenaAllocator.init(a7r);
        defer arena.deinit();
        timer.reset();
        // var tree = try Tree.walk(arena.allocator(), "/run/media/asdf/Windows/", null);
        var tree = try Tree.walk(arena.allocator(), "/", null);
        // defer tree.deinit();
        const walk_elapsed = timer.read();
        std.log.info(
            "Done walking tree with {} items in {d} seconds", 
            .{ tree.list.items.len, @divFloor(walk_elapsed, std.time.ns_per_s) }
        );

        var new_ids = try arena.allocator().alloc(Index.Id, tree.list.items.len);
        defer arena.allocator().free(new_ids);
        timer.reset();
        for (tree.list.items) |t_entry, ii| {
            const i_entry = t_entry
                .conv(
                    Index.Id, 
                    SwappingAllocator.Ptr, 
                    new_ids[t_entry.parent], 
                    try sa7r.dupeJustPtr(t_entry.name),
                );
            new_ids[ii] = try index.file_created(i_entry);
        }
        const index_elapsed = timer.read();
        std.log.info(
            "Done adding items to index in {d} seconds", 
            .{ @divFloor(index_elapsed, std.time.ns_per_s) }
        );
    }
    // std.debug.print("file size: {} KiB\n", .{ (pager.pages.items.len * pager.config.page_size) / 1024 });
    // std.debug.print("page count: {} pages\n", .{ pager.pages.items.len });
    // std.debug.print("page meta bytes: {} KiB\n", .{ (pager.pages.items.len * @sizeOf(mod_mmap.MmapPager.Page)) / 1024  });
    // std.debug.print("free pages: {} pages\n", .{ pager.free_list.len });
    // std.debug.print("free pages bytes: {} KiB\n", .{ (pager.free_list.len * @sizeOf(usize)) / 1024});
    // std.debug.print("index table pages: {} pages\n", .{ index.table.pages.items.len });
    // std.debug.print("index table bytes: {} KiB\n", .{ (index.table.pages.items.len * @sizeOf(usize)) / 1024});
    // std.debug.print("index meta pages: {} pages\n", .{ index.meta.pages.items.len });
    // std.debug.print("index meta bytes: {} KiB\n", .{ (index.meta.pages.items.len * @sizeOf(Index.RowMeta)) / 1024});

    var stdin = std.io.getStdIn();
    var stdin_rdr = stdin.reader();
    var phrase = std.ArrayList(u8).init(a7r);
    defer phrase.deinit();
    var matcher = index.matcher();
    defer matcher.deinit();
    var weaver = Index.FullPathWeaver.init();
    defer weaver.deinit(index.a7r);

    while (true) {
        std.debug.print("Ready to search: ", .{});
        try stdin_rdr.readUntilDelimiterArrayList(&phrase, '\n', 1024 * 1024);
        std.log.info("Searching...", .{});
        _ = timer.reset();
        var matches = try matcher.str_match(phrase.items);
        const elapsed = timer.read();
        var ii:usize = 0;
        for (matches) |id| {
            ii += 1;
            const path = try weaver.pathOf(&index, id, '/');
            std.debug.print("{}. {s}\n", .{ ii, path });
        }
        std.log.info(
            "{} results in {d} seconds", 
            .{matches.len, @intToFloat(f64, elapsed) / std.time.ns_per_s},
        );
    }
}

fn in_mem() !void {
    const Index = mod_index.Index;

    // var fixed_a7r = std.heap.FixedBufferAllocator.init(mmap_mem);

    // var a7r = fixed_a7r.threadSafeAllocator();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){
        // .backing_allocator = fixed_a7r.allocator(),
    };
    defer _ = gpa.deinit();

    var a7r = gpa.allocator();

    var index = Index.init(a7r);
    defer index.deinit();
    defer {
        for(index.table.items(.name)) |name|{
            a7r.free(name);
        }
    }

    var timer = try std.time.Timer.start();
    {
        std.log.info("Walking tree from /", . {});

        var arena = std.heap.ArenaAllocator.init(a7r);
        defer arena.deinit();
        timer.reset();
        var tree = try Tree.walk(arena.allocator(), "/", null);
        // defer tree.deinit();
        const walk_elapsed = timer.read();
        std.log.info(
            "Done walking tree with {} items in {d} seconds", 
            .{ tree.list.items.len, @divFloor(walk_elapsed, std.time.ns_per_s) }
        );

        var new_ids = try arena.allocator().alloc(Index.Id, tree.list.items.len);
        defer arena.allocator().free(new_ids);
        timer.reset();
        for (tree.list.items) |t_entry, ii| {
            const i_entry = try t_entry.conv(
                Index.Id, 
                []u8, 
                new_ids[t_entry.parent], 
                try a7r.dupe(u8, t_entry.name)
            );
            new_ids[ii] = try index.file_created(i_entry);
        }
        const index_elapsed = timer.read();
        std.log.info(
            "Done adding items to index in {d} seconds", 
            .{ @divFloor(index_elapsed, std.time.ns_per_s) }
        );
    }
    var stdin = std.io.getStdIn();
    var stdin_rdr = stdin.reader();
    var phrase = std.ArrayList(u8).init(a7r);
    defer phrase.deinit();
    var matcher = index.matcher();
    defer matcher.deinit();
    var weaver = Index.FullPathWeaver.init();
    defer weaver.deinit(index.a7r);
    while (true) {
        std.debug.print("Ready to search: ", .{});
        try stdin_rdr.readUntilDelimiterArrayList(&phrase, '\n', 1024 * 1024);
        std.log.info("Searching...", .{});
        _ = timer.reset();
        var matches = try matcher.str_match(phrase.items);
        const elapsed = timer.read();
        var ii:usize = 0;
        for (matches) |id| {
            ii += 1;
            const path = try weaver.pathOf(&index, id, '/');
            std.debug.print("{}. {s}\n", .{ ii, path });
        }
        std.log.info(
            "{} results in {d} seconds", 
            .{matches.len, @intToFloat(f64, elapsed) / std.time.ns_per_s},
        );
    }
}

test {
    std.testing.refAllDecls(@This());
    _ = mod_gram;
    _ = mod_utils;
    _ = mod_treewalking;
    _ = mod_plist;
    _ = mod_index;
    _ = mod_mmap;
    // _ = mod_tpool;
    // _ = BinarySearchTree;
}

// fn BinarySearchTree(
//     comptime T: type, 
//     comptime Ctx: type, 
//     comptime compare_fn: fn(context: Ctx, a: T, b: T) std.math.Order,
//     comptime equal_fn: fn(context: Ctx, a: T, b: T) bool,
// ) type {
//     return struct {
//         const Self = @This();
//         const Node = struct {
//             item: T,
//             parent: usize,
//             left_child: ?usize,
//             right_child: ?usize,
//         };
//         nodes: std.ArrayList(Node),

//         fn init(a7r: Allocator) Self {
//             return Self {
//                 .nodes = std.ArrayList(Node).init(a7r),
//             };
//         }

//         fn deinit(self: *Self) void {
//             self.nodes.deinit();
//         }

//         fn insert(self: *Self, item: T) void {
//             if (self.nodes.items.len == 0 ) {
//                 self.nodes.append(
//                     Node {
//                         .item = item,
//                         .parent = 0,
//                         .left_child = null,
//                         .right_child = null,
//                     }
//                 );
//             }
//         }

//         test "BST.insert" {
//         }
//     };
// }

// fn BTree(comptime order: usize) type {
//      return struct {
//         const Node = struct {
//             children: [order]Node,
//         };
//         root: Node,
//     };
// }

pub const mod_index = struct {
    const SwappingList = mod_mmap.SwappingList;
    const Ptr = mod_mmap.SwappingAllocator.Ptr;

    pub const SwappingIndex = struct {
        const Self = @This();
        pub const Id = packed struct {
            id: u24,
            gen: u8,
        };
        pub const Entry = FsEntry(Id, Ptr);
        const RowMeta = struct {
            free: bool,
            gen: u8,
        };
        const FreeSlot = Id;
        const FreeSlots = std.PriorityQueue(
            FreeSlot, 
            void, 
            struct {
                fn cmp(_: void, a: FreeSlot, b: FreeSlot) std.math.Order {
                    return std.math.order(a.gen, b.gen);
                }
            }.cmp
        );

        a7r: Allocator,
        sa7r: mod_mmap.SwappingAllocator,
        table: SwappingList(Entry),
        meta: SwappingList(RowMeta),
        free_slots: FreeSlots,

        pub fn init(allocator: Allocator, pager: mod_mmap.Pager, sa7r: mod_mmap.SwappingAllocator) Self {
            var self = Self {
                .a7r = allocator,
                .sa7r = sa7r,
                .table = SwappingList(Entry).init(allocator, pager),
                .meta = SwappingList(RowMeta).init(allocator, pager),
                .free_slots = FreeSlots.init(allocator, .{}),
            };
            return self;
        }

        pub fn deinit(self: *Self) void {
            // for (self.table.items(.name)) |name|{
            //     self.a7r.free(name);
            // }
            self.table.deinit();
            self.meta.deinit();
            self.free_slots.deinit();
        }

        pub fn file_created(self: *Self, entry: Entry) !Id {
            // const i_entry = try t_entry
            //     .conv(Index.Id, new_ids[t_entry.parent])
            //     .clone(name_arena.allocator());
            if (self.free_slots.removeOrNull()) |id| {
                var row = try self.meta.get(id.id);
                try self.table.set(id.id, entry);
                row.gen += 1;
                row.free = false;
                return Id {
                    .id = id.id,
                    .gen = row.gen,
                };
            } else {
                const id = self.meta.len;
                try self.meta.append(RowMeta {
                    .free = false,
                    .gen = 0,
                });
                // TODO: this shit 
                errdefer _ = self.meta.pop() catch unreachable;
                try self.table.append(entry);
                return Id {
                    .id = @intCast(u24, id),
                    .gen = 0,
                };
            }
        }

        const IndexGetErr = error { StaleHandle,};

        pub fn isStale(self: *Self, id: Id) !bool {
            const row = try self.meta.get(id.id);
            return row.gen > id.gen;
        }

        /// Be sure to clone the returned result
        pub fn get(self: *Self, id: Id) !*Entry {
            if (try self.isStale(id)) return IndexGetErr.StaleHandle;
            return try self.table.get(id.id);
        }

        pub fn idAt(self: *Self, idx: usize) !Id {
            const row = try self.meta.get(idx);
            return Id {
                .id = @intCast(u24, idx),
                .gen = row.gen,
            };
        }

        pub const FullPathWeaver = struct {
            pub const NameOfErr = error { NotFound };
            buf: std.ArrayListUnmanaged(u8),

            pub fn init() FullPathWeaver {
                return FullPathWeaver {
                    .buf = std.ArrayListUnmanaged(u8){},
                };
            }
            pub fn deinit(self: *FullPathWeaver, allocator: Allocator) void {
                self.buf.deinit(allocator);
            }
            /// The returned slice is invalidated not long after.
            pub fn pathOf(self: *FullPathWeaver, index: *SwappingIndex, id: Id, delimiter: u8) ![]const u8 {
                self.buf.clearRetainingCapacity();
                var next_id = id;
                // const names = index.table.items(.name);
                // const parents = index.table.items(.parent);
                while (true) {
                    if (try index.isStale(next_id)) return IndexGetErr.StaleHandle;

                    const entry = try index.table.get(next_id.id);
                    const name = try index.sa7r.swapIn(entry.name);
                    defer index.sa7r.swapOut(entry.name);

                    try self.buf.appendSlice(index.a7r, name);
                    std.mem.reverse(u8, self.buf.items[(self.buf.items.len - name.len)..]);
                    try self.buf.append(index.a7r, delimiter);
                    next_id = entry.parent;
                    // FIXME: a better sentinel
                    if (next_id.id == 0) {
                        break;
                    }
                }
                std.mem.reverse(u8, self.buf.items[0..]);
                return self.buf.items;
            }
        };

        pub fn matcher(self: *Self) StrMatcher {
            return StrMatcher.init(self.a7r, self);
        }

        pub const StrMatcher = struct {
            // allocator: Allocator,
            out_vec: std.ArrayList(Id),
            index: *Self,
            
            pub fn init(allocator: Allocator, index: *Self) StrMatcher {
                return StrMatcher{
                    .out_vec = std.ArrayList(Id).init(allocator),
                    .index = index,
                };
            }

            pub fn deinit(self: *StrMatcher) void {
                self.out_vec.deinit();
            }

            pub fn str_match(
                self: *StrMatcher, 
                string: []const u8, 
            ) ![]const Id {
                self.out_vec.clearRetainingCapacity();
                var it = self.index.table.iterator();
                defer it.close();
                var ii: usize = 0;
                while (try it.next()) |entry| {
                    const name = try self.index.sa7r.swapIn(entry.name);
                    defer self.index.sa7r.swapOut(entry.name);
                    if (std.mem.indexOf(u8, name, string)) |_| {
                        try self.out_vec.append(try self.index.idAt(ii));
                    }
                    ii += 1;
                }
                return self.out_vec.items;
            }
        };
    };

    test "SwappingIndex.usage" {
        var a7r = std.testing.allocator;
        var mmap_pager = try mod_mmap.MmapPager.init(a7r, "/tmp/SwappingIndex.usage", .{});
        defer mmap_pager.deinit();

        var lru = try mod_mmap.LRUSwapCache.init(a7r, mmap_pager.pager(), 1);
        defer lru.deinit();

        var pager = lru.pager();

        var ma7r = mod_mmap.MmapSwappingAllocator(.{}).init(a7r, pager);
        defer ma7r.deinit();

        var sa7r = ma7r.allocator();
        var index = SwappingIndex.init(a7r, pager, sa7r);
        defer index.deinit();

        defer {
            var it = index.table.iterator();
            defer it.close();
            while (it.next() catch unreachable) |entry|{
                sa7r.free(entry.name);
            }
        }

        var entry = SwappingIndex.Entry {
            .name = try sa7r.dupeJustPtr("/"),
            .parent = SwappingIndex.Id { .id = 0, .gen = 0 },
            .kind = SwappingIndex.Entry.Kind.Directory,
            .depth = 0,
            .size = 0,
            .inode = 0,
            .dev = 0,
            .mode = 0,
            .uid = 0,
            .gid = 0,
            .ctime = 0,
            .atime = 0,
            .mtime = 0,
        };
        const id = try index.file_created(entry);
        var ret = try index.get(id);
        try std.testing.expectEqual(entry, ret.*);
    }

    const Index = struct {
        const Self = @This();
        pub const Id = packed struct {
            id: u24,
            gen: u8,
        };

        comptime {
            // TODO: THiS gets fucked if usize bytes are above 255
            if (@sizeOf(Id) != @sizeOf(u32)) {
                var buf = [_]u8{0} ** 64;
                var msg = std.fmt.bufPrint(
                    &buf, 
                    "Ptr size mismatch: {} !=  {}", 
                    .{ @sizeOf(Id), @sizeOf(u32) }
                ) catch @panic("wtf");
                @compileError(msg);
            }
        }

        pub const Entry = FsEntry(Id, []u8);
        const RowMeta = struct {
            free: bool,
            gen: u8,
        };

        const FreeSlot = Id;
        const FreeSlots = std.PriorityQueue(
            FreeSlot, 
            void, 
            struct {
                fn cmp(_: void, a: FreeSlot, b: FreeSlot) std.math.Order {
                    return std.math.order(a.gen, b.gen);
                }
            }.cmp
        );

        a7r: Allocator,
        table: std.MultiArrayList(Entry),
        meta: std.ArrayListUnmanaged(RowMeta),
        free_slots: FreeSlots,


        pub fn init(allocator: Allocator) Self {
            var self = Self {
                .a7r = allocator,
                .table = std.MultiArrayList(Entry){},
                .meta = std.ArrayListUnmanaged(RowMeta){},
                .free_slots = FreeSlots.init(allocator, .{}),
            };
            return self;
        }

        pub fn deinit(self: *Self) void {
            // for (self.table.items(.name)) |name|{
            //     self.a7r.free(name);
            // }
            self.table.deinit(self.a7r);
            self.meta.deinit(self.a7r);
            self.free_slots.deinit();
        }

        fn file_created(self: *Self, entry: Entry) !Id {
            if (self.free_slots.removeOrNull()) |id| {
                var row = self.meta.items[id.id];
                self.table.set(id.id, entry);
                row.gen += 1;
                row.free = false;
                return Id {
                    .id = id.id,
                    .gen = row.gen,
                };
            } else {
                const id = self.meta.items.len;
                try self.meta.append(self.a7r, RowMeta {
                    .free = false,
                    .gen = 0,
                });
                errdefer _ = self.meta.pop();
                try self.table.append(self.a7r, entry);
                return Id {
                    .id = @intCast(u24, id),
                    .gen = 0,
                };
            }
        }

        const IndexGetErr = error { StaleHandle,};

        fn isStale(self: *const Self, id: Id) bool {
            const row = self.meta.items[id.id];
            return row.gen > id.gen;
        }

        /// Be sure to clone the returned result
        fn get(self: *const Self, id: Id) !Entry {
            if (self.isStale(id)) return IndexGetErr.StaleHandle;
            return self.table.get(id.id);
        }

        fn idAt(self: *const Self, idx: usize) Id {
            const row = self.meta.items[idx];
            return Id {
                .id = @intCast(u24, idx),
                .gen = row.gen,
            };
        }

        pub const FullPathWeaver = struct {
            pub const NameOfErr = error { NotFound };
            buf: std.ArrayListUnmanaged(u8),

            pub fn init() FullPathWeaver {
                return FullPathWeaver {
                    .buf = std.ArrayListUnmanaged(u8){},
                };
            }
            pub fn deinit(self: *FullPathWeaver, allocator: Allocator) void {
                self.buf.deinit(allocator);
            }
            /// The returned slice is invalidated not long after.
            pub fn pathOf(self: *FullPathWeaver, index: *const Index, id: Id, delimiter: u8) ![]const u8 {
                self.buf.clearRetainingCapacity();
                var next_id = id;
                const names = index.table.items(.name);
                const parents = index.table.items(.parent);
                while (true) {
                    if (index.isStale(next_id)) return IndexGetErr.StaleHandle;

                    const name = names[next_id.id];
                    try self.buf.appendSlice(index.a7r, name);
                    std.mem.reverse(u8, self.buf.items[(self.buf.items.len - name.len)..]);
                    try self.buf.append(index.a7r, delimiter);

                    const parent = parents[next_id.id];
                    next_id = parent;
                    // FIXME: a better sentinel
                    if (next_id.id == 0) {
                        break;
                    }
                }
                std.mem.reverse(u8, self.buf.items[0..]);
                return self.buf.items;
            }
        };

        pub fn matcher(self: *const Self) StrMatcher {
            return StrMatcher.init(self.a7r, self);
        }

        pub const StrMatcher = struct {
            // allocator: Allocator,
            out_vec: std.ArrayList(Id),
            index: *const Self,
            
            pub fn init(allocator: Allocator, index: *const Self) StrMatcher {
                return StrMatcher{
                    .out_vec = std.ArrayList(Id).init(allocator),
                    .index = index,
                };
            }

            pub fn deinit(self: *StrMatcher) void {
                self.out_vec.deinit();
            }

            pub fn str_match(
                self: *StrMatcher, 
                string: []const u8, 
            ) ![]const Id {
                self.out_vec.clearRetainingCapacity();
                for (self.index.table.items(.name)) |name, ii| {
                    if (std.mem.indexOf(u8, name, string)) |_| {
                        try self.out_vec.append(self.index.idAt(ii));
                    }
                }
                return self.out_vec.items;
            }
        };
    };

    test "Index.usage" {
        var a7r = std.testing.allocator;

        var index = Index.init(a7r);
        defer index.deinit();

        defer {
            for(index.table.items(.name)) |name|{
                a7r.free(name);
            }
        }

        var entry = Index.Entry {
            // .name = try a7r.dupe(u8, "manameisjeff"),
            .name = try a7r.dupe(u8, "/"),
            .parent = Index.Id { .id = 0, .gen = 0 },
            .kind = Index.Entry.Kind.Directory,
            .depth = 0,
            .size = 0,
            .inode = 0,
            .dev = 0,
            .mode = 0,
            .uid = 0,
            .gid = 0,
            .ctime = 0,
            .atime = 0,
            .mtime = 0,
        };
        const id = try index.file_created(entry);
        var ret = try index.get(id);
        try std.testing.expectEqual(entry, ret);
    }
};


pub const mod_plist = struct {
    const Appender = mod_utils.Appender;

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
};


// test "anyopaque.fn"{
//     const inner = struct {
//         num: usize,
//         fn my_fn(self: *@This(), also: usize) void {
//             std.debug.print("num = {}, also = {}", . {self.num, also} );
//         }
//     };
//     const fn_ptr = @ptrCast(*const anyopaque, inner.my_fn);
//     var args = .{ inner {.num = 10 }, 20 };
//     var arg_ptr = @ptrCast(*anyopaque, &args);
//     _ = arg_ptr;
//     @call(.{}, fn_ptr, .{});
// }

// const mod_tpool = struct {
//     const ThreadPool = struct {
//         const Self = @This();
//         const Allocator = Allocator;
//         const Task = struct {
//             args: *anyopaque,
//             func: fn (*anyopaque) void,
//         };

//         allocator: Allocator,
//         threads: []std.Thread,
//         queue: std.atomic.Queue(Task),
//         break_signal: bool = false,
//         dead_signal: []bool,

//         /// You can use `std.Thread.getCpuCount` to set the count.
//         fn init(allocator: Allocator, size: usize, ) !Self {
//             var threads = try allocator.alloc(std.Thread, size);
//             errdefer allocator.free(threads);
//             var dead_signal = try allocator.alloc(bool, size);
//             errdefer allocator.free(dead_signal);

//             const queue = std.atomic.Queue(Task).init();

//             var self = Self {
//                 .allocator = allocator,
//                 .threads = threads,
//                 .queue = queue,
//                 .tasks = std.ArrayListUnmanaged(Task){},
//             };

//             for (self.threads) |*thread, id| {
//                 thread.* = try std.Thread.spawn(.{}, Self.thread_start, .{ &self, id });
//                 dead_signal[id] = false;
//             }

//             return self;
//         }

//         /// Detachs threads after waiting for timeout.
//         fn deinit(self: *Self, timeout_ns: u64) void{
//             self.break_signal = true;
//             std.time.sleep(timeout_ns);
//             for (self.dead_signal) |signal, id| {
//                 if(signal) {
//                     self.threads[id].join();
//                 } else {
//                     self.threads[id].detach();
//                 }
//             }
//             self.allocator.free(self.threads);
//             self.allocator.free(self.dead_signal);
//         }

//         fn thread_start (self: *Self, id: usize) noreturn {
//             while (true) {
//                 if (self.break_signal){
//                     break;
//                 }
//                 if (self.queue.get()) |node|{
//                     var task = node.data;
//                     @call(.{}, task.func, .{ task.args });
//                     self.allocator.destroy(node);
//                 }else{
//                     _ = std.Thread.yield();
//                 }
//             }
//             self.dead_signal[id] = true;
//         }
//         fn do() void {
//             std.debug.todo("we need to do");
//         }
//     };
// };
