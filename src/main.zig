const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

pub const mod_utils = @import("utils.zig");
pub const mod_gram = @import("gram.zig");
pub const mod_treewalking = @import("treewalking.zig");

const FsEntry = mod_treewalking.FsEntry;
const Tree = mod_treewalking.Tree;
const PlasticTree = mod_treewalking.PlasticTree;

const log_level = std.log.Level.debug;

pub fn main() !void {
    const Index = mod_index.Index;

    // const backing_file_size = 1024 * 1024 * 1024;
    // var mmap_file = try std.fs.createFileAbsolute("/tmp/comb.db", .{
    //     .read = true,
    // });
    // defer mmap_file.close();
    // try std.os.ftruncate(mmap_file.handle, backing_file_size);
    // const mmap_mem = try std.os.mmap(
    //     null, 
    //     backing_file_size,
    //     std.os.PROT.READ | std.os.PROT.WRITE,
    //     std.os.MAP.SHARED, // | std.os.MAP.ANONYMOUS,
    //     mmap_file.handle,
    //     // -1,
    //     0,
    // );
    // defer std.os.munmap(mmap_mem);

    // var fixed_a7r = std.heap.FixedBufferAllocator.init(mmap_mem);

    // var a7r = fixed_a7r.threadSafeAllocator();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){
        // .backing_allocator = fixed_a7r.allocator(),
    };
    defer _ = gpa.deinit();

    var a7r = gpa.allocator();
    
    var index = Index.init(a7r);
    defer index.deinit();

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

        var new_ids = try a7r.alloc(Index.Id, tree.list.items.len);
        defer a7r.free(new_ids);
        timer.reset();
        for (tree.list.items) |t_entry, ii| {
            const i_entry = try t_entry.conv(Index.Id, new_ids[t_entry.parent]).clone(a7r);
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
    var matcher = index.matcher();
    var weaver = Index.FullPathWeaver.init();
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
    // _ = mod_tpool;
    // _ = BinarySearchTree;
    _ = MmapPageAllocator;
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

const MmapPageAllocator = struct {
    const Self = @This();
    const Config = struct {
        page_size: usize = std.mem.page_size,
    };

    const Page = struct {
        slice: ?[]align(std.mem.page_size)u8,
        free: bool,
    };
    const PageId = usize;

    // const Allocation = struct {
    //     // the first page of the allocation
    //     start: usize,
    //     len: usize,
    // };

    const FreeList = std.PriorityQueue(
        PageId,
        void,
        struct {
            // we wan't earlier pages to be filled in earlier
            fn cmp(ctx: void, a: PageId, b: PageId) std.math.Order {
                _  = ctx;
                return std.math.order(a, b);
            }
        }.cmp
    );

    config: Config,
    a7r: Allocator,

    backing_file: std.fs.File,
    pages: std.ArrayListUnmanaged(Page),
    slice_page_map: std.AutoHashMapUnmanaged(usize, PageId),
    free_list: FreeList,

    pub fn init(a7r: Allocator, backing_file_path: [] const u8, config: Config) !Self {
        // const backing_file_size = 1024 * 1024 * 1024;
        var mmap_file = try std.fs.createFileAbsolute(backing_file_path, .{
            .read = true,
        });
        var self = Self {
            .config = config,
            .a7r = a7r,
            .backing_file = mmap_file,
            .pages = std.ArrayListUnmanaged(Page){},
            .free_list = FreeList.init(a7r, .{}),
            .slice_page_map = std.AutoHashMapUnmanaged(usize, PageId){},
        };
        return self;
    }

    fn deinit(self: *Self) void {
        self.backing_file.close();
        self.pages.deinit(self.a7r);
        self.slice_page_map.deinit(self.a7r);
        self.free_list.deinit();
    }

    fn alloc(self: *Self) !PageId {
        if (self.free_list.removeOrNull()) |id|{
            var page = &self.pages.items[id];
            page.free = false;
            return id;
        } else {
            // ensure capacity before trying anything
            try self.pages.ensureUnusedCapacity(self.a7r, 1);
            // ensure capacity on the free list to avoid error checking on free calls
            try self.free_list.ensureUnusedCapacity(1);
            try std.os.ftruncate(
                self.backing_file.handle, 
                (self.pages.items.len + 1) * self.config.page_size
            );
            try self.pages.append(
                self.a7r, 
                Page {
                    .slice = null,
                    .free = false,
                }
            );
            return self.pages.items.len - 1;
        }
    }

    fn swapAndFree(self: *Self, id: PageId) !void {
        self.swapOut(id);
        try self.free(id);
    }

    /// This doesn't swap out the page.
    fn free(self: *Self, id: PageId) void {
        if (id >= self.pages.items.len) {
            return;
        } 
        var page = &self.pages.items[id];
        page.free = true;

        if (id == self.pages.items.len - 1) {
            var ii = id - 1;
            while (true) : ({ ii -= 1; }) {
                _ = self.pages.popOrNull();
                if (ii == 0 or !(self.pages.items[ii].free)) break;
            }
        } else {
            // we've been growing the free list along the page list so it's ok
            self.free_list.add(id) catch unreachable;
        }
    }

    fn swapIn(self: *Self, id: PageId) ![]align(std.mem.page_size)u8 {
        if (self.pages.items.len <= id) {
            return error.PageNotAllocated;
        }
        var page = self.pages.items[id];
        if (page.free) {
            return error.PageNotAllocated;
        }
        if (page.slice) |slice| {
            return slice;
        } else {
            var slice = try std.os.mmap(
                null, 
                self.config.page_size,
                std.os.PROT.READ | std.os.PROT.WRITE,
                std.os.MAP.SHARED, // | std.os.MAP.ANONYMOUS,
                self.backing_file.handle,
                // -1,
                id * self.config.page_size,
            );
            errdefer std.os.munmap(slice);
            try self.slice_page_map.put(self.a7r, @ptrToInt(slice.ptr), id);
            page.slice = slice;
            return slice;
        }
    }

    fn swapOut(self: *Self, page_id: usize) void {
        if (self.pages.items.len <= page_id) {
            return; 
        }
        var page = self.pages.items[page_id];
        if (page.slice) |slice| {
            std.os.munmap(slice);
            _ = self.slice_page_map.remove(@ptrToInt(slice.ptr));
            page.slice = null;
        }
    }

    test "MmapPageAllocator.usage" {
        var a7r = std.testing.allocator;
        const page_size = std.mem.page_size;
        var pager = try MmapPageAllocator.init(a7r, "/tmp/MmmapUsage.test", .{
            .page_size = page_size,
        });
        defer pager.deinit();

        const item_per_page = page_size / @sizeOf(usize) ;
        const page_count = 10;
        var nums = [_]usize{0} ** (page_count * item_per_page);
        for (nums) |*num, ii| {
            num.* = ii;
        }
        var pages: [page_count]PageId = undefined;
        for (pages) |*id, pii| {
            id.* = try pager.alloc();
            var bytes = try pager.swapIn(id.*);
            defer pager.swapOut(id.*);
            for (std.mem.bytesAsSlice(usize, bytes)) |*num, ii| {
                num.* = nums[(pii * item_per_page) + ii];
            }
        }
        for (pages) |id, pii| {
            var bytes = try pager.swapIn(id);
            defer pager.swapOut(id);
            for (std.mem.bytesAsSlice(usize, bytes)) |num, ii| {
                try std.testing.expectEqual(nums[(pii * item_per_page) + ii], num);
            }
        }
        {
            const page_id = page_count / 2;
            pager.free(pages[page_id]);
            try std.testing.expectError(error.PageNotAllocated, pager.swapIn(pages[page_id]));

            var new_stuff = [_]usize{0} ** item_per_page;
            for (new_stuff) |*num, ii| {
                num.* = item_per_page - ii;
                nums[(page_id * item_per_page) + ii] = item_per_page - ii;
            }

            var id = try pager.alloc();
            var bytes = try pager.swapIn(id);
            defer pager.swapOut(id);
            for (std.mem.bytesAsSlice(usize, bytes)) |*num, ii| {
                num.* = new_stuff[ii];
            }
        }
        for (pages) |id, pii| {
            var bytes = try pager.swapIn(id);
            defer pager.swapOut(id);
            for (std.mem.bytesAsSlice(usize, bytes)) |num, ii| {
                try std.testing.expectEqual(nums[(pii * item_per_page) + ii], num);
            }
        }
    }
};


fn SwappingList(comptime T: type) type {
    return struct {
        const Self = @This();
        const PageId = MmapPageAllocator.PageId;

        const PageCache = struct {
            const InPage = struct {
                idx: usize,
                slice: []T,
            };

            in_page: ?InPage = null, 

            fn new() PageCache {
                return PageCache {
                    .in_page = null
                };
            }

            /// Panics if index is out of bound.
            inline fn ptrTo(self: *PageCache, list: *const Self, index: usize) !*T {
                const page_idx = list.pageIdxOf(index);
                const page_slice = try self.swapIn(list, page_idx);
                return &page_slice[index - (page_idx * list.per_page)];
            }

            fn swapIn(self: *PageCache, list: *const Self, page_idx: usize) ![]T {
                if (self.in_page) |page| {
                    if (page.idx == page_idx) {
                        return page.slice;
                    } else {
                        list.pager.swapOut(list.pages.items[page.idx]);
                        const bytes = try list.pager.swapIn(list.pages.items[page_idx]);
                        const slice = std.mem.bytesAsSlice(T, bytes);
                        self.in_page = InPage {
                            .idx = page_idx,
                            .slice = slice,
                        };
                        return slice;
                    }
                } else {
                    const bytes = try list.pager.swapIn(list.pages.items[page_idx]);
                    const slice = std.mem.bytesAsSlice(T, bytes);
                    self.in_page = InPage {
                        .idx = page_idx,
                        .slice = slice,
                    };
                    return slice;
                }
            }

            fn swapOut(self: *PageCache, list: *const Self) void {
                if (self.in_page) |page| {
                    list.pager.swapOut(list.pages.items[page.idx]);
                    self.in_page = null;
                }
            }
        };

        a7r: Allocator,
        pager: *MmapPageAllocator,
        len: usize,
        capacity: usize,
        pages: std.ArrayListUnmanaged(PageId),
        cache: PageCache,
        /// items per page
        per_page: usize,

        fn init(a7r: Allocator, pager: *MmapPageAllocator) Self {
            return Self {
                .a7r = a7r,
                .len = 0,
                .capacity = 0,
                .pager = pager,
                .pages = std.ArrayListUnmanaged(PageId){},
                .per_page = pager.config.page_size / @sizeOf(T),
                .cache = PageCache.new(),
            };
        }

        fn deinit(self: *Self) void {
            self.cache.swapOut(self);
            for (self.pages.items) |id|{
                self.pager.free(id);
            }
            self.pages.deinit(self.a7r);
        }

        fn ensureUnusedCapacity(self: *Self, n_items: usize) !void {
            if (self.capacity > self.len + n_items) {
                return;
            }
            const new_cap = if (self.capacity == 0) self.per_page else self.capacity * 2;
            var new_pages_req = (new_cap - self.capacity) / self.per_page;
            std.debug.assert(new_pages_req > 0);
            try self.pages.ensureUnusedCapacity(self.a7r, new_pages_req);
            while (new_pages_req > 0) : ({ new_pages_req -= 1; }) {
                const id = try self.pager.alloc();
                try self.pages.append(self.a7r, id);
                self.capacity += self.per_page;
            }
            // std.debug.print("capacity increased to: {}\n",.{ self.capacity });
        }

        fn append(self: *Self, item: T) !void {
            try self.ensureUnusedCapacity(1);
            var ptr = try self.cache.ptrTo(self, self.len);
            ptr.* = item;
            self.len += 1;
        }

        fn pageIdxOf(self: Self, idx: usize) usize {
            return idx / self.per_page;
        }

        const Iterator = struct {
            cur: usize,
            stop: usize,
            cache: PageCache,
            list: *const Self,

            fn new(list: *const Self, from: usize, to: usize) Iterator {
                return Iterator {
                    .cur = from,
                    .stop = to,
                    .list = list,
                    .cache = PageCache.new(),
                };
            }

            fn close(self: *Iterator) void {
                self.cache.swapOut(self.list);
            }

            fn next(self: *Iterator) !?*T {
                if (self.cur == self.stop) {
                    return null;
                }
                var ptr = try self.cache.ptrTo(self.list, self.cur);
                self.cur += 1;
                return ptr;
            }
        };

        /// Might panic if the list is modified before the iterator is closed.
        /// Be sure to close the iterator after usage.
        fn iterator(self: *const Self) Iterator {
            return Iterator.new(self, 0, self.len);
        }
    };
}

test "SwappingList.usage" {
    const page_size = std.mem.page_size;
    const a7r = std.testing.allocator;
    var pager = try MmapPageAllocator.init(a7r, "/tmp/SwappingList.usage", .{
        .page_size = page_size,
    });
    defer pager.deinit();
    var list = SwappingList(usize).init(a7r, &pager);
    defer list.deinit();

    const item_per_page = page_size / @sizeOf(usize) ;
    const page_count = 10;
    var nums = [_]usize{0} ** (page_count * item_per_page);
    for (nums) |*num, ii| {
        num.* = ii;
        try list.append(ii);
    }
    {
        var it = list.iterator();
        defer it.close();
        var ii: usize = 0;
        while (try it.next()) |num| {
            try std.testing.expectEqual(ii, num.*);
            ii += 1;
        }
    }
}

const mod_index = struct {
    const Index = struct {
        const Self = @This();
        pub const Id = packed struct {
            id: u24,
            gen: u8,
        };
        pub const Entry = FsEntry(Id);
        const RowMeta = struct {
            free: bool,
            gen: u8,
        };
        const FreeSlot = Id;

        a7r: Allocator,
        table: std.MultiArrayList(Entry),
        meta: std.ArrayListUnmanaged(RowMeta),
        free_slots: std.PriorityQueue(FreeSlot, void, free_slot_cmp),

        fn free_slot_cmp(_: void, a: FreeSlot, b: FreeSlot) std.math.Order {
            return std.math.order(a.gen, b.gen);
        }
        // fn slot_gen_cmp(ctx: *Self, a: usize, b: usize) std.PriorityQueue.Order {
        //     const row_a = ctx.meta[a];
        //     const row_b = ctx.meta[b];
        //     return std.math.order(row_a.gen, row_b.gen);
        // }

        pub fn init(allocator: Allocator) Self {
            var self = Self {
                .a7r = allocator,
                .table = std.MultiArrayList(Entry){},
                .meta = std.ArrayListUnmanaged(RowMeta){},
                .free_slots = std.PriorityQueue(FreeSlot, void, free_slot_cmp).init(allocator, .{}),
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
                    break :blk mod_utils.Appender(GramPos).new(&curry{ .map = &self.cache, .allocator = allocator }, curry.append);
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
                        mod_utils.Appender(GramPos).new(&self.grams, std.ArrayList(GramPos).append)
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
