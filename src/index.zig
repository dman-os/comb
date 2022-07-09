const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.dbg;

const mod_treewalking = @import("treewalking.zig");
const FsEntry = mod_treewalking.FsEntry;

const mod_mmap = @import("mmap.zig");
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

    ha7r: Allocator,
    sa7r: mod_mmap.SwappingAllocator,
    table: SwappingList(Entry),
    meta: SwappingList(RowMeta),
    free_slots: FreeSlots,

    pub fn init(ha7r: Allocator, pager: mod_mmap.Pager, sa7r: mod_mmap.SwappingAllocator) Self {
        var self = Self {
            .ha7r = ha7r,
            .sa7r = sa7r,
            .table = SwappingList(Entry).init(pager),
            .meta = SwappingList(RowMeta).init(pager),
            .free_slots = FreeSlots.init(ha7r, .{}),
        };
        return self;
    }

    pub fn deinit(self: *Self) void {
        self.table.deinit(self.ha7r);
        self.meta.deinit(self.ha7r);
        self.free_slots.deinit();
    }

    pub fn file_created(self: *Self, entry: Entry) !Id {
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
            try self.meta.append(self.ha7r, RowMeta {
                .free = false,
                .gen = 0,
            });
            // TODO: this shit 
            errdefer _ = self.meta.pop() catch unreachable;
            try self.table.append(self.ha7r, entry);
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

                try self.buf.appendSlice(index.ha7r, name);
                std.mem.reverse(u8, self.buf.items[(self.buf.items.len - name.len)..]);
                try self.buf.append(index.ha7r, delimiter);
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
        return StrMatcher.init(self.ha7r, self);
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
