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

const mod_plist = @import("plist.zig");

/// _The_ database.
pub const Database = struct {
    const Self = @This();

    /// Handle for each `Entry`
    pub const Id = packed struct {
        id: u24,
        gen: u8,
        comptime {
            if (@sizeOf(Id) != @sizeOf(u32)) {
                @compileError(std.fmt.comptimePrint(
                    "unexpected size mismatch: (" ++ @typeName(Id) ++ "){} != (" ++ @typeName(u32) ++ ") {}\n",
                    .{ @sizeOf(Id), @sizeOf(u32) }
                ));
            }
            if (@bitSizeOf(Id) != @bitSizeOf(u32)) {
                @compileError(std.fmt.comptimePrint(
                    "unexpected bitSize mismatch: (" ++ @typeName(Id) ++ "){} != (" ++ @typeName(u32) ++ ") {}\n",
                    .{ @bitSizeOf(Id), @bitSizeOf(u32) }
                ));
            }
        }
    };
    /// Represents a single file system object.
    pub const Entry = FsEntry(Id, Ptr);

    pub const Config = struct {
        /// Path delimiter 
        delimiter: u8 = '\\',
    };

    /// Metadata about an `Entry` at a given id.
    const RowMeta = struct {
        free: bool,
        gen: u8,
    };
    /// Information about a free slot. Right now, just the `Id` suffices.
    const FreeSlot = Id;

    /// Our free spots. We use a priority queue to fill in earlier spots first
    /// to avoid fragmentation reducing the no. of pages required and thus
    /// page cache misses.
    const FreeSlots = std.PriorityQueue(
        FreeSlot,
        void,
        struct {
            fn cmp(_: void, a: FreeSlot, b: FreeSlot) std.math.Order {
                return std.math.order(a.gen, b.gen);
            }
        }.cmp
    );

    /// Our index, a trigram posting list with a gram length of 3.
    /// We associate each trigram with a set of `Id`.
    const PList = mod_plist.SwappingPostingList(Id, 3);

    config: Config,
    /// The heap allocator.
    ha7r: Allocator,
    sa7r: mod_mmap.SwappingAllocator,
    pager: mod_mmap.Pager,
    /// Our actual big list.
    table: SwappingList(Entry),
    meta: SwappingList(RowMeta),
    plist: PList = .{},
    free_slots: FreeSlots,

    pub fn init(
        ha7r: Allocator, 
        pager: mod_mmap.Pager, 
        sa7r: mod_mmap.SwappingAllocator,
        config: Config,
    ) Self {
        var self = Self {
            .ha7r = ha7r,
            .sa7r = sa7r,
            .pager = pager,
            .table = SwappingList(Entry).init(pager.pageSize()),
            .meta = SwappingList(RowMeta).init(pager.pageSize()),
            .free_slots = FreeSlots.init(ha7r, .{}),
            .config = config,
        };
        return self;
    }

    pub fn deinit(self: *Self) void {
        var it = self.table.iterator(self.sa7r, self.pager);
        while (it.next() catch unreachable) |entry|{
            self.sa7r.free(entry.name);
        }
        it.close();

        self.table.deinit(self.ha7r, self.sa7r, self.pager);
        self.meta.deinit(self.ha7r, self.sa7r, self.pager);
        self.free_slots.deinit();
        self.plist.deinit(self.ha7r, self.sa7r, self.pager);
    }

    /// Add an entry to the database.
    pub fn file_created(self: *Self, entry: *const FsEntry(Id, []const u8)) !Id {
        // clone the entry but store the name on the swap this time
        const swapped_entry = entry.clone(
            try self.sa7r.dupeJustPtr(entry.name)
        );
        const id = if (self.free_slots.removeOrNull()) |id| blk: {
            // mind the `try`s and their order

            var row = try self.meta.swapIn(self.sa7r, self.pager, id.id);
            defer self.meta.swapOut(self.sa7r, self.pager, id.id);

            try self.table.set(self.sa7r, self.pager, id.id, swapped_entry);

            row.gen += 1;
            row.free = false;
            break :blk Id {
                .id = id.id,
                .gen = row.gen,
            };
        } else blk: {
            const idx = self.meta.len;
            try self.meta.append(self.ha7r, self.sa7r, self.pager, RowMeta {
                .free = false,
                .gen = 0,
            });
            // TODO: this shit
            errdefer _ = self.meta.pop(self.sa7r, self.pager) catch unreachable;

            try self.table.append(self.ha7r, self.sa7r, self.pager, swapped_entry);

            break :blk Id {
                .id = @intCast(u24, idx),
                .gen = 0,
            };
        };
        try self.plist.insert(
            self.ha7r, 
            self.sa7r, 
            self.pager, 
            id, 
            entry.name, 
            std.ascii.spaces[0..]
            // &[_]u8{ self.config.delimiter }
        );
        return id;
    }

    pub const DatabseErr = error { StaleHandle,};

    pub fn isStale(self: *Self, id: Id) !bool {
        if (self.meta.len <= id.id) return true;
        const row = try self.meta.swapIn(self.sa7r, self.pager, id.id);
        defer self.meta.swapOut(self.sa7r, self.pager, id.id);
        return row.gen > id.gen;
    }

    pub fn idAt(self: *Self, idx: usize) !Id {
        const row = try self.meta.swapIn(self.sa7r, self.pager, idx);
        defer self.meta.swapOut(self.sa7r, self.pager, idx);
        return Id {
            .id = @intCast(u24, idx),
            .gen = row.gen,
        };
    }

    pub fn reader(self: *Self) Reader {
        return Reader.init(self);
    }

    /// Get items from the db.
    pub const Reader = struct {
        // TODO: can this be a const pointer?
        db: *Database,
        /// The index of the last `table` item swapped in
        swapped_in_idx_table: ?usize = null,

        pub fn init(db: *Database) @This() {
            return @This() {
                .db = db,
            };
        }

        pub fn deinit(self: *@This()) void {
            self.swapOutTableCache();
        }

        fn swapOutTableCache(self: *@This()) void {
            if (self.swapped_in_idx_table) |idx| {
                self.db.table.swapOut(self.db.sa7r, self.db.pager, idx);
                self.swapped_in_idx_table = null;
            }
        }

        /// The returned slice borrows from `self` and is invalidated by the 
        /// next call of this method.
        pub fn get(self: *@This(), id: Id) !*Entry {
            if (try self.db.isStale(id)) return error.StaleHandle;
            self.swapOutTableCache();
            const item = try self.db.table.swapIn(self.db.sa7r, self.db.pager, id.id);
            self.swapped_in_idx_table = id.id;
            return item;
        }
    };

    /// Stitches together the full path of an `Entry` given the `Id`.
    pub const FullPathWeaver = struct {
        buf: std.ArrayListUnmanaged(u8) = .{},

        pub fn deinit(self: *@This(), a7r: Allocator) void {
            self.buf.deinit(a7r);
        }
        /// The returned slice is invalidated not long after.
        pub fn pathOf(
            self: *FullPathWeaver, 
            db: *Database, 
            id: Id, 
            delimiter: u8
        ) ![]const u8 {
            self.buf.clearRetainingCapacity();
            var next_id = id;
            // const names = db.table.items(.name);
            // const parents = db.table.items(.parent);
            while (true) {
                if (try db.isStale(next_id)) return error.StaleHandle;

                const cur_id = next_id.id;
                const entry = try db.table.swapIn(db.sa7r, db.pager, cur_id);
                defer db.table.swapOut(db.sa7r, db.pager, cur_id);

                const name = try db.sa7r.swapIn(entry.name);
                defer db.sa7r.swapOut(entry.name);

                try self.buf.appendSlice(db.ha7r, name);
                std.mem.reverse(u8, self.buf.items[(self.buf.items.len - name.len)..]);
                try self.buf.append(db.ha7r, delimiter);
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

    pub fn naiveNameMatcher(self: *Self) NaiveNameMatcher {
        return NaiveNameMatcher.init(self);
    }

    /// Rowscan's for a string amongst the the `Entry` `name`s in the db.
    pub const NaiveNameMatcher = struct {
        out_vec: std.ArrayListUnmanaged(Id) = .{},
        db: *Database,
       
        pub fn init(db: *Database) @This() {
            return @This(){
                .db = db,
            };
        }

        pub fn deinit(self: *@This()) void {
            self.out_vec.deinit(self.db.ha7r);
        }

        /// The returned slice borrows from `self` and is invalidated by the 
        /// next call of this method.
        pub fn match(
            self: *@This(),
            string: []const u8,
        ) ![]const Id {
            self.out_vec.clearRetainingCapacity();
            var it = self.db.table.iterator(self.db.sa7r, self.db.pager);
            defer it.close();
            var ii: usize = 0;
            while (try it.next()) |entry| {
                const name = try self.db.sa7r.swapIn(entry.name);
                defer self.db.sa7r.swapOut(entry.name);
                if (std.mem.indexOf(u8, name, string)) |_| {
                    const id = try self.db.idAt(ii);
                    try self.out_vec.append(self.db.ha7r, id);
                }
                ii += 1;
            }
            return self.out_vec.items;
        }
    };

    pub fn plistNameMatcher(self: *Self) PlistNameMatcher {
        return PlistNameMatcher.init(self);
    }

    /// Queries for a string amongst the `Entry` `name`s using the `SwappingPostingList` index.
    pub const PlistNameMatcher = struct {
        db: *Database,
        inner: PList.StrMatcher = .{},

        pub fn init(db: *Database) @This() {
            return @This(){ .db = db };
        }

        pub fn deinit(self: *@This()) void {
            self.inner.deinit(self.db.ha7r);
        }

        pub fn match(self: *@This(), string: []const u8) ![]const Id {
            return try self.inner.str_match(
                self.db.ha7r, 
                self.db.sa7r, 
                self.db.pager,
                &self.db.plist,
                string,
                std.ascii.spaces[0..],
            );
        }
    };
};

test "Database.usage" {
    var a7r = std.testing.allocator;
    var mmap_pager = try mod_mmap.MmapPager.init(a7r, "/tmp/SwappingIndex.usage", .{});
    defer mmap_pager.deinit();

    var lru = try mod_mmap.LRUSwapCache.init(a7r, mmap_pager.pager(), 1);
    defer lru.deinit();

    var pager = lru.pager();

    var ma7r = mod_mmap.MmapSwappingAllocator(.{}).init(a7r, pager);
    defer ma7r.deinit();

    var sa7r = ma7r.allocator();
    var db = Database.init(a7r, pager, sa7r, .{});
    defer db.deinit();
    var reader = db.reader();
    defer reader.deinit();

    var entry = FsEntry(Database.Id, []const u8) {
        .name = "/",
        .parent = Database.Id { .id = 0, .gen = 0 },
        .kind = Database.Entry.Kind.Directory,
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

    const id = try db.file_created(&entry);
    var ret = try reader.get(id);

    try std.testing.expectEqual(entry, ret.clone(entry.name));
    const dbName = try sa7r.swapIn(ret.name);
    defer sa7r.swapOut(ret.name);
    try std.testing.expectEqualSlices(u8, entry.name, dbName);
}
