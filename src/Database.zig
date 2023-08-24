const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.dbg;

const mod_treewalking = @import("treewalking.zig");
const FsEntry = mod_treewalking.FsEntry;

const mod_mmap = @import("mmap.zig");
const SwapList = mod_mmap.SwapList;
const Ptr = mod_mmap.SwapAllocator.Ptr;
const Pager = mod_mmap.Pager;
const SwapAllocator = mod_mmap.SwapAllocator;

const Query = @import("Query.zig");

const mod_gram = @import("gram.zig");

const mod_plist = @import("plist.zig");

pub const Quexecutor = @import("Database/Quexecutor.zig");

test {
    _ = Quexecutor;
}

const Self = @This();
const Database = Self;

/// Handle for each `Entry`
pub const Id = packed struct {
    gen: u8,
    id: u24,
    comptime {
        if (@sizeOf(Id) != @sizeOf(u32)) {
            @compileError(std.fmt.comptimePrint("unexpected size mismatch: (" ++ @typeName(Id) ++ "){} != (" ++ @typeName(u32) ++ ") {}\n", .{ @sizeOf(Id), @sizeOf(u32) }));
        }
        if (@bitSizeOf(Id) != @bitSizeOf(u32)) {
            @compileError(std.fmt.comptimePrint("unexpected bitSize mismatch: (" ++ @typeName(Id) ++ "){} != (" ++ @typeName(u32) ++ ") {}\n", .{ @bitSizeOf(Id), @bitSizeOf(u32) }));
        }
    }

    inline fn toInt(self: @This()) u32 {
        return @as(u32, @bitCast(self));
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
const FreeSlots = std.PriorityQueue(FreeSlot, void, struct {
    fn cmp(_: void, a: FreeSlot, b: FreeSlot) std.math.Order {
        return std.math.order(a.gen, b.gen);
    }
}.cmp);

pub const _gram_len = 3;

/// Our index, a trigram posting list with a gram length of 3.
/// We associate each trigram with a set of `Id`.
const PList = mod_plist.SwapPostingList(Id, _gram_len);

pub const _Gram = mod_gram.Gram(_gram_len);

pub const DatabseErr = error{
    StaleHandle,
};

config: Config,
/// The heap allocator.
ha7r: Allocator,
sa7r: SwapAllocator,
pager: Pager,
/// Our actual big list.
table: SwapList(Entry),
meta: SwapList(RowMeta),
plist: PList = .{},
childOfIndex: std.AutoHashMapUnmanaged(Id, std.AutoHashMapUnmanaged(Id, void)) = .{},
descendantOfIndex: std.AutoHashMapUnmanaged(Id, std.AutoHashMapUnmanaged(Id, void)) = .{},
free_slots: FreeSlots,
lock: std.Thread.Mutex = .{},

pub fn init(
    ha7r: Allocator,
    pager: Pager,
    sa7r: SwapAllocator,
    config: Config,
) Self {
    var self = Self{
        .ha7r = ha7r,
        .sa7r = sa7r,
        .pager = pager,
        .meta = SwapList(RowMeta).init(pager.pageSize()),
        .table = SwapList(Entry).init(pager.pageSize()),
        .free_slots = FreeSlots.init(ha7r, {}),
        .config = config,
    };
    return self;
}

pub fn deinit(self: *Self) void {
    {
        var it = self.meta.iterator(self.sa7r, self.pager);
        defer it.close();
        var idx: usize = 0;
        while (it.next() catch @panic("no no no")) |row| {
            if (!row.free) {
                const entry = self.table.swapIn(self.sa7r, self.pager, idx) catch @panic("no no no!");
                defer self.table.swapOut(self.sa7r, self.pager, idx);
                self.sa7r.free(entry.name);
            }
            idx += 1;
        }
    }

    self.table.deinit(self.ha7r, self.sa7r, self.pager);
    self.meta.deinit(self.ha7r, self.sa7r, self.pager);
    self.free_slots.deinit();
    self.plist.deinit(self.ha7r, self.sa7r, self.pager);
    {
        var it = self.childOfIndex.iterator();
        while (it.next()) |entry| {
            // entry.value_ptr.deinit(self.ha7r, self.sa7r, self.pager);
            entry.value_ptr.deinit(self.ha7r);
        }
        self.childOfIndex.deinit(self.ha7r);
    }
    {
        var it = self.descendantOfIndex.iterator();
        while (it.next()) |entry| {
            // entry.value_ptr.deinit(self.ha7r, self.sa7r, self.pager);
            entry.value_ptr.deinit(self.ha7r);
        }
        self.descendantOfIndex.deinit(self.ha7r);
    }
}

fn setAtIdx(self: *Self, idx: usize, entry: Entry) !void {
    try self.table.set(self.sa7r, self.pager, idx, entry);
}

pub fn treeCreated(self: *Self, tree: *const mod_treewalking.Tree, root_parent: Id) !void {
    self.lock.lock();
    defer self.lock.unlock();

    var new_ids = try self.ha7r.alloc(Id, tree.list.items.len);
    defer self.ha7r.free(new_ids);

    // handle the special root case first at index 0
    {
        const t_entry = tree.list.items[0];
        const i_entry = t_entry.conv(
            Id,
            []const u8,
            root_parent,
            t_entry.name,
        );
        new_ids[0] = try self.fileCreatedUnsafe(&i_entry);
    }
    for (tree.list.items[1..], 0..) |t_entry, ii| {
        const i_entry = t_entry.conv(
            Id,
            []const u8,
            new_ids[t_entry.parent],
            t_entry.name,
        );
        new_ids[ii + 1] = try self.fileCreatedUnsafe(&i_entry);
    }
}

pub inline fn fileCreated(self: *Self, entry: *const FsEntry(Id, []const u8)) !Id {
    self.lock.lock();
    defer self.lock.unlock();
    return self.fileCreatedUnsafe(entry);
}

/// Add an entry to the database.
fn fileCreatedUnsafe(self: *Self, entry: *const FsEntry(Id, []const u8)) !Id {
    // clone the entry but store the name on the swap this time
    const swapped_entry = entry.clone(try self.sa7r.dupeJustPtr(entry.name));
    const id = if (self.free_slots.removeOrNull()) |id| blk: {
        // mind the `try`s and their order

        var row = try self.meta.swapIn(self.sa7r, self.pager, id.id);
        defer self.meta.swapOut(self.sa7r, self.pager, id.id);

        try self.setAtIdx(id.id, swapped_entry);

        row.gen += 1;
        row.free = false;
        break :blk Id{
            .id = id.id,
            .gen = row.gen,
        };
    } else blk: {
        const idx = self.meta.len;
        try self.meta.append(self.ha7r, self.sa7r, self.pager, RowMeta{
            .free = false,
            .gen = 0,
        });
        // TODO: this shit
        errdefer _ = self.meta.pop(self.sa7r, self.pager) catch unreachable;

        try self.table.append(self.ha7r, self.sa7r, self.pager, swapped_entry);

        break :blk Id{
            .id = @as(u24, @intCast(idx)),
            .gen = 0,
        };
    };
    try self.plist.insert(self.ha7r, self.sa7r, self.pager, id, entry.name, std.ascii.whitespace[0..]
    // &[_]u8{ self.config.delimiter }
    );
    {
        var kv = try self.childOfIndex.getOrPut(self.ha7r, entry.parent);
        if (!kv.found_existing) {
            // kv.value_ptr.* = SwapList(Id).init(self.pager.pageSize());
            kv.value_ptr.* = .{};
        }
        try kv.value_ptr.put(self.ha7r, id, {});
    }
    // skip populating the desc of index
    if (false) {
        var id_self = id;
        var parent = entry.parent;
        while (
        // FIXME: a better way of detecting root
        id_self.id != parent.id) {
            var kv = try self.descendantOfIndex.getOrPut(self.ha7r, parent);
            if (!kv.found_existing) {
                // kv.value_ptr.* = SwapList(Id).init(self.pager.pageSize());
                kv.value_ptr.* = .{};
            }
            try kv.value_ptr.put(self.ha7r, id, {});

            id_self = parent;

            var old_parent = parent;
            // println(" id: {}, parent: {}, entry: {} ", .{ id, parent, entry });
            var parent_desc = try self.table.swapIn(self.sa7r, self.pager, old_parent.id);
            defer self.table.swapOut(self.sa7r, self.pager, old_parent.id);
            parent = parent_desc.parent;
        }
    }
    return id;
}

pub fn fileDeleted(self: *Self, id: Id) !bool {
    self.lock.lock();
    defer self.lock.unlock();
    return self.fileDeletedUnsafe(id, false);
}

fn fileDeletedUnsafe(self: *Self, id: Id, recusring: bool) !bool {
    if (try self.isStaleUnsafe(id)) return false;
    // recursively free any children
    if (self.childOfIndex.fetchRemove(id)) |kv| {
        var children = kv.value;
        defer children.deinit(self.ha7r);
        var it = children.iterator();
        while (it.next()) |pair| {
            var was_deleted = try self.fileDeletedUnsafe(pair.key_ptr.*, true);
            if (was_deleted) {
                // @panic("found an actual live child");
            }
            // @panic("children list is not empty");
        }
    }

    // do all faillible things first
    var row = try self.meta.swapIn(self.sa7r, self.pager, id.id);
    defer self.meta.swapOut(self.sa7r, self.pager, id.id);

    const entry = try self.table.swapIn(self.sa7r, self.pager, id.id);
    defer self.table.swapOut(self.sa7r, self.pager, id.id);

    try self.free_slots.add(id);

    row.free = true;
    // remove entry from indices

    var name = try self.sa7r.swapIn(entry.name);
    errdefer self.sa7r.swapOut(entry.name);
    // remove from plist
    try self.plist.remove(self.ha7r, self.sa7r, self.pager, id, name, std.ascii.whitespace[0..], struct {
        fn isEql(lhs: Id, rhs: Id) bool {
            return lhs.toInt() == rhs.toInt();
        }
    }.isEql);

    if (!recusring) {
        // remove from parent's `childOfIndex`
        var siblings = self.childOfIndex.get(entry.parent) orelse @panic("no siblings index");
        std.debug.assert(siblings.remove(id));
    }

    self.sa7r.swapOut(entry.name);
    self.sa7r.free(entry.name);
    // TODO: remove from ancestors' `descendantOfIndex`

    return true;
}

pub fn fileMoved(self: *Self, id: Id, new_name: []const u8, new_parent_opt: ?Id) !void {
    self.lock.lock();
    defer self.lock.unlock();
    return self.fileMovedUnsafe(id, new_name, new_parent_opt);
}

pub fn fileMovedUnsafe(self: *Self, id: Id, new_name: []const u8, new_parent_opt: ?Id) !void {
    if (try self.isStaleUnsafe(id)) return error.StaleHandle;

    const entry = try self.table.swapIn(self.sa7r, self.pager, id.id);
    defer self.table.swapOut(self.sa7r, self.pager, id.id);

    var old_name_swap = entry.name;
    defer self.sa7r.free(old_name_swap);

    var old_name = try self.sa7r.swapIn(old_name_swap);
    errdefer self.sa7r.swapOut(old_name_swap);

    try self.plist.insert(self.ha7r, self.sa7r, self.pager, id, new_name, std.ascii.whitespace[0..]
    // &[_]u8{ self.config.delimiter }
    );

    entry.name = try self.sa7r.dupeJustPtr(new_name);

    try self.plist.remove(self.ha7r, self.sa7r, self.pager, id, old_name, std.ascii.whitespace[0..], struct {
        fn isEql(lhs: Id, rhs: Id) bool {
            return lhs.toInt() == rhs.toInt();
        }
    }.isEql);

    if (new_parent_opt) |new_parent| {
        if (entry.parent.toInt() != new_parent.toInt()) {
            // remove from parent's `childOfIndex`
            var old_siblings = self.childOfIndex.get(entry.parent) orelse @panic("no siblings index");
            std.debug.assert(old_siblings.remove(id));

            entry.parent = new_parent;

            var kv = try self.childOfIndex.getOrPut(self.ha7r, new_parent);
            if (!kv.found_existing) {
                // kv.value_ptr.* = SwapList(Id).init(self.pager.pageSize());
                kv.value_ptr.* = .{};
            }
            try kv.value_ptr.put(self.ha7r, id, {});
        }
    }
}

pub fn fileModified(self: *Self, id: Id, entry: *const FsEntry(void, void)) !void {
    self.lock.lock();
    defer self.lock.unlock();
    return self.fileModifiedUnsafe(id, entry);
}

fn fileModifiedUnsafe(self: *Self, id: Id, entry: *const FsEntry(void, void)) !void {
    if (try self.isStaleUnsafe(id)) return error.StaleHandle;

    const entry_ptr = try self.table.swapIn(self.sa7r, self.pager, id.id);
    defer self.table.swapOut(self.sa7r, self.pager, id.id);

    entry_ptr.* = entry.conv(Id, Ptr, entry_ptr.parent, entry_ptr.name);
}

/// This locks the db.
pub fn isStale(self: *Self, id: Id) !bool {
    self.lock.lock();
    defer self.lock.unlock();
    return self.isStaleUnsafe(id);
}

fn isStaleUnsafe(self: *Self, id: Id) !bool {
    if (self.meta.len <= id.id) return true;
    const row = try self.meta.swapIn(self.sa7r, self.pager, id.id);
    defer self.meta.swapOut(self.sa7r, self.pager, id.id);
    return row.gen > id.gen;
}

fn idAt(self: *Self, idx: usize) !Id {
    const row = try self.meta.swapIn(self.sa7r, self.pager, idx);
    defer self.meta.swapOut(self.sa7r, self.pager, idx);
    return Id{
        .id = @as(u24, @intCast(idx)),
        .gen = row.gen,
    };
}

threadlocal var pathBufGetAt: [std.fs.MAX_PATH_BYTES]u8 = [_]u8{0} ** std.fs.MAX_PATH_BYTES;
/// The returned Entry's `name` is invalidated by the next call of this method.
/// This locks the db.
pub fn getAtId(self: *Database, id: Id) !FsEntry(Id, []const u8) {
    self.lock.lock();
    defer self.lock.unlock();
    return self.getAtIdUnsafe(id);
}

fn getAtIdUnsafe(self: *Database, id: Id) !FsEntry(Id, []const u8) {
    if (try self.isStaleUnsafe(id)) return error.StaleHandle;
    const entry = try self.table.swapIn(self.sa7r, self.pager, id.id);
    defer self.table.swapOut(self.sa7r, self.pager, id.id);
    const name = try self.sa7r.swapIn(entry.name);
    defer self.sa7r.swapOut(entry.name);
    @memcpy(pathBufGetAt[0..name.len], name);
    return entry.conv(Id, []const u8, entry.parent, pathBufGetAt[0..name.len]);
}

// pub const Writer = struct {
// };

/// Stitches together the full path of an `Entry` given the `Id`.
pub const FullPathWeaver = struct {
    buf: std.ArrayListUnmanaged(u8) = .{},

    pub fn deinit(self: *@This(), a7r: Allocator) void {
        self.buf.deinit(a7r);
    }
    /// The returned slice is invalidated not long after.
    /// This locks the db.
    pub fn pathOf(self: *FullPathWeaver, db: *Database, id: Id, delimiter: u8) ![]const u8 {
        self.buf.clearRetainingCapacity();
        var next_id = id;
        // const names = db.table.items(.name);
        // const parents = db.table.items(.parent);

        db.lock.lock();
        defer db.lock.unlock();

        while (true) {
            if (try db.isStaleUnsafe(next_id)) return error.StaleHandle;

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
    /// This locks the db.
    pub fn match(
        self: *@This(),
        string: []const u8,
    ) ![]const Id {
        self.db.lock.lock();
        defer self.db.lock.unlock();

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
    _ = self;
    return PlistNameMatcher{};
}

/// Queries for a string amongst the `Entry` `name`s using the `SwapPostingList` index.
pub const PlistNameMatcher = struct {
    inner: PList.StrMatcher = .{},

    pub fn deinit(self: *@This(), db: *Database) void {
        self.inner.deinit(db.ha7r);
    }

    /// This locks the db.
    pub fn match(self: *@This(), db: *Database, string: []const u8) ![]const Id {
        db.lock.lock();
        defer db.lock.unlock();
        return try self.inner.strMatch(
            db.ha7r,
            db.sa7r,
            db.pager,
            &db.plist,
            string,
            std.ascii.whitespace[0..],
        );
    }
};

test "Database.usage" {
    var a7r = std.testing.allocator;
    var mmap_pager = try mod_mmap.MmapPager.init(a7r, "/tmp/Database.usage", .{});
    defer mmap_pager.deinit();

    var lru = try mod_mmap.LRUSwapCache.init(a7r, mmap_pager.pager(), 1);
    defer lru.deinit();

    var pager = lru.pager();

    var ma7r = mod_mmap.PagingSwapAllocator(.{}).init(a7r, pager);
    defer ma7r.deinit();

    var sa7r = ma7r.allocator();
    var db = Database.init(a7r, pager, sa7r, .{});
    defer db.deinit();

    var entry = FsEntry(Id, []const u8){
        .name = "/",
        .parent = Id{ .id = 0, .gen = 0 },
        .kind = Entry.Kind.directory,
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

    const id = try db.fileCreated(&entry);
    var ret = try db.getAtId(id);

    try std.testing.expectEqual(entry, ret.clone(entry.name));
    try std.testing.expectEqualStrings(entry.name, ret.name);
}

pub fn genRandFile(path: []const u8) FsEntry([]const u8, []const u8) {
    var prng = std.crypto.random;
    const name = if (std.mem.eql(u8, path, "/")) "/" else std.fs.path.basename(path);
    return FsEntry([]const u8, []const u8){
        .name = name,
        .parent = std.fs.path.dirname(path) orelse "/"[0..],
        .kind = FsEntry([]const u8, []const u8).Kind.file,
        .depth = std.mem.count(u8, path, "/"),
        .size = prng.int(u64),
        .inode = prng.int(u64),
        .dev = prng.int(u64),
        .mode = 666,
        .uid = prng.int(u32),
        .gid = prng.int(u32),
        .ctime = prng.int(i64),
        .atime = prng.int(i64),
        .mtime = prng.int(i64),
    };
}

/// Add the files to the db while making sure any implied parent dir in path
/// string is also present.
/// The metadata for parents is randomly generated.
/// Assumes db is empty.
/// Returns a heap allocated array of `Id`s assigned to the `entries` in the
/// respective indices.
pub fn fileList2PlasticTree2Db(ha7r: Allocator, entries: []const FsEntry([]const u8, []const u8), db: *Database) ![]Id {
    var declared_map = try ha7r.alloc(Id, entries.len);

    var path_id_map = std.StringHashMap(Id).init(ha7r);
    defer {
        var it = path_id_map.keyIterator();
        var count: usize = 0;
        while (it.next()) |key| {
            ha7r.free(key.*);
            count += 1;
        }
        path_id_map.deinit();
    }

    var stack = std.ArrayList(FsEntry([]const u8, []const u8)).init(ha7r);
    defer stack.deinit();

    // for (declared_map) |*item| {
    //     item.* = Id { .id = 0, .gen = 0 };
    // }
    // if (true) return declared_map;

    for (entries, 0..) |entry, ii| {
        // println("entry.name={s} entry.parent={s}", .{ entry.name, entry.parent });

        defer stack.clearRetainingCapacity();
        try stack.append(entry);
        while (true) {
            // println(
            //     "name: {s}, parent: {s}",
            //     .{
            //         stack.items[stack.items.len - 1].name,
            //         stack.items[stack.items.len - 1].parent,
            //      }
            // );
            if (path_id_map.contains(stack.items[stack.items.len - 1].parent) or
                std.mem.eql(u8, stack.items[stack.items.len - 1].name, "/")) break;
            try stack.append(genRandFile(stack.items[stack.items.len - 1].parent));
        }
        var id: ?Id = null;
        while (stack.popOrNull()) |s_entry| {
            var is_root = std.mem.eql(u8, s_entry.name, "/");

            const parent = path_id_map.get(s_entry.parent) orelse if (is_root)
                Id{ .id = 0, .gen = 0 }
            else {
                println("name: {s}, parent: {s}", .{ s_entry.name, s_entry.parent });
                unreachable;
            };

            var entry_id = try db.fileCreated(&s_entry.conv(Id, []const u8, parent, s_entry.name));
            // println(
            //    "inserted entry: {s}/{s} at id {}",
            //    .{ s_entry.parent, s_entry.name, entry_id }
            // );
            try path_id_map.putNoClobber(try std.fs.path.join(ha7r, &.{ s_entry.parent, s_entry.name }),
            // if (is_root)
            //     try ha7r.dupe(u8, "/")
            // else
            //     try std.fmt.allocPrint(ha7r, "{s}/{s}", .{ s_entry.parent, s_entry.name }),
            entry_id);
            id = entry_id;
        }
        declared_map[ii] = id orelse @panic("id not set");
    }
    return declared_map;
}

const DbTest = struct {
    const Case = struct {
        name: []const u8,
        query: []const u8,
        entries: []const FsEntry([]const u8, []const u8),
        expected: []const usize,
        preQueryAction: ?*const fn (*Database, []Id) anyerror!void = null,
    };

    fn run(table: []const Case) !void {
        var ha7r = std.testing.allocator;
        var big_buf = [_]u8{0} ** 1024;
        for (table) |case| {
            var mmap_pager = try mod_mmap.MmapPager.init(ha7r, try std.fmt.bufPrint(big_buf[0..], "/tmp/Database.query.{s}", .{case.name}), .{});
            defer mmap_pager.deinit();

            var lru = try mod_mmap.LRUSwapCache.init(ha7r, mmap_pager.pager(), 1);
            defer lru.deinit();

            var pager = lru.pager();

            var ma7r = mod_mmap.PagingSwapAllocator(.{}).init(ha7r, pager);
            defer ma7r.deinit();

            var sa7r = ma7r.allocator();

            var db = Database.init(ha7r, pager, sa7r, .{});
            defer db.deinit();

            var querier = Quexecutor.init(&db);
            defer querier.deinit();

            var declared_map = try fileList2PlasticTree2Db(ha7r, case.entries, &db);
            defer ha7r.free(declared_map);

            if (case.preQueryAction) |action| {
                try action(&db, declared_map);
            }

            var parser = Query.Parser{};
            defer parser.deinit(ha7r);

            // var parsed_query = try Query.parse(ha7r, case.query);
            var parsed_query = try parser.parse(ha7r, case.query);
            defer parsed_query.deinit(ha7r);
            var results = try ha7r.dupe(Id, try querier.query(&parsed_query));
            defer ha7r.free(results);
            var expected_id_list = try ha7r.alloc(Id, case.expected.len);
            defer ha7r.free(expected_id_list);
            for (case.expected, 0..) |ex_idx, ii| {
                expected_id_list[ii] = declared_map[ex_idx];
            }
            const lt = struct {
                fn lt(cx: void, lhs: Id, rhs: Id) bool {
                    _ = cx;
                    return lhs.id < rhs.id;
                }
            };
            std.sort.pdq(Id, expected_id_list, {}, lt.lt);
            std.sort.pdq(Id, results, {}, lt.lt);
            std.testing.expectEqualSlices(Id, expected_id_list, results) catch |err| {
                println("unequal slices:\n\tresult: {any},\n\texpected: {any}\n\traw: {s}\n\tquery: {}", .{ results, expected_id_list, case.query, parsed_query });
                println("{any}", .{declared_map});
                return err;
            };
        }
    }
};

// fn randFile(name: []const u8) FsEntry()
test "Db.e2e" {
    var table = [_]DbTest.Case{
        .{
            .name = "supports_empty_query",
            .entries = &.{
                genRandFile("/"),
                genRandFile("/you"),
                genRandFile("/you/are"),
                genRandFile("/you/are/the"),
                genRandFile("/you/are/the/generation"),
                genRandFile("/you/are/the/generation/that"),
                genRandFile("/you/are/the/generation/that/bought"),
                genRandFile("/you/are/the/generation/that/bought/more"),
                genRandFile("/you/are/the/generation/that/bought/more/shoes"),
            },
            .query = "",
            .expected = &.{ 0, 1, 2, 3, 4, 5, 6, 7, 8 },
        },
        .{
            .name = "supports_name_match",
            .entries = &.{
                genRandFile("/because/needle"),
                genRandFile("/dreaming/costs/money"),
                genRandFile("/my/dear/needle"),
            },
            .query = "needle",
            .expected = &.{
                0,
                2,
            },
        },
        .{
            .name = "supports_path_match",
            .query = "kero/kero",
            .entries = &.{
                genRandFile("/kero/bonito/kero"),
                genRandFile("/kero/kero"),
                genRandFile("/kero/kero/bonito"),
                genRandFile("/bonito/kero/kero"),
            },
            .expected = &.{ 1, 3 },
        },
        // .{
        //     .name = "supports_double_quotes_to_match_whitespace",
        //     .query = "\"borgouise bologna\"",
        //     .entries = &.{
        //         genRandFile("/rodrigo/borgouise"),
        //         genRandFile("/rodrigo/borgouise bologna"),
        //         genRandFile("/rodrigo/bologna"),
        //         genRandFile("/rodrigo/borgia"),
        //     },
        //     .expected = &.{1},
        // },
        // .{
        //     .name = "supports_double_quotes_to_match_whitespace_in_path",
        //     .query = "\"rodrigo/borgouise bologna/sandwitch\"",
        //     .entries = &.{
        //         genRandFile("/borgouise bologna/sandwitch"),
        //         genRandFile("/rodrigo/borgouise bologna/sandwitch"),
        //         genRandFile("/antonio/borgouise bologna/sandwitch"),
        //     },
        //     .expected = &.{1},
        // },
    };
    try DbTest.run(table[0..]);
}

// pub const ColumnarDatabase = struct {
//     const Self = @This();
//
//     /// Represents a single file system object.
//     pub const Entry = FsEntry(Id, Ptr);
//
//     /// An enum identifying a column
//     pub const Column = std.meta.FieldEnum(Entry);
//
//     fn ColumnType(field: Column) type {
//         return std.meta.fieldInfo(Entry, field).field_type;
//     }
//     /// Metadata about the columns in sliceform for ergonomic "foreaching"
//     const columns = blk: {
//         const ColumnDeets = struct {
//             column: Column,
//             // type: type,
//         };
//         const fields = std.meta.fields(Entry);
//         var cols: [fields.len]ColumnDeets = undefined;
//         inline for (fields) |info, ii| {
//             cols[ii] = ColumnDeets {
//                 .column = std.enums.nameCast(Column, info.name),
//                 // .type = info.field_type,
//             };
//         }
//         break :blk cols;
//     };
//
//     fn ColumnStore(comptime T: type) type {
//         return struct {
//             list: SwapList(T),
//
//             pub fn init(page_size: usize) @This() {
//                 return @This() {
//                     .list = SwapList(T).init(page_size),
//                 };
//             }
//
//             pub fn deinit(
//                 self: *@This(),
//                 ha7r: Allocator,
//                 sa7r: SwapAllocator,
//                 pager: Pager
//             ) void {
//                 self.list.deinit(ha7r, sa7r, pager);
//             }
//         };
//     }
//
//     const ErasedColumnStore = struct {
//         ptr: *anyopaque,
//         column: Column,
//
//         pub fn init(comptime column: Column, ha7r: Allocator, page_size: usize) !@This() {
//             const Store = ColumnStore(ColumnType(column));
//             var ptr = try ha7r.create(ColumnStore(ColumnType(column)));
//             ptr.* = Store.init(page_size);
//             return @This() {
//                 .ptr = ptr,
//                 .column = column,
//             };
//         }
//         pub fn deinit(
//             self: *@This(),
//             comptime column: Column,
//             ha7r: Allocator,
//             sa7r: SwapAllocator,
//             pager: Pager
//         ) void {
//             self.cast(column).deinit(ha7r, sa7r, pager);
//             ha7r.destroy(self.ptr);
//         }
//
//         pub inline fn cast(self: @This(), comptime column: Column) *ColumnStore(ColumnType(column)) {
//             std.debug.assert(column == self.column);
//             var aligned = @alignCast(@alignOf(*ColumnStore(ColumnType(column))), self.ptr);
//             return @ptrCast(*ColumnStore(ColumnType(column)), aligned);
//         }
//     };
//
//     // const Table = blk: {
//     //     break :blk @Type(std.builtin.Type{
//     //         .Struct = .{
//     //             .layout = .Auto,
//     //             .decls = &.{}
//     //         }
//     //     });
//     // };
//     const Table = std.EnumArray(Column, ErasedColumnStore);
//     table: Table,
//
//     pub fn init() Self {
//         return Self {
//             .table = blk: {
//                 var table = Table.initUndefined();
//                 inline for (columns) |col| {
//                     self.table.set(
//                         col.column,
//                         try ErasedColumnStore.init(col.column, self.ha7r, pager.pageSize())
//                     );
//                 }
//                 break :blk table;
//             },
//         };
//     }
//
//     pub fn deinit(self: *Self) void {
//         inline for (columns) |col| {
//             self.table.getPtr(col.column).deinit(
//                 col.column,
//                 self.ha7r, self.sa7r, self.pager,
//             );
//         }
//     }
//
//     fn setAtIdx(self: *Self, idx: usize, entry: Entry) !void {
//         inline for (columns) |col| {
//             try self.table.getPtr(col.column).cast(col.column).list.set(
//                 self.sa7r,
//                 self.pager,
//                 idx,
//                 @field(entry, @tagName(col.column))
//             );
//         }
//     }
//
//     fn appendEntry(self: *Self, entry: Entry) !void {
//         inline for (columns) |col| {
//             try self.table.getPtr(col.column).cast(col.column).list.append(
//                 self.ha7r, self.sa7r, self.pager, @field(entry, @tagName(col.column))
//             );
//         }
//     }
// };
