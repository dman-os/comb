const std = @import("std");
const Allocator = std.mem.Allocator;
usingnamespace @import("utils.zig");

test {
    _ = PlasticTree;
    _ = Tree;
}

pub fn FsEntry(comptime P: type, comptime N: type) type {
    return struct {
        pub const Kind = std.fs.File.Kind;
        name: N,
        parent: P,
        kind: Kind,
        depth: usize,
        size: u64,
        inode: u64,
        dev: u64,
        mode: u32,
        uid: u32,
        gid: u32,
        ctime: i64,
        atime: i64,
        mtime: i64,

        pub fn fromMeta(
            name: N, 
            parent: P, 
            depth: usize,
            meta: std.fs.File.MetadataLinux,
        ) FsEntry(P, N) {
            var statx = meta.statx;
            return @This() {
                .name = name,
                .kind = kindFromMode(statx.mode),
                .depth = depth,
                .parent = parent,
                .size = statx.size,
                .inode = statx.ino,
                .dev = makedev(statx),
                .mode = statx.mode,
                .uid = statx.uid,
                .gid = statx.gid,
                .ctime = statx.ctime.tv_sec,
                .atime = statx.atime.tv_sec,
                .mtime = statx.mtime.tv_sec,
            };
        }

        pub fn clone(orig: *const @This(), new_name: anytype) FsEntry(P, @TypeOf(new_name)) {
            return FsEntry(P, @TypeOf(new_name)) {
                // .name = try a7r.dupe(u8, orig.name),
                .name = new_name,
                .parent = orig.parent,
                .kind = orig.kind,
                .depth = orig.depth,
                .size = orig.size,
                .inode = orig.inode,
                .dev = orig.dev,
                .mode = orig.mode,
                .uid = orig.uid,
                .gid = orig.gid,
                .ctime = orig.ctime,
                .atime = orig.atime,
                .mtime = orig.mtime,
            };
        }

        /// This clones it fo sure
        pub fn conv(
            orig: *const @This(), 
            comptime NP: type, 
            comptime NN: type, 
            new_parent: NP,
            new_name: NN,
        ) FsEntry(NP, NN) {
            return FsEntry(NP, NN) {
                .name = new_name,
                .parent = new_parent,
                .kind = orig.kind,
                .depth = orig.depth,
                .size = orig.size,
                .inode = orig.inode,
                .dev = orig.dev,
                .mode = orig.mode,
                .uid = orig.uid,
                .gid = orig.gid,
                .ctime = orig.ctime,
                .atime = orig.atime,
                .mtime = orig.mtime,
            };
        }
    };
}

/// Name and parent will be basename and dirname slices of the path.
/// Use [`conv`] with conjuction [`Allocator.dupe`] to make make them owned.
pub fn entryFromAbsolutePath(path: []const u8) !FsEntry([]const u8, []const u8) {
    const name = std.fs.path.basename(path);
    const parent = std.fs.path.dirname(path) orelse name;

    const posix_path = try std.os.toPosixPath(path);
    var meta = try metaNoFollow(std.os.AT.FDCWD, &posix_path);
    var statx = meta.statx;

    return FsEntry([]const u8, []const u8) {
        .name = name,
        .kind = kindFromMode(statx.mode),
        .depth = std.mem.count(u8, path, "/"),
        .parent = parent,
        .size = statx.size,
        .inode = statx.ino,
        .dev = makedev(statx),
        .mode = statx.mode,
        .uid = statx.uid,
        .gid = statx.gid,
        .ctime = statx.ctime.tv_sec,
        .atime = statx.atime.tv_sec,
        .mtime = statx.mtime.tv_sec,
    };
}

const ReadMetaError = error {
    UnsupportedSyscall
};

/// Name has to be sentinel terminated, I think.
/// Modified from zig std lib
/// `dir_handle` will be ignored if `path_name` is an absolute path
pub fn metaNoFollow(dir_handle: std.os.fd_t, path_name: [*]const u8) !std.fs.File.MetadataLinux {
    const os = std.os;
    var stx = std.mem.zeroes(os.linux.Statx);
    const rcx = os.linux.statx(
        dir_handle, 
        path_name,
        os.linux.AT.EMPTY_PATH | os.linux.AT.SYMLINK_NOFOLLOW, 
        os.linux.STATX_BASIC_STATS | 
        os.linux.STATX_BTIME, 
        &stx
    );

    switch (os.errno(rcx)) {
        .SUCCESS => {},
        .ACCES => return os.OpenError.AccessDenied,
        .BADF => unreachable,
        .FAULT => unreachable,
        .INVAL => unreachable,
        .LOOP => unreachable,
        .NOENT => return os.OpenError.FileNotFound,
        .NAMETOOLONG => return os.OpenError.NameTooLong,
        .NOTDIR => return os.OpenError.NotDir,
        // NOSYS happens when `statx` is unsupported, which is the case on kernel versions before 4.11
        // Here, we call `fstat` and fill `stx` with the data we need
        .NOSYS => {
            @panic("statx not spported in kernel");
            // return ReadMetaError.UnsupportedSyscall;
        },
        .NOMEM => return os.OpenError.SystemResources,
        else => |err| return os.unexpectedErrno(err),
    }
    return std.fs.File.MetadataLinux {
        .statx = stx,
    };
}

/// Lifted this from rust libc binidings
pub fn makedev(statx: std.os.linux.Statx) u64 {
    // return (@as(u64, statx.dev_major) << 32 ) & @as(u64, statx.dev_minor);
    const major = @as(u64, statx.dev_major);
    const minor = @as(u64, statx.dev_minor);
    var dev: u64 = 0;
    dev |= (major & 0x00000fff) << 8;
    dev |= (major & 0xfffff000) << 32;
    dev |= (minor & 0x000000ff) << 0;
    dev |= (minor & 0xffffff00) << 12;
    return dev;
}

fn kindFromMode(mode: std.os.mode_t) std.fs.File.Kind {
    const S = std.os.linux.S;
    const Kind = std.fs.File.Kind;
    return switch (mode & S.IFMT) {
        S.IFREG => Kind.File,
        S.IFDIR => Kind.Directory,
        S.IFLNK => Kind.SymLink,
        S.IFSOCK => Kind.UnixDomainSocket,
        S.IFCHR => Kind.CharacterDevice,
        S.IFBLK => Kind.BlockDevice,
        S.IFIFO => Kind.NamedPipe,
        else => Kind.Unknown
    };
}

/// As a convention, the root of the tree's at index 0 and has itself set as
/// a parent.
pub const Tree = struct {
    pub fn walk(allocator: Allocator, path: []const u8, limit: ?usize) !Tree {
        var walker = Tree.Walker.init(allocator, limit orelse std.math.maxInt(usize));
        errdefer walker.deinit();
        const dev = blk: {
            var dir = try std.fs.openDirAbsolute(path, .{});
            defer dir.close();
            const meta = try dir.metadata();
            break :blk makedev(meta.inner.statx);
        };
        try walker.scanDir(path, std.fs.cwd(), 0, 0, dev, );
        return walker.toTree();
    }

    pub const Entry = FsEntry(usize, []u8);
    list: std.ArrayList(Entry),
    allocator: Allocator,

    pub fn init(allocator: Allocator) Tree {
        return Tree {
            .allocator = allocator,
            .list = std.ArrayList(Entry).init(allocator),
        };
    }

    pub fn deinit(self: *Tree) void {
        for (self.list.items) |entry| {
            self.allocator.free(entry.name);
        }
        self.list.deinit();
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
        pub fn pathOf(self: *FullPathWeaver, allocator: Allocator, tree: *const Tree, id: usize, delimiter: u8) ![]const u8 {
            self.buf.clearRetainingCapacity();
            var next_id = id;
            while (true) {
                const entry = tree.list.items[next_id];
                try self.buf.appendSlice(allocator, entry.name);
                std.mem.reverse(u8, self.buf.items[(self.buf.items.len - entry.name.len)..]);
                try self.buf.append(allocator, delimiter);

                next_id = entry.parent;
                if(next_id == 0) {
                    break;
                }
            }
            std.mem.reverse(u8, self.buf.items[0..]);
            return self.buf.items;
        }
    };

    pub const Walker = struct {
        remaining: usize,
        tree: Tree,
        weaver: FullPathWeaver,
        // buf: std.ArrayList(u8),

        log_interval: usize = 10_000,

        pub const Error = 
            Allocator.Error || 
            std.fs.File.OpenError || 
            std.fs.File.MetadataError || 
            std.fs.IterableDir.Iterator.Error ||
            std.os.UnexpectedError;

        fn init(allocator: Allocator, limit: usize) @This() {
            return @This() {
                .remaining = limit,
                .tree = Tree.init(allocator),
                // .buf = std.ArrayList(u8).init(allocator),
                .weaver = FullPathWeaver.init(),
            };
        }

        fn deinit(self: *@This()) void{
            self.tree.deinit();
            // self.buf.deinit();
            self.weaver.deinit(self.tree.allocator);
        }

        fn toTree(self: *@This()) Tree {
            self.weaver.deinit(self.tree.allocator);
            return self.tree;
        }

        fn append(self: *Walker, entry: Entry) !void {
            try self.tree.list.append(entry);
            self.remaining -= 1;
            if (self.tree.list.items.len % self.log_interval == 0) {
                const path = try self.weaver.pathOf(
                    self.tree.allocator, 
                    &self.tree,
                    self.tree.list.items.len - 1, 
                    '/'
                );
                // std.debug.print(
                std.log.info(
                    "scanned {} items, now on: {s}", 
                    .{ self.tree.list.items.len, path }
                );
            }
        }

        fn scanDir(
            self: *@This(), 
            path: []const u8, 
            parent: std.fs.Dir,
            parent_id: usize, 
            depth: usize, 
            // parent_dev: std.meta.Tuple(&.{u32, u32}), 
            parent_dev: u64, 
        ) Error!void {
            // std.debug.print("error happened at path = {s}\n", .{path});
            var iterable_dir = parent.openIterableDir(path, .{ .no_follow = true }) catch |err| {
                switch(err){
                    std.fs.Dir.OpenError.AccessDenied => {
                        const parent_path = try self.weaver.pathOf(
                            self.tree.allocator, 
                            &self.tree,
                            parent_id, 
                            '/'
                        );
                        std.log.debug(
                            "AccessDenied opening dir {s}/{s}", .{ parent_path, path, });
                        return;
                    },
                    else => return err,
                }
            };
            defer iterable_dir.close();
            var dir = iterable_dir.dir;

            const dev = blk: {
                const meta = try dir.metadata();
                var entry = Entry.fromMeta(
                    try self.tree.allocator.dupe(u8, path),
                    parent_id,
                    depth,
                    meta.inner
                );
                try self.append(entry);
                // we don't examine other devices
                if (entry.dev != parent_dev) {
                    const parent_path = try self.weaver.pathOf(
                        self.tree.allocator, 
                        &self.tree,
                        parent_id, 
                        '/'
                    );
                    std.log.debug(
                        "device ({}) != parent dev ({}), skipping dir at = {s}/{s}", 
                        .{ entry.dev, parent_dev, parent_path, path },
                    );
                    return;
                }
                break :blk entry.dev;
            };
            const dir_id = self.tree.list.items.len - 1;

            var it = iterable_dir.iterate();
            while (self.remaining > 0) {
                // handle the error first
                if (it.next()) |next| {
                    // check if there's a file left in the dir
                    const entry = next orelse break;
                    if (entry.kind == .Directory) {
                        try self.scanDir(entry.name, dir, dir_id, depth + 1, dev);
                    } else {
                        const posix_name = try std.os.toPosixPath(entry.name);
                        const meta = metaNoFollow(dir.fd, &posix_name) catch |err| {
                            const parent_path = try self.weaver.pathOf(
                                self.tree.allocator, 
                                &self.tree,
                                dir_id, 
                                '/'
                            );
                            switch(err){
                                std.os.OpenError.AccessDenied => {
                                    std.log.debug(
                                        "AccessDenied opening file {s}/{s}", .{ parent_path, entry.name, });
                                    continue;
                                },
                                else => { 
                                    std.debug.print(
                                    // std.log.info(
                                        "Unexpected err {} at {s}/{s}", .{ err, parent_path, entry.name, });
                                    return err;
                                },
                            }
                        };
                        try self.append(Entry.fromMeta(
                            try self.tree.allocator.dupe(u8, entry.name),
                            dir_id,
                            depth,
                            meta
                        ));
                    }
                } else |err| switch (err) {
                    std.fs.IterableDir.Iterator.Error.AccessDenied => {
                        const parent_path = try self.weaver.pathOf(
                            self.tree.allocator, 
                            &self.tree,
                            parent_id, 
                            '/'
                        );
                        std.log.debug(
                            "AccessDenied on iteration for dir at: {s}/{s}", .{ parent_path, path, });
                    },
                    else => return err,
                }
            }
        }
    };

    test "Tree.usage" {
        if (true) return error.SkipZigTest;

        const size: usize = 10_000;
        var allocator = std.testing.allocator;
        var tree = try walk(allocator, "/", size);
        defer tree.deinit();
        var weaver = FullPathWeaver.init();
        defer weaver.deinit(allocator);
        for (tree.list.items) |file, id| {
            const path = try weaver.pathOf(allocator, tree, id, '/');
            std.debug.print(
                "{} | kind = {} | parent = {} | size = {} | path = {s}\n", 
                .{ id, file.kind, file.parent, file.size, path }
            );
        }
        try std.testing.expectEqual(size, tree.list.items.len);
    }
};

pub const PlasticTree = struct {
    const Self = @This();

    pub const Entry = struct {
        name: []u8,
        depth: usize,
        parent: u64,
    };

    pub const Config = struct {
        size: usize = 1_000_000,
        max_dir_size: usize = 1_000,
        file_v_dir: f64 = 0.7,
        max_name_len: usize = 25,
    };

    arena_allocator: std.heap.ArenaAllocator,
    list: std.ArrayList(Entry),
    config: Config,

    pub fn init(config: Config, allocer: Allocator) !Self {
        var arena = std.heap.ArenaAllocator.init(allocer);
        errdefer arena.deinit();
        return Self {
            .list = try std.ArrayList(Entry).initCapacity(arena.allocator(), config.size),
            .arena_allocator = arena,
            .config = config,
        };
    }

    fn allocator(self: *Self) Allocator {
        return self.arena_allocator.allocator();
    }

    pub fn deinit(self: *Self) void {
        // for (self.list.items) |entry| {
        //     // self.allocator.free(@ptrCast([*]u8, entry.name.ptr));
        //     self.allocator.free(entry.name);
        // }
        // self.list.deinit();
        
        // deinit the arena allocator instead of...
        self.arena_allocator.deinit();
    }

    pub fn gen(self: *Self) !void {
        // the root node
        var name = try self.allocator().alloc(u8, 1);
        name[0] = '/';
        try self.list.append(Entry { .name = name, .depth = 0, .parent = 0 });

        // generate the rest
        try self.genDir(0, 1, self.config.size - 1);
    }

    fn genName(self: *Self) ![]u8 {
        var rng = std.crypto.random;

        // random length
        const len = 1 + rng.uintAtMost(usize, self.config.max_name_len - 1);
        var name = try self.allocator().alloc(u8, len);

        // random chars from ascii range
        for (name[0..]) |*char| {
            char.* = 32 + rng.uintAtMost(u8, 127 - 32);
            // remove prohibited characters
            // TODO: make this a config option
            if (char.* == '/'){
                char.* = '0';
            }
        }

        return name;
    }

    fn genDir(self: *Self, dir_id: u64, depth: usize, share: usize) Allocator.Error!void {
        var rng = std.crypto.random;

        const child_count = rng.uintAtMost(usize, std.math.min(share, self.config.max_dir_size));

        var remaining_share = share - child_count;
        var ii: usize = 0;
        while(ii < child_count): ({ ii += 1; }){
            const name = try self.genName();
            try self.list.append(Entry { .name = name, .depth = depth, .parent = dir_id });

            if(remaining_share > 0 and rng.float(f64) > self.config.file_v_dir){
                const sub_dir_share = @floatToInt(usize, rng.float(f64) * @intToFloat(f64, remaining_share));
                remaining_share -= sub_dir_share;
                try self.genDir(self.list.items.len - 1, depth + 1, sub_dir_share);
            }
        }
        if (remaining_share > 0){
            const name = try self.genName();
            try self.list.append(Entry { .name = name, .depth = depth, .parent = dir_id });
            try self.genDir(self.list.items.len - 1, depth + 1, remaining_share - 1);
        }
    }
    test "PlasticTree.usage" {
        // var allocator = std.heap.GeneralPurposeAllocator(.{}){};
        var list = try PlasticTree.init(.{ .size = 1_000 }, std.testing.allocator);
        defer list.deinit();

        try list.gen();
        if (false) {
            for (list.list.items[0..50]) |entry, id|{
                std.debug.print(
                    "id: {any} | depth: {any} | parent: {any} | name: {s}\n", 
                    .{ id, entry.depth, entry.parent, entry.name }
                );
            }
            var size = list.list.items.len * @sizeOf(PlasticTree.Entry);
            var max_depth: usize = 0;
            for (list.list.items) |entry|{
                size += entry.name.len;
                if (max_depth < entry.depth){
                    max_depth = entry.depth;
                }
            }
            std.debug.print("max depth = {}\n", .{ max_depth });
            std.debug.print("total bytes = {}\n", .{ size });
        }
    }
};
