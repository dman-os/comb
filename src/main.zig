const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const Packed = packed struct {
    a: u8,
    b: u32,
};

fn addFortyTwo(x: anytype) @TypeOf(x) {
    return x + 42;
}

pub fn main() anyerror!void {
    const p = Packed{ .a = 1, .b = 2 };
    std.debug.print("{}\n", .{@TypeOf(&p.b)});
    std.debug.print("{}\n", .{@sizeOf(@TypeOf(&p.b))});
    std.debug.print("{}\n", .{@sizeOf(@TypeOf(p))});
    std.debug.print("{}\n", .{addFortyTwo(10)});
    std.debug.print("{}\n", .{@TypeOf(@as([]const u8, "hellow!"))});
}

test {
    std.testing.refAllDecls(@This());
    _ = mod_gram;
    _ = mod_utils;
    _ = mod_plist;
    _ = PlasticTree;
    // _ = mod_tpool;
    _ = Tree;
}

const Tree = struct {
    fn walk(allocator: Allocator, path: []const u8, limit: ?usize) !Tree {
        var walker = Tree.Walker.init(allocator, limit orelse std.math.maxInt(usize));
        errdefer walker.deinit();
        const dev = blk: {
            const file = try std.fs.openFileAbsolute(path, .{});
            defer file.close();
            const meta = try file.metadata();
            // meta.inner.
            const statx = switch(builtin.target.os.tag){
                .linux => meta.inner.statx,
                else => unreachable,
            };
            break :blk (@as(u64, statx.dev_major) << 32 ) & @as(u64, statx.dev_minor);
        };
        try walker.scanDir(path, std.fs.cwd(), 0, 0, dev, );
        return walker.toTree();
    }

    const Entry = struct {
        pub const Kind = std.fs.File.Kind;
        name: []u8,
        kind: Kind,
        depth: usize,
        parent: u64,
    };
    list: std.ArrayList(Entry),
    allocator: Allocator,

    fn init(allocator: Allocator) !Tree {
        return Tree {
            .allocator = allocator,
            .list = std.ArrayList(Entry).init(allocator),
        };
    }

    fn deinit(self: *Tree) void {
        for (self.list.items) |entry| {
            self.allocator.free(entry.name);
        }
        self.list.deinit();
    }

    const FullPathWeaver = struct {
        const NameOfErr = error { NotFound };
        buf: std.ArrayListUnmanaged(u8),

        fn init() FullPathWeaver {
            return FullPathWeaver {
                .buf = std.ArrayListUnmanaged(u8){},
            };
        }
        fn deinit(self: *FullPathWeaver, allocator: Allocator) void {
            self.buf.deinit(allocator);
        }
        /// The returned slice is invalidated not long after.
        fn pathOf(self: *FullPathWeaver, allocator: Allocator, tree: Tree, id: usize, delimiter: u8) ![]const u8 {
            self.buf.clearAndFree(allocator);
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

        log_interval: usize = 10_000,

        pub const Error = 
            Allocator.Error || 
            std.fs.File.OpenError || 
            std.fs.File.MetadataError || 
            std.fs.Dir.Iterator.Error ||
            std.os.UnexpectedError;

        fn init(allocator: Allocator, limit: usize) @This() {
            return @This() {
                .remaining = limit,
                .tree = try Tree.init(allocator),
                .weaver = FullPathWeaver.init(),
            };
        }

        fn deinit(self: *@This()) void{
            self.tree.deinit();
            self.weaver.deinit(self.tree.allocator);
        }

        fn toTree(self: *@This()) Tree {
            self.weaver.deinit(self.tree.allocator);
            return self.tree;
        }

        fn append(self: *Walker, entry: Entry) !void {
            try self.tree.list.append(entry);
            self.remaining -= 1;
            if (self.remaining % self.log_interval == 0) {
                const path = try self.weaver.pathOf(
                    self.tree.allocator, 
                    self.tree,
                    self.tree.list.items.len - 1, 
                    '/'
                );
                std.debug.print(
                // std.log.info(
                    "scanned {} items, now on: {s}\n", 
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
            parent_dev: u64
        ) Error!void {
            // std.debug.print("error happened at path = {s}\n", .{path});
            var dir = parent.openDir(path,.{ .iterate = true, .no_follow = true }) catch |err| {
                switch(err){
                    std.fs.Dir.OpenError.AccessDenied => {
                        const parent_path = try self.weaver.pathOf(
                            self.tree.allocator, 
                            self.tree,
                            parent_id, 
                            '/'
                        );
                        std.debug.print(
                        // std.log.info(
                            "AccessDenied opening dir {s}/{s}\n", .{ parent_path, path, });
                        return;
                    },
                    else => return err,
                }
            };
            defer dir.close();

            try self.append(Entry {
                .name = try self.tree.allocator.dupe(u8, path),
                .kind = .Directory,
                .depth = depth,
                .parent = parent_id,
            });

            const dev = blk: {
                const meta = try dir.metadata();
                // @compileLog(builtin.target.os.tag);
                const statx = switch(builtin.target.os.tag) {
                    .linux => meta.inner.statx,
                    else => unreachable,
                };
                break :blk (@as(u64, statx.dev_major) << 32 ) & @as(u64, statx.dev_minor);
            };
            // we don't examine other devices
            if (dev != parent_dev) return;
            const dir_id = self.tree.list.items.len - 1;

            var it = dir.iterate();
            while (self.remaining > 0) {
                // handle the error first
                if (it.next()) |next| {
                    // check if there's a file left in the dir
                    const entry = next orelse break;

                    if (entry.kind == .Directory){
                        try self.scanDir(entry.name, dir, dir_id, depth + 1, dev);
                    }else {
                        // const file = try std.fs.openFileAbsolute(entry.name, .{});
                        // defer file.close();
                        // const stat = try file.stat();
                        // const meta = try file.metadata();
                        try self.append(Entry {
                            .name = try self.tree.allocator.dupe(u8, entry.name),
                            .kind = entry.kind,
                            .depth = depth,
                            .parent = parent_id,
                        });
                    }
                } else |err| switch (err) {
                    std.fs.Dir.Iterator.Error.AccessDenied => {
                        const parent_path = try self.weaver.pathOf(
                            self.tree.allocator, 
                            self.tree,
                            parent_id, 
                            '/'
                        );
                        std.debug.print(
                        // std.log.info(
                            "AccessDenied on iteration for dir at: {s}/{s}\n", .{ parent_path, path, });
                    },
                    else => return err,
                }
            }
        }
    };

    test "zwalk" {
        const size: usize = 10_000;
        var allocator = std.testing.allocator;
        var tree = try walk(allocator, "/", size);
        defer tree.deinit();
        var weaver = FullPathWeaver.init();
        defer weaver.deinit(allocator);
        for (tree.list.items) |file, id| {
            const path = try weaver.pathOf(allocator, tree, id, '/');
            std.debug.print(
                "{} | kind = {} | parent = {} | path = {s}\n", 
                .{ id, file.kind, file.parent, path }
            );
        }
        try std.testing.expectEqual(size, tree.list.items.len);
    }
};

test "plist.bench.walk" {
    std.testing.log_level = std.log.Level.debug;
    const size: usize = 1_000_000;

    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();

    var allocator = arena.allocator();
    var tree = try Tree.walk(allocator, "/", size);
    // defer tree.deinit();

    var plist = mod_plist.PostingListUnmanaged(usize, 3).init();
    // defer plist.deinit(allocator);

    for (tree.list.items) |entry, id|{
        try plist.insert(allocator, id, entry.name, std.ascii.spaces[0..]);
    }

    const iterations = 50;

    var avg_elapsed: u64 = undefined;
    var low_bound: u64 = undefined;
    var up_bound: u64 = undefined;

    {
        var timer = try std.time.Timer.start();
        _ = try plist.str_match(allocator, "this_file_cannot_exist_1234567890e456789", std.ascii.spaces[0..]);
        // defer vec.deinit(allocator);
        const elapsed = timer.read();
        avg_elapsed = elapsed;
        low_bound = elapsed;
        up_bound = elapsed;
    }
    var ii: usize = 0;
    while (ii < (iterations - 1)) : ({ ii += 1; }){
        var timer = try std.time.Timer.start();
        _ = try plist.str_match(allocator, "this_file_cannot_exist_1234567890e456789", std.ascii.spaces[0..]);
        // defer vec.deinit(allocator);

        const elapsed = timer.read();
        avg_elapsed = @divTrunc(elapsed + avg_elapsed, 2);
        low_bound = if (elapsed < low_bound) elapsed else low_bound;
        up_bound = if (elapsed > up_bound) elapsed else up_bound;
    }
    std.debug.print(
        "[{}ns; {}ns; {}ns] on str_match for {} iterations\n", 
        .{ 
            @divFloor(low_bound, 1), 
            @divFloor(avg_elapsed, 1), 
            @divFloor(up_bound, 1), 
            iterations 
        }
    );
}

const PlasticTree = struct {
    const Self = @This();

    const Entry = struct {
        name: []u8,
        depth: usize,
        parent: u64,
    };

    const Config = struct {
        size: usize = 1_000_000,
        max_dir_size: usize = 1_000,
        file_v_dir: f64 = 0.7,
    };

    arena_allocator: std.heap.ArenaAllocator,
    list: std.ArrayList(Entry),
    config: Config,

    fn init(config: Config, allocer: Allocator) !Self {
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

    fn deinit(self: *Self) void {
        // for (self.list.items) |entry| {
        //     // self.allocator.free(@ptrCast([*]u8, entry.name.ptr));
        //     self.allocator.free(entry.name);
        // }
        // self.list.deinit();
        
        // deinit the arena allocator instead of...
        self.arena_allocator.deinit();
    }

    fn gen(self: *Self) !void {
        // the root node
        var name = try self.allocator().alloc(u8, 1);
        name[0] = '/';
        try self.list.append(Entry { .name = name, .depth = 0, .parent = 0 });

        // generate the rest
        try self.gen_dir(0, 1, self.config.size - 1);
    }

    fn gen_name(self: *Self) ![]u8 {
        var rng = std.crypto.random;

        // random length
        const len = 1 + rng.uintAtMost(usize, 31);
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

    fn gen_dir(self: *Self, dir_id: u64, depth: usize, share: usize) Allocator.Error!void {
        var rng = std.crypto.random;

        const child_count = rng.uintAtMost(usize, std.math.min(share, self.config.max_dir_size));

        var remaining_share = share - child_count;
        var ii: usize = 0;
        while(ii < child_count): ({ ii += 1; }){
            const name = try self.gen_name();
            try self.list.append(Entry { .name = name, .depth = depth, .parent = dir_id });

            if(remaining_share > 0 and rng.float(f64) > self.config.file_v_dir){
                const sub_dir_share = @floatToInt(usize, rng.float(f64) * @intToFloat(f64, remaining_share));
                remaining_share -= sub_dir_share;
                try self.gen_dir(self.list.items.len - 1, depth + 1, sub_dir_share);
            }
        }
        if (remaining_share > 0){
            const name = try self.gen_name();
            try self.list.append(Entry { .name = name, .depth = depth, .parent = dir_id });
            try self.gen_dir(self.list.items.len - 1, depth + 1, remaining_share - 1);
        }
    }
    test "plastic_tree" {
        // var allocator = std.heap.GeneralPurposeAllocator(.{}){};
        var list = try PlasticTree.init(.{ .size = 1_000 }, std.testing.allocator);
        defer list.deinit();

        try list.gen();
        // for (list.list.items[0..50]) |entry, id|{
        //     std.debug.print(
        //         "id: {any} | depth: {any} | parent: {any} | name: {s}\n", 
        //         .{ id, entry.depth, entry.parent, entry.name }
        //     );
        // }
        // var size = list.list.items.len * @sizeOf(PlasticTree.Entry);
        // var max_depth: usize = 0;
        // for (list.list.items) |entry|{
        //     size += entry.name.len;
        //     if (max_depth < entry.depth){
        //         max_depth = entry.depth;
        //     }
        // }
        // std.debug.print("max depth = {}\n", .{ max_depth });
        // std.debug.print("total bytes = {}\n", .{ size });
    }
};


test "plist.bench.gen" {
    var tree = try PlasticTree.init(.{ .size = 1_000_000 }, std.testing.allocator);
    defer tree.deinit();
    var allocator = tree.allocator();

    try tree.gen();

    var plist = mod_plist.PostingListUnmanaged(usize, 3).init();
    // defer plist.deinit(allocator);

    for (tree.list.items) |entry, id|{
        try plist.insert(allocator, id, entry.name, std.ascii.spaces[0..]);
    }

    const iterations = 50;

    var avg_elapsed: u64 = undefined;
    var low_bound: u64 = undefined;
    var up_bound: u64 = undefined;

    {
        var timer = try std.time.Timer.start();
        _ = try plist.str_match(allocator, "this_file_cannot_exist_1234567890e456789", std.ascii.spaces[0..]);
        // defer vec.deinit(allocator);
        const elapsed = timer.read();
        avg_elapsed = elapsed;
        low_bound = elapsed;
        up_bound = elapsed;
    }
    var ii: usize = 0;
    while (ii < (iterations - 1)) : ({ ii += 1; }){
        var timer = try std.time.Timer.start();
        _ = try plist.str_match(allocator, "this_file_cannot_exist_1234567890e456789", std.ascii.spaces[0..]);
        // defer vec.deinit(allocator);

        const elapsed = timer.read();
        avg_elapsed = @divTrunc(elapsed + avg_elapsed, 2);
        low_bound = if (elapsed < low_bound) elapsed else low_bound;
        up_bound = if (elapsed > up_bound) elapsed else up_bound;
    }
    std.debug.print(
        "[{}ns; {}ns; {}ns] on str_match for {} iterations\n", 
        .{ 
            @divFloor(low_bound, 1), 
            @divFloor(avg_elapsed, 1), 
            @divFloor(up_bound, 1), 
            iterations 
        }
    );
}

const mod_plist = struct {
    fn PostingListUnmanaged(comptime I: type, comptime gram_len: u4) type {
        if (gram_len == 0) {
            @compileError("gram_len is 0");
        }
        return struct {
            const Self = @This();
            const Gram = mod_gram.Gram(gram_len);
            const GramPos = mod_gram.GramPos(gram_len);
            const GramRef = struct {
                id: I,
                pos: usize,
            };
            map: std.AutoHashMapUnmanaged(Gram, std.ArrayListUnmanaged(GramRef)),
            cache: std.AutoHashMapUnmanaged(GramPos, void),

            fn init() Self {
                return Self{
                    .map = std.AutoHashMapUnmanaged(Gram, std.ArrayListUnmanaged(GramRef)){},
                    .cache = std.AutoHashMapUnmanaged(GramPos, void){},
                };
            }

            fn deinit(self: *Self, allocator: Allocator) void {
                self.cache.deinit(allocator);
                var it = self.map.valueIterator();
                while (it.next()) |list| {
                    list.deinit(allocator);
                }
                self.map.deinit(allocator);
            }

            fn insert(self: *Self, allocator: Allocator, id: I, name: []const u8, delimiters: []const u8) !void {
                self.cache.clearAndFree(allocator);

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
                            entry.value_ptr.* = std.ArrayListUnmanaged(GramRef){};
                            break :blk entry.value_ptr;
                        }
                    };
                    try list.append(allocator, .{ .id = id, .pos = gpos.pos });
                }
            }

            /// Be sure to `deinit` the result with the allocater given here.
            /// FIXME: optimize
            fn str_match(self: *Self, allocator: Allocator, string: []const u8, delimiters: []const u8) !std.ArrayListUnmanaged(I) {
                // self.cache.clearAndFree(allocator);
                var check = std.AutoHashMap(I, void).init(allocator);
                defer check.deinit();
                var out_vec = std.ArrayList(I).init(allocator);
                defer out_vec.deinit();

                var grams = std.ArrayList(GramPos).init(allocator);
                defer grams.deinit();
                try mod_gram.grammer(gram_len, string, false, delimiters, mod_utils.Appender(GramPos).new(&grams, std.ArrayList(GramPos).append));

                var is_init = false;
                for (grams.items) |gpos| {
                    const gram = gpos.gram;
                    if (self.map.get(gram)) |list| {
                        if (is_init) {
                            check.clearAndFree();
                            for (out_vec.items) |id| {
                                try check.put(id, {});
                            }
                            out_vec.clearAndFree();

                            for (list.items) |ref| {
                                if (check.contains(ref.id)) {
                                    try out_vec.append(ref.id);
                                }
                            }
                        } else {
                            for (list.items) |ref| {
                                try out_vec.append(ref.id);
                            }
                            is_init = true;
                        }
                    }
                }
                return out_vec.moveToUnmanaged();
            }
        };
    }
    test "plist.str_match" {
        comptime var table = .{
            .{ 
                .name = "single_gram", 
                .items = ([_][]const u8{ "Bilbo Baggins", "Frodo Baggins", "Bagend", "Thorin Oakenshield" })[0..], 
                .query = "Bag", 
                .expected = &.{ 0, 1, 2 } 
            },
            .{ 
                .name = "multi_gram", 
                .items = ([_][]const u8{ "Bilbo Baggins", "Frodo Baggins", "Bagend" })[0..], 
                .query = "Bagend", 
                .expected = &.{2} 
            },
            .{ 
                .name = "boundary_actual.1", 
                .items = ([_][]const u8{ "Gandalf", "Sauron", "Galandriel" })[0..], 
                .query = "Ga", 
                .expected = &.{0, 2} 
            },
            .{ 
                .name = "boundary_actual.2", 
                .items = ([_][]const u8{ "Gandalf", "Sauron", "Galandriel" })[0..], 
                .query = "Sau", 
                .expected = &.{1} 
            },
            .{ 
                .name = "boundary_delimter", 
                .items = ([_][]const u8{ " Gandalf", " Sauron", " Lady\nGalandriel" })[0..], 
                .query = "Ga", 
                .expected = &.{0, 2} 
            },
        };
        inline for (table) |case| {
            var plist = PostingListUnmanaged(u64, 3).init();
            defer plist.deinit(std.testing.allocator);
            for (case.items) |name, id| {
                try plist.insert(std.testing.allocator, @as(u64, id), name, std.ascii.spaces[0..]);
            }
            var res = try plist.str_match(std.testing.allocator, case.query, std.ascii.spaces[0..]);
            defer res.deinit(std.testing.allocator);
            std.testing.expectEqualSlices(u64, case.expected, res.items) catch |err| {
                std.debug.print("{s}\n{}\n!=\n{any}", .{ case.name, case.expected, res.items });
                return err;
            };
        }
    }
};

const mod_gram = struct {
    pub const TEC: u8 = 0;
    pub const Appender = mod_utils.Appender;

    pub fn Gram(comptime gram_len: u4) type {
        return [gram_len]u8;
    }

    pub fn GramPos(comptime gram_len: u4) type {
        return struct {
            const Self = @This();
            pos: usize,
            gram: [gram_len]u8,

            fn new(pos: usize, gram: [gram_len]u8) Self {
                return Self{ .pos = pos, .gram = gram };
            }
        };
    }

    /// This will tokenize the string before gramming it according to the provided delimiter.
    /// For example, provide std.ascii.spaces to tokenize using whitespace.
    pub fn grammer(comptime gram_len: u4, string: []const u8, boundary_grams: bool, delimiters: []const u8, out: Appender(GramPos(gram_len))) !void {
        if (gram_len == 0) {
            @compileError("gram_len is 0");
        }
        if (delimiters.len > 0) {
            var iter = std.mem.tokenize(u8, string, delimiters);
            while (iter.next()) |token| {
                try token_grammer(gram_len, token, @ptrToInt(token.ptr) - @ptrToInt(string.ptr), boundary_grams, out);
            }
        } else {
            try token_grammer(gram_len, string, 0, boundary_grams, out);
        }
    }
    fn token_grammer(comptime gram_len: u4, string: []const u8, offset: usize, boundary_grams: bool, out: Appender(GramPos(gram_len))) !void {
        if (gram_len == 0) {
            @compileError("gram_len is 0");
        }
        if (gram_len <= string.len) {
            if (boundary_grams) {
                try left_boundaries(gram_len, string, offset, out);
            }
            var pos: usize = 0;
            while (pos + gram_len <= string.len) : ({
                pos += 1;
            }) {
                var gram: [gram_len]u8 = undefined;
                comptime var ii = 0;
                inline while (ii < gram_len) : ({
                    ii += 1;
                }) {
                    gram[ii] = string[pos + ii];
                }
                try out.append(GramPos(gram_len).new(offset + pos, gram));
            }
            if (boundary_grams) {
                try right_boundaries(gram_len, string, offset, out);
            }
        } else {
            // left boundaries
            // we can't use the fn `left_boundaries`, which's partially comtime
            // because we're constrained by the string.len now (which is shorter
            // than gram_len)
            if (boundary_grams) {
                var ii: usize = 1;
                // we'll emit it the `string.len - 1` times
                while (ii < string.len) : ({
                    ii += 1;
                }) {
                    // var gram = [1]u8{TEC} ** gram_len;
                    // var gram_ii = gram_len - ii;
                    // var str_ii = 0;
                    // while (gram_ii < gram_len) : ({
                    //     gram_ii += 1;
                    //     str_ii += 1;
                    // }) {
                    //     gram[gram_ii] = string[str_ii];
                    // }
                    const gram = fill_gram(gram_len, string[0..ii], gram_len - ii);
                    try out.append(GramPos(gram_len).new(offset, gram));
                }
            }
            // fill it in from the right
            // i.e TECs on the left
            // this isn't a boundary since the string.len is shorter than gram_len
            {
                const gram = fill_gram(gram_len, string, gram_len - string.len);
                try out.append(GramPos(gram_len).new(offset, gram));
            }
            // if it's short enough to have TECs on both side
            if (boundary_grams) {
                var ii = gram_len - string.len - 1;
                while (ii > 0) : ({
                    ii -= 1;
                }) {
                    const gram = fill_gram(gram_len, string, ii);
                    try out.append(GramPos(gram_len).new(offset, gram));
                }
            }
            // fill it in from the left
            // i.e. TECS on the right
            {
                const gram = fill_gram(gram_len, string, 0);
                try out.append(GramPos(gram_len).new(offset, gram));
            }
            // right boundaries
            if (boundary_grams) {
                var start: usize = 1;
                // we'll emit it the `string.len - 1` times
                while (start < string.len) : ({
                    start += 1;
                }) {
                    const gram = fill_gram(gram_len, string[start..], 0);
                    try out.append(GramPos(gram_len).new(offset + start, gram));
                }
            }
        }
    }
    /// Panics if gram_len - start > string.len
    inline fn fill_gram(comptime gram_len: u4, string: []const u8, start: usize) Gram(gram_len) {
        var gram = [1]u8{TEC} ** gram_len;
        for (string) |char, ii| {
            gram[start + ii] = char;
        }
        // var ii = start;
        // while(ii < gram_len): ({ ii += 1; }){
        //     gram[ii] = string[ii - start];
        // }
        return gram;
    }
    inline fn left_boundaries(comptime gram_len: u4, string: []const u8, offset: usize, out: Appender(GramPos(gram_len))) !void {
        // the following code will do something similar to what's shown below but for any gram_len
        // the commented out example is how it'd look if gram_len is 3
        // -- out.append(GramPos(2).new(0, .{ TEC, TEC, str[pos] }));
        // -- out.append(GramPos(2).new(0, .{ TEC, str[pos], str[pos + 1] }));

        // append `gram_len - 1` times where each gram is full of TEC
        // this won't enter if gram_len == 1. One length grams can't have boundary grams
        comptime var fill_count = 1; // i.e. the chars that are not TEC
        inline while (fill_count < gram_len) : ({
            fill_count += 1;
        }) {
            // create completely empty gram
            var gram = [1]u8{TEC} ** gram_len;
            // we start at an earlier index each iteration
            // meaning, we progressively fill more of the last few gram positions
            // from the string each iter
            comptime var gram_ii = gram_len - fill_count;
            comptime var str_ii = 0;
            inline while (gram_ii < gram_len) : ({
                gram_ii += 1;
                str_ii += 1;
            }) {
                gram[gram_ii] = string[str_ii];
            }
            try out.append(GramPos(gram_len).new(offset, gram));
        }
    }
    inline fn right_boundaries(comptime gram_len: u4, string: []const u8, offset: usize, out: Appender(GramPos(gram_len))) !void {
        const pos = string.len - gram_len;
        // the following code will do something similar to what's shown below but for any gram_len
        // the commented out example is how it'd look if gram_len is 3
        // -- out.append(GramPos(2).new(pos + 1, .{ string[pos + 1], string[pos + 2], TEC }));
        // -- out.append(GramPos(2).new(pos + 2, .{ string[pos + 2], TEC, TEC }));

        // similar to the right boundaries gram. Read those comments
        comptime var tec_count = 1;
        inline while (tec_count < gram_len) : ({
            tec_count += 1;
        }) {
            var gram = [1]u8{TEC} ** gram_len;
            // this time, we fill progressively less from the string
            comptime var gram_ii = 0;
            inline while (gram_ii < (gram_len - tec_count)) : ({
                gram_ii += 1;
            }) {
                gram[gram_ii] = string[pos + tec_count + gram_ii];
            }
            try out.append(GramPos(gram_len).new(offset + pos + tec_count, gram));
        }
    }

    test "grammer.trigram" {
        // const Case = std.meta.Tuple(.{ []const u8, std.meta.Tuple(.{[]const u8, bool, []Gram(3)}) });
        comptime var table = .{
            .{ .name = "boundaries.3", .string = "etc", .boundary_grams = true, .expected = &.{
                GramPos(3).new(0, .{ TEC, TEC, 'e' }),
                GramPos(3).new(0, .{ TEC, 'e', 't' }),
                GramPos(3).new(0, .{ 'e', 't', 'c' }),
                GramPos(3).new(1, .{ 't', 'c', TEC }),
                GramPos(3).new(2, .{ 'c', TEC, TEC }),
            } },
            .{ .name = "no_boundaries.3", .string = "etc", .boundary_grams = false, .expected = &.{
                GramPos(3).new(0, .{ 'e', 't', 'c' }),
            } },
            .{ .name = "boundaries.2", .string = ".h", .boundary_grams = true, .expected = &.{
                GramPos(3).new(0, .{ TEC, TEC, '.' }),
                GramPos(3).new(0, .{ TEC, '.', 'h' }),
                GramPos(3).new(0, .{ '.', 'h', TEC }),
                GramPos(3).new(1, .{ 'h', TEC, TEC }),
            } },
            .{ .name = "no_boundaries.2", .string = ".h", .boundary_grams = false, .expected = &.{
                GramPos(3).new(0, .{ TEC, '.', 'h' }),
                GramPos(3).new(0, .{ '.', 'h', TEC }),
            } },
            .{ .name = "boundaries.1", .string = "h", .boundary_grams = true, .expected = &.{
                GramPos(3).new(0, .{ TEC, TEC, 'h' }),
                GramPos(3).new(0, .{ TEC, 'h', TEC }),
                GramPos(3).new(0, .{ 'h', TEC, TEC }),
            } },
            .{ .name = "no_boundaries.1", .string = "h", .boundary_grams = false, .expected = &.{
                GramPos(3).new(0, .{ TEC, TEC, 'h' }),
                GramPos(3).new(0, .{ 'h', TEC, TEC }),
            } },
            .{ .name = "boundaries.multi", .string = "homeuser", .boundary_grams = true, .expected = &.{
                GramPos(3).new(0, .{ TEC, TEC, 'h' }),
                GramPos(3).new(0, .{ TEC, 'h', 'o' }),
                GramPos(3).new(0, .{ 'h', 'o', 'm' }),
                GramPos(3).new(1, .{ 'o', 'm', 'e' }),
                GramPos(3).new(2, .{ 'm', 'e', 'u' }),
                GramPos(3).new(3, .{ 'e', 'u', 's' }),
                GramPos(3).new(4, .{ 'u', 's', 'e' }),
                GramPos(3).new(5, .{ 's', 'e', 'r' }),
                GramPos(3).new(6, .{ 'e', 'r', TEC }),
                GramPos(3).new(7, .{ 'r', TEC, TEC }),
            } },
            .{ .name = "no_boundaries.multi", .string = "homeuser", .boundary_grams = false, .expected = &.{
                GramPos(3).new(0, .{ 'h', 'o', 'm' }),
                GramPos(3).new(1, .{ 'o', 'm', 'e' }),
                GramPos(3).new(2, .{ 'm', 'e', 'u' }),
                GramPos(3).new(3, .{ 'e', 'u', 's' }),
                GramPos(3).new(4, .{ 'u', 's', 'e' }),
                GramPos(3).new(5, .{ 's', 'e', 'r' }),
            } },
            .{ .name = "whitespace_is_boundary.1", .string = " home user", .boundary_grams = true, .expected = &.{
                GramPos(3).new(1, .{ TEC, TEC, 'h' }),
                GramPos(3).new(1, .{ TEC, 'h', 'o' }),
                GramPos(3).new(1, .{ 'h', 'o', 'm' }),
                GramPos(3).new(2, .{ 'o', 'm', 'e' }),
                GramPos(3).new(3, .{ 'm', 'e', TEC }),
                GramPos(3).new(4, .{ 'e', TEC, TEC }),
                GramPos(3).new(6, .{ TEC, TEC, 'u' }),
                GramPos(3).new(6, .{ TEC, 'u', 's' }),
                GramPos(3).new(6, .{ 'u', 's', 'e' }),
                GramPos(3).new(7, .{ 's', 'e', 'r' }),
                GramPos(3).new(8, .{ 'e', 'r', TEC }),
                GramPos(3).new(9, .{ 'r', TEC, TEC }),
            } },
            .{ .name = "whitespace_is_boundary.2", .string = " home   user", .boundary_grams = true, .expected = &.{
                GramPos(3).new(1, .{ TEC, TEC, 'h' }),
                GramPos(3).new(1, .{ TEC, 'h', 'o' }),
                GramPos(3).new(1, .{ 'h', 'o', 'm' }),
                GramPos(3).new(2, .{ 'o', 'm', 'e' }),
                GramPos(3).new(3, .{ 'm', 'e', TEC }),
                GramPos(3).new(4, .{ 'e', TEC, TEC }),
                GramPos(3).new(8, .{ TEC, TEC, 'u' }),
                GramPos(3).new(8, .{ TEC, 'u', 's' }),
                GramPos(3).new(8, .{ 'u', 's', 'e' }),
                GramPos(3).new(9, .{ 's', 'e', 'r' }),
                GramPos(3).new(10, .{ 'e', 'r', TEC }),
                GramPos(3).new(11, .{ 'r', TEC, TEC }),
            } },
            .{ .name = "whitespace_is_boundary.3", .string = " home\tuser", .boundary_grams = true, .expected = &.{
                GramPos(3).new(1, .{ TEC, TEC, 'h' }),
                GramPos(3).new(1, .{ TEC, 'h', 'o' }),
                GramPos(3).new(1, .{ 'h', 'o', 'm' }),
                GramPos(3).new(2, .{ 'o', 'm', 'e' }),
                GramPos(3).new(3, .{ 'm', 'e', TEC }),
                GramPos(3).new(4, .{ 'e', TEC, TEC }),
                GramPos(3).new(6, .{ TEC, TEC, 'u' }),
                GramPos(3).new(6, .{ TEC, 'u', 's' }),
                GramPos(3).new(6, .{ 'u', 's', 'e' }),
                GramPos(3).new(7, .{ 's', 'e', 'r' }),
                GramPos(3).new(8, .{ 'e', 'r', TEC }),
                GramPos(3).new(9, .{ 'r', TEC, TEC }),
            } },
            .{ .name = "pure_delimiter.1", .string = " ", .boundary_grams = true, .expected = &.{} },
            .{ .name = "pure_delimiter.2", .string = "    ", .boundary_grams = true, .expected = &.{} },
            .{ .name = "pure_delimiter.3", .string = "", .boundary_grams = true, .expected = &.{} },
        };
        inline for (table) |case| {
            var list = std.ArrayList(GramPos(3)).init(std.testing.allocator);
            defer list.deinit();
            try grammer(3, case.string, case.boundary_grams, &std.ascii.spaces, Appender(GramPos(3)).new(&list, std.ArrayList(GramPos(3)).append));
            std.testing.expectEqualSlices(GramPos(3), case.expected, list.items) catch |err| {
                std.debug.print("\nerror on {s}\n{s}\n !=\n {s}\n", .{ case.name, case.expected, list.items });
                return err;
            };
        }
    }
    test "grammer.quadgram" {
        // const Case = std.meta.Tuple(.{ []const u8, std.meta.Tuple(.{[]const u8, bool, []Gram(3)}) });
        comptime var table = .{
            .{ .name = "boundaries.4", .string = "root", .boundary_grams = true, .expected = &.{
                GramPos(4).new(0, .{ TEC, TEC, TEC, 'r' }),
                GramPos(4).new(0, .{ TEC, TEC, 'r', 'o' }),
                GramPos(4).new(0, .{ TEC, 'r', 'o', 'o' }),
                GramPos(4).new(0, .{ 'r', 'o', 'o', 't' }),
                GramPos(4).new(1, .{ 'o', 'o', 't', TEC }),
                GramPos(4).new(2, .{ 'o', 't', TEC, TEC }),
                GramPos(4).new(3, .{ 't', TEC, TEC, TEC }),
            } },
            .{ .name = "no_boundaries.4", .string = "root", .boundary_grams = false, .expected = &.{
                GramPos(4).new(0, .{ 'r', 'o', 'o', 't' }),
            } },
            .{ .name = "boundaries.3", .string = "etc", .boundary_grams = true, .expected = &.{
                GramPos(4).new(0, .{ TEC, TEC, TEC, 'e' }),
                GramPos(4).new(0, .{ TEC, TEC, 'e', 't' }),
                GramPos(4).new(0, .{ TEC, 'e', 't', 'c' }),
                GramPos(4).new(0, .{ 'e', 't', 'c', TEC }),
                GramPos(4).new(1, .{ 't', 'c', TEC, TEC }),
                GramPos(4).new(2, .{ 'c', TEC, TEC, TEC }),
            } },
            .{ .name = "no_boundaries.3", .string = "etc", .boundary_grams = false, .expected = &.{
                GramPos(4).new(0, .{ TEC, 'e', 't', 'c' }),
                GramPos(4).new(0, .{ 'e', 't', 'c', TEC }),
            } },
            .{ .name = "boundaries.2", .string = ".h", .boundary_grams = true, .expected = &.{
                GramPos(4).new(0, .{ TEC, TEC, TEC, '.' }),
                GramPos(4).new(0, .{ TEC, TEC, '.', 'h' }),
                GramPos(4).new(0, .{ TEC, '.', 'h', TEC }),
                GramPos(4).new(0, .{ '.', 'h', TEC, TEC }),
                GramPos(4).new(1, .{ 'h', TEC, TEC, TEC }),
            } },
            .{ .name = "no_boundaries.2", .string = ".h", .boundary_grams = false, .expected = &.{
                GramPos(4).new(0, .{ TEC, TEC, '.', 'h' }),
                GramPos(4).new(0, .{ '.', 'h', TEC, TEC }),
            } },
            .{ .name = "boundaries.1", .string = "h", .boundary_grams = true, .expected = &.{
                GramPos(4).new(0, .{ TEC, TEC, TEC, 'h' }),
                GramPos(4).new(0, .{ TEC, TEC, 'h', TEC }),
                GramPos(4).new(0, .{ TEC, 'h', TEC, TEC }),
                GramPos(4).new(0, .{ 'h', TEC, TEC, TEC }),
            } },
            .{ .name = "no_boundaries.1", .string = "h", .boundary_grams = false, .expected = &.{
                GramPos(4).new(0, .{ TEC, TEC, TEC, 'h' }),
                GramPos(4).new(0, .{ 'h', TEC, TEC, TEC }),
            } },
            .{ .name = "boundaries.multi", .string = "homeuser", .boundary_grams = true, .expected = &.{
                GramPos(4).new(0, .{ TEC, TEC, TEC, 'h' }),
                GramPos(4).new(0, .{ TEC, TEC, 'h', 'o' }),
                GramPos(4).new(0, .{ TEC, 'h', 'o', 'm' }),
                GramPos(4).new(0, .{ 'h', 'o', 'm', 'e' }),
                GramPos(4).new(1, .{ 'o', 'm', 'e', 'u' }),
                GramPos(4).new(2, .{ 'm', 'e', 'u', 's' }),
                GramPos(4).new(3, .{ 'e', 'u', 's', 'e' }),
                GramPos(4).new(4, .{ 'u', 's', 'e', 'r' }),
                GramPos(4).new(5, .{ 's', 'e', 'r', TEC }),
                GramPos(4).new(6, .{ 'e', 'r', TEC, TEC }),
                GramPos(4).new(7, .{ 'r', TEC, TEC, TEC }),
            } },
            .{ .name = "no_boundaries.multi", .string = "homeuser", .boundary_grams = false, .expected = &.{
                GramPos(4).new(0, .{ 'h', 'o', 'm', 'e' }),
                GramPos(4).new(1, .{ 'o', 'm', 'e', 'u' }),
                GramPos(4).new(2, .{ 'm', 'e', 'u', 's' }),
                GramPos(4).new(3, .{ 'e', 'u', 's', 'e' }),
                GramPos(4).new(4, .{ 'u', 's', 'e', 'r' }),
            } },
            .{ .name = "whitespace_is_boundary.1", .string = " home user", .boundary_grams = true, .expected = &.{
                GramPos(4).new(1, .{ TEC, TEC, TEC, 'h' }),
                GramPos(4).new(1, .{ TEC, TEC, 'h', 'o' }),
                GramPos(4).new(1, .{ TEC, 'h', 'o', 'm' }),
                GramPos(4).new(1, .{ 'h', 'o', 'm', 'e' }),
                GramPos(4).new(2, .{ 'o', 'm', 'e', TEC }),
                GramPos(4).new(3, .{ 'm', 'e', TEC, TEC }),
                GramPos(4).new(4, .{ 'e', TEC, TEC, TEC }),
                GramPos(4).new(6, .{ TEC, TEC, TEC, 'u' }),
                GramPos(4).new(6, .{ TEC, TEC, 'u', 's' }),
                GramPos(4).new(6, .{ TEC, 'u', 's', 'e' }),
                GramPos(4).new(6, .{ 'u', 's', 'e', 'r' }),
                GramPos(4).new(7, .{ 's', 'e', 'r', TEC }),
                GramPos(4).new(8, .{ 'e', 'r', TEC, TEC }),
                GramPos(4).new(9, .{ 'r', TEC, TEC, TEC }),
            } },
        };
        inline for (table) |case| {
            var arr = std.ArrayList(GramPos(4)).init(std.testing.allocator);
            defer arr.deinit();
            try grammer(4, case.string, case.boundary_grams, &std.ascii.spaces, Appender(GramPos(4)).new(&arr, std.ArrayList(GramPos(4)).append));
            std.testing.expectEqualSlices(GramPos(4), case.expected, arr.items) catch |err| {
                std.debug.print("\nerror on {s}\n{s}\n !=\n {s}\n", .{ case.name, case.expected, arr.items });
                return err;
            };
        }
    }
};

const mod_utils = struct {
    fn Appender(comptime T: type) type {
        return struct {
            const Self = @This();
            const Err = Allocator.Error;

            vptr: *anyopaque,
            func: fn (*anyopaque, T) Err!void,

            fn new(coll: anytype, func: fn (@TypeOf(coll), T) Err!void) Self {
                if (!comptime std.meta.trait.isSingleItemPtr(@TypeOf(coll))) {
                    @compileError("was expecting single item pointer, got type = " ++ @typeName(@TypeOf(coll)));
                }
                return Self{
                    .vptr = @ptrCast(*anyopaque, coll),
                    .func = @ptrCast(fn (*anyopaque, T) Err!void, func),
                };
            }

            fn append(self: Self, item: T) Err!void {
                try self.func(self.vptr, item);
            }
        };
    }

    test "appender.list" {
        var list = std.ArrayList(u32).init(std.testing.allocator);
        defer list.deinit();
        var appender = Appender(u32).new(&list, std.ArrayList(u32).append);
        try appender.append(10);
        try appender.append(20);
        try appender.append(30);
        try appender.append(40);
        try std.testing.expectEqualSlices(u32, ([_]u32{ 10, 20, 30, 40 })[0..], list.items);
    }

    test "appender.set" {
        var set = std.AutoHashMap(u32, void).init(std.testing.allocator);
        defer set.deinit();
        const curry = struct {
            fn append(ptr: *std.AutoHashMap(u32, void), item: u32) !void {
                try ptr.put(item, {});
            }
        };
        var appender = Appender(u32).new(&set, curry.append);
        try appender.append(10);
        try appender.append(20);
        try appender.append(30);
        try appender.append(40);
        inline for (([_]u32{ 10, 20, 30, 40 })[0..]) |item| {
            try std.testing.expect(set.contains(item));
        }
    }
};

// const mod_index = struct {
//     const Entry = struct {
//         name: []const u8,
//     };
//     const Index = struct{
//     };
// };

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
