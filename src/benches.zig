const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

pub const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.println;

const comb = @import("main.zig");
const mod_plist = comb.mod_plist;
const mod_gram = comb.mod_gram;
const mod_mmap = comb.mod_mmap;
const Tree = comb.mod_treewalking.Tree;
const PlasticTree = comb.mod_treewalking.PlasticTree;

const mod_bench = struct {
    const BenchConfig = struct {
        min_warmup_time_ns: usize = 5_000_000_000,
        // min_warmup_iterations: usize = 50,
        min_measure_time_ns: usize = 30_000_000_000,
        // min_measure_iterations: usize =  60,
    };

    fn bench(name: []const u8, payload: anytype, func: fn(@TypeOf(payload))void, config: BenchConfig) !void {
        const expected_avg = blk: { 
            var warmup_time = config.min_warmup_time_ns;
            // var warmup_iterations = config.min_warmup_iterations;
            // {
            //     var timer = try std.time.Timer.start();
            //     func(payload);
            //     const elapsed = timer.read();
            //     const warmup_iterations_estimate =  @divTrunc(config.min_warmup_time_ns, elapsed);
            // }
            std.debug.print(
                "Warming up for {d} ms\n", 
                .{ @intToFloat(f64, warmup_time) / 1_000_000 }
            );
            var timer = try std.time.Timer.start();
            var warmup_it: usize = 0;
            while (timer.read() < warmup_time) : ({ warmup_it += 1; }){
                func(payload);
            }
            const total = timer.read();
            const avg = total / warmup_it;
            std.debug.print(
                "Warming done. Took {d} ms with {} iterations\n", 
                .{ @intToFloat(f64, total) / 1_000_000, warmup_it },
            );
            break :blk avg;
        };
        // const iterations = 
        //     if (expected_avg * config.min_measure_iterations < config.min_measure_time_ns)
        //         @divTrunc(config.min_measure_time_ns, expected_avg)
        //     else config.min_measure_iterations;
        var msr_time = config.min_measure_time_ns;
        // if (expected_avg * config.min_measure_iterations > msr_time) {
        //     msr_time = config.min_measure_iterations * expected_avg;
        // }
        const iterations_est = @divTrunc(msr_time, expected_avg);
        std.debug.print(
            "Measuring for {d} ms with {} iterations\n", 
            .{ @intToFloat(f64, msr_time) / 1_000_000, iterations_est}
        );

        var avg_elapsed: usize = expected_avg;
        var low_bound: usize = std.math.maxInt(usize);
        var up_bound: usize = 0;

        var timer = try std.time.Timer.start();
        var total_time: u64 = 0;
        var iterations: usize = 0;
        while (total_time < msr_time) : ({ iterations += 1; }) {
            total_time += timer.lap();
            func(payload);
            const elapsed = timer.lap();
            total_time += elapsed;
            avg_elapsed = @divTrunc(elapsed + avg_elapsed, 2);
            low_bound = if (elapsed < low_bound) elapsed else low_bound;
            up_bound = if (elapsed > up_bound) elapsed else up_bound;
        }
        std.debug.print(
            "{s}: [low {d}ms; average {d}ms; upper {d}ms] for {} iterations\n", 
            .{ 
                name,
                @intToFloat(f64, low_bound) / 1_000_000, 
                @intToFloat(f64, avg_elapsed) / 1_000_000, 
                @intToFloat(f64, up_bound) / 1_000_000, 
                iterations 
            }
        );
    }
};

fn bench_plist(
    comptime I: type,
    comptime gram_len: u4,
    plist: *mod_plist.PostingListUnmanaged(I, gram_len), 
    allocator: Allocator, 
    search_str: [] const u8
) !void {
    {
        var max: usize = 0;
        var max_gram = [_]u8{mod_gram.TEC} ** gram_len;
        var min: usize = std.math.maxInt(usize);
        var min_gram = [_]u8{mod_gram.TEC} ** gram_len;
        var bucket_count: usize = 0;
        var entry_count: usize = 0;
        var avg_len_list: f64 = 0.0;
        var dist_list_len = std.AutoArrayHashMap(usize, usize).init(allocator);
        defer dist_list_len.deinit();

        var it = plist.map.iterator();
        while(it.next()) |*pair|{
            const gram = pair.key_ptr.*;
            const list = pair.value_ptr;
            const len = list.items.len;

            var occ = try dist_list_len.getOrPutValue(len, 0);
            occ.value_ptr.* += 1;

            bucket_count += 1;
            entry_count += len;

            if (len > max){
                max = len;
                max_gram = gram;
            }
            if (len < min){
                min = len;
                min_gram = gram;
            }
            avg_len_list = (avg_len_list + @intToFloat(f64, len)) * 0.5;
        }
        const mode_list_len = blk: {
            var max_occ: usize = 0;
            var max_occ_count: usize = 0;
            var iter = dist_list_len.iterator();
            while (iter.next()) |pair|{
                const occ = pair.value_ptr.*;
                if (occ > max_occ_count) { 
                    max_occ_count = occ;
                    max_occ = pair.key_ptr.*;
                }
            }
            break :blk max_occ;
        };

        std.debug.print(
            \\gram_count = {}
            \\entry_count = {}
            \\max with {} = {s}
            \\min with {} = {s}
            \\list len avg = {}
            \\list len mode = {}
            \\
            , .{ bucket_count, entry_count, max, max_gram, min, min_gram, avg_len_list, mode_list_len }
        );
    }

    const BenchCtx = struct {
        const Self = @This();
        allocator: Allocator,
        plist: *mod_plist.PostingListUnmanaged(I, gram_len),
        search_str: []const u8,
        matcher: mod_plist.PostingListUnmanaged(I, gram_len).StrMatcher,
        fn do(self: *Self) void {
            _ = self.matcher.str_match(
                    self.plist, 
                    self.search_str,
                    std.ascii.spaces[0..]
            ) catch @panic("wtf");
        }
    };
    var ctx = BenchCtx{ 
        .allocator = allocator, 
        .plist = plist,
        .search_str = search_str,
        .matcher = mod_plist.PostingListUnmanaged(I, gram_len).str_matcher(allocator),
    };
    defer {
        ctx.matcher.deinit();
    }
    try mod_bench.bench("str_match", &ctx, BenchCtx.do, .{});
}

test "rowscan.bench.gen" {
    const size: usize = 1_000_000;
    // var allocator = std.testing.allocator;
    
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    // var gp = std.heap.GeneralPurposeAllocator(.{}){};
    // defer _ = gp.deinit();
    // var allocator = gp.allocator();

    var tree = try PlasticTree.init(.{ .size = size }, allocator);
    defer tree.deinit();

    try tree.gen();
    std.debug.print("done generating fake tree of size {}\n", .{ size });

    const BenchCtx = struct {
        const Self = @This();
        allocator: Allocator,
        tree: *PlasticTree,
        search_str: []const u8,
        matches: std.ArrayList(usize),
        fn do(self: *Self) void {
            self.matches.clearRetainingCapacity();
            for (self.tree.list.items) |entry, id| {
                if (std.mem.indexOf(u8, entry.name, self.search_str)) |_| {
                    self.matches.append(id) catch @panic("wtf");
                }
            }
        }
    };
    var ctx = BenchCtx{ 
        .allocator = allocator, 
        .tree = &tree,
        // .search_str = tree.list.items[tree.list.items.len - 1].name,
        .search_str = "abc",
        .matches = std.ArrayList(usize).init(allocator),
    };
    defer {
        ctx.matches.deinit();
    }
    try mod_bench.bench("rowscan", &ctx, BenchCtx.do, .{});
}

test "rowscan.bench.SwappingIndex" {
    const Index = comb.mod_index.SwappingIndex;
    const size: usize = 1_000_000;

    var a7r = std.testing.allocator;

    const file_p = "/tmp/comb.bench.SwappingIndex.rowscan";
    var mmap_pager = try mod_mmap.MmapPager.init(a7r, file_p, .{});
    defer mmap_pager.deinit();

    var lru = try mod_mmap.LRUSwapCache.init(a7r, mmap_pager.pager(), (16 * 1024 * 1024) / std.mem.page_size);
    defer lru.deinit();

    var pager = lru.pager();

    var ma7r = mod_mmap.MmapSwappingAllocator(.{}).init(a7r, pager);
    defer ma7r.deinit();
    var sa7r = ma7r.allocator();

    var index = Index.init(a7r, pager, sa7r);
    defer index.deinit();

    defer {
        var it = index.table.iterator();
        defer it.close();
        while (it.next() catch unreachable) |entry|{
            sa7r.free(entry.name);
        }
    }
    // var name_arena = std.heap.ArenaAllocator.init(a7r);
    // defer name_arena.deinit();
    // var name_a7r = name_arena.allocator();

    var timer = try std.time.Timer.start();
    {
        var arena = std.heap.ArenaAllocator.init(a7r);
        defer arena.deinit();
        var allocator = arena.allocator();

        var tree = try PlasticTree.init(.{ .size = size }, allocator);
        defer tree.deinit();

        timer.reset();
        try tree.gen();
        std.log.info(
            "Done generating tree of size {} items in {d} seconds", 
            .{ size, @divFloor(timer.read(), std.time.ns_per_s) }
        );

        var new_ids = try allocator.alloc(Index.Id, tree.list.items.len);
        defer allocator.free(new_ids);
        timer.reset();
        for (tree.list.items) |t_entry, ii| {
            // const i_entry = try t_entry.conv(Index.Id, ).clone(name_arena.allocator());
            const i_entry = Index.Entry {
                .name = try sa7r.dupeJustPtr(t_entry.name),
                .parent = new_ids[t_entry.parent],
                .depth = t_entry.depth,
                .kind = Index.Entry.Kind.File,
                .size = 1024 * 1024,
                .inode = ii,
                .dev = 01,
                .mode = 6,
                .uid = 1000,
                .gid = 10001,
                .ctime = 02,
                .atime = 02,
                .mtime = 02,
            };
            new_ids[ii] = try index.file_created(i_entry);
        }
        std.log.info(
            "Done adding items to index in {d} seconds", 
            .{ @divFloor(timer.read(), std.time.ns_per_s) }
        );
    }

    var matcher = index.matcher();
    defer matcher.deinit();

    // var weaver = Index.FullPathWeaver.init();
    // defer weaver.deinit(index.a7r);
    const BenchCtx = struct {
        const Self = @This();
        matcher: *Index.StrMatcher,
        fn do(self: *Self) void {
            _ = self.matcher.str_match("abc") catch {
                std.debug.print("fucked\n", .{});
                std.debug.dumpStackTrace(@errorReturnTrace() orelse unreachable);
                @panic("wtf");
            };
        }
    };
    var ctx = BenchCtx{ 
        .matcher = &matcher,
    };
    try mod_bench.bench("SwappingIndex.rowscan", &ctx, BenchCtx.do, .{});
}

test "plist.bench.gen" {
    const size: usize = 1_000_000;
    const id_t = u32;
    const gram_len = 3;

    // var allocator = std.testing.allocator;
    
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    var allocator = arena.allocator();

    // var gp = std.heap.GeneralPurposeAllocator(.{}){};
    // defer _ = gp.deinit();
    // var allocator = gp.allocator();
    
    var tree = try PlasticTree.init(.{ .size = size }, allocator);
    defer tree.deinit();

    try tree.gen();
    std.debug.print("done generating fake tree of size {}\n", .{ size });

    var plist = mod_plist.PostingListUnmanaged(id_t, gram_len).init();
    // defer plist.deinit(allocator);

    var longest: usize = 0;
    var longest_name = try allocator.alloc(u8, 1);
    defer allocator.free(longest_name);
    for (tree.list.items) |entry, id|{
        try plist.insert(allocator, @intCast(id_t, id), entry.name, std.ascii.spaces[0..]);
        if (id % 10_000 == 0 ) {
            std.debug.print("added {} items to plist, now at {s}\n", .{ id, entry.name });
        }
        if (entry.name.len > longest){
            longest = entry.name.len;
            std.debug.print("got long at {s}\n", .{entry.name});
            allocator.free(longest_name);
            longest_name = try allocator.dupe(u8, entry.name);
        }
    }
    std.debug.print("done adding to plist {} items\n", .{ size });
    try bench_plist(id_t, gram_len, &plist, allocator, longest_name);
}

test "plist.bench.walk" {
    const size: usize = 1_000_000;
    const id_t = usize;
    const gram_len = 1;

    // var allocator = std.testing.allocator;

    // var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    // defer arena.deinit();
    // var allocator = arena.allocator();

    var gp = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gp.deinit();
    var allocator = gp.allocator();

    var tree = try Tree.walk(allocator, "/", size);
    defer tree.deinit();
    std.debug.print("done walking tree for {} items\n", .{ tree.list.items.len });
    var weaver = Tree.FullPathWeaver.init();
    defer weaver.deinit(allocator);

    var plist = mod_plist.PostingListUnmanaged(id_t, gram_len).init();
    defer plist.deinit(allocator);
    const search_str = ss: {
        var deepest: usize = 0;
        var deepest_name = try allocator.alloc(u8, 1);
        defer allocator.free(deepest_name);

        var longest: usize = 0;
        var longest_name = try allocator.alloc(u8, 1);
        defer allocator.free(longest_name);

        var dist_name_len = std.AutoArrayHashMap(usize, usize).init(allocator);
        defer dist_name_len.deinit();

        var avg_len = @intToFloat(f64, tree.list.items[0].name.len);
        for (tree.list.items) |entry, id| {
            try plist.insert(allocator, id, entry.name, std.ascii.spaces[0..]);
            if (id % 10_000 == 0 ) {
                std.debug.print("added {} items to plist, now at {s}\n", .{ id, entry.name });
            }
            if (entry.depth > deepest) {
                deepest = entry.depth;
                allocator.free(deepest_name);
                const path = try weaver.pathOf(allocator, tree, id, '/');
                deepest_name = try allocator.dupe(u8, path);
            }
            avg_len = (avg_len + @intToFloat(f64, entry.name.len)) * 0.5;
            if (entry.name.len > longest) {
                longest = entry.name.len;
                allocator.free(longest_name);
                const path = try weaver.pathOf(allocator, tree, id, '/');
                longest_name = try allocator.dupe(u8, path);
            }
            var occ = try dist_name_len.getOrPutValue(entry.name.len, 0);
            occ.value_ptr.* += 1;
        }
        std.debug.print("done adding to plist for {} items\n", .{ tree.list.items.len });

        const mode_name_len = blk: {
            var max_occ: usize = 0;
            var max_occ_count: usize = 0;
            var it = dist_name_len.iterator();
            while (it.next()) |pair|{
                const occ = pair.value_ptr.*;
                if (occ > max_occ_count) { 
                    max_occ_count = occ;
                    max_occ = pair.key_ptr.*;
                }
            }
            break :blk max_occ;
        };
        std.debug.print(
            \\deepest at depth {} == {s}
            \\longest name len at length {} == {s}
            \\name len avg = {}
            \\name len mode = {}
            \\
            , .{ deepest, deepest_name, longest, longest_name, avg_len, mode_name_len },
        );

        break :ss longest_name;
    };
    try bench_plist(id_t, gram_len, &plist, allocator, search_str);
}
