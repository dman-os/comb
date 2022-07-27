const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

pub const mod_utils = @import("utils.zig");
pub const println = mod_utils.println;
pub const dbg = mod_utils.dbg;

pub const mod_gram = @import("gram.zig");

pub const mod_treewalking = @import("treewalking.zig");
const Tree = mod_treewalking.Tree;
const PlasticTree = mod_treewalking.PlasticTree;

pub const mod_plist = @import("plist.zig");

pub const mod_mmap = @import("mmap.zig");
const SwappingAllocator = mod_mmap.SwappingAllocator;

pub const mod_index = @import("index.zig");

pub const log_level = std.log.Level.debug;

pub const mod_fanotify = @import("fanotify.zig");

pub fn main() !void {
    // try in_mem();
    // try swapping();
    try fanotify_demo();
}

fn fanotify_demo() !void {
    const FAN = mod_fanotify.FAN;
    const fd = blk: {
        const fd = try mod_fanotify.init(
            FAN.CLASS.NOTIF,
            FAN.INIT.REPORT_FID
                | FAN.INIT.REPORT_DIR_FID
                | FAN.INIT.REPORT_NAME,
            std.os.O.RDONLY | std.os.O.CLOEXEC,
        );
        try mod_fanotify.mark(
            fd,
            FAN.MARK.MOD.ADD,
            FAN.MARK.FILESYSTEM,
            FAN.EVENT.ONDIR
                | FAN.EVENT.CREATE
                | FAN.EVENT.ATTRIB
                | FAN.EVENT.DELETE
                | FAN.EVENT.MOVED_TO
                ,
            std.os.AT.FDCWD,
           "/" 
        );
        break :blk fd;
    };
    defer std.os.close(fd);
    var pollfds = [_]std.os.pollfd{ 
        std.os.pollfd {
            .fd = fd,
            .events = std.os.POLL.IN,
            .revents = 0,
        } 
    };
    std.log.info("entering poll loop", .{});
    var buf = [_]u8{0} ** 256;
    while (true) {
        const poll_num = try std.os.poll(&pollfds, -1);

        if (poll_num < 1) {
            std.log.err("err on poll: {}", .{ std.os.errno(poll_num) });
            @panic("err on poll");
        }
        for (&pollfds) |pollfd| {
            if (
                pollfd.revents == 0 
                or (pollfd.revents & std.os.POLL.IN) == 0
            ) {
                continue;
            }
            const fanotify_fd = pollfd.fd;
            const read_len = try std.os.read(fanotify_fd, &buf);

            var ptr = &buf[0];
            var left_bytes = read_len;
            var event_count: usize = 0;
            while (true) {
                const meta_ptr = @ptrCast(*const align(1) mod_fanotify.event_metadata, ptr);
                const meta = meta_ptr.*;

                if (
                    left_bytes < @sizeOf(mod_fanotify.event_metadata)
                    or left_bytes < @intCast(usize, meta.event_len)
                    // FIXME: is this case even possible?
                    or meta.event_len < @sizeOf(mod_fanotify.event_metadata)
                ) {
                    break;
                }
                if (meta.vers != mod_fanotify.METADATA_VERSION) {
                    @panic("unexpected " ++ @typeName(mod_fanotify.event_metadata) ++ " version");
                }
                ptr = @intToPtr(*u8, @ptrToInt(ptr) + meta.event_len);
                left_bytes -= meta.event_len;
                event_count += 1;
            }
            std.log.info("{} events read this cycle", .{ event_count });
        }
    }
}

fn swapping () !void {
    const Index = mod_index.SwappingIndex;
    const PList = mod_plist.SwappingPostingList(Index.Id, 3);

    // var fixed_a7r = std.heap.FixedBufferAllocator.init(mmap_mem);
    // var a7r = fixed_a7r.threadSafeAllocator();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){
        // .backing_allocator = fixed_a7r.allocator(),
    };
    defer _ = gpa.deinit();

    var a7r = gpa.allocator();
    
    var mmap_pager = try mod_mmap.MmapPager.init(a7r, "/tmp/comb.db", .{});
    defer mmap_pager.deinit();

    var lru = try mod_mmap.LRUSwapCache.init(a7r, mmap_pager.pager(), (16 * 1024 * 1024) / std.mem.page_size);
    // var lru = try mod_mmap.LRUSwapCache.init(a7r, mmap_pager.pager(), 1);
    defer lru.deinit();

    var pager = lru.pager();

    var ma7r = mod_mmap.MmapSwappingAllocator(.{}).init(a7r, pager);
    defer ma7r.deinit();
    var sa7r = ma7r.allocator();

    var index = Index.init(a7r, pager, sa7r);
    defer index.deinit();

    var plist = PList.init(a7r, sa7r, pager);
    defer plist.deinit();

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
            try plist.insert(
                new_ids[ii],
                t_entry.name,
                std.ascii.spaces[0..]
            );
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
    var matcher = plist.matcher();
    defer matcher.deinit();
    var weaver = Index.FullPathWeaver.init();
    defer weaver.deinit(index.ha7r);

    while (true) {
        std.debug.print("Ready to search: ", .{});
        try stdin_rdr.readUntilDelimiterArrayList(&phrase, '\n', 1024 * 1024);
        std.log.info("Searching...", .{});
        _ = timer.reset();
        var matches = try matcher.str_match(
            &plist,
            phrase.items,
            std.ascii.spaces[0..],
        );
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
