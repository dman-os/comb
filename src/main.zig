const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

pub const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.dbg;
const Option = mod_utils.Option;

pub const mod_gram = @import("gram.zig");

pub const mod_treewalking = @import("treewalking.zig");
const Tree = mod_treewalking.Tree;
const PlasticTree = mod_treewalking.PlasticTree;

pub const mod_plist = @import("plist.zig");

pub const mod_mmap = @import("mmap.zig");
const SwapAllocator = mod_mmap.SwapAllocator;

pub const Database = @import("Database.zig");
const Db = Database;

pub const mod_index = @import("index.zig");

pub const log_level = std.log.Level.debug;

pub const mod_fanotify = @import("fanotify.zig");

pub const Query = @import("Query.zig");

pub fn main() !void {
    try swapping();
    // try fanotify_demo();
    // try mod_fanotify.demo();
}

const FanotifyThread = struct {
    ha7r: Allocator,
    db: *Db,
    thread: ?std.Thread = null,
    die_signal: bool = false,
    querier: Db.Quexecutor,

    fn init(ha7r: Allocator, db: *Db) @This() {
        return @This() { 
            .ha7r = ha7r, 
            .db = db,
            .querier = Db.Quexecutor.init(db),
        };
    }

    fn deinit(self: *@This()) void {
        self.die_signal = true;
        if (self.thread) |thread| {
            thread.detach();
        }
        self.querier.deinit();
    }

    fn start(self: *@This()) !void {
        self.thread = try std.Thread.spawn(.{}, @This().threadFn, .{ self });
    }

    fn threadFn(self: *@This()) !void {
        try mod_fanotify.listener(
            self.ha7r,
            "/",
            &self.die_signal,
            mod_utils.Appender(mod_fanotify.FanotifyEvent).new(
                self,
                @This().appendEvent
            )
        );
    }

    fn appendEvent(self: *@This(), event_in: mod_fanotify.FanotifyEvent) !void {
        _ = self;
        var event = event_in;
        std.log.info("evt: {any}", .{ event });
        defer event.deinit(self.ha7r);
        //const FAN = mod_fanotify.FAN;
        if (
            event.kind.attrib and
            event.name == null and
            event.dir == null 
        ) {
            std.log.debug("neutrino attrib event", .{});
        } else if (
            event.kind.create and event.kind.delete
        ) {
            std.log.debug("neutrino create event", .{});
        } else if (
            event.kind.create 
        ) {
            //var dir = event.dir.?;
            //const fb = Query.Filter.Clause.Builder.init(self.ha7r);
            //try fb.addNameMatch(dir);
            //var query = Query.Builder
            //                .init()
            //                .withFilter(
            //                )
            //                .build();
            //
            //var parent = self.querier.query();
            // self.db.fileCreated()
        }
        else {
            std.log.warn("unreconized event: {any}", .{ event });
        }
        // std.debug.todo("append");
    }
};

fn swapping () !void {
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

    var ma7r = mod_mmap.PagingSwapAllocator(.{}).init(a7r, pager);
    defer ma7r.deinit();
    var sa7r = ma7r.allocator();

    var db = Db.init(a7r, pager, sa7r, .{});
    defer db.deinit();

    var name_arena = std.heap.ArenaAllocator.init(a7r);
    defer name_arena.deinit();

    var fanotify_th = FanotifyThread.init(a7r, &db);
    defer fanotify_th.deinit();
    try fanotify_th.start();

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

        var new_ids = try arena.allocator().alloc(Db.Id, tree.list.items.len);
        defer arena.allocator().free(new_ids);
        timer.reset();
        for (tree.list.items) |t_entry, ii| {
            const i_entry = t_entry.conv(
                Db.Id, 
                []const u8, 
                new_ids[t_entry.parent], 
                t_entry.name,
            );
            new_ids[ii] = try db.fileCreated(&i_entry);
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
    // std.debug.print("index meta bytes: {} KiB\n", .{ (index.meta.pages.items.len * @sizeOf(Db.RowMeta)) / 1024});

    var stdin = std.io.getStdIn();
    var stdin_rdr = stdin.reader();
    var phrase = std.ArrayList(u8).init(a7r);
    defer phrase.deinit();
    // var matcher = db.plistNameMatcher();
    // defer matcher.deinit(&db);
    var querier = Db.Quexecutor.init(&db);
    defer querier.deinit();
    var weaver = Db.FullPathWeaver{};
    defer weaver.deinit(a7r);

    while (true) {
        std.debug.print("Ready to search: ", .{});
        try stdin_rdr.readUntilDelimiterArrayList(&phrase, '\n', 1024 * 1024);
        std.log.info("Searching...", .{});

        _ = timer.reset();
        var query = try Query.parse(a7r, phrase.items);
        defer query.deinit(a7r);
        // var matches = try matcher.match(&db, phrase.items);
        var matches = try querier.query(&query);
        const elapsed = timer.read();

        for (matches) |id, ii| {
            const path = try weaver.pathOf(&db, id, '/');
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
    // _ = ThreadPool;
    _ = mod_utils;
    _ = mod_mmap;
    _ = mod_gram;
    _ = mod_plist;
    _ = mod_index;
    _ = mod_fanotify;
    _ = mod_treewalking;
    _ = Database;
    _ = Query;
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

//test "anyopaque.fn"{
//    const inner = struct {
//        num: usize,
//        fn my_fn(self: *@This(), also: usize) void {
//            std.debug.print("num = {}, also = {}", . {self.num, also} );
//        }
//    };
//    const fn_ptr = @ptrCast(*const anyopaque, inner.my_fn);
//    var args = .{ inner {.num = 10 }, 20 };
//    var arg_ptr = @ptrCast(*anyopaque, &args);
//    _ = arg_ptr;
//    @call(.{}, fn_ptr, .{});
//}

// const ThreadPool = struct {
//     const Self = @This();
//     const Task = struct {
//         args: *anyopaque,
//         func: fn (*anyopaque) void,
//     };
//     const Queue = std.atomic.Queue(Task);
// 
//     ha7r: Allocator,
//     queue: Queue,
//     opt_threads: ?[]std.Thread = null,
//     break_signal: bool = false,
//     opt_death_signal: ?[]bool = null,
// 
//     /// You can use `std.Thread.getCpuCount` to set the count.
//     fn init(ha7r: Allocator) Self {
//         return Self {
//             .ha7r = ha7r,
//             .queue = Queue.init(),
//         };
//     }
// 
//     fn start(self: *Self, size: usize) !void {
//         if (self.opt_threads != null) return error.ThreadPoolAreadyStarted;
//         var threads = try self.ha7r.alloc(std.Thread, size);
//         errdefer self.ha7r.free(threads);
//         var death_signal = try self.ha7r.alloc(bool, size);
//         errdefer self.ha7r.free(death_signal);
//         // var nameBuf = [_]u8{0} ** 128;
//         for (threads) |*thread, id| {
//             thread.* = try std.Thread.spawn(.{}, Self.threadStart, .{ self, id });
//             // try thread.setName(
//             //     try std.fmt.bufPrint(nameBuf[0..], "thread_pool_worker_{}", .{ id })
//             // );
//             death_signal[id] = false;
//         }
//         self.opt_threads = threads;
//         self.opt_death_signal = death_signal;
//     }
// 
//     /// Detachs threads after waiting for timeout.
//     fn deinit(self: *Self, timeout_ns: u64) void {
//         if (self.opt_threads == null) return;
//         var threads = self.opt_threads.?;
//         var death_signal = self.opt_death_signal.?;
//         self.break_signal = true;
//         std.time.sleep(timeout_ns);
//         for (death_signal) |signal, id| {
//             if (signal) {
//                 threads[id].join();
//             } else {
//                 threads[id].detach();
//             }
//         }
//         self.ha7r.free(threads);
//         self.ha7r.free(death_signal);
//     }
// 
//     fn threadStart(self: *Self, id: usize) void {
//         var death_signal = self.opt_death_signal.?;
//         while (true) {
//             if (self.break_signal){
//                 break;
//             }
//             if (self.queue.get()) |node|{
//                 var task = node.data;
//                 @call(.{}, task.func, .{ task.args });
//                 self.ha7r.destroy(node);
//             } else{
//                 std.Thread.yield() catch @panic("ThreadYieldErr");
//             }
//         }
//         death_signal[id] = true;
//     }
// 
//     fn do(
//         self: *Self,
//         //comptime func: fn (*anyopaque) void,
//         comptime func: anytype,
//         args: anytype,
//     ) !void {
//         var node = try self.ha7r.create(Queue.Node);
//         node.data = Task {
//             .args = @as(*anyopaque, args),
//             .func = @ptrCast(fn (*anyopaque) void, func)
//         };
//         self.queue.put(node);
//     }
// 
//     test "ThreadPool" {
//         var ha7r = std.testing.allocator;
//         var pool = ThreadPool.init(ha7r);
//         defer pool.deinit(1 * 1_000_000_000);
//         try pool.start(8);
//         // const MyTask = struct {
//         //     id: usize,
//         //     fn printThreadName(self: *@This()) void {
//         //         var id = std.Thread.getCurrentId();
//         //         println("task {} from thread {}", .{ self.id, id });
//         //     }
//         // };
//         // var tasks: [300]MyTask = undefined;
//         // for (tasks) |*task, id|{
//         //     task.* = MyTask { .id = id };
//         //     try pool.do(MyTask.printThreadName, task);
//         // }
//     }
// };
