const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mod_utils = @import("../utils.zig");
const println = mod_utils.println;
const isElevated = mod_utils.isElevated;
const dbg = mod_utils.dbg;
const Option = mod_utils.Option;
const OptionStr = Option([]const u8);
const Queue = mod_utils.Mpmc;

const Db = @import("../Database.zig");
const Query = @import("../Query.zig");

const mod_treewalking = @import("../treewalking.zig");
const FsEntry = mod_treewalking.FsEntry;

const mod_mmap = @import("../mmap.zig");

const mod_fanotify = @import("../fanotify.zig");
const Config = mod_fanotify.Config;
const FanotifyEvent = mod_fanotify.FanotifyEvent;

ha7r: Allocator,
db: *Db,
listener: FanotifyEventListener,
mapper: FanotifyEventMapper,
worker: FsEventWorker,
fan_event_q: *Queue(FanotifyEvent),
fs_event_q: *Queue(FsEvent),

pub fn init(ha7r: Allocator, db: *Db, config: Config) !@This() {
    var fan_event_q = try ha7r.create(Queue(FanotifyEvent));
    fan_event_q.* = Queue(FanotifyEvent).init();
    var fs_event_q = try ha7r.create(Queue(FsEvent));
    fs_event_q.* = Queue(FsEvent).init();
    return @This() {
        .ha7r = ha7r,
        .db = db,
        .fan_event_q = fan_event_q,
        .fs_event_q = fs_event_q,
        .listener = FanotifyEventListener.init(ha7r, config, fan_event_q),
        .mapper = FanotifyEventMapper.init(ha7r, db, fan_event_q, fs_event_q),
        .worker = FsEventWorker.init(ha7r, db, fs_event_q)
    };
}

pub fn join(self: *@This()) void {
    self.listener.join();
    self.mapper.join();
    self.worker.join();
    while (self.fan_event_q.getOrNull()) |node| {
        node.data.deinit(self.ha7r);
        self.ha7r.destroy(node);
    }
    self.ha7r.destroy(self.fan_event_q);
    while (self.fs_event_q.getOrNull()) |node| {
        node.data.deinit(self.ha7r);
        self.ha7r.destroy(node);
    }
    self.ha7r.destroy(self.fs_event_q);
}

pub fn start(self: *@This()) !void {
    try self.worker.start();
    try self.mapper.start();
    try self.listener.start();
}

const FanotifyEventListener = struct {
    ha7r: Allocator,
    config: Config,
    event_q: *Queue(FanotifyEvent),
    thread: ?std.Thread = null,
    die_signal: std.Thread.ResetEvent = .{},

    fn init(ha7r: Allocator, fan_config: Config, que: *Queue(FanotifyEvent)) @This() {
        return @This() { 
            .ha7r = ha7r, 
            .config = fan_config,
            .event_q = que,
        };
    }

    fn join(self: *@This()) void {
        self.die_signal.set();
        if (self.thread) |thread| {
            thread.join();
        }
    }

    fn start(self: *@This()) !void {
        self.thread = try std.Thread.spawn(.{}, @This().threadFn, .{ self });
    }

    fn threadFn(self: *@This()) !void {
        try mod_fanotify.listener(
            self.ha7r,
            &self.die_signal,
            mod_utils.Appender(FanotifyEvent).new(
                self,
                @This().appendEvent
            ),
            self.config
        );
    }

    fn appendEvent(
        self: *@This(), 
        event: FanotifyEvent
    ) std.mem.Allocator.Error!void {
        defer std.log.info(@typeName(FanotifyEventListener) ++ " detected event: {any}", .{ event });
        var node = try self.ha7r.create(Queue(FanotifyEvent).Node);
        node.* = Queue(FanotifyEvent).Node {
            .data = event,
        };
        self.event_q.put(node); 
    }
};

const FsEvent = union(enum) {
    fileCreated: FileCreated,
    fileDeleted: FileDeleted,
    dirDeleted: DirDeleted,
    fileMoved: FileMoved,
    dirMoved: DirMoved,
    fileModified: FileModified,

    pub const FileCreated = struct {
        timestamp: i64,
        entry: FsEntry([]const u8, []const u8),

        fn deinit(self: *@This(), ha7r: Allocator) void {
            ha7r.free(self.entry.parent);
            ha7r.free(self.entry.name);
        }
    };

    pub const FileDeleted = struct {
        timestamp: i64,
        dir: []const u8,
        name: []const u8,

        fn deinit(self: *@This(), ha7r: Allocator) void {
            ha7r.free(self.dir);
            ha7r.free(self.name);
        }
    };

    pub const DirDeleted = FileDeleted;

    pub const FileMoved = struct {
        timestamp: i64,
        // id: Db.Id,
        old_dir: []const u8,
        old_name: []const u8,
        new_dir: []const u8,
        new_name: []const u8,

        fn deinit(self: *@This(), ha7r: Allocator) void {
            ha7r.free(self.old_dir);
            ha7r.free(self.old_name);
            ha7r.free(self.new_dir);
            ha7r.free(self.new_name);
        }
    };

    pub const DirMoved = FileMoved;

    pub const FileModified = FileCreated;

    fn deinit(self: *@This(), ha7r: Allocator) void {
        switch(self.*) {
            .fileCreated => |*event| {
                event.deinit(ha7r);
            },
            .fileDeleted => |*event| {
                event.deinit(ha7r);
            },
            .dirDeleted => |*event| {
                event.deinit(ha7r);
            },
            .fileMoved => |*event| {
                event.deinit(ha7r);
            },
            .dirMoved => |*event| {
                event.deinit(ha7r);
            },
            .fileModified => |*event| {
                event.deinit(ha7r);
            },
        }
    }
};

const FanotifyEventMapper = struct {
    const MapperErr = error {
        NameIsNull,
        DirIsNull,
        UnrecognizedEvent,
        ParentNotFound,
        NeutrinoDuringMeta
    };

    ha7r: Allocator,
    db: *Db,
    querier: Db.Quexecutor,
    fan_event_q: *Queue(FanotifyEvent),
    fs_event_q: *Queue(FsEvent),
    die_signal: std.Thread.ResetEvent = .{},
    thread: ?std.Thread = null,

    fn init(
        ha7r: Allocator, 
        db: *Db, 
        fan_que: *Queue(FanotifyEvent),
        fs_que: *Queue(FsEvent),
    ) @This() {
        return @This() { 
            .ha7r = ha7r, 
            .db = db,
            .querier = Db.Quexecutor.init(db),
            .fan_event_q = fan_que,
            .fs_event_q = fs_que,
        };
    }

    fn join(self: *@This()) void {
        self.die_signal.set();
        if (self.thread) |thread| {
            thread.join();
        }
        self.querier.deinit();
    }

    fn start(self: *@This()) !void {
        self.thread = try std.Thread.spawn(.{}, @This().threadFn, .{ self });
    }

    fn threadFn(self: *@This()) !void {
        var count: usize = 0;
        while(
            !self.die_signal.isSet()
        ) {
            if (count > 0 and count % 1_000 == 0) {
                std.log.info("handled {} events", .{ count });
            }
            if (self.fan_event_q.getTimed(500_000_000)) |node| {
                if (self.die_signal.isSet()) return;
                defer self.ha7r.destroy(node);
                defer node.data.deinit(self.ha7r);
                try self.handleEvent(node.data);
                count += 1;
            } else |_| {
                // _ = err;
                std.Thread.yield() catch @panic("ThreadYieldErr");
            }
        }
    }

    fn handleEvent(
        self: *@This(), 
        event: FanotifyEvent
    ) std.mem.Allocator.Error!void {
        defer std.log.debug(@typeName(FanotifyEventMapper) ++ " handled event: {any}", .{ event });
        if(self.tryMap(event)) |opt| {
            if (opt) |fs_event| {
                var node = try self.ha7r.create(Queue(FsEvent).Node);
                node.* = Queue(FsEvent).Node {
                    .data = fs_event,
                };
                self.fs_event_q.put(node); 
            } else {
                std.log.debug("no FsEvent when processing event {}", .{ event });
            }
        } else |err| {
            std.log.err("error {} processing event {}", .{ err, event });
        }
    }

    pub fn tryMap(self: *@This(), event: FanotifyEvent) !?FsEvent {
        // NOTE: the ordering of these tests is important for sometimes,
        // we get hard to make sense of flags set like both create and a delete 
        // (which is treated as a delete here since we're checking for deletes first)
        if (
            event.kind.attrib and
            event.name == null and
            event.dir == null 
        ) {
            std.log.debug("neutrino attrib event", .{});
            return null;
        } else if (
            event.kind.create and event.kind.delete
        ) {
            std.log.debug("neutrino create event", .{});
            return null;
        } else if (
            event.kind.moved_to and event.kind.delete
        ) {
            std.log.debug("neutrino move event", .{});
            return null;
        } else if (
            event.kind.attrib
        ) {
            std.log.debug("entry attrib modified event: {}", .{ event });
            return null;
        } else if (
            event.kind.modify
        ) {
            std.log.debug("entry modified event: {}", .{ event });
            var inner = try self.tryToFileModifiedEvent(event);
            return FsEvent { .fileModified = inner };
        } else if (
            event.kind.moved_to and event.kind.ondir
        ) {
            std.log.debug("dir moved event: {}", .{ event });
            return null;
        } else if (
            event.kind.moved_to
        ) {
            std.log.debug("file moved event: {}", .{ event });
            return null;
        } else if (
            event.kind.delete and event.kind.ondir
        ) {
            std.log.debug("dir deleted event: {}", .{ event });
            return null;
        } else if (
            event.kind.delete
        ) {
            std.log.debug("file deleted event: {}", .{ event });
            var inner = try self.tryToFileDeletedEvent(event);
            return FsEvent { .fileDeleted = inner };
        } else if (
            event.kind.create
        ) {
            std.log.debug("entry created event: {}", .{ event });
            var inner = try self.tryToFileCreatedEvent(event);
            return FsEvent { .fileCreated = inner };
        } else {
            std.log.err("unreconized event: {any}", .{ event });
            return MapperErr.UnrecognizedEvent;
        }
    }

    fn tryToFileCreatedEvent(self: *@This(), event: FanotifyEvent) !FsEvent.FileCreated {
        const name = event.name orelse return error.NameIsNull;
        const dir = event.dir orelse return error.DirIsNull;
        const entry = 
            mod_treewalking.entryFromAbsolutePath2(
                try self.ha7r.dupe(u8, dir), 
                try self.ha7r.dupe(u8, name)
            ) catch |err| return switch(err) {
                std.os.OpenError.FileNotFound => error.NeutrinoDuringMeta,
                else => err
            };
        return FsEvent.FileCreated {
            .timestamp = event.timestamp,
            .entry = entry
        };
    }

    fn tryToFileDeletedEvent(self: *@This(), event: FanotifyEvent) !FsEvent.FileDeleted {
        const name = event.name orelse return error.NameIsNull;
        const dir = event.dir orelse return error.DirIsNull;
        return FsEvent.FileDeleted {
            .timestamp = event.timestamp,
            .name = try self.ha7r.dupe(u8, name),
            .dir = try self.ha7r.dupe(u8, dir),
        };
    }

    fn tryToFileModifiedEvent(self: *@This(), event: FanotifyEvent) !FsEvent.FileModified {
        return try self.tryToFileCreatedEvent(event);
    }

    fn assertNeutrino(self: *@This(), event: FanotifyEvent) void {
        _ = self;
        _ = event;
    }
};


const FsEventWorker = struct {
    ha7r: Allocator,
    db: *Db,
    querier: Db.Quexecutor,
    weaver: Db.FullPathWeaver = .{},
    event_q: *Queue(FsEvent),
    thread: ?std.Thread = null,
    die_signal: std.Thread.ResetEvent = .{},

    fn init(ha7r: Allocator, db: *Db, que: *Queue(FsEvent)) @This() {
        return @This() { 
            .ha7r = ha7r, 
            .db = db,
            .querier = Db.Quexecutor.init(db),
            .event_q = que,
        };
    }

    fn join(self: *@This()) void {
        self.die_signal.set();
        if (self.thread) |thread| {
            thread.join();
        }
        self.querier.deinit();
        self.weaver.deinit(self.ha7r);
    }

    fn start(self: *@This()) !void {
        self.thread = try std.Thread.spawn(.{}, @This().threadFn, .{ self });
    }

    fn threadFn(self: *@This()) !void {
        while(
            !self.die_signal.isSet()
        ) {
            if (self.event_q.getTimed(500_000_000)) |node| {
                defer self.ha7r.destroy(node);
                defer node.data.deinit(self.ha7r);
                try self.handleEvent(node.data);
            } else |_| {
                // _ = err;
                std.Thread.yield() catch @panic("ThreadYieldErr");
            }
        }
    }

    fn handleEvent(
        self: *@This(), 
        fs_event: FsEvent
    ) !void {
        defer std.log.debug(@typeName(FsEventWorker) ++ " handled event: {}", .{ fs_event });
        switch(fs_event) {
            .fileCreated => |event| {
                try self.handleFileCreatedEvent(event);
            },
            // .fileDeleted => |*event| {
            // },
            else => {}
        }
    }

    fn handleFileCreatedEvent(
        self: *@This(), 
        event: FsEvent.FileCreated
    ) !void {
        const parent_id = (try self.idAtPath(event.entry.parent)) 
            orelse return error.ParentNotFound;
        const entry = event.entry.conv(
            Db.Id, []const u8,
            parent_id, event.entry.name,
        );
        _ = try self.db.fileCreated(&entry);
    }

    fn handleFileDeletedEvent(
        self: *@This(), 
        event: FsEvent.FileDeleted
    ) !void {
        const full_path = mod_utils.pathJoin(&.{ event.dir, event.name });
        const id = (try self.idAtPath(full_path)) 
            orelse return error.FileNotFound;
        _ = try self.db.fileDeleted(id);
    }

    fn idAtPath(self: *@This(), path: []const u8) !?Db.Id {
        var parser = Query.Parser{};
        defer parser.deinit(self.ha7r);
        var query = parser.parse(self.ha7r, path) catch @panic("error parsing path to query");
        defer query.deinit(self.ha7r);
        var timer = std.time.Timer.start() catch @panic("timer unsupported");
        const candidates = self.querier.query(&query) catch |err| {
            println("error querying: {}", .{ err });
            @panic("error querying");
        };
        if (candidates.len == 0) {
            return null;
        }
        var elapsed = @intToFloat(f64,timer.read()) / @intToFloat(f64, std.time.ns_per_s);
        if (candidates.len > 1) {
            var arena = std.heap.ArenaAllocator.init(self.ha7r);
            defer arena.deinit();
            var ha7r = arena.allocator();
            var paths = std.ArrayList([]const u8).init(ha7r);
            for (candidates) |id| {
                const cand_path = try self.weaver.pathOf(self.db, id, '/');
                try paths.append(try ha7r.dupe(u8, cand_path));
            }
            std.log.warn(
                \\ -- found {} possilbe candidates in {} secs 
                \\ --    path: {s}
                \\ --    candidates: {s}
                ,.{ candidates.len, elapsed, path, paths.items }
            );
        } 
        std.log.debug(
            " -- found {} possilbe candidates in {} secs", 
            .{ candidates.len, elapsed }
        );
        return candidates[0];
    }
};

const TestFanotifyWorker = struct {
    const Context = struct {
        ha7r: Allocator,
        sa7r: mod_mmap.SwapAllocator,
        db: *Db,
        parser: *Query.Parser,
        querier: *Db.Quexecutor,
        weaver: *Db.FullPathWeaver,
        payload: ?*anyopaque = null,
        root_dir: *std.fs.Dir,
        tmpfs_path: []u8,

        fn query(cx: @This(), query_str: []const u8) ![]const Db.Id {
            var parsed_query = try cx.parser.parse(cx.ha7r, query_str);
            defer parsed_query.deinit(cx.ha7r);
            return try cx.querier.query(&parsed_query);
        }

        fn queryNone(cx: @This(), query_str: []const u8) !void {
            var ids = try cx.query(query_str);
            try std.testing.expectEqual(@as(usize, 0), ids.len);
        }

        fn queryOne(
            cx: @This(), query_str: []const u8
        ) !std.meta.Tuple(&[_]type{ Db.Id, FsEntry(Db.Id, []const u8), }) {
            const ids = try cx.query(query_str);
            if (ids.len > 1) { 
                return error.MoreThanOneResults; 
            }
            if (ids.len == 0) return error.EmptyResult;
            const entry = try cx.db.getAtId(ids[0]);
            return .{ ids[0], entry };
        }

        fn actualPath(cx: @This(), test_path: []const u8) []const u8 {
            return mod_utils.pathJoin(&.{ cx.tmpfs_path, test_path });
        }

        fn testPath(cx: @This(), actual_path: []const u8) []const u8 {
            return actual_path[cx.tmpfs_path.len..];
        }
    };

    const Listener2MapperFly = struct {
        ha7r: Allocator,
        listener_q: *Queue(FanotifyEvent),
        mapper_q: *Queue(FanotifyEvent),
        die_signal: std.Thread.ResetEvent = .{},
        seen_event_count: usize = 0,
        canary_detected: std.Thread.ResetEvent = .{},
        tmpfs_path: []const u8,

        fn threadFn(self: *@This()) !void {
            // println("fly 1 online", .{ });
            while(
                !self.die_signal.isSet()
            ) {
                if (self.listener_q.getTimed(500_000)) |node| {
                    // println("fly 1 got event: {}", .{ node.data });
                    // Don't forward any events until the canary's detected
                    if (!self.canary_detected.isSet()) {
                        defer self.ha7r.destroy(node);
                        defer node.data.deinit(self.ha7r);
                        
                        // if it's the canary file, set the signal
                        if (node.data.name) |name| {
                            if (std.mem.eql(u8, name, canary_file_name)) {
                                self.canary_detected.set();
                            }
                        }
                    } else {
                        // if it's the canary file coming through again
                        if (node.data.name) |name| {
                            if (std.mem.eql(u8, name, canary_file_name)) {
                                defer self.ha7r.destroy(node);
                                defer node.data.deinit(self.ha7r);
                                continue;
                            }
                        }
                        // node.data.dir = if (node.data.old_dir) |dir| blk: {
                        //     if (std.mem.startsWith(u8, dir, self.tmpfs_path)) {
                        //         defer self.ha7r.free(dir);
                        //         break :blk try self.ha7r.dupe(
                        //             u8, dir[self.tmpfs_path.len..]
                        //         );
                        //     } else {
                        //         break :blk dir;
                        //     }
                        // } else null;
                        // node.data.old_dir = if (node.data.old_dir) |dir| blk: {
                        //     if (std.mem.startsWith(u8, dir, self.tmpfs_path)) {
                        //         defer self.ha7r.free(dir);
                        //         break :blk try self.ha7r.dupe(
                        //             u8, dir[self.tmpfs_path.len..]
                        //         );
                        //     } else {
                        //         break :blk dir;
                        //     }
                        // } else null;
                        self.mapper_q.put(node);
                        _ = @atomicRmw(
                            usize, &self.seen_event_count, .Add, 1, .SeqCst,
                        );
                    }
                } else |_| {
                    // _ = err;
                    std.Thread.yield() catch @panic("ThreadYieldErr");
                }
            }
        }
    };

    const canary_file_name = "canary";
    const canary_path = "canary/" ++ canary_file_name;


    const PrePollFn = *const fn (cx: *Context) anyerror!void;
    /// Return the number of expected events
    const TouchFn = *const fn (cx: *Context) anyerror!usize;
    const TestFn = *const fn (cx: Context) anyerror!void;

    fn run(
        ha7r: Allocator,
        pre_poll_fn: PrePollFn,
        touch_fn: TouchFn,
        test_fn: TestFn,
    ) !void {
        // init the `TmpDir` and `tmpfs`
        
        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        var tmpfs_path = blk: {
            const tmpfs_name = "tmpfs";
            try tmp_dir.dir.makeDir(tmpfs_name);
            var path = try mod_utils.fdPath(tmp_dir.dir.fd);
            break :blk try std.mem.concatWithSentinel(ha7r, u8, &.{path, "/", tmpfs_name}, 0);
        };
        defer ha7r.free(tmpfs_path);

        try mod_fanotify.mount("tmpfs", tmpfs_path, "tmpfs", 0, 0);
        defer {
            const resp = std.os.linux.umount(tmpfs_path);
            switch (std.os.errno(resp)) {
                .SUCCESS => {},
                else => |err| {
                    println("err: {}", .{err});
                    @panic("umount failed");
                },
            }
        }

        var tmpfs_dir = try tmp_dir.dir.openDir("tmpfs", .{});
        defer tmpfs_dir.close();

        // init the swap allocator and pager
        
        var db_path = try std.fs.path.join(
            ha7r, &.{ try mod_utils.fdPath(tmp_dir.dir.fd), "comb.db" }
        );
        defer ha7r.free(db_path);

        // init the SwapAllocator and Pager
        
        var mmap_pager = try mod_mmap.MmapPager.init(ha7r, db_path, .{});
        defer mmap_pager.deinit();
        var lru = try mod_mmap.LRUSwapCache.init(ha7r, mmap_pager.pager(), 1);
        defer lru.deinit();
        var pager = lru.pager();

        var ma7r = mod_mmap.PagingSwapAllocator(.{}).init(ha7r, pager);
        defer ma7r.deinit();
        var sa7r = ma7r.allocator();

        // init the db and the associated types

        var db = Db.init(ha7r, pager, sa7r, .{ });
        defer db.deinit();
        var parser = Query.Parser{};
        defer parser.deinit(ha7r);
        var querier = Db.Quexecutor.init(&db);
        defer querier.deinit();
        var weaver = Db.FullPathWeaver{};
        defer weaver.deinit(ha7r);

        // init the channels

        var fan_event_q_listener = try ha7r.create(Queue(FanotifyEvent));
        fan_event_q_listener.* = Queue(FanotifyEvent).init();
        defer ha7r.destroy(fan_event_q_listener);
        defer while (fan_event_q_listener.getOrNull()) |node| {
            node.data.deinit(ha7r);
            ha7r.destroy(node);
            println("found FanotifyEvent in fan_event_q_listener at defer", .{});
        };

        var fan_event_q_mapper = try ha7r.create(Queue(FanotifyEvent));
        fan_event_q_mapper.* = Queue(FanotifyEvent).init();
        defer ha7r.destroy(fan_event_q_mapper);
        defer while (fan_event_q_mapper.getOrNull()) |node| {
            node.data.deinit(ha7r);
            ha7r.destroy(node);
            println("found FanotifyEvent in fan_event_q_mapper at defer", .{});
        };

        var fs_event_q_mapper = try ha7r.create(Queue(FsEvent));
        fs_event_q_mapper.* = Queue(FsEvent).init();
        defer ha7r.destroy(fs_event_q_mapper);
        defer while (fs_event_q_mapper.getOrNull()) |node| {
            node.data.deinit(ha7r);
            ha7r.destroy(node);
            println("found FsEvent in fs_event_q_mapper at defer", .{});
        };

        var fs_event_q_worker = try ha7r.create(Queue(FsEvent));
        fs_event_q_worker.* = Queue(FsEvent).init();
        defer ha7r.destroy(fs_event_q_worker);
        defer while (fs_event_q_worker.getOrNull()) |node| {
            node.data.deinit(ha7r);
            ha7r.destroy(node);
            println("found FsEvent in fs_event_q_worker at defer", .{});
        };

        // init the workers
        var listener = FanotifyEventListener.init(
            ha7r, .{ 
                // listen on the `tmpfs`
                .mark_fs_path = tmpfs_path, 
                // make quit poll leave early so that it gets kill signals asap
                .poll_timeout = 0 
            }, 
            fan_event_q_listener
        );
        defer listener.join();

        // fly_1 is responsible for:
        // - detecting the canary event signifying the fanotify listener is up
        //   and polling before continuing with the `touch_fn`
        // - filtering out canary events so that they don't reach the mapper
        var fly_1 = Listener2MapperFly {
            .ha7r = ha7r,
            .listener_q = fan_event_q_listener,
            .mapper_q = fan_event_q_mapper,
            .tmpfs_path = tmpfs_path
        };

        var mapper = FanotifyEventMapper.init(
            ha7r, &db, fan_event_q_mapper, fs_event_q_mapper
        );
        defer mapper.join();

        var worker = FsEventWorker.init(ha7r, &db, fs_event_q_worker);
        var cleanup_worker_on_err = true;
        // we manually join the worker later so only `errdefer` required
        errdefer if (cleanup_worker_on_err) worker.join();

        var cx = Context {
            .ha7r = ha7r,
            .sa7r = sa7r,
            .db = &db, 
            .parser = &parser,
            .querier = &querier,
            .weaver = &weaver,
            .root_dir = &tmpfs_dir,
            .tmpfs_path = tmpfs_path
        };

        // _ = pre_poll_fn;
        try pre_poll_fn(&cx);

        try worker.start();
        try mapper.start();
        var fly_1_thread = try std.Thread.spawn(
            .{}, 
            Listener2MapperFly.threadFn, 
            .{ &fly_1 }
        );
        defer fly_1_thread.join();
        defer fly_1.die_signal.set();
        try listener.start();

        var timer = try std.time.Timer.start();

        // wait for canary
        while(true) {
            try tmpfs_dir.writeFile(canary_file_name, "MAGIC NUMBER");
            if (timer.read() > 3 * 1_000_000_000) {
                return error.CanaryTimeout;
            }
            // if canary is detected, we can be sure that the fanotify
            // listener is online and ready for our touches
            fly_1.canary_detected.timedWait(500_000) catch continue;
            break;
        }

        var expected_events_count = try touch_fn(&cx);
        // _ = touch_fn;
        // var expected_events_count: usize = 1;

        // This is fly_2. It runs on the current thread though that wasn't the
        // case at first. 
        // It is responsible for 2 things:
        //    - hand over events from the mapper to the worker
        //    - count the events and only continue to the `test_fn` after the 
        //      expected number of events have been detected
        var seen_event_count: usize = 0;
        timer.reset();
        while(
            seen_event_count < expected_events_count
        ) {
            if (timer.read() > 1 * 1_000_000_000) {
                println("got {} events at timeout", .{ seen_event_count });
                return error.TimeoutEvents;
            }
            if (fs_event_q_mapper.getTimed(500_000)) |node| {
                // println("fly 2 got event: {}", .{ node.data });
                // defer self.ha7r.destroy(node);
                // defer node.data.deinit(self.ha7r);
                fs_event_q_worker.put(node);
                seen_event_count += 1;
            } else |_| {
                // _ = err;
                std.Thread.yield() catch @panic("ThreadYieldErr");
            }
        }

        // important: wait for worker to bo done handling the events before we
        // test
        worker.join();
        cleanup_worker_on_err = false;

        // _ = test_fn;
        try test_fn(cx);
    }
};

// TODO: cases to test
//
// [x] file created/delete/move/write/change attrib
// [ ] dir create/delete/move/write/change attrib
// [ ] hard link create/delete/move
// [ ] soft link create/delete/move

test "FanotifyWorker.fileCreated" {
    if (builtin.single_threaded) return error.SkipZigTest;
    if (!isElevated()) return error.SkipZigTest;

    const ha7r = std.testing.allocator;

    const parent_dir = "takethemoney";
    const file_name = "you_fool";
    const target_path = parent_dir ++ "/" ++ file_name;
    // const file_name = "money";
    // const target_path = file_name;

    const actions = struct {
        const Context = TestFanotifyWorker.Context;

        fn prePoll(cx: *Context) anyerror!void {
            try cx.root_dir.makeDir(parent_dir);
            var parent_abs_path = try cx.root_dir.realpathAlloc(cx.ha7r, parent_dir);
            defer cx.ha7r.free(parent_abs_path);
            var parent_entry = try mod_treewalking.entryFromAbsolutePath(parent_abs_path);
            var ids = try Db.fileList2PlasticTree2Db(
                cx.ha7r, 
                &.{ parent_entry }, 
                cx.db
            );
            defer cx.ha7r.free(ids);
        }

        fn touch(cx: *Context) anyerror!usize {
            var file = try cx.root_dir.createFile(target_path, .{});
            defer file.close();
            return 1;
        }

        fn expect(cx: Context) anyerror!void {
            var db_entry = try cx.queryOne(file_name);
            var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
            const actual_path = cx.actualPath(target_path);
            try std.testing.expectEqualSlices(u8, actual_path, db_path);
            const actual_entry = try mod_treewalking.entryFromAbsolutePath(actual_path);
            try std.testing.expectEqual(
                actual_entry.conv(
                    Db.Id, []const u8,
                    db_entry[1].parent, actual_entry.name
                ), 
                db_entry[1].clone(actual_entry.name)
            );
        }
    };
    try TestFanotifyWorker.run(
        ha7r,
        actions.prePoll,
        actions.touch,
        actions.expect,
    );
}

test "FanotifyWorker.fileDeleted" {
    if (builtin.single_threaded) return error.SkipZigTest;
    if (!isElevated()) return error.SkipZigTest;

    const ha7r = std.testing.allocator;


    const parent_dir = "takethemoney";
    const file_name = "you_fool";
    const target_path = parent_dir ++ "/" ++ file_name;
    // const file_name = "money";
    // const target_path = file_name;

    const actions = struct {
        const Context = TestFanotifyWorker.Context;

        fn prePoll(cx: *Context) anyerror!void {
            try cx.root_dir.makeDir(parent_dir);
            var file = try cx.root_dir.createFile(target_path, .{});
            defer file.close();
            var abs_path = try cx.root_dir.realpathAlloc(cx.ha7r, target_path);
            defer cx.ha7r.free(abs_path);
            var entry = try mod_treewalking.entryFromAbsolutePath(abs_path);
            var ids = try Db.fileList2PlasticTree2Db(
                cx.ha7r, 
                &.{ entry }, 
                cx.db
            );
            defer cx.ha7r.free(ids);

            var db_entry = try cx.queryOne(file_name);
            var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
            try std.testing.expectEqualSlices(u8, cx.actualPath(target_path), db_path);
        }

        fn touch(cx: *Context) anyerror!usize {
            try cx.root_dir.deleteFile(target_path);
            return 1;
        }

        fn expect(cx: Context) anyerror!void {
            try cx.queryNone(file_name);
        }
    };
    try TestFanotifyWorker.run(
        ha7r,
        actions.prePoll,
        actions.touch,
        actions.expect,
    );
}

test "FanotifyWorker.dirDeleted" {
    if (builtin.single_threaded) return error.SkipZigTest;
    if (!isElevated()) return error.SkipZigTest;

    const ha7r = std.testing.allocator;


    const root_name = "dogdog";
    const file1_name = "backwards";
    const file1_path = root_name++"/"++file1_name;
    const dir1_name = "red";
    const dir1_path = root_name++"/"++dir1_name;
    const dir2_name = "leg";
    const dir2_path = dir1_path++"/"++dir2_name;
    const file2_name = "money";
    const file2_path = dir2_path++"/"++file2_name;

    const actions = struct {
        const Context = TestFanotifyWorker.Context;

        fn prePoll(cx: *Context) anyerror!void {
            try cx.root_dir.makeDir(root_name);
            (try cx.root_dir.createFile(file1_path, .{})).close();
            try cx.root_dir.makeDir(dir1_path);
            try cx.root_dir.makeDir(dir2_path);
            (try cx.root_dir.createFile(file2_path, .{})).close();
            var abs_path1 = try cx.root_dir.realpathAlloc(cx.ha7r, file1_path);
            defer cx.ha7r.free(abs_path1);
            var entry1 = try mod_treewalking.entryFromAbsolutePath(abs_path1);

            var abs_path2 = try cx.root_dir.realpathAlloc(cx.ha7r, file2_path);
            defer cx.ha7r.free(abs_path2);
            var entry2 = try mod_treewalking.entryFromAbsolutePath(abs_path2);

            var ids = try Db.fileList2PlasticTree2Db(
                cx.ha7r, 
                &.{ entry1, entry2 }, 
                cx.db
            );
            defer cx.ha7r.free(ids);

            var db_entry = try cx.queryOne(file1_name);
            var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
            try std.testing.expectEqualSlices(u8, cx.actualPath(file1_path), db_path);
            db_entry = try cx.queryOne(file2_name);
            db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
            try std.testing.expectEqualSlices(u8, cx.actualPath(file2_path), db_path);
        }

        fn touch(cx: *Context) anyerror!usize {
            try cx.root_dir.deleteTree(root_name);
            return 1;
        }

        fn expect(cx: Context) anyerror!void {
            try cx.queryNone(root_name);
            try cx.queryNone(file1_name);
            try cx.queryNone(file2_name);
            try cx.queryNone(dir1_name);
            try cx.queryNone(dir2_name);
        }
    };
    try TestFanotifyWorker.run(
        ha7r,
        actions.prePoll,
        actions.touch,
        actions.expect,
    );
}

test "FanotifyWorker.fileMoved" {
    if (builtin.single_threaded) return error.SkipZigTest;
    if (!isElevated()) return error.SkipZigTest;

    const ha7r = std.testing.allocator;


    const root_name = "dogdog";
    const file1_name = "backwards";
    const file1_path = root_name++"/"++file1_name;
    const dir1_name = "red";
    const dir1_path = root_name++"/"++dir1_name;
    const dir2_name = "leg";
    const dir2_path = dir1_path++"/"++dir2_name;
    const target_name = "money";
    const target_path = dir2_path++"/"++target_name;

    const actions = struct {
        const Context = TestFanotifyWorker.Context;

        fn prePoll(cx: *Context) anyerror!void {
            try cx.root_dir.makeDir(root_name);
            (try cx.root_dir.createFile(file1_path, .{})).close();
            try cx.root_dir.makeDir(dir1_path);
            try cx.root_dir.makeDir(dir2_path);
            var abs_path1 = try cx.root_dir.realpathAlloc(cx.ha7r, file1_path);
            defer cx.ha7r.free(abs_path1);
            var entry1 = try mod_treewalking.entryFromAbsolutePath(abs_path1);

            var ids = try Db.fileList2PlasticTree2Db(
                cx.ha7r, 
                &.{ entry1 }, 
                cx.db
            );
            defer cx.ha7r.free(ids);

            var db_entry = try cx.queryOne(file1_name);
            var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
            try std.testing.expectEqualSlices(u8, cx.actualPath(file1_path), db_path);
            // var db_entry = try cx.queryOne(target_path);
            // // try std.testing.expectEqual(evt_entry, db_entry.clone(evt_entry.name));
        }

        fn touch(cx: *Context) anyerror!usize {
            try cx.root_dir.rename(file1_path, target_path);
            return 1;
        }

        fn expect(cx: Context) anyerror!void {
            try cx.queryNone(root_name);
            try cx.queryNone(file1_name);

            var db_entry = try cx.queryOne(target_name);
            var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
            try std.testing.expectEqualSlices(u8, cx.actualPath(target_path), db_path);
        }
    };
    try TestFanotifyWorker.run(
        ha7r,
        actions.prePoll,
        actions.touch,
        actions.expect,
    );
}

test "FanotifyWorker.dirMoved" {
    if (builtin.single_threaded) return error.SkipZigTest;
    if (!isElevated()) return error.SkipZigTest;

    const ha7r = std.testing.allocator;

    const root_name = "dogdog";
    const target_name = "co4croah";
    const file1_name = "backwards";
    const file1_path = root_name++"/"++file1_name;
    const file1_target_path = target_name++"/"++file1_name;
    const dir1_name = "red";
    const dir1_path = root_name++"/"++dir1_name;
    const dir1_target_path = target_name++"/"++dir1_name;
    const dir2_name = "leg";
    const dir2_path = dir1_path++"/"++dir2_name;
    const dir2_target_path = dir1_target_path++"/"++dir2_name;
    const file2_name = "money";
    const file2_path = dir2_path++"/"++file2_name;
    const file2_target_path = dir2_target_path++"/"++file2_name;

    const actions = struct {
        const Context = TestFanotifyWorker.Context;

        fn prePoll(cx: *Context) anyerror!void {
            try cx.root_dir.makeDir(root_name);
            (try cx.root_dir.createFile(file1_path, .{})).close();
            try cx.root_dir.makeDir(dir1_path);
            try cx.root_dir.makeDir(dir2_path);
            var abs_path1 = try cx.root_dir.realpathAlloc(cx.ha7r, file1_path);
            defer cx.ha7r.free(abs_path1);
            var entry1 = try mod_treewalking.entryFromAbsolutePath(abs_path1);

            var abs_path2 = try cx.root_dir.realpathAlloc(cx.ha7r, file2_path);
            defer cx.ha7r.free(abs_path2);
            var entry2 = try mod_treewalking.entryFromAbsolutePath(abs_path2);

            var ids = try Db.fileList2PlasticTree2Db(
                cx.ha7r, 
                &.{ entry1, entry2 }, 
                cx.db
            );
            defer cx.ha7r.free(ids);

            var db_entry = try cx.queryOne(file1_name);
            var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
            try std.testing.expectEqualSlices(u8, cx.actualPath(file1_path), db_path);
            db_entry = try cx.queryOne(file2_name);
            db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
            try std.testing.expectEqualSlices(u8, cx.actualPath(file2_path), db_path);
        }

        fn touch(cx: *Context) anyerror!usize {
            try cx.root_dir.rename(root_name, target_name);
            return 1;
        }

        fn expect(cx: Context) anyerror!void {
            {
                try cx.queryNone(root_name);
                var db_entry = try cx.queryOne(target_name);
                var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
                try std.testing.expectEqualSlices(u8, cx.actualPath(root_name), db_path);
            }
            {
                try cx.queryNone(file1_path);
                var db_entry = try cx.queryOne(file1_target_path);
                var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
                try std.testing.expectEqualSlices(u8, cx.actualPath(file1_target_path), db_path);
            }
            {
                try cx.queryNone(dir1_path);
                var db_entry = try cx.queryOne(dir1_target_path);
                var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
                try std.testing.expectEqualSlices(u8, cx.actualPath(dir1_target_path), db_path);
            }
            {
                try cx.queryNone(dir2_path);
                var db_entry = try cx.queryOne(dir2_target_path);
                var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
                try std.testing.expectEqualSlices(u8, cx.actualPath(dir2_target_path), db_path);
            }
            {
                try cx.queryNone(file2_path);
                var db_entry = try cx.queryOne(file2_target_path);
                var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
                try std.testing.expectEqualSlices(u8, cx.actualPath(file2_target_path), db_path);
            }
        }
    };
    try TestFanotifyWorker.run(
        ha7r,
        actions.prePoll,
        actions.touch,
        actions.expect,
    );
}

test "FanotifyWorker.fileModified" {
    if (builtin.single_threaded) return error.SkipZigTest;
    if (!isElevated()) return error.SkipZigTest;

    const ha7r = std.testing.allocator;

    const root_name = "dogdog";
    const file1_name = "backwards";
    const file1_path = root_name++"/"++file1_name;
    const file2_name = "skag";
    const file2_path = root_name++"/"++file2_name;

    const actions = struct {
        const Context = TestFanotifyWorker.Context;

        fn prePoll(cx: *Context) anyerror!void {
            try cx.root_dir.makeDir(root_name);
            (try cx.root_dir.createFile(file1_path, .{})).close();
            (try cx.root_dir.createFile(file2_path, .{})).close();

            var abs_path1 = try cx.root_dir.realpathAlloc(cx.ha7r, file1_path);
            defer cx.ha7r.free(abs_path1);
            var entry1 = try mod_treewalking.entryFromAbsolutePath(abs_path1);

            var abs_path2 = try cx.root_dir.realpathAlloc(cx.ha7r, file2_path);
            defer cx.ha7r.free(abs_path2);
            var entry2 = try mod_treewalking.entryFromAbsolutePath(abs_path2);

            var ids = try Db.fileList2PlasticTree2Db(
                cx.ha7r, 
                &.{ entry1, entry2 }, 
                cx.db
            );
            defer cx.ha7r.free(ids);

            var db_entry = try cx.queryOne(file1_name);
            var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
            try std.testing.expectEqualSlices(u8, cx.actualPath(file1_path), db_path);
            db_entry = try cx.queryOne(file2_name);
            db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
            try std.testing.expectEqualSlices(u8, cx.actualPath(file2_path), db_path);
        }

        fn touch(cx: *Context) anyerror!usize {
            try cx.root_dir.writeFile(file1_path, "stuff");

            var file = try cx.root_dir.openFile(file2_path, .{});
            defer file.close();
            try file.setPermissions(
                std.fs.File.Permissions{ .inner = std.fs.File.PermissionsUnix.unixNew(777) }
            );
            return 2;
        }

        fn expect(cx: Context) anyerror!void {
            {
                var db_entry = try cx.queryOne(file1_name);
                var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
                const actual_path = cx.actualPath(file1_path);
                try std.testing.expectEqualSlices(u8, actual_path, db_path);
                const actual_entry = try mod_treewalking.entryFromAbsolutePath(actual_path);
                try std.testing.expectEqual(
                    actual_entry.conv(
                        Db.Id, []const u8,
                        db_entry[1].parent, actual_entry.name
                    ), 
                    db_entry[1].clone(actual_entry.name)
                );
            }
            {
                var db_entry = try cx.queryOne(file2_name);
                var db_path = try cx.weaver.pathOf(cx.db, db_entry[0], '/');
                const actual_path = cx.actualPath(file2_path);
                try std.testing.expectEqualSlices(u8, actual_path, db_path);
                const actual_entry = try mod_treewalking.entryFromAbsolutePath(actual_path);
                try std.testing.expectEqual(
                    actual_entry.conv(
                        Db.Id, []const u8,
                        db_entry[1].parent, actual_entry.name
                    ), 
                    db_entry[1].clone(actual_entry.name)
                );
            }
        }
    };
    try TestFanotifyWorker.run(
        ha7r,
        actions.prePoll,
        actions.touch,
        actions.expect,
    );
}
