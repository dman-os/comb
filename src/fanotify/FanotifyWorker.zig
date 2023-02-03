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
    dirMoved,
    fileModified: FileModified,

    pub const FileCreated = struct {
        timestamp: i64,
        entry: FsEntry(Db.Id, []const u8),

        fn deinit(self: *@This(), ha7r: Allocator) void {
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
        id: Db.Id,
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
            .fileModified => |*event| {
                event.deinit(ha7r);
            },
            else => {}
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
    thread: ?std.Thread = null,
    db: *Db,
    querier: Db.Quexecutor,
    reader: Db.Reader = .{},
    weaver: Db.FullPathWeaver = .{},
    fan_event_q: *Queue(FanotifyEvent),
    fs_event_q: *Queue(FsEvent),
    die_signal: std.Thread.ResetEvent = .{},

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
        self.reader.deinit(self.db);
        self.weaver.deinit(self.ha7r);
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
        defer std.log.info(@typeName(FanotifyEventMapper) ++ " handled event: {any}", .{ event });
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
        const parent_id = (try self.idAtPath(dir)) orelse return error.ParentNotFound;
        var full_path = try std.fs.path.join(
            self.ha7r,
            &.{dir, name},
        );
        defer self.ha7r.free(full_path);
        const entry = mod_treewalking.entryFromAbsolutePath(full_path)
            catch |err| return switch(err) {
                std.os.OpenError.FileNotFound => error.NeutrinoDuringMeta,
                else => err
            };
        return FsEvent.FileCreated {
            .timestamp = event.timestamp,
            .entry = entry.conv(
                Db.Id, []const u8,
                parent_id, try self.ha7r.dupe(u8, entry.name),
            )
        };
    }

    fn tryToFileDeletedEvent(self: *@This(), event: FanotifyEvent) !FsEvent.FileDeleted {
        _ = self;
        const name = event.name orelse return error.NameIsNull;
        const dir = event.dir orelse return error.DirIsNull;
        return FsEvent.FileDeleted {
            .timestamp = event.timestamp,
            .name = name,
            .dir = dir,
        };
    }

    fn tryToFileModifiedEvent(self: *@This(), event: FanotifyEvent) !FsEvent.FileModified {
        return try self.tryToFileCreatedEvent(event);
    }

    fn idAtPath(self: *@This(), path: []const u8) !?Db.Id {
        var parser = Query.Parser{};
        defer parser.deinit(self.ha7r);
        var query = parser.parse(self.ha7r, path) catch @panic("error parsing path to query");
        defer query.deinit(self.ha7r);
        // var parents = try self.querier.query(&query);
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

    fn assertNeutrino(self: *@This(), event: FanotifyEvent) void {
        _ = self;
        _ = event;
    }
};

const FsEventWorker = struct {
    ha7r: Allocator,
    db: *Db,
    thread: ?std.Thread = null,
    querier: Db.Quexecutor,
    event_q: *Queue(FsEvent),
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
        defer println(@typeName(FsEventWorker) ++ " handled event: {}", .{ fs_event });
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
        _ = try self.db.fileCreated(&event.entry);
    }
};

const TestFsEventWorker = struct {
    const Context = struct {
        ha7r: Allocator,
        sa7r: mod_mmap.SwapAllocator,
        db: *Db,
        parser: *Query.Parser,
        querier: *Db.Quexecutor,
        reader: *Db.Reader,
        payload: ?*anyopaque = null,

        fn query(cx: @This(), query_str: []const u8) ![]const Db.Id {
            var parsed_query = try cx.parser.parse(cx.ha7r, query_str);
            defer parsed_query.deinit(cx.ha7r);
            return try cx.querier.query(&parsed_query);
        }

        fn query_one(cx: @This(), query_str: []const u8) !*Db.Entry {
            const ids = try cx.query(query_str);
            try std.testing.expectEqual(@as(usize, 1), ids.len);

            return try cx.reader.get(cx.db, ids[0]);
        }
    };

    fn run(
        ha7r: Allocator,
        events_fn: *const fn (cx: Context) anyerror![]FsEvent,
        test_fn: *const fn (cx: Context, events: []const FsEvent) anyerror!void
    ) anyerror!void {
        var tmp_dir = std.testing.tmpDir(.{});
        defer tmp_dir.cleanup();

        // var db_path = try tmp_dir.dir.realpathAlloc(ha7r, "comb.db");
        var db_path = try std.fs.path.join(
            ha7r, &.{ try mod_utils.fdPath(tmp_dir.dir.fd), "comb.db" }
        );
        defer ha7r.free(db_path);
        
        var mmap_pager = try mod_mmap.MmapPager.init(ha7r, db_path, .{});
        defer mmap_pager.deinit();

        var lru = try mod_mmap.LRUSwapCache.init(ha7r, mmap_pager.pager(), 1);
        defer lru.deinit();

        var pager = lru.pager();

        var ma7r = mod_mmap.PagingSwapAllocator(.{}).init(ha7r, pager);
        defer ma7r.deinit();

        var sa7r = ma7r.allocator();

        var db = Db.init(ha7r, pager, sa7r, .{ });
        defer db.deinit();

        // var fan_event_q = try ha7r.create(Queue(FanotifyEvent));
        // fan_event_q.* = Queue(FanotifyEvent).init();
        // defer ha7r.destroy(fan_event_q);
        var fs_event_q = try ha7r.create(Queue(FsEvent));
        fs_event_q.* = Queue(FsEvent).init();
        defer ha7r.destroy(fs_event_q);

        // var mapper = FanotifyEventMapper.init(ha7r, db, fan_event_q, fs_event_q);
        // defer mapper.deinit();

        var worker = FsEventWorker.init(ha7r, &db, fs_event_q);
        defer worker.join();
        var parser = Query.Parser{};
        defer parser.deinit(ha7r);

        var querier = Db.Quexecutor.init(&db);
        defer querier.deinit();

        var reader = db.reader();
        defer reader.deinit(&db);

        const cx = Context { 
            .ha7r = ha7r,
            .sa7r = sa7r,
            .db = &db, 
            .parser = &parser,
            .querier = &querier,
            .reader = &reader,
        };
        var events = try events_fn(cx);
        defer {
            for (events) |*event| {
                event.deinit(ha7r);
            }
            ha7r.free(events);
        }

        for (events) |event| {
            try worker.handleEvent(event);
        }

        try test_fn(cx, events);
    }
};

test "FsEventWorker.fileCreated" {
    if (builtin.single_threaded) return error.SkipZigTest;

    const ha7r = std.testing.allocator;
    const target_path = "/hot/in/it";

    const closures = struct {
        const Context = TestFsEventWorker.Context;
        fn events_fn(cx: Context) ![]FsEvent {
            var ids = try Db.fileList2PlasticTree2Db(
                cx.ha7r, 
                blk: { 
                    var parent = Db.genRandFile(
                        std.fs.path.dirname(target_path) orelse "/"[0..]
                    );
                    parent.kind = .Directory;
                    break :blk &.{ parent }; 
                }, 
                cx.db,
            );
            defer cx.ha7r.free(ids);

            const parent_id = ids[0];

            var entry = Db.genRandFile(target_path)
                .conv(
                    Db.Id, 
                    []const u8, 
                    parent_id, 
                    try cx.ha7r.dupe(u8, std.fs.path.basename(target_path))
                );

            var event = FsEvent {
                .fileCreated = FsEvent.FileCreated {
                    .timestamp = std.time.timestamp(),
                    .entry = entry
                }
            };
            return try cx.ha7r.dupe(FsEvent, &.{ event });
        }

        fn test_fn(cx: Context, events: []const FsEvent) !void {
            const evt_entry = events[0].fileCreated.entry;
            var db_entry = try cx.query_one(target_path);

            try std.testing.expectEqual(evt_entry, db_entry.clone(evt_entry.name));
            const dbName = try cx.sa7r.swapIn(db_entry.name);
            defer cx.sa7r.swapOut(db_entry.name);
            try std.testing.expectEqualSlices(u8, evt_entry.name, dbName);
        }
    };

    try TestFsEventWorker.run(
        ha7r,
        closures.events_fn,
        closures.test_fn,
    );
}

test "FsEventWorker.fileDeleted" {
    if (true) return error.SkipZigTest;
    if (builtin.single_threaded) return error.SkipZigTest;

    const ha7r = std.testing.allocator;
    const target_path = "/hot/in/it";
    const closures = struct {
        const Context = TestFsEventWorker.Context;

        fn events_fn(cx: Context) ![]FsEvent {
            var ids = try Db.fileList2PlasticTree2Db(
                cx.ha7r, 
                &.{ Db.genRandFile(target_path) }, 
                cx.db,
            );
            defer cx.ha7r.free(ids);

            const dir = try cx.ha7r.dupe(
                u8, std.fs.path.dirname(target_path) orelse "/"[0..]
            );
            const name = try cx.ha7r.dupe(u8, std.fs.path.basename(target_path)); 
            var event = FsEvent {
                .fileDeleted = FsEvent.FileDeleted {
                    .timestamp = std.time.timestamp(),
                    .dir = dir,
                    .name = name
                }
            };
            return try cx.ha7r.dupe(FsEvent, &.{ event });
        }

        fn test_fn(cx: Context, events: []const FsEvent) !void {
            _ = events;
            var ids = try cx.query(target_path);
            try std.testing.expectEqual(@as(usize, 0), ids.len);
        }
    };

    try TestFsEventWorker.run(
        ha7r,
        closures.events_fn,
        closures.test_fn,
    );
}

test "FsEventWorker.dirDeleted" {
    if (true) return error.SkipZigTest;
    if (builtin.single_threaded) return error.SkipZigTest;

    const ha7r = std.testing.allocator;
    const target_path = "/just";
    const closures = struct {
        const Context = TestFsEventWorker.Context;

        fn events_fn(cx: Context) ![]FsEvent {
            var ids = try Db.fileList2PlasticTree2Db(
                cx.ha7r, 
                &.{ 
                    Db.genRandFile("/just/relax/"),
                    Db.genRandFile("/please/relax/"),
                    Db.genRandFile("/just/go/to/sleep"),
                }, 
                cx.db,
            );
            defer cx.ha7r.free(ids);

            const dir = try cx.ha7r.dupe(
                u8, std.fs.path.dirname(target_path) orelse "/"[0..]
            );
            const name = try cx.ha7r.dupe(u8, std.fs.path.basename(target_path)); 
            var event = FsEvent {
                .dirDeleted = FsEvent.FileDeleted {
                    .timestamp = std.time.timestamp(),
                    .dir = dir,
                    .name = name
                }
            };
            return try cx.ha7r.dupe(FsEvent, &.{ event });
        }

        fn test_fn(cx: Context, events: []const FsEvent) !void {
            _ = events;
            var ids = try cx.query(target_path);
            try std.testing.expectEqual(@as(usize, 0), ids.len);
        }
    };

    try TestFsEventWorker.run(
        ha7r,
        closures.events_fn,
        closures.test_fn,
    );
}

test "FsEventWorker.fileMoved" {
    if (true) return error.SkipZigTest;
    if (builtin.single_threaded) return error.SkipZigTest;

    const ha7r = std.testing.allocator;
    const target_path = "/doll/eyes/doll/part";
    const new_path = "/mine/is/forever";
    const closures = struct {
        const Context = TestFsEventWorker.Context;

        fn events_fn(cx: Context) ![]FsEvent {
            const new_dir = try cx.ha7r.dupe(
                u8, std.fs.path.dirname(new_path) orelse "/"[0..]
            );
            const new_name = try cx.ha7r.dupe(u8, std.fs.path.basename(new_path)); 

            const old_dir = try cx.ha7r.dupe(
                u8, std.fs.path.dirname(target_path) orelse "/"[0..]
            );
            const old_name = try cx.ha7r.dupe(u8, std.fs.path.basename(target_path)); 

            var ids = try Db.fileList2PlasticTree2Db(
                cx.ha7r, 
                &.{ 
                    Db.genRandFile(target_path),
                    Db.genRandFile(new_path),
                }, 
                cx.db,
            );
            defer cx.ha7r.free(ids);
            var event = FsEvent {
                .fileMoved = FsEvent.FileMoved {
                    .timestamp = std.time.timestamp(),
                    .id = ids[0],
                    .old_dir = old_dir,
                    .old_name = old_name,
                    .new_dir = new_dir,
                    .new_name = new_name,
                }
            };
            return try cx.ha7r.dupe(FsEvent, &.{ event });
        }

        fn test_fn(cx: Context, events: []const FsEvent) !void {
            _ = events;
            _ = cx;
            // TODO:
            // var ids = try cx.query(target_path);
            // try std.testing.expectEqual(@as(usize, 0), ids.len);

            // var ids = try cx.query(new_path);
            // try std.testing.expectEqual(@as(usize, 1), ids.len);
        }
    };

    try TestFsEventWorker.run(
        ha7r,
        closures.events_fn,
        closures.test_fn,
    );
}

const TestFanotifyWorker = struct {
    const Context = struct {
        ha7r: Allocator,
        sa7r: mod_mmap.SwapAllocator,
        db: *Db,
        parser: *Query.Parser,
        querier: *Db.Quexecutor,
        reader: *Db.Reader,
        payload: ?*anyopaque = null,
        root_dir: *std.fs.Dir,

        fn query(cx: @This(), query_str: []const u8) ![]const Db.Id {
            var parsed_query = try cx.parser.parse(cx.ha7r, query_str);
            defer parsed_query.deinit(cx.ha7r);
            return try cx.querier.query(&parsed_query);
        }

        fn query_one(cx: @This(), query_str: []const u8) !*Db.Entry {
            const ids = try cx.query(query_str);
            if (ids.len > 1) { 
                return error.MoreThanOneResults; 
            }
            if (ids.len == 0) return error.EmptyResult;
            return try cx.reader.get(cx.db, ids[0]);
        }
    };

    const Listener2MapperFly = struct {
        ha7r: Allocator,
        listener_q: *Queue(FanotifyEvent),
        mapper_q: *Queue(FanotifyEvent),
        die_signal: std.Thread.ResetEvent = .{},
        seen_event_count: usize = 0,
        canary_detected: std.Thread.ResetEvent = .{},

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
                                println("setting canary", .{ });
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

    const Mapper2WorkerFly = struct {
        ha7r: Allocator,
        mapper_q: *Queue(FsEvent),
        worker_q: *Queue(FsEvent),
        die_signal: std.Thread.ResetEvent = .{},
        seen_event_count: usize = 0,

        fn threadFn(self: *@This()) !void {
            // println("fly 2 online", .{ });
            while(
                !self.die_signal.isSet()
            ) {
                if (self.mapper_q.getTimed(500_000)) |node| {
                    // println("fly 2 got event: {}", .{ node.data });
                    // defer self.ha7r.destroy(node);
                    // defer node.data.deinit(self.ha7r);
                    self.worker_q.put(node);
                    _ = @atomicRmw(
                        usize, &self.seen_event_count, .Add, 1, .SeqCst,
                    );
                } else |_| {
                    // _ = err;
                    std.Thread.yield() catch @panic("ThreadYieldErr");
                }
            }
        }
    };


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
        var reader = db.reader();
        defer reader.deinit(&db);

        // init the channels

        var fan_event_q_listener = try ha7r.create(Queue(FanotifyEvent));
        fan_event_q_listener.* = Queue(FanotifyEvent).init();
        defer ha7r.destroy(fan_event_q_listener);

        var fan_event_q_mapper = try ha7r.create(Queue(FanotifyEvent));
        fan_event_q_mapper.* = Queue(FanotifyEvent).init();
        defer ha7r.destroy(fan_event_q_mapper);

        var fs_event_q_mapper = try ha7r.create(Queue(FsEvent));
        fs_event_q_mapper.* = Queue(FsEvent).init();
        defer ha7r.destroy(fs_event_q_mapper);

        var fs_event_q_worker = try ha7r.create(Queue(FsEvent));
        fs_event_q_worker.* = Queue(FsEvent).init();
        defer ha7r.destroy(fs_event_q_worker);

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

        var fly_1 = Listener2MapperFly {
            .ha7r = ha7r,
            .listener_q = fan_event_q_listener,
            .mapper_q = fan_event_q_mapper,
        };

        var mapper = FanotifyEventMapper.init(
            ha7r, &db, fan_event_q_mapper, fs_event_q_mapper
        );
        defer mapper.join();

        var fly_2 = Mapper2WorkerFly  {
            .ha7r = ha7r,
            .mapper_q = fs_event_q_mapper,
            .worker_q = fs_event_q_worker,
        };

        var worker = FsEventWorker.init(ha7r, &db, fs_event_q_worker);
        defer worker.join();

        var cx = Context {
            .ha7r = ha7r,
            .sa7r = sa7r,
            .db = &db, 
            .parser = &parser,
            .querier = &querier,
            .reader = &reader,
            .root_dir = &tmpfs_dir
        };

        try pre_poll_fn(&cx);
        // _ = pre_poll_fn;

        try worker.start();
        var fly_2_thread = try std.Thread.spawn(
            .{}, 
            Mapper2WorkerFly.threadFn, 
            .{ &fly_2 }
        );
        defer fly_2_thread.join();
        defer fly_2.die_signal.set();
        try mapper.start();
        var fly_1_thread = try std.Thread.spawn(
            .{}, 
            Listener2MapperFly.threadFn, 
            .{ &fly_1 }
        );
        defer fly_1_thread.join();
        defer fly_1.die_signal.set();
        try listener.start();

        // wait for canary
        // if canary is detected, we can be sure that listener is online
        // and ready for our touches
        var timer = try std.time.Timer.start();
        while(true) {
            try tmpfs_dir.writeFile(canary_file_name, "MAGIC NUMBER");
            if (timer.read() > 3 * 1_000_000_000) {
                @panic("timeout waiting for canary");
            }
            fly_1.canary_detected.timedWait(500_000) catch continue;
            break;
        }

        var expected_events_count = try touch_fn(&cx);
        // _ = touch_fn;
        // var expected_events_count: usize = 1;

        timer.reset();
        while (
            @atomicLoad(usize, &fly_2.seen_event_count, .SeqCst)
            < 
            expected_events_count
        ) {
            std.Thread.yield() catch @panic("ThreadYieldErr");
            if (timer.read() > 3 * 1_000_000_000) {
                println("got {} events at timeout", .{fly_2.seen_event_count});
                @panic("timeout waiting for events");
            }
        }

        // _ = test_fn;
        try test_fn(cx);
    }
};

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

        fn pre_poll(cx: *Context) anyerror!void {
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
            var db_entry = try cx.query_one(file_name);
            // var db_entry = try cx.query_one(target_path);
            // // try std.testing.expectEqual(evt_entry, db_entry.clone(evt_entry.name));
            const dbName = try cx.sa7r.swapIn(db_entry.name);
            defer cx.sa7r.swapOut(db_entry.name);
            try std.testing.expectEqualSlices(u8, file_name, dbName);
        }
    };
    try TestFanotifyWorker.run(
        ha7r,
        actions.pre_poll,
        actions.touch,
        actions.expect,
    );
}