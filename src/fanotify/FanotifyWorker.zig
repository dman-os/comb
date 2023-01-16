const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mod_utils = @import("../utils.zig");
const println = mod_utils.println;
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
        .listener  = FanotifyEventListener.init(ha7r, config, fan_event_q),
        .mapper = FanotifyEventMapper.init(ha7r, db, fan_event_q, fs_event_q),
        .worker = FsEventWorker.init(ha7r, db, fs_event_q)
    };
}

pub fn deinit(self: *@This()) void {
    self.listener.deinit();
    self.mapper.deinit();
    self.worker.deinit();
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

    fn deinit(self: *@This()) void {
        self.die_signal.set();
        if (self.thread) |thread| {
            thread.detach();
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
    fileDeleted,
    dirDeleted,
    fileMoved,
    dirMoved,
    fileModified,

    pub const FileCreated = struct {
        timestamp: i64,
        entry: FsEntry(Db.Id, []const u8),

        fn deinit(self: *@This(), ha7r: Allocator) void {
            ha7r.free(self.entry.name);
        }
    };

    fn deinit(self: *@This(), ha7r: Allocator) void {
        switch(self.*) {
            .fileCreated => |*event| {
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

    fn deinit(self: *@This()) void {
        self.die_signal.set();
        if (self.thread) |thread| {
            thread.detach();
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
        while(true) {
            if (self.die_signal.isSet()) {
                break;
            }
            if (count > 0 and count % 1_000 == 0) {
                std.log.info("handled {} events", .{ count });
            }
            if (self.fan_event_q.getTimed(500_000_000)) |node| {
                defer self.ha7r.destroy(node);
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
        event_in: FanotifyEvent
    ) std.mem.Allocator.Error!void {
        var event = event_in;
        std.log.debug("evt: {any}", .{ event });
        defer event.deinit(self.ha7r);
        //const FAN = FAN;
        // std.debug.todo("append");
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
            return null;
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
            return null;
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

    fn tryToFileCreatedEvent(self: *@This(), event: FanotifyEvent) !FsEvent.FileCreated {
        const name = event.name orelse return error.NameIsNull;
        const dir = event.dir orelse return error.DirIsNull;
        const parent_id = (try self.idAtPath(dir)) orelse return error.ParentNotFound;
        std.debug.assert(dir[dir.len - 1] != '/'); // FIXME: remove this
        var full_path = try std.mem.concatWithSentinel(
            self.ha7r,
            u8,
            &.{dir, "/", name},
            0
        );
        defer self.ha7r.free(full_path);
        const meta = mod_treewalking.metaNoFollow(0, &(try std.os.toPosixPath(full_path))) 
            catch |err| return switch(err) {
                std.os.OpenError.FileNotFound => error.NeutrinoDuringMeta,
                else => err
            };
        return FsEvent.FileCreated {
            .timestamp = event.timestamp,
            .entry = FsEntry(Db.Id, []const u8).fromMeta(
                try self.ha7r.dupe(u8, name),
                parent_id,
                std.mem.count(u8, full_path, "/"),
                meta,
            )
        };
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

    fn deinit(self: *@This()) void {
        self.die_signal.set();
        if (self.thread) |thread| {
            thread.detach();
        }
        self.querier.deinit();
    }

    fn start(self: *@This()) !void {
        self.thread = try std.Thread.spawn(.{}, @This().threadFn, .{ self });
    }

    fn threadFn(self: *@This()) !void {
        while(true) {
            if (self.die_signal.isSet()) {
                break;
            }
            if (self.event_q.getTimed(500_000_000)) |node| {
                defer self.ha7r.destroy(node);
                try self.handleEvent(node.data);
            } else |_| {
                // _ = err;
                std.Thread.yield() catch @panic("ThreadYieldErr");
            }
        }
    }

    fn handleEvent(
        self: *@This(), 
        event: FsEvent
    ) std.mem.Allocator.Error!void {
        _ = self;
        println("got event: {}",.{ event });
    }
};
