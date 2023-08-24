const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn dbg(val: anytype) @TypeOf(val) {
    switch (@typeInfo(@TypeOf(val))) {
        .Pointer => |ptr| {
            if (ptr.size == .Slice)
                std.debug.print("{s}\n", .{val})
            else
                std.debug.print("{any}\n", .{val});
        },
        else => std.debug.print("{any}\n", .{val}),
    }
    return val;
}

pub fn println(comptime fmt: []const u8, args: anytype) void {
    var stderr_mutex = std.debug.getStderrMutex();
    stderr_mutex.lock();
    defer stderr_mutex.unlock();
    const stderr = std.io.getStdErr().writer();
    nosuspend stderr.print(fmt, args) catch return;
    nosuspend _ = stderr.write("\n") catch return;
}

pub fn Option(comptime T: type) type {
    return union(enum) {
        const Self = @This();
        some: T,
        none: void,

        pub const None = Self{ .none = {} };

        pub inline fn toNative(self: Self) ?T {
            return switch (self) {
                .some => |val| val,
                .none => null,
            };
        }
        pub inline fn fromNative(opt: ?T) Self {
            if (opt) |val| {
                return Self{ .some = val };
            } else {
                return Self.None;
            }
        }
    };
}

pub fn Appender(comptime T: type) type {
    return struct {
        const Self = @This();
        const Err = Allocator.Error;

        vptr: *anyopaque,
        func: *const fn (*anyopaque, T) Err!void,

        pub fn new(coll: anytype, func: *const fn (@TypeOf(coll), T) Err!void) Self {
            if (!comptime std.meta.trait.isSingleItemPtr(@TypeOf(coll))) {
                @compileError("was expecting single item pointer, got type = " ++ @typeName(@TypeOf(coll)));
            }
            return Self{
                .vptr = @as(*anyopaque, @constCast(@ptrCast(coll))),
                .func = @as(*const fn (*anyopaque, T) Err!void, @ptrCast(func)),
            };
        }

        pub const Curry = struct {
            pub const UnmanagedSet = struct {
                a7r: Allocator,
                set: *std.AutoHashMapUnmanaged(T, void),
                pub fn put(curry: *const @This(), item: T) !void {
                    try curry.set.put(curry.a7r, item, {});
                }
            };
            pub const UnamanagedList = struct {
                a7r: Allocator,
                list: *std.ArrayListUnmanaged(T),
                pub fn append(curry: *const @This(), item: T) !void {
                    try curry.list.append(curry.a7r, item);
                }
            };
        };

        pub fn append(self: Self, item: T) Err!void {
            try self.func(self.vptr, item);
        }

        pub fn forList(list: *std.ArrayList(T)) Self {
            return Self.new(list, std.ArrayList(T).append);
        }

        pub fn forSet(set: *std.AutoHashMap(T, void)) Self {
            const curry = struct {
                fn append(ptr: *std.AutoHashMap(T, void), item: T) !void {
                    try ptr.put(item, {});
                }
            };
            return Self.new(set, curry.append);
        }
    };
}

test "Appender.list" {
    var list = std.ArrayList(u32).init(std.testing.allocator);
    defer list.deinit();
    var appender = Appender(u32).forList(&list);
    try appender.append(10);
    try appender.append(20);
    try appender.append(30);
    try appender.append(40);
    try std.testing.expectEqualSlices(u32, ([_]u32{ 10, 20, 30, 40 })[0..], list.items);
}

test "Appender.set" {
    var set = std.AutoHashMap(u32, void).init(std.testing.allocator);
    defer set.deinit();
    var appender = Appender(u32).forSet(&set);
    try appender.append(10);
    try appender.append(20);
    try appender.append(30);
    try appender.append(40);
    inline for (([_]u32{ 10, 20, 30, 40 })[0..]) |item| {
        try std.testing.expect(set.contains(item));
    }
}

threadlocal var pathBufFdPath: [std.fs.MAX_PATH_BYTES]u8 = [_]u8{0} ** std.fs.MAX_PATH_BYTES;
/// Returns the absolute path of the given file handle. Allocate the returned
/// slice to heap before next usage of this function on the same thread or woe be u.
pub fn fdPath(fd: std.os.fd_t) ![]const u8 {
    // return try std.os.getFdPath(fd, &pathBuf);
    // const prefix = "/proc/self/fd/";
    // var fd_buf = prefix ++ ([_]u8{0} ** (128 - prefix.len));
    // var fbs = std.io.fixedBufferStream(&fd_buf[prefix.len..]);
    // std.fmt.formatInt(fd, 10, .lower, .{}, fbs.writer()) catch unreachable;
    // const fd_path = fd_buf[0..prefix.len + fbs.pos];
    // return std.fs.readLinkAbsolute(fd_path, &pathBuf);

    var fd_buf = [_]u8{0} ** 128;
    const fd_path = std.fmt.bufPrint(&fd_buf, "/proc/self/fd/{}", .{fd}) catch unreachable;
    return std.fs.readLinkAbsolute(fd_path, &pathBufFdPath);
}

threadlocal var pathBufPathJoin: [std.fs.MAX_PATH_BYTES]u8 = [_]u8{0} ** std.fs.MAX_PATH_BYTES;

///  Allocate the returned slice to heap before next usage of this function on
///  the same thread or woe be u.
pub fn pathJoin(paths: []const []const u8) []const u8 {
    var fba7r = std.heap.FixedBufferAllocator.init(&pathBufPathJoin);
    const full_thing = std.fs.path.join(
        fba7r.allocator(),
        paths,
    ) catch unreachable;
    return full_thing;
}

pub fn isElevated() bool {
    return std.os.linux.geteuid() == 0;
}

// fn Trait(
//     comptime required: []type,
//     // comptime required: fn (type) type,
// ) type {
//     return struct {
//         fn impl(comptime T: type) type {

//         }
//     };
// }
// const Swapalloc = Trait(struct {
// });

/// Modifies `std.atomic.Queue` to use `std.atomic.Condition` for efficeint
/// waiting
pub fn Mpmc(comptime T: type) type {
    return struct {
        pub const Self = @This();
        pub const Node = std.TailQueue(T).Node;
        const assert = std.debug.assert;

        head: ?*Node = null,
        tail: ?*Node = null,
        mutex: std.Thread.Mutex = .{},
        condition: std.Thread.Condition = .{},

        /// Initializes a new queue. The queue does not provide a `deinit()`
        /// function, so the user must take care of cleaning up the queue elements.
        pub fn init() Self {
            return Self{};
        }

        /// Appends `node` to the queue.
        /// The lifetime of `node` must be longer than lifetime of queue.
        pub fn put(self: *Self, node: *Node) void {
            node.next = null;

            self.mutex.lock();
            defer self.mutex.unlock();

            node.prev = self.tail;
            self.tail = node;
            if (node.prev) |prev_tail| {
                prev_tail.next = node;
            } else {
                assert(self.head == null);
                self.head = node;
            }
            self.condition.signal();
        }

        fn getInner(self: *Self) ?*Node {
            const head = self.head orelse return null;
            self.head = head.next;
            if (head.next) |new_head| {
                new_head.prev = null;
            } else {
                self.tail = null;
            }
            // This way, a get() and a remove() are thread-safe with each other.
            head.prev = null;
            head.next = null;
            return head;
        }

        /// Gets a previously inserted node or returns `null` if there is none.
        /// It is safe to `get()` a node from the queue while another thread tries
        /// to `remove()` the same node at the same time.
        pub fn getOrNull(self: *Self) ?*Node {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.getInner();
        }

        pub fn get(self: *Self) *Node {
            self.mutex.lock();
            defer self.mutex.unlock();
            while (self.head == null) {
                self.condition.wait(&self.mutex);
            }
            // FIXME: we get again if getInner returns null
            // implying that some other consumer got there
            // before we did. no clue how well this works
            return self.getInner() orelse self.get();
        }

        pub fn getTimed(self: *Self, timeout_ns: u64) error{Timeout}!*Node {
            self.mutex.lock();
            defer self.mutex.unlock();
            var timeout_ns_rem = timeout_ns;
            while (self.head == null) {
                if (timeout_ns_rem == 0) return error.Timeout;
                const start = std.time.nanoTimestamp();
                try self.condition.timedWait(&self.mutex, timeout_ns);
                const end = std.time.nanoTimestamp();
                const elapsed = @as(u64, @intCast(end - start));
                timeout_ns_rem = if (timeout_ns_rem > elapsed) timeout_ns_rem - elapsed else 0;
            }
            return self.getInner() orelse self.getTimed(timeout_ns_rem);
        }

        /// Prepends `node` to the front of the queue.
        /// The lifetime of `node` must be longer than the lifetime of the queue.
        pub fn unget(self: *Self, node: *Node) void {
            node.prev = null;

            self.mutex.lock();
            defer self.mutex.unlock();

            const opt_head = self.head;
            self.head = node;
            if (opt_head) |old_head| {
                node.next = old_head;
            } else {
                assert(self.tail == null);
                self.tail = node;
            }
        }

        /// Removes a node from the queue, returns whether node was actually removed.
        /// It is safe to `remove()` a node from the queue while another thread tries
        /// to `get()` the same node at the same time.
        pub fn remove(self: *Self, node: *Node) bool {
            self.mutex.lock();
            defer self.mutex.unlock();

            if (node.prev == null and node.next == null and self.head != node) {
                return false;
            }

            if (node.prev) |prev| {
                prev.next = node.next;
            } else {
                self.head = node.next;
            }
            if (node.next) |next| {
                next.prev = node.prev;
            } else {
                self.tail = node.prev;
            }
            node.prev = null;
            node.next = null;
            return true;
        }

        /// Returns `true` if the queue is currently empty.
        /// Note that in a multi-consumer environment a return value of `false`
        /// does not mean that `get` will yield a non-`null` value!
        pub fn isEmpty(self: *Self) bool {
            self.mutex.lock();
            defer self.mutex.unlock();
            return self.head == null;
        }
    };
}
