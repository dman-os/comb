const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn dbg(val: anytype) @TypeOf(val){
    std.debug.print("{any}\n", .{val});
    return val;
}

pub fn Appender(comptime T: type) type {
    return struct {
        const Self = @This();
        const Err = Allocator.Error;

        vptr: *anyopaque,
        func: fn (*anyopaque, T) Err!void,

        pub fn new(coll: anytype, func: fn (@TypeOf(coll), T) Err!void) Self {
            if (!comptime std.meta.trait.isSingleItemPtr(@TypeOf(coll))) {
                @compileError("was expecting single item pointer, got type = " ++ @typeName(@TypeOf(coll)));
            }
            return Self{
                .vptr = @ptrCast(*anyopaque, coll),
                .func = @ptrCast(fn (*anyopaque, T) Err!void, func),
            };
        }

        pub fn append(self: Self, item: T) Err!void {
            try self.func(self.vptr, item);
        }

        pub fn for_list(list: *std.ArrayList(T)) Self {
            return Self.new(list, std.ArrayList(T).append);
        }

        pub fn for_set(set: *std.AutoHashMap(T, void)) Self {
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
    var appender = Appender(u32).for_list(&list);
    try appender.append(10);
    try appender.append(20);
    try appender.append(30);
    try appender.append(40);
    try std.testing.expectEqualSlices(u32, ([_]u32{ 10, 20, 30, 40 })[0..], list.items);
}

test "Appender.set" {
    var set = std.AutoHashMap(u32, void).init(std.testing.allocator);
    defer set.deinit();
    var appender = Appender(u32).for_set(&set);
    try appender.append(10);
    try appender.append(20);
    try appender.append(30);
    try appender.append(40);
    inline for (([_]u32{ 10, 20, 30, 40 })[0..]) |item| {
        try std.testing.expect(set.contains(item));
    }
}
