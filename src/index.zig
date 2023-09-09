const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.dbg;

const mod_treewalking = @import("treewalking.zig");
const FsEntry = mod_treewalking.FsEntry;

const mod_mmap = @import("mmap.zig");
const SwapList = mod_mmap.SwapList;
const Ptr = mod_mmap.SwapAllocator.Ptr;

const mod_plist = @import("plist.zig");

pub fn BST(
    comptime T: type,
    comptime Ctx: type,
    comptime cmp: fn (cx: Ctx, a: T, b: T) std.math.Order,
) type {
    return struct {
        const Self = @This();

        parent: ?*const Self = null,
        item: T,
        left_child: ?*Self = null,
        right_child: ?*Self = null,

        pub fn insert(self: *Self, cx: Ctx, in: *Self) void {
            switch (cmp(cx, self.item, in.item)) {
                .lt => {
                    // println("node {} is adding {} to left", .{ self.item, in.item });
                    if (self.left_child) |child| {
                        child.insert(cx, in);
                    } else {
                        self.left_child = in;
                        in.parent = self;
                    }
                },
                .eq, .gt => {
                    // println("node {} is adding {} to left", .{ self.item, in.item });
                    if (self.right_child) |child| {
                        child.insert(cx, in);
                    } else {
                        self.right_child = in;
                        in.parent = self;
                    }
                },
            }
        }

        pub fn find(self: *Self, cx: Ctx, val: T) ?*Self {
            switch (cmp(cx, self.item, val)) {
                .eq => {
                    return self;
                },
                .lt => {
                    if (self.left_child) |child| {
                        return child.find(cx, val);
                    } else {
                        return null;
                    }
                },
                .gt => {
                    // println("node {} is adding {} to left", .{ self.item, in.item });
                    if (self.right_child) |child| {
                        return child.find(cx, val);
                    } else {
                        return null;
                    }
                },
            }
        }

        pub fn initHeap(ha: Allocator, item: T) !*Self {
            var ptr = try ha.create(Self);
            ptr.* = Self{
                .item = item,
            };
            return ptr;
        }

        pub fn deinitHeap(ha: Allocator, self: *Self) void {
            defer ha.destroy(self);
            if (self.left_child) |child| {
                deinitHeap(ha, child);
            }
            if (self.right_child) |child| {
                deinitHeap(ha, child);
            }
        }
    };
}

test "BST.insert" {
    const MyBst = BST(
        usize,
        void,
        struct {
            fn cmp(cx: void, a: usize, b: usize) std.math.Order {
                _ = cx;
                return std.math.order(a, b);
            }
        }.cmp,
        // .{
        //     .extension = BSTConfig.heapExtension,
        // },
    );
    const ha = std.testing.allocator;

    const tree = try MyBst.initHeap(ha, 500);
    defer MyBst.deinitHeap(ha, tree);
    const children = [_]usize{ 123, 53, 98823, 123, 534, 123, 54, 7264, 21, 0, 23 };
    for (children) |val| {
        const child = try MyBst.initHeap(ha, val);
        tree.insert({}, child);
    }
    try std.testing.expect(tree.find({}, 500) != null);
    try std.testing.expect(tree.find({}, 0) != null);
    try std.testing.expect(tree.find({}, 54) != null);
    try std.testing.expect(tree.find({}, 1223) == null);
}

// const BTreeConfig = struct {
//     order: usize,
// };

// fn BTree(
//     comptime T: type,
//     comptime order: usize,
// ) type {
//      return struct {
//         const Node = union(enum) {
//             internal: struct {
//             },
//             leaf: struct {
//                 children: []T,
//             },
//         };
//         root: Node,
//     };
// }
