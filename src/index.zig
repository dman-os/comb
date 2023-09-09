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

pub const BSTConfig = struct {
    // link_to_parent: bool = true,
    extension: fn (type, type) type = noExtension,

    pub fn noExtension(comptime T: type, comptime Tree: type) type {
        _ = T;
        _ = Tree;
        return struct {};
    }
    pub fn heapExtension(comptime T: type, comptime Tree: type) type {
        return struct {
            pub fn initHeap(ha: Allocator, item: T) !*Tree {
                var ptr = try ha.create(Tree);
                ptr.* = Tree{
                    .item = item,
                };
                return ptr;
            }
            pub fn deinitHeap(ha: Allocator, self: *Tree) void {
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
};

pub fn BST(
    comptime T: type,
    comptime Ctx: type,
    comptime cmp: fn (cx: Ctx, a: T, b: T) std.math.Order,
    // comptime config: BSTConfig,
    // comptime init: fn (cx: Ctx, item: T, parent: *anyopaque) *anyopaque,
) type {
    return struct {
        const Self = @This();
        // pub usingnamespace config.extension(T, Self);

        // usingnamespace if (config.link_to_parent) struct {
        //     parent: ?*const Self = null,
        // } else struct {};
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
