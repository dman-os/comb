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

        item: T,
        parent: ?*Self = null,
        left_child: ?*Self = null,
        right_child: ?*Self = null,

        pub fn insert(self: *Self, cx: Ctx, in: *Self) void {
            switch (cmp(cx, in.item, self.item)) {
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

        pub fn transplant(self: *Self, in: ?*Self) void {
            if (self.parent) |parent| {
                if (self == parent.left_child) {
                    parent.left_child = in;
                } else {
                    parent.right_child = in;
                }
                if (in) |incoming| {
                    incoming.parent = parent;
                }
            }
        }

        pub fn delete(self: *Self) ?*Self {
            defer self.parent = null;
            defer self.right_child = null;
            defer self.left_child = null;
            if (self.left_child) |left| {
                if (self.right_child) |right| {
                    // find the successor which should be the min from
                    // the right branch
                    const succ = right.min();
                    if (succ.parent != self) {
                        // transplant it with its right child
                        // NOTE: it doesn't have a left child since it's a min
                        succ.transplant(succ.right_child);
                        succ.right_child = right;
                        right.parent = succ;
                    }
                    self.transplant(succ);
                    succ.left_child = left;
                    left.parent = succ;
                    return succ;
                } else {
                    self.transplant(left);
                    return left;
                }
            } else {
                self.transplant(self.right_child);
                return self.right_child orelse self.parent;
            }
        }

        pub fn find(self: *Self, cx: Ctx, val: T) ?*Self {
            switch (cmp(cx, val, self.item)) {
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

        pub fn min(self: *Self) *Self {
            if (self.left_child) |child| {
                return child.min();
            } else {
                return self;
            }
        }

        pub fn max(self: *Self) *Self {
            if (self.right_child) |child| {
                return child.max();
            } else {
                return self;
            }
        }

        pub fn predecessor(self: *Self) ?*Self {
            if (self.left_child) |child| {
                return child.max();
            }
            var x = self;
            var y = self.parent;
            while (true) {
                if (y) |parent| {
                    if (parent.left_child) |child| {
                        if (x == child) {
                            x = parent;
                            y = parent.parent;
                            continue;
                        }
                    }
                }
                break;
            }
            return y;
        }

        pub fn successor(self: *Self) ?*Self {
            if (self.right_child) |child| {
                return child.min();
            }
            var x = self;
            var y = self.parent;
            while (true) {
                if (y) |parent| {
                    if (parent.right_child) |child| {
                        if (x == child) {
                            x = parent;
                            y = parent.parent;
                            continue;
                        }
                    }
                }
                break;
            }
            return y;
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

    var tree = try MyBst.initHeap(ha, 500);
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
    try std.testing.expectEqual(@as(usize, 123), ((tree.find({}, 123) orelse unreachable).successor() orelse unreachable).item);
    try std.testing.expectEqual(@as(usize, 54), ((tree.find({}, 123) orelse unreachable).predecessor() orelse unreachable).item);
    try std.testing.expectEqual(@as(usize, 54), ((tree.find({}, 53) orelse unreachable).successor() orelse unreachable).item);
    try std.testing.expectEqual(@as(usize, 53), ((tree.find({}, 54) orelse unreachable).predecessor() orelse unreachable).item);
    try std.testing.expectEqual(@as(usize, 98823), tree.max().item);
    try std.testing.expectEqual(@as(usize, 0), tree.min().item);
    {
        const find = tree.find({}, 23) orelse unreachable;
        defer MyBst.deinitHeap(ha, find);
        try std.testing.expectEqual(@as(usize, 21), (find.delete() orelse unreachable).item);
    }
    {
        const find = tree;
        defer MyBst.deinitHeap(ha, find);
        tree = find.delete() orelse unreachable;
        try std.testing.expectEqual(@as(usize, 534), tree.item);
    }
    {
        const find = try MyBst.initHeap(ha, 500);
        defer MyBst.deinitHeap(ha, find);
        try std.testing.expect(find.delete() == null);
    }
}

// const BTreeConfig = struct {
//     order: usize = 12,
// };

// Order will be multiplied by to ensure an even value.
fn BTree(
    comptime T: type,
    comptime Ctx: type,
    comptime cmp: fn (cx: Ctx, a: T, b: T) std.math.Order,
    // comptime config: BTreeConfig,
    comptime order: usize,
) type {
    return struct {
        const Self = @This();
        const Node = union(enum) {
            // multiply by 2 to keep max children even and max keys odd
            const KeyArray = std.BoundedArray(T, (2 * order) - 1);
            const ChildrenArray = std.BoundedArray(*Node, 2 * order);
            const Leaf =struct {
                keys: KeyArray,
            };
            const Internal = struct {
                keys: KeyArray,
                children: ChildrenArray,

                fn splitChildIfFull(self: *@This(), ha: Allocator, child_ii: usize) !bool {
                    switch (self.children.slice()[child_ii].*) {
                        .leaf => |*node| {
                            if (node.keys.len == (2 * order) - 1) {
                                const right = try Node.fromSliceLeaf(ha, node.keys.slice()[order..]);
                                const mid = node.keys.get(order - 1);
                                node.keys.resize(order - 1) catch unreachable;
                                if (child_ii == self.children.len - 1) {
                                    self.keys.append(mid) catch unreachable;
                                    self.children.append(right) catch unreachable;
                                } else {
                                    self.keys.insert(child_ii, mid) catch unreachable;
                                    self.children.insert(child_ii, right) catch unreachable;
                                }
                                return true;
                            }
                            return false;
                        },
                        .internal => |*node| {
                            if (node.keys.len == (2 * order) - 1) {
                                const right = try Node.fromSliceInternal(ha, node.keys.slice()[order..], node.children.slice()[order..]);
                                node.children.resize(order) catch unreachable;
                                const mid = node.keys.get(order - 1);
                                node.keys.resize(order - 1) catch unreachable;
                                if (child_ii == self.children.len - 1) {
                                    self.keys.append(mid) catch unreachable;
                                    self.children.append(right) catch unreachable;
                                } else {
                                    self.keys.insert(child_ii, mid) catch unreachable;
                                    self.children.insert(child_ii, right) catch unreachable;
                                }
                                return true;
                            }
                            return false;
                        },
                    }
                    // println(
                    //     "splitting {*} left.len = {}, left.c.len = {?}, right.len = {}, right.c.len = {?} ",
                    //     .{
                    //         self,
                    //         self.keys.len,
                    //         if (self.children) |xx| xx.len else null,
                    //         right.keys.len,
                    //         if (right.children) |xx| xx.len else null
                    //     }
                    // );
                }
            };

            leaf: Leaf,
            internal: Internal,

            fn deinit(self: *Node, ha: Allocator) void {
                switch (self.*) {
                    .internal => |int| {
                        for (int.children.slice()) |child| {
                            child.deinit(ha);
                        }
                    },
                    else => {},
                }
                ha.destroy(self);
            }

            fn initLeaf(ha: Allocator, first: T) !*Node {
                const self = try ha.create(Node);
                self.* = Node{
                    .leaf = .{
                        .keys = KeyArray.fromSlice(&[_]T{first}) catch unreachable,
                    },
                };
                return self;
            }

            fn fromSliceLeaf(ha: Allocator, keys: []const T) !*Node {
                const self = try ha.create(Node);
                self.* = Node{ .leaf = .{
                    .keys = KeyArray.fromSlice(keys) catch unreachable,
                } };
                return self;
            }

            fn fromSliceInternal(ha: Allocator, keys: []const T, children: []*Node) !*Node {
                const self = try ha.create(Node);
                self.* = Node{ .internal = .{
                    .keys = KeyArray.fromSlice(keys) catch unreachable,
                    .children = ChildrenArray.fromSlice(children) catch unreachable,
                } };
                return self;
            }

            fn len(self: *const Node) usize {
                return switch (self.*) {
                    .leaf => |node| node.keys.len,
                    .internal => |node| node.keys.len,
                };
            }

            fn insert(self: *Node, ha: Allocator, cx: Ctx, item: T) !void {
                switch (self.*) {
                    .leaf => |*node| {
                        for (node.keys.slice(), 0..) |key, ii| {
                            // FIXME: replace with binary search
                            if (cmp(cx, item, key) == .lt) {
                                node.keys.insert(ii, item) catch unreachable;
                                return;
                            }
                        }
                        // it's above all our keys so add it at the end
                        node.keys.append(item) catch unreachable;
                    },
                    .internal => |*node| {
                        var ii = blk: {
                            for (node.keys.slice(), 0..) |key, ii| {
                                // FIXME: replace with binary search
                                if (cmp(cx, item, key) == .lt) {
                                    break :blk ii;
                                }
                            }
                            // it's above all our keys so add it to the last child
                            break :blk node.children.len - 1;
                        };
                        if (try node.splitChildIfFull(ha, ii)) {
                            if (cmp(cx, item, node.keys.get(ii)) == .gt) {
                                // add to the new daughter node
                                ii += 1;
                            }
                        }
                        const chosen_child = node.children.get(ii);
                        try chosen_child.insert(ha, cx, item);
                    },
                }
            }

            /// This returns the node containing the key and the index of the key
            pub fn find(self: *@This(), cx: Ctx, needle: T) ?struct { *Node, usize } {
                switch (self.*) {
                    .leaf => |node| {
                        for (node.keys.slice(), 0..) |key, ii| {
                            switch (cmp(cx, needle, key)) {
                                .eq => {
                                    return .{ self, ii };
                                },
                                .lt => {
                                    return null;
                                },
                                .gt => {},
                            }
                        }
                        return null;
                    },
                    .internal => |node| {
                        for (node.keys.slice(), 0..) |key, ii| {
                            switch (cmp(cx, needle, key)) {
                                .eq => {
                                    return .{ self, ii };
                                },
                                .lt => {
                                    return node.children.get(ii).find(cx, needle);
                                },
                                .gt => {},
                            }
                        }
                        return node.children.get(node.children.len - 1).find(cx, needle);
                    },
                }
            }

            fn print(self: *const Node) void {
                for (self.keys.slice(), 0..) |key, kk| {
                    println("node {*} key[{}] = {}", .{ self, kk, key });
                }
                if (self.children) |children| {
                    for (children.slice()) |child| {
                        child.print();
                    }
                }
            }
        };

        root: ?*Node = null,
        ha: Allocator,

        pub fn init(ha7r: Allocator) Self {
            return Self{
                .ha = ha7r,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.root) |node| {
                node.deinit(self.ha);
            }
        }

        pub fn insert(self: *Self, cx: Ctx, item: T) !void {
            if (self.root) |node| {
                if (node.len() == (2 * order) - 1) {
                    var internal = Node.Internal{
                        .keys = Node.KeyArray.init(0) catch unreachable,
                        .children = Node.ChildrenArray.fromSlice(&[_]*Node{node}) catch unreachable,
                    };
                    _ = try internal.splitChildIfFull(self.ha, 0);
                    var new_root = try self.ha.create(Node);
                    new_root.* = Node{ .internal = internal };
                    try new_root.insert(self.ha, cx, item);
                    self.root = new_root;
                } else {
                    try node.insert(self.ha, cx, item);
                }
            } else {
                self.root = try Node.initLeaf(self.ha, item);
            }
        }

        pub fn find(self: *Self, cx: Ctx, needle: T) ?T {
            if (self.root) |root| {
                if (root.find(cx, needle)) |found| {
                    return switch (found[0].*) {
                        .leaf => |node| node.keys.get(found[1]),
                        .internal => |node| node.keys.get(found[1])
                    };
                }
            }
            return null;
        }

        pub fn print(self: *Self) void {
            if (self.root) |node| {
                node.print();
            }
        }
    };
}

test "BTree.api" {
    const MyBtree = BTree(
        usize,
        void,
        struct {
            fn cmp(cx: void, a: usize, b: usize) std.math.Order {
                _ = cx;
                return std.math.order(a, b);
            }
        }.cmp,
        12,
        // .{
        //     .extension = BSTConfig.heapExtension,
        // },
    );
    const ha = std.testing.allocator;

    var tree = MyBtree.init(ha);
    defer tree.deinit();
    for (0..10_000) |ii| {
        _ = ii;
        try tree.insert({}, std.crypto.random.uintAtMost(usize, 10_000));
    }
    const items = [_]usize{ 123, 53, 98823, 123, 534, 123, 54, 7264, 21, 0, 23 };
    for (items) |val| {
        try tree.insert({}, val);
    }
    // tree.print();
    try std.testing.expect(tree.find({}, 123) != null);
    try std.testing.expect(tree.find({}, 0) != null);
    try std.testing.expect(tree.find({}, 54) != null);
    // try std.testing.expect(tree.find({}, 1223) == null);
}

// pub fn RBT(
//     comptime T: type,
//     comptime Ctx: type,
//     comptime cmp: fn (cx: Ctx, a: T, b: T) std.math.Order,
// ) type {
//     return struct {
//         const Self = @This();
//         const Color = enum { red, black};
//
//         item: T,
//         red: Color = .black,
//         parent: ?*Self = null,
//         left_child: ?*Self = null,
//         right_child: ?*Self = null,
//     };
// }
//
