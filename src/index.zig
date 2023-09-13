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

fn BTree(
    comptime T: type,
    comptime Ctx: type,
    comptime cmp: fn (cx: Ctx, a: T, b: T) std.math.Order,
    // comptime config: BTreeConfig,
    comptime order: usize,
) type {
    return struct {
        const Self = @This();
        const Node = struct {
            // multiply by 2 to keep max children even and max keys odd
            const KeyArray = std.BoundedArray(T, (2 * order) - 1);
            const ChildrenArray = std.BoundedArray(*Node, 2 * order);

            keys: KeyArray,
            children: ?ChildrenArray = null,

            fn deinit(self: *Node, ha: Allocator) void {
                if (self.children) |children| {
                    for (children.slice()) |child| {
                        child.deinit(ha);
                    }
                }
                ha.destroy(self);
            }

            fn init(ha: Allocator) !*Node {
                const self = try ha.create(Node);
                self.* = Node{
                    .keys = KeyArray.init(0) catch unreachable,
                };
                return self;
            }

            fn initWithChildren(ha: Allocator, left: *Node, mid: T, right: *Node) !*Node {
                const self = try ha.create(Node);
                var children = ChildrenArray.init(0) catch unreachable;
                children.append(left) catch unreachable;
                children.append(right) catch unreachable;
                self.* = Node{
                    .keys = KeyArray.init(0) catch unreachable,
                    .children = children,
                };
                self.keys.append(mid) catch unreachable;
                return self;
            }

            fn fromSlice(ha: Allocator, keys: []const T, children: ?[]*Node) !*Node {
                const self = try ha.create(Node);
                self.* = Node{
                    .keys = KeyArray.fromSlice(keys) catch unreachable,
                };
                if (children) |slice| {
                    self.children = ChildrenArray.fromSlice(slice) catch unreachable;
                }
                return self;
            }

            pub fn isLeaf(self: *const Node) bool {
                return self.children == null;
            }

            pub fn len(self: *const Node) usize {
                return self.keys.len;
            }

            pub const InsertOutcome = union(enum) {
                routine,
                // replaced: T,
                split: struct {
                    new_key: T,
                    new_child: *Node,
                },
            };

            pub fn insert(self: *Node, ha: Allocator, cx: Ctx, item: T) !InsertOutcome {
                defer {
                    if (self.children) |xx| {
                        std.debug.assert(self.keys.len < xx.len);
                            // println("happened when inserting into {*}, len = {}, c.len = {?}", 
                            //     .{ 
                            //         self,
                            //         self.keys.len, 
                            //         xx.len, 
                            //     }
                            // );
                    }
                }
                if (self.children) |*children| {
                    // internal node case
                    const chosen_child = self.insertIndex(cx, item);
                    // we always add the value to the child
                    const outcome = try children.get(chosen_child).insert(ha, cx, item);
                    switch (outcome) {
                        // the child has split into two
                        .split => |child_split| {
                            // we need to add the new separating key
                            // and the new child
                            if (self.keys.len == (2 * order) - 1) {
                                // TODO: we can probably reuse chosen_child here
                                // full case
                                const self_split = try self.split(ha);
                                if (cmp(cx, child_split.new_key, self_split.new_key) == .gt) {
                                    // insert to new child
                                    self_split.new_child.insertNonFull(cx, child_split.new_key, child_split.new_child);
                                } else {
                                    // insert to self
                                    self.insertNonFull(cx, child_split.new_key, child_split.new_child);
                                }
                                return InsertOutcome{ .split = .{ .new_key = self_split.new_key, .new_child = self_split.new_child } };
                            } 
                            if (chosen_child == (children.len - 1)) {
                                children.append(child_split.new_child) catch unreachable;
                                self.keys.append(child_split.new_key) catch unreachable;
                            } else {
                                children.insert(chosen_child + 1, child_split.new_child) catch unreachable;
                                self.keys.insert(chosen_child, child_split.new_key) catch unreachable;
                            }
                            return InsertOutcome{ .routine = {} };
                        },
                        else => return outcome,
                    }
                }
                // leaf node case
                if (self.keys.len == (2 * order) - 1) {
                    // full case
                    const self_split = try self.split(ha);
                    if (cmp(cx, item, self_split.new_key) == .gt) {
                        // insert to new child
                        self_split.new_child.insertNonFull(cx, item, null);
                    } else {
                        // insert to self
                        self.insertNonFull(cx, item, null);
                    }
                    return InsertOutcome{ .split = .{ .new_key = self_split.new_key, .new_child = self_split.new_child } };
                } 
                self.insertNonFull(cx, item, null);
                return InsertOutcome{ .routine = {} };
            }

            fn insertNonFull(self: *Node, cx: Ctx, item: T, new_child: ?*Node) void {
                std.debug.assert(self.keys.len < (2 * order) - 1);
                const ii = self.insertIndex(cx, item);
                if (ii == self.keys.len) {
                    self.keys.append(item) catch unreachable;
                    if (new_child) |child| {
                        if (self.children) |*children| {
                            children.append(child) catch unreachable;
                        } else {
                            unreachable;
                        }
                    }
                } else {
                    self.keys.insert(ii, item) catch unreachable;
                    if (new_child) |child| {
                        if (self.children) |*children| {
                            children.insert(ii, child) catch unreachable;
                        } else {
                            unreachable;
                        }
                    }
                }
            }

            fn insertIndex(self: *Node, cx: Ctx, item: T) usize {
                const slice = self.keys.slice();
                for (slice, 0..) |key, ii| {
                    // FIXME: replace with binary search
                    if (cmp(cx, item, key) == .lt) {
                        return ii;
                    }
                }
                // it's above all our keys so add it to the last child
                return slice.len;
            }

            fn split(self: *Node, ha: Allocator) !struct {
                new_key: T,
                new_child: *Node,
            } {
                std.debug.assert(self.keys.len == (2 * order) - 1);
                const right = try Node.fromSlice(ha, self.keys.slice()[order..], null);
                if (self.children) |*children| {
                    right.children = ChildrenArray.fromSlice(children.slice()[order..]) catch unreachable;
                    children.resize(order) catch unreachable;
                }
                const mid = self.keys.get(order - 1);
                self.keys.resize(order - 1) catch unreachable;
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
                return .{ .new_key = mid, .new_child = right };
            }

            pub fn find(self: *@This(), cx: Ctx, needle: T) ?T {
                for (self.keys.slice(), 0..) |key, ii| {
                    switch (cmp(cx, needle, key)) {
                        .eq => {
                            return key;
                        },
                        .lt => {
                            if (self.children) |children| {
                                return children.get(ii).find(cx, needle);
                            } else {
                                return null;
                            }
                        },
                        .gt => {},
                    }
                }
                if (self.children) |children| {
                    return children.get(children.len - 1).find(cx, needle);
                } else {
                    return null;
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
                switch (try node.insert(self.ha, cx, item)) {
                    .routine => {},
                    .split => |split| {
                        self.root = try Node.initWithChildren(self.ha, node, split.new_key, split.new_child);
                    },
                }
            } else {
                var node = try Node.init(self.ha);
                node.keys.append(item) catch unreachable;
                self.root = node;
            }
        }

        pub fn find(self: *Self, cx: Ctx, needle: T) ?T {
            if (self.root) |node| {
                return node.find(cx, needle);
            } else {
                return null;
            }
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
