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
const SwapAllocator = mod_mmap.SwapAllocator;
const Pager = mod_mmap.Pager;
const Ptr = mod_mmap.SwapAllocator.Ptr;

const mod_plist = @import("plist.zig");

// const BTreeConfig = struct {
//     order: usize = 12,
// };

// Order will be multiplied by to ensure an even value.
fn SwapBTree(
    comptime T: type,
    comptime Ctx: type,
    comptime cmp: fn (cx: Ctx, a: T, b: T) std.math.Order,
    // comptime config: BTreeConfig,
    comptime order: usize,
) type {
    if (order == 0) {
        @compileError("order is 0");
    }
    return struct {
        const Self = @This();
        const Node = union(enum) {
            const KeyArray = std.BoundedArray(T, (2 * order) - 1);
            const ChildrenArray = std.BoundedArray(Ptr, 2 * order);
            const NodeNPtr = struct {
                node: *Node,
                ptr: Ptr,
            };

            const Leaf = struct {
                keys: KeyArray,
            };
            const Internal = struct {
                keys: KeyArray,
                children: ChildrenArray,

                fn splitChildIfFull(self: *@This(), sa: SwapAllocator, child_ii: usize) !bool {
                    const child_ptr = self.children.slice()[child_ii];
                    const child = try Node.swapIn(sa, child_ptr);
                    defer sa.swapOut(child_ptr);
                    switch (child.*) {
                        .leaf => |*node| {
                            if (node.keys.len == (2 * order) - 1) {
                                const right_ptr = try Node.fromSliceLeafPtr(sa, node.keys.slice()[order..]);
                                const mid = node.keys.get(order - 1);
                                node.keys.resize(order - 1) catch unreachable;
                                if (child_ii == self.children.len - 1) {
                                    self.keys.append(mid) catch unreachable;
                                    self.children.append(right_ptr) catch unreachable;
                                } else {
                                    self.keys.insert(child_ii, mid) catch unreachable;
                                    self.children.insert(child_ii + 1, right_ptr) catch unreachable;
                                }
                                return true;
                            }
                            return false;
                        },
                        .internal => |*node| {
                            if (node.keys.len == (2 * order) - 1) {
                                const right_ptr = try Node.fromSliceInternalPtr(sa, node.keys.slice()[order..], node.children.slice()[order..]);
                                node.children.resize(order) catch unreachable;
                                const mid = node.keys.get(order - 1);
                                node.keys.resize(order - 1) catch unreachable;
                                if (child_ii == self.children.len - 1) {
                                    self.keys.append(mid) catch unreachable;
                                    self.children.append(right_ptr) catch unreachable;
                                } else {
                                    self.keys.insert(child_ii, mid) catch unreachable;
                                    self.children.insert(child_ii + 1, right_ptr) catch unreachable;
                                }
                                return true;
                            }
                            return false;
                        },
                    }
                }
            };
            leaf: Leaf,
            internal: Internal,

            fn deinit(sa: SwapAllocator, ptr: Ptr) !void {
                defer sa.free(ptr);
                const self = try Node.swapIn(sa, ptr);
                defer sa.swapOut(ptr);
                switch (self.*) {
                    .internal => |int| {
                        for (int.children.slice()) |child_ptr| {
                            try Node.deinit(sa, child_ptr);
                        }
                    },
                    else => {},
                }
            }

            // This assumes that the Node at the ptr has not children
            fn deinitUnchecked(sa: SwapAllocator, ptr: Ptr) void {
                defer sa.free(ptr);
            }

            fn swapIn(sa: SwapAllocator, ptr: Ptr) !*Node {
                const slice = try sa.swapIn(ptr);
                std.debug.assert(slice.len >= @sizeOf(Node));
                const hptr: *align(@alignOf(Node)) [@sizeOf(Node)]u8 = @alignCast(@ptrCast(slice.ptr));
                return std.mem.bytesAsValue(Node, hptr);
            }

            fn fromSliceLeaf(sa: SwapAllocator, key_slice: []const T) !NodeNPtr {
                const ptr = try sa.alloc(@sizeOf(Node));
                const node = try Node.swapIn(sa, ptr);
                node.* = Node{
                    .leaf = .{
                        .keys = KeyArray.fromSlice(key_slice) catch unreachable,
                    },
                };
                return NodeNPtr{ .node = node, .ptr = ptr };
            }

            fn fromSliceLeafPtr(sa: SwapAllocator, key_slice: []const T) !Ptr {
                const nodenptr = try Node.fromSliceLeaf(sa, key_slice);
                sa.swapOut(nodenptr.ptr);
                return nodenptr.ptr;
            }

            fn fromSliceInternal(sa: SwapAllocator, key_slice: []const T, children: []const Ptr) !NodeNPtr {
                std.debug.assert(key_slice.len == children.len - 1);
                const ptr = try sa.alloc(@sizeOf(Node));
                const node = try Node.swapIn(sa, ptr);
                node.* = Node{ .internal = .{
                    .keys = KeyArray.fromSlice(key_slice) catch unreachable,
                    .children = ChildrenArray.fromSlice(children) catch unreachable,
                } };
                return NodeNPtr{ .node = node, .ptr = ptr };
            }

            fn fromSliceInternalPtr(sa: SwapAllocator, key_slice: []const T, children: []const Ptr) !Ptr {
                const nodenptr = try Node.fromSliceInternal(sa, key_slice, children);
                sa.swapOut(nodenptr.ptr);
                return nodenptr.ptr;
            }

            fn constKeys(self: *const Node) *const KeyArray {
                return switch (self.*) {
                    .leaf => |*node| &node.keys,
                    .internal => |*node| &node.keys,
                };
            }

            fn keys(self: *Node) *KeyArray {
                return switch (self.*) {
                    .leaf => |*node| &node.keys,
                    .internal => |*node| &node.keys,
                };
            }

            fn len(self: *const Node) usize {
                return switch (self.*) {
                    .leaf => |node| node.keys.len,
                    .internal => |node| node.keys.len,
                };
            }

            fn insert(self: *Node, sa: SwapAllocator, cx: Ctx, item: T) !void {
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
                        if (try node.splitChildIfFull(sa, ii)) {
                            if (cmp(cx, item, node.keys.get(ii)) == .gt) {
                                // add to the new daughter node
                                ii += 1;
                            }
                        }
                        const chosen_ptr = node.children.get(ii);
                        const chosen_child = try Node.swapIn(sa, chosen_ptr);
                        defer sa.swapOut(chosen_ptr);
                        try chosen_child.insert(sa, cx, item);
                    },
                }
            }

            fn delete(self: *Node, sa: SwapAllocator, cx: Ctx, needle: T) !?T {
                switch (self.*) {
                    .leaf => |*node| {
                        for (node.keys.slice(), 0..) |key, ii| {
                            switch (cmp(cx, needle, key)) {
                                .eq => {
                                    return node.keys.orderedRemove(ii);
                                },
                                .lt => break,
                                .gt => {},
                            }
                        }
                        return null;
                    },
                    .internal => |*node| {
                        const child_ii = blk: {
                            for (node.keys.slice(), 0..) |key, ii| {
                                switch (cmp(cx, needle, key)) {
                                    .eq => {
                                        const child_ptr = node.children.get(ii);
                                        const child = try Node.swapIn(sa, child_ptr);
                                        defer sa.swapOut(child_ptr);
                                        // return node.keys.orderedRemove(ii);
                                        if (child.len() >= order) {
                                            node.keys.set(ii, try child.deleteMax(sa, cx));
                                            return key;
                                        }

                                        const child_b_ptr = node.children.get(ii + 1);
                                        const child_b = try Node.swapIn(sa, child_b_ptr);
                                        if (child_b.len() >= order) {
                                            // NOTE: we only defer swapOut in this block
                                            defer sa.swapOut(child_ptr);
                                            node.keys.set(ii, try child_b.deleteMin(sa, cx));
                                            return key;
                                        }

                                        // NOTE: we always swap out before merge since it'll free the mergee
                                        sa.swapOut(child_ptr);
                                        try child.merge(sa, node.keys.orderedRemove(ii), node.children.orderedRemove(ii + 1));
                                        // FIXME: why does the book rec recursive delete here instead of just excluding needle from the merge
                                        return try child.delete(sa, cx, needle);
                                    },
                                    .lt => break :blk ii,
                                    .gt => {},
                                }
                            }
                            break :blk node.children.len - 1;
                        };
                        const chosen_ptr = node.children.get(child_ii);
                        var chosen_child = try Node.swapIn(sa, chosen_ptr);
                        var swap_out_child = true;
                        defer if (swap_out_child) sa.swapOut(chosen_ptr);
                        if (chosen_child.len() < order) {
                            const has_left = child_ii > 0;
                            const has_right = child_ii < node.children.len - 1;
                            if (has_left) {
                                const left_ptr = node.children.get(child_ii - 1);
                                const left = try Node.swapIn(sa, left_ptr);
                                defer sa.swapOut(left_ptr);
                                if (left.len() >= order) {
                                    // give chosen key[chosen]
                                    chosen_child.keys().insert(0, node.keys.get(child_ii - 1)) catch unreachable;
                                    // replace key[chosen] with key[last] from left
                                    node.keys.set(child_ii - 1, left.keys().pop());
                                    switch (chosen_child.*) {
                                        // give chosen last child from left
                                        .internal => |*chosen_internal| {
                                            chosen_internal.children.insert(0, left.internal.children.pop()) catch unreachable;
                                        },
                                        else => {},
                                    }
                                    return try chosen_child.delete(sa, cx, needle);
                                } else if (has_right) {
                                    const right_ptr = node.children.get(child_ii + 1);
                                    const right = try Node.swapIn(sa, right_ptr);
                                    defer sa.swapOut(right_ptr);
                                    // if both siblings and chosen are under order
                                    // TODO: what's the point of checking right here? it doesn't get modified
                                    if (right.len() < order) {
                                        sa.swapOut(chosen_ptr);
                                        // we disable the defer block for chosen to avoid double
                                        // swap out as `merge` will deinit it
                                        swap_out_child = false;
                                        try left.merge(sa, node.keys.orderedRemove(child_ii - 1), node.children.orderedRemove(child_ii));
                                        return try left.delete(sa, cx, needle);
                                    }
                                }
                            }
                            if (has_right) {
                                const right_ptr = node.children.get(child_ii + 1);
                                const right = try Node.swapIn(sa, right_ptr);
                                defer sa.swapOut(right_ptr);
                                if (right.len() >= order) {
                                    // give chosen key[chosen]
                                    chosen_child.keys().append(node.keys.get(child_ii)) catch unreachable;
                                    // replace key[chosen] with key[first] from right
                                    node.keys.set(child_ii, right.keys().orderedRemove(0));
                                    switch (chosen_child.*) {
                                        // give chosen last child from right
                                        .internal => |*chosen_internal| {
                                            chosen_internal.children.append(right.internal.children.orderedRemove(0)) catch unreachable;
                                        },
                                        else => {},
                                    }
                                    return try chosen_child.delete(sa, cx, needle);
                                }
                            }
                        }
                        return try chosen_child.delete(sa, cx, needle);
                    },
                }
            }

            fn deleteMin(self: *Node, sa: SwapAllocator, cx: Ctx) !T {
                switch (self.*) {
                    .leaf => |*node| {
                        return node.keys.orderedRemove(0);
                    },
                    .internal => |*node| {
                        const chosen_ptr = node.children.get(0);
                        var chosen_child = try Node.swapIn(sa, chosen_ptr);
                        defer sa.swapOut(chosen_ptr);
                        if (chosen_child.len() < order) {
                            const right_ptr = node.children.get(1);
                            const right = try Node.swapIn(sa, right_ptr);
                            if (right.len() >= order) {
                                defer sa.swapOut(right_ptr);
                                // give chosen key[chosen]
                                chosen_child.keys().append(node.keys.get(0)) catch unreachable;
                                // replace self.key[chosen] with key[first] from right
                                node.keys.set(0, right.keys().orderedRemove(0));
                                switch (chosen_child.*) {
                                    // give chosen first child from right
                                    .internal => |*chosen_internal| {
                                        chosen_internal.children.append(right.internal.children.orderedRemove(0)) catch unreachable;
                                    },
                                    else => {},
                                }
                            } else {
                                sa.swapOut(right_ptr);
                                // both are under order, merge them
                                try chosen_child.merge(sa, node.keys.orderedRemove(0), node.children.orderedRemove(1));
                            }
                        }
                        return try chosen_child.deleteMin(sa, cx);
                    },
                }
            }

            fn deleteMax(self: *Node, sa: SwapAllocator, cx: Ctx) !T {
                switch (self.*) {
                    .leaf => |*node| {
                        return node.keys.pop();
                    },
                    .internal => |*node| {
                        const chosen_ptr = node.children.get(node.children.len - 1);
                        var chosen_child = try Node.swapIn(sa, chosen_ptr);
                        var swap_out_child = true;
                        defer if (swap_out_child) sa.swapOut(chosen_ptr);
                        if (chosen_child.len() < order) {
                            const left_ptr = node.children.get(node.children.len - 2);
                            const left = try Node.swapIn(sa, left_ptr);
                            defer sa.swapOut(left_ptr);
                            if (left.len() >= order) {
                                // give chosen key[chosen]
                                chosen_child.keys().insert(0, node.keys.pop()) catch unreachable;
                                // replace self.key[chosen] with key[last] from left
                                node.keys.append(left.keys().pop()) catch unreachable;
                                switch (chosen_child.*) {
                                    // give chosen last child from left
                                    .internal => |*chosen_internal| {
                                        chosen_internal.children.insert(0, left.internal.children.pop()) catch unreachable;
                                    },
                                    else => {},
                                }
                            } else {
                                sa.swapOut(chosen_ptr);
                                swap_out_child = false;
                                // both are under order, merge them
                                try left.merge(sa, node.keys.pop(), node.children.pop());
                                // NOTE: we delete from left after the merge
                                return try left.deleteMax(sa, cx);
                            }
                        }
                        return try chosen_child.deleteMax(sa, cx);
                    },
                }
            }

            // this assumes both self and right are of the same Node type.
            fn merge(self: *Node, sa: SwapAllocator, median: T, right_ptr: Ptr) !void {
                switch (self.*) {
                    .leaf => |*node| {
                        const right = try Node.swapIn(sa, right_ptr);
                        defer Node.deinitUnchecked(sa, right_ptr);
                        defer sa.swapOut(right_ptr);

                        node.keys.append(median) catch unreachable;
                        node.keys.appendSlice(right.leaf.keys.slice()) catch unreachable;
                    },
                    .internal => |*node| {
                        const right = try Node.swapIn(sa, right_ptr);
                        defer Node.deinitUnchecked(sa, right_ptr);
                        defer sa.swapOut(right_ptr);
                        defer right.internal.children.resize(0) catch unreachable;

                        node.keys.append(median) catch unreachable;
                        node.keys.appendSlice(right.internal.keys.slice()) catch unreachable;

                        node.children.appendSlice(right.internal.children.slice()) catch unreachable;
                    },
                }
            }

            /// This returns the node containing the key and the index of the key
            fn find(sa: SwapAllocator, ptr: Ptr, cx: Ctx, needle: T) !?struct {
                usize,
                *Node,
                Ptr,
            } {
                const self = try Node.swapIn(sa, ptr);
                var swap_out = true;
                defer if (swap_out) {
                    sa.swapOut(ptr);
                };
                switch (self.*) {
                    .leaf => |node| {
                        for (node.keys.slice(), 0..) |key, ii| {
                            switch (cmp(cx, needle, key)) {
                                .eq => {
                                    swap_out = false;
                                    return .{ ii, self, ptr };
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
                                    swap_out = false;
                                    return .{ ii, self, ptr };
                                },
                                .lt => {
                                    const child_ptr = node.children.get(ii);
                                    return try Node.find(sa, child_ptr, cx, needle);
                                },
                                .gt => {},
                            }
                        }
                        const child_ptr = node.children.get(node.children.len - 1);
                        return try Node.find(sa, child_ptr, cx, needle);
                    },
                }
            }

            fn countKeys(self: *const Node, sa: SwapAllocator) !usize {
                switch (self.*) {
                    .leaf => |node| {
                        return node.keys.len;
                    },
                    .internal => |node| {
                        var total: usize = node.keys.len;
                        for (node.children.slice()) |child_ptr| {
                            const child = try Node.swapIn(sa, child_ptr);
                            defer sa.swapOut(child_ptr);
                            total += try child.countKeys(sa);
                        }
                        return total;
                    },
                }
            }

            fn print(self: *const Node, sa: SwapAllocator, depth: usize) void {
                for (self.constKeys().slice(), 0..) |key, kk| {
                    println("node {} on depth {} key[{}] = {}", .{ @intFromPtr(self), depth, kk, key });
                }
                switch (self.*) {
                    .internal => |node| {
                        for (node.children.slice()) |child_ptr| {
                            const child = try Node.swapIn(sa, child_ptr);
                            defer sa.swapOut(child_ptr);
                            try child.print(sa, depth + 1);
                        }
                    },
                    else => {},
                }
            }
        };

        // ha: Allocator,
        sa: SwapAllocator,
        // pager: Pager,
        root: ?Ptr = null,

        pub fn init(sa: SwapAllocator) Self {
            return Self{
                .sa = sa,
            };
        }

        pub fn deinit(self: *Self) !void {
            if (self.root) |root_ptr| {
                try Node.deinit(self.sa, root_ptr);
                self.root = null;
            }
        }

        pub fn insert(self: *Self, cx: Ctx, item: T) !void {
            if (self.root) |root_ptr| {
                const node = try Node.swapIn(self.sa, root_ptr);
                defer self.sa.swapOut(root_ptr);
                if (node.len() == (2 * order) - 1) {
                    var internal = Node.Internal{
                        .keys = Node.KeyArray.init(0) catch unreachable,
                        .children = Node.ChildrenArray.fromSlice(&[_]Ptr{root_ptr}) catch unreachable,
                    };
                    _ = try internal.splitChildIfFull(self.sa, 0);

                    const new_root_ptr = try self.sa.alloc(@sizeOf(Node));
                    const new_root = try Node.swapIn(self.sa, new_root_ptr);
                    defer self.sa.swapOut(new_root_ptr);
                    new_root.* = Node{ .internal = internal };
                    try new_root.insert(self.sa, cx, item);

                    self.root = new_root_ptr;
                } else {
                    try node.insert(self.sa, cx, item);
                }
            } else {
                self.root = try Node.fromSliceLeafPtr(self.sa, &[_]T{item});
            }
        }

        pub fn find(self: *Self, cx: Ctx, needle: T) !?T {
            if (self.root) |root_ptr| {
                if (try Node.find(self.sa, root_ptr, cx, needle)) |found| {
                    defer self.sa.swapOut(found[2]);
                    return switch (found[1].*) {
                        .leaf => |node| node.keys.get(found[0]),
                        .internal => |node| node.keys.get(found[0]),
                    };
                }
            }
            return null;
        }

        pub fn delete(self: *Self, cx: Ctx, needle: T) !?T {
            if (self.root) |root_ptr| {
                var deinit_root = false;
                defer if (deinit_root) Node.deinitUnchecked(self.sa, root_ptr);
                const root = try Node.swapIn(self.sa, root_ptr);
                defer self.sa.swapOut(root_ptr);
                if (try root.delete(self.sa, cx, needle)) |found| {
                    switch (root.*) {
                        .leaf => |*node| if (node.keys.len == 0) {
                            deinit_root = true;
                            self.root = null;
                        },
                        .internal => |*node| if (node.keys.len == 0) {
                            deinit_root = true;
                            defer node.children.resize(0) catch unreachable;
                            self.root = node.children.get(0);
                        },
                    }
                    return found;
                }
            }
            return null;
        }

        pub fn countKeys(self: *const Self) !usize {
            if (self.root) |root_ptr| {
                const root = try Node.swapIn(self.sa, root_ptr);
                defer self.sa.swapOut(root_ptr);
                return try root.countKeys(self.sa);
            } else {
                return 0;
            }
        }

        pub fn count_depth(self: *const Self) !usize {
            if (self.root) |rn| {
                var depth: usize = 1;
                var ptr = rn;
                while (true) {
                    const node = try Node.swapIn(self.sa, ptr);
                    defer self.sa.swapOut(ptr);
                    switch (node.*) {
                        .internal => |in| ptr = in.children.get(0),
                        else => break,
                    }
                    depth += 1;
                }
                return depth;
            } else {
                return 0;
            }
        }

        pub fn print(self: *Self) !void {
            if (self.root) |ptr| {
                const node = try Node.swapIn(self.sa, ptr);
                defer self.sa.swapOut(ptr);
                try node.print(self.sa, 0);
            }
        }
    };
}

test "SwapBTree.insert" {
    const page_size = std.mem.page_size;
    const MyBtree = SwapBTree(
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
    var mmap_pager = try mod_mmap.MmapPager.init(ha, "/tmp/SwapBTree.insert", .{
        .page_size = page_size,
    });
    defer mmap_pager.deinit();

    var lru = try mod_mmap.LRUSwapCache.init(ha, mmap_pager.pager(), 1);
    defer lru.deinit();
    var pager = lru.pager();

    var msa = mod_mmap.PagingSwapAllocator(.{}).init(ha, pager);
    defer msa.deinit();
    var sa = msa.allocator();

    var tree = MyBtree.init(sa);
    defer tree.deinit() catch unreachable;
    for (0..10_000) |ii| {
        _ = ii;
        try tree.insert({}, std.crypto.random.uintAtMost(usize, 10_000));
    }
    const items = [_]usize{ 123, 53, 98823, 123, 534, 123, 54, 7264, 21, 0, 23 };
    for (items) |val| {
        try tree.insert({}, val);
    }

    // tree.print();
    try std.testing.expect(try tree.find({}, 123) != null);
    try std.testing.expect(try tree.find({}, 0) != null);
    try std.testing.expect(try tree.find({}, 54) != null);
    // try std.testing.expect(tree.find({}, 1223) == null);
}

test "SwapBTree.delete" {
    const page_size = std.mem.page_size;
    const MyBtree = SwapBTree(
        usize,
        void,
        struct {
            fn cmp(cx: void, a: usize, b: usize) std.math.Order {
                _ = cx;
                return std.math.order(a, b);
            }
        }.cmp,
        3,
        // .{
        //     .extension = BSTConfig.heapExtension,
        // },
    );
    const ha = std.testing.allocator;
    var mmap_pager = try mod_mmap.MmapPager.init(ha, "/tmp/SwapBTree.insert", .{
        .page_size = page_size,
    });
    defer mmap_pager.deinit();

    var lru = try mod_mmap.LRUSwapCache.init(ha, mmap_pager.pager(), 1);
    defer lru.deinit();
    var pager = lru.pager();

    var msa = mod_mmap.PagingSwapAllocator(.{}).init(ha, pager);
    defer msa.deinit();
    var sa = msa.allocator();

    var tree = MyBtree.init(sa);
    defer tree.deinit() catch unreachable;
    const items = "CGMPTXABDEFJKLNOQRSUVYZ";
    for (items) |val| {
        try tree.insert({}, val);
    }
    // tree.print();
    // try std.testing.expectEqual(@as(usize, 23), tree.countKeys());
    // try std.testing.expectEqual(@as(usize, 3), tree.count_depth());
    try std.testing.expect(try tree.find({}, 'F') != null);
    try std.testing.expect(try tree.delete({}, 'F') != null);
    try std.testing.expect(try tree.find({}, 'F') == null);
    try std.testing.expectEqual(@as(usize, 22), try tree.countKeys());
    try std.testing.expect(try tree.delete({}, 'M') != null);
    try std.testing.expectEqual(@as(usize, 21), try tree.countKeys());
    try std.testing.expect(try tree.delete({}, 'G') != null);
    try std.testing.expectEqual(@as(usize, 20), try tree.countKeys());
    try std.testing.expect(try tree.delete({}, 'D') != null);
    try std.testing.expectEqual(@as(usize, 19), try tree.countKeys());
    try std.testing.expect(try tree.delete({}, 'B') != null);
    try std.testing.expectEqual(@as(usize, 18), try tree.countKeys());
    const del_items = "CPTXAEJKLNOQRSUVYZ";
    for (del_items) |val| {
        try std.testing.expect(try tree.delete({}, val) != null);
    }
    try std.testing.expect(try tree.count_depth() == 0);
    // try std.testing.expect(tree.find({}, 1223) == null);
}

fn BTree(
    comptime T: type,
    comptime Ctx: type,
    comptime cmp: fn (cx: Ctx, a: T, b: T) std.math.Order,
    // comptime config: BTreeConfig,
    comptime order: usize,
) type {
    if (order == 0) {
        @compileError("order is 0");
    }
    return struct {
        const Self = @This();
        const Node = union(enum) {
            // multiply by 2 to keep max children even and max keys odd
            const KeyArray = std.BoundedArray(T, (2 * order) - 1);
            const ChildrenArray = std.BoundedArray(*Node, 2 * order);
            const Leaf = struct {
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
                                    self.children.insert(child_ii + 1, right) catch unreachable;
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
                                    self.children.insert(child_ii + 1, right) catch unreachable;
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

            fn fromSliceLeaf(ha: Allocator, key_slice: []const T) !*Node {
                const self = try ha.create(Node);
                self.* = Node{ .leaf = .{
                    .keys = KeyArray.fromSlice(key_slice) catch unreachable,
                } };
                return self;
            }

            fn fromSliceInternal(ha: Allocator, key_slice: []const T, children: []*Node) !*Node {
                const self = try ha.create(Node);
                self.* = Node{ .internal = .{
                    .keys = KeyArray.fromSlice(key_slice) catch unreachable,
                    .children = ChildrenArray.fromSlice(children) catch unreachable,
                } };
                return self;
            }

            fn constKeys(self: *const Node) *const KeyArray {
                return switch (self.*) {
                    .leaf => |*node| &node.keys,
                    .internal => |*node| &node.keys,
                };
            }

            fn keys(self: *Node) *KeyArray {
                return switch (self.*) {
                    .leaf => |*node| &node.keys,
                    .internal => |*node| &node.keys,
                };
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

            fn delete(self: *Node, ha: Allocator, cx: Ctx, needle: T) ?T {
                switch (self.*) {
                    .leaf => |*node| {
                        for (node.keys.slice(), 0..) |key, ii| {
                            switch (cmp(cx, needle, key)) {
                                .eq => {
                                    return node.keys.orderedRemove(ii);
                                },
                                .lt => break,
                                .gt => {},
                            }
                        }
                        return null;
                    },
                    .internal => |*node| {
                        const child_ii = blk: {
                            for (node.keys.slice(), 0..) |key, ii| {
                                switch (cmp(cx, needle, key)) {
                                    .eq => {
                                        // return node.keys.orderedRemove(ii);
                                        if (node.children.get(ii).len() >= order) {
                                            node.keys.set(ii, node.children.get(ii).deleteMax(ha, cx));
                                            return key;
                                        } else if (node.children.get(ii + 1).len() >= order) {
                                            node.keys.set(ii, node.children.get(ii + 1).deleteMin(ha, cx));
                                            return key;
                                        } else {
                                            node.children.get(ii).merge(ha, node.keys.orderedRemove(ii), node.children.orderedRemove(ii + 1));
                                            // FIXME: why does the book rec recursive delete here instead of just excluding needle from the merge
                                            return node.children.get(ii).delete(ha, cx, needle);
                                        }
                                    },
                                    .lt => break :blk ii,
                                    .gt => {},
                                }
                            }
                            break :blk node.children.len - 1;
                        };
                        var chosen_child = node.children.get(child_ii);
                        if (chosen_child.len() < order) {
                            const has_left = child_ii > 0;
                            const has_right = child_ii < node.children.len - 1;
                            if (has_left) {
                                var left = node.children.get(child_ii - 1);
                                if (left.len() >= order) {
                                    // give chosen key[chosen]
                                    chosen_child.keys().insert(0, node.keys.get(child_ii - 1)) catch unreachable;
                                    // replace key[chosen] with key[last] from left
                                    node.keys.set(child_ii - 1, left.keys().pop());
                                    switch (chosen_child.*) {
                                        // give chosen last child from left
                                        .internal => |*chosen_internal| {
                                            chosen_internal.children.insert(0, left.internal.children.pop()) catch unreachable;
                                        },
                                        else => {},
                                    }
                                    return chosen_child.delete(ha, cx, needle);
                                } else if (has_right) {
                                    const right = node.children.get(child_ii + 1);
                                    // if both siblings and chosen are under order
                                    if (right.len() < order) {
                                        // TODO: what's the point of checking right here? it doesn't get modified
                                        left.merge(ha, node.keys.orderedRemove(child_ii - 1), node.children.orderedRemove(child_ii));
                                        return left.delete(ha, cx, needle);
                                    }
                                }
                            }
                            if (has_right) {
                                const right = node.children.get(child_ii + 1);
                                if (right.len() >= order) {
                                    // give chosen key[chosen]
                                    chosen_child.keys().append(node.keys.get(child_ii)) catch unreachable;
                                    // replace key[chosen] with key[first] from right
                                    node.keys.set(child_ii, right.keys().orderedRemove(0));
                                    switch (chosen_child.*) {
                                        // give chosen last child from right
                                        .internal => |*chosen_internal| {
                                            chosen_internal.children.append(right.internal.children.orderedRemove(0)) catch unreachable;
                                        },
                                        else => {},
                                    }
                                    return chosen_child.delete(ha, cx, needle);
                                }
                            }
                        }
                        return chosen_child.delete(ha, cx, needle);
                    },
                }
            }

            fn deleteMin(self: *Node, ha: Allocator, cx: Ctx) T {
                switch (self.*) {
                    .leaf => |*node| {
                        return node.keys.orderedRemove(0);
                    },
                    .internal => |*node| {
                        var chosen_child = node.children.get(0);
                        if (chosen_child.len() < order) {
                            var right = node.children.get(1);
                            if (right.len() >= order) {
                                // give chosen key[chosen]
                                chosen_child.keys().append(node.keys.get(0)) catch unreachable;
                                // replace self.key[chosen] with key[first] from right
                                node.keys.set(0, right.keys().orderedRemove(0));
                                switch (chosen_child.*) {
                                    // give chosen first child from right
                                    .internal => |*chosen_internal| {
                                        chosen_internal.children.append(right.internal.children.orderedRemove(0)) catch unreachable;
                                    },
                                    else => {},
                                }
                            } else {
                                // both are under order, merge them
                                chosen_child.merge(ha, node.keys.orderedRemove(0), node.children.orderedRemove(1));
                            }
                        }
                        return chosen_child.deleteMin(ha, cx);
                    },
                }
            }

            fn deleteMax(self: *Node, ha: Allocator, cx: Ctx) T {
                switch (self.*) {
                    .leaf => |*node| {
                        return node.keys.pop();
                    },
                    .internal => |*node| {
                        var chosen_child = node.children.get(node.children.len - 1);
                        if (chosen_child.len() < order) {
                            var left = node.children.get(node.children.len - 2);
                            if (left.len() >= order) {
                                // give chosen key[chosen]
                                chosen_child.keys().insert(0, node.keys.pop()) catch unreachable;
                                // replace self.key[chosen] with key[last] from left
                                node.keys.append(left.keys().pop()) catch unreachable;
                                switch (chosen_child.*) {
                                    // give chosen last child from left
                                    .internal => |*chosen_internal| {
                                        chosen_internal.children.insert(0, left.internal.children.pop()) catch unreachable;
                                    },
                                    else => {},
                                }
                            } else {
                                // both are under order, merge them
                                left.merge(ha, node.keys.pop(), node.children.pop());
                                // NOTE: we delete from left after the merge
                                return left.deleteMax(ha, cx);
                            }
                        }
                        return chosen_child.deleteMax(ha, cx);
                    },
                }
            }

            // this assumes both self and right are of the same Node type.
            fn merge(self: *Node, ha: Allocator, median: T, right: *Node) void {
                switch (self.*) {
                    .leaf => |*node| {
                        node.keys.append(median) catch unreachable;
                        node.keys.appendSlice(right.leaf.keys.slice()) catch unreachable;
                        right.deinit(ha);
                    },
                    .internal => |*node| {
                        node.keys.append(median) catch unreachable;
                        node.keys.appendSlice(right.internal.keys.slice()) catch unreachable;

                        node.children.appendSlice(right.internal.children.slice()) catch unreachable;
                        right.internal.children.resize(0) catch unreachable;
                        right.deinit(ha);
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

            fn countKeys(self: *const Node) usize {
                switch (self.*) {
                    .leaf => |node| {
                        return node.keys.len;
                    },
                    .internal => |node| {
                        var total: usize = node.keys.len;
                        for (node.children.slice()) |child| {
                            total += child.countKeys();
                        }
                        return total;
                    },
                }
            }

            fn print(self: *const Node, depth: usize) void {
                for (self.constKeys().slice(), 0..) |key, kk| {
                    println("node {} on depth {} key[{}] = {}", .{ @intFromPtr(self), depth, kk, key });
                }
                switch (self.*) {
                    .internal => |node| {
                        for (node.children.slice()) |child| {
                            child.print(depth + 1);
                        }
                    },
                    else => {},
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
                        .internal => |node| node.keys.get(found[1]),
                    };
                }
            }
            return null;
        }

        pub fn delete(self: *Self, cx: Ctx, needle: T) ?T {
            if (self.root) |root| {
                if (root.delete(self.ha, cx, needle)) |found| {
                    switch (root.*) {
                        .leaf => |*node| if (node.keys.len == 0) {
                            defer root.deinit(self.ha);
                            self.root = null;
                        },
                        .internal => |*node| if (node.keys.len == 0) {
                            defer root.deinit(self.ha);
                            defer node.children.resize(0) catch unreachable;
                            self.root = node.children.get(0);
                        },
                    }
                    return found;
                }
            }
            return null;
        }

        pub fn countKeys(self: *const Self) usize {
            if (self.root) |node| {
                return node.countKeys();
            } else {
                return 0;
            }
        }

        pub fn count_depth(self: *const Self) usize {
            if (self.root) |rn| {
                var depth: usize = 1;
                var node = rn;
                while (true) {
                    switch (node.*) {
                        .internal => |in| node = in.children.get(0),
                        else => break,
                    }
                    depth += 1;
                }
                return depth;
            } else {
                return 0;
            }
        }

        pub fn print(self: *Self) void {
            if (self.root) |node| {
                node.print(0);
            }
        }
    };
}

test "BTree.insert" {
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

test "BTree.delete" {
    const MyBtree = BTree(
        u8,
        void,
        struct {
            fn cmp(cx: void, a: u8, b: u8) std.math.Order {
                _ = cx;
                return std.math.order(a, b);
            }
        }.cmp,
        3,
        // .{
        //     .extension = BSTConfig.heapExtension,
        // },
    );
    const ha = std.testing.allocator;

    var tree = MyBtree.init(ha);
    defer tree.deinit();
    const items = "CGMPTXABDEFJKLNOQRSUVYZ";
    for (items) |val| {
        try tree.insert({}, val);
    }
    // tree.print();
    try std.testing.expectEqual(@as(usize, 23), tree.countKeys());
    try std.testing.expectEqual(@as(usize, 3), tree.count_depth());
    try std.testing.expect(tree.find({}, 'F') != null);
    try std.testing.expect(tree.delete({}, 'F') != null);
    try std.testing.expect(tree.find({}, 'F') == null);
    try std.testing.expectEqual(@as(usize, 22), tree.countKeys());
    try std.testing.expect(tree.delete({}, 'M') != null);
    try std.testing.expectEqual(@as(usize, 21), tree.countKeys());
    try std.testing.expect(tree.delete({}, 'G') != null);
    try std.testing.expectEqual(@as(usize, 20), tree.countKeys());
    try std.testing.expect(tree.delete({}, 'D') != null);
    try std.testing.expectEqual(@as(usize, 19), tree.countKeys());
    try std.testing.expect(tree.delete({}, 'B') != null);
    try std.testing.expectEqual(@as(usize, 18), tree.countKeys());
    const del_items = "CPTXAEJKLNOQRSUVYZ";
    for (del_items) |val| {
        try std.testing.expect(tree.delete({}, val) != null);
    }
    try std.testing.expect(tree.count_depth() == 0);
    // try std.testing.expect(tree.find({}, 1223) == null);
}

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
