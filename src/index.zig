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

pub fn orderToFillPageSwapBTreeMap(
    comptime K: type,
    comptime V: type,
    page_size: usize,
) usize {
    const order = orderToFillPageSwapBTree(SwapBTreeMap(
        K,
        V,
        void,
        struct {
            fn cmp(cx: void, a: K, b: K) std.math.Order {
                _ = cx;
                _ = a;
                _ = b;
                unreachable;
            }
        }.cmp,
        1,
    ).Entry, page_size);
    return order;
}

pub fn SwapBTreeMap(
    comptime K: type,
    comptime V: type,
    comptime Ctx: type,
    comptime cmp: fn (cx: Ctx, a: K, b: K) std.math.Order,
    comptime order: usize,
) type {
    return struct {
        const Self = @This();
        pub const Entry = struct { key: K, val: V };
        const Tree = SwapBTree(
            Entry,
            Ctx,
            struct {
                fn entry_cmp(cx: Ctx, a: Entry, b: Entry) std.math.Order {
                    return cmp(cx, a.key, b.key);
                }
            }.entry_cmp,
            order,
        );

        tree: Tree,

        pub fn init() Self {
            return .{ .tree = Tree.init() };
        }

        pub fn deinit(self: *Self, sa: SwapAllocator) !void {
            try self.tree.deinit(sa);
        }

        pub fn set(self: *Self, sa: SwapAllocator, ctx: Ctx, key: K, val: V) !?V {
            if (try self.tree.insert(sa, ctx, .{ .key = key, .val = val })) |old| {
                return old.val;
            }
            return null;
        }

        pub fn get(self: *const Self, sa: SwapAllocator, ctx: Ctx, key: K) !?V {
            if (try self.tree.find(sa, ctx, .{ .key = key, .val = undefined })) |find| {
                return find.val;
            }
            return null;
        }

        pub fn delete(self: *Self, sa: SwapAllocator, ctx: Ctx, key: K) !?V {
            if (try self.tree.remove(sa, ctx, .{ .key = key, .val = undefined })) |find| {
                return find.val;
            }
            return null;
        }

        pub fn size(self: *const Self, sa: SwapAllocator) !usize {
            return try self.tree.size(sa);
        }
    };
}

test "SwapBTreeMap" {
    const page_size = std.mem.page_size;
    const MyMap = SwapBTreeMap(usize, []const u8, void, struct {
        fn cmp(cx: void, a: usize, b: usize) std.math.Order {
            _ = cx;
            return std.math.order(a, b);
        }
    }.cmp, 12);
    const ha = std.testing.allocator;
    var mmap_pager = try mod_mmap.MmapPager.init(ha, "/tmp/SwapBTreeMap.insert", .{
        .page_size = page_size,
    });
    defer mmap_pager.deinit();

    var lru = try mod_mmap.LRUSwapCache.init(ha, mmap_pager.pager(), 1);
    defer lru.deinit();
    var pager = lru.pager();

    var msa = mod_mmap.PagingSwapAllocator(.{}).init(ha, pager);
    defer msa.deinit();
    var sa = msa.allocator();

    var map = MyMap.init();
    defer map.deinit(sa) catch unreachable;

    try std.testing.expect(try map.set(sa, {}, 1, "one") == null);
    try std.testing.expect(try map.set(sa, {}, 1, "wan") != null);
    try std.testing.expect(try map.set(sa, {}, 1, "uno") != null);
    try std.testing.expect(try map.set(sa, {}, 1, "and") != null);
    try std.testing.expect(try map.set(sa, {}, 2, "tew") == null);
    try std.testing.expect(try map.set(sa, {}, 3, "tree") == null);
    try std.testing.expect(try map.set(sa, {}, 4, "for") == null);
    try std.testing.expect(try map.set(sa, {}, 5, "fif") == null);
    try std.testing.expect(try map.set(sa, {}, 6, "sex") == null);
    try std.testing.expectEqual(@as(usize, 6), try map.size(sa));
}

pub fn orderToFillPageSwapBTree(comptime T: type, page_size: usize) usize {

    // let t = @sizeof(T)
    // let p = @sizeof(Ptr)
    // let y = page_size;
    // t(2x - 1) + 2xp = y
    // 2xt - t + 2xp = y
    // 2xt + 2xp = y - t
    // xt + xp = (y - t) / 2
    // x(t + p) = (y - t) / 2
    // x = (y - t) / 2t + 2p
    const val_size = @sizeOf(T);
    const order = (page_size - val_size) / (2 * (val_size + @sizeOf(Ptr)));
    const Tree = SwapBTree(
        T,
        void,
        struct {
            fn cmp(cx: void, a: T, b: T) std.math.Order {
                _ = cx;
                _ = a;
                _ = b;
                unreachable;
            }
        }.cmp,
        order,
    );
    if (@sizeOf(Tree.Node) > page_size) {
        @compileError("this baby's not working");
    }
    return order;
}

// const BTreeConfig = struct {
//     order: usize = 12,
// };

// Order will be multiplied by to ensure an even value.
pub fn SwapBTree(
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
        const KeyArray = std.BoundedArray(T, (2 * order) - 1);

        const Leaf = struct {
            keys: KeyArray,

            fn swapIn(sa: SwapAllocator, ptr: Ptr) !*Leaf {
                const slice = try sa.swapIn(ptr);
                std.debug.assert(slice.len >= @sizeOf(@This()));
                const hptr: *align(@alignOf(@This())) [@sizeOf(@This())]u8 = @alignCast(
                    @ptrCast(slice.ptr),
                );
                return std.mem.bytesAsValue(@This(), hptr);
            }

            fn fromSlice(sa: SwapAllocator, key_slice: []const T) !struct {
                ptr: Ptr,
                leaf: *Leaf,
            } {
                const ptr = try sa.alloc(@sizeOf(Leaf));
                const leaf = try Leaf.swapIn(sa, ptr);
                leaf.* = .{
                    .keys = KeyArray.fromSlice(key_slice) catch unreachable,
                };
                return .{ .ptr = ptr, .leaf = leaf };
            }

            fn fromSlicePtr(sa: SwapAllocator, key_slice: []const T) !NodePtr {
                const nodenptr = try Leaf.fromSlice(sa, key_slice);
                sa.swapOut(nodenptr.ptr);
                return .{ .leaf = nodenptr.ptr };
            }

            fn insert(self: *Leaf, cx: Ctx, item: T) !?T {
                for (self.keys.slice(), 0..) |key, ii| {
                    switch (cmp(cx, item, key)) {
                        .eq => {
                            self.keys.set(ii, item);
                            return key;
                        },
                        .lt => {
                            self.keys.insert(ii, item) catch unreachable;
                            return null;
                        },
                        .gt => {},
                    }
                }
                // it's above all our keys so add it at the end
                self.keys.append(item) catch unreachable;
                return null;
            }

            fn remove(self: *@This(), cx: Ctx, needle: T) !?T {
                for (self.keys.slice(), 0..) |key, ii| {
                    switch (cmp(cx, needle, key)) {
                        .eq => {
                            return self.keys.orderedRemove(ii);
                        },
                        .lt => break,
                        .gt => {},
                    }
                }
                return null;
            }
        };

        const Internal = struct {
            const ChildrenArray = union(enum) {
                const Array = std.BoundedArray(Ptr, 2 * order);
                const Slice = union(enum) {
                    leaf: []Ptr,
                    internal: []Ptr,

                    fn len(self: *const @This()) usize {
                        return switch (self.*) {
                            .leaf => |arr| arr.len,
                            .internal => |arr| arr.len,
                        };
                    }

                    fn subslice(self: @This(), start: usize, end: ?usize) Slice {
                        if (end) |end_ii| {
                            return switch (self) {
                                .leaf => |sl| .{ .leaf = sl[start..end_ii] },
                                .internal => |sl| .{ .internal = sl[start..end_ii] },
                            };
                        }
                        return switch (self) {
                            .leaf => |sl| .{ .leaf = sl[start..] },
                            .internal => |sl| .{ .internal = sl[start..] },
                        };
                    }
                };

                // children types are gomogenous
                leaf: Array,
                internal: Array,

                fn fromSlice(children_slice: Slice) !@This() {
                    return switch (children_slice) {
                        .leaf => |children| .{ .leaf = try Array.fromSlice(children) },
                        .internal => |children| .{ .internal = try Array.fromSlice(children) },
                    };
                }

                fn len(self: *const @This()) usize {
                    return switch (self.*) {
                        .leaf => |arr| arr.len,
                        .internal => |arr| arr.len,
                    };
                }

                fn get(self: *@This(), ii: usize) NodePtr {
                    return switch (self.*) {
                        .leaf => |arr| .{ .leaf = arr.get(ii) },
                        .internal => |arr| .{ .internal = arr.get(ii) },
                    };
                }

                fn slice(self: *@This()) Slice {
                    return switch (self.*) {
                        .leaf => |*arr| .{ .leaf = arr.slice() },
                        .internal => |*arr| .{ .internal = arr.slice() },
                    };
                }

                fn pop(self: *@This()) NodePtr {
                    return switch (self.*) {
                        .leaf => |*arr| .{ .leaf = arr.pop() },
                        .internal => |*arr| .{ .internal = arr.pop() },
                    };
                }

                fn append(self: *@This(), node: NodePtr) !void {
                    switch (self.*) {
                        .leaf => |*arr| switch (node) {
                            .leaf => |ptr| try arr.append(ptr),
                            else => unreachable,
                        },
                        .internal => |*arr| switch (node) {
                            .internal => |ptr| try arr.append(ptr),
                            else => unreachable,
                        },
                    }
                }

                fn appendSlice(self: *@This(), children_slice: Slice) !void {
                    switch (self.*) {
                        .leaf => |*arr| switch (children_slice) {
                            .leaf => |native_slice| try arr.appendSlice(native_slice),
                            else => unreachable,
                        },
                        .internal => |*arr| switch (children_slice) {
                            .internal => |native_slice| try arr.appendSlice(native_slice),
                            else => unreachable,
                        },
                    }
                }

                fn insert(self: *@This(), ii: usize, node: NodePtr) !void {
                    switch (self.*) {
                        .leaf => |*arr| switch (node) {
                            .leaf => |ptr| try arr.insert(ii, ptr),
                            else => unreachable,
                        },
                        .internal => |*arr| switch (node) {
                            .internal => |ptr| try arr.insert(ii, ptr),
                            else => unreachable,
                        },
                    }
                }

                fn orderedRemove(self: *@This(), ii: usize) NodePtr {
                    return switch (self.*) {
                        .leaf => |*arr| .{ .leaf = arr.orderedRemove(ii) },
                        .internal => |*arr| .{
                            .internal = arr.orderedRemove(ii),
                        },
                    };
                }

                fn resize(self: *@This(), sx: usize) !void {
                    switch (self.*) {
                        .leaf => |*arr| try arr.resize(sx),
                        .internal => |*arr| try arr.resize(sx),
                    }
                }
            };

            keys: KeyArray,
            children: ChildrenArray,

            fn swapIn(sa: SwapAllocator, ptr: Ptr) !*@This() {
                const slice = try sa.swapIn(ptr);
                std.debug.assert(slice.len >= @sizeOf(@This()));
                const hptr: *align(@alignOf(@This())) [@sizeOf(@This())]u8 = @alignCast(
                    @ptrCast(slice.ptr),
                );
                return std.mem.bytesAsValue(@This(), hptr);
            }

            fn fromSlice(
                sa: SwapAllocator,
                key_slice: []const T,
                children: ChildrenArray.Slice,
            ) !struct { ptr: Ptr, internal: *Internal } {
                std.debug.assert(key_slice.len == children.len() - 1);
                const ptr = try sa.alloc(@sizeOf(Internal));
                const internal = try Internal.swapIn(sa, ptr);
                internal.* = .{
                    .keys = KeyArray.fromSlice(key_slice) catch unreachable,
                    .children = ChildrenArray.fromSlice(children) catch unreachable,
                };
                return .{ .ptr = ptr, .internal = internal };
            }

            fn fromSlicePtr(
                sa: SwapAllocator,
                key_slice: []const T,
                children: ChildrenArray.Slice,
            ) !NodePtr {
                const nodenptr = try Internal.fromSlice(sa, key_slice, children);
                sa.swapOut(nodenptr.ptr);
                return .{ .internal = nodenptr.ptr };
            }

            fn insert(
                self: *@This(),
                sa: SwapAllocator,
                cx: Ctx,
                item: T,
            ) SwapAllocator.Error!?T {
                var ii = blk: {
                    for (self.keys.slice(), 0..) |key, ii| {
                        switch (cmp(cx, item, key)) {
                            .eq => {
                                self.keys.set(ii, item);
                                return key;
                            },
                            .lt => {
                                break :blk ii;
                            },
                            .gt => {},
                        }
                    }
                    // it's above all our keys so add it to the last child
                    break :blk self.children.len() - 1;
                };
                if (try self.splitChildIfFull(sa, ii)) {
                    if (cmp(cx, item, self.keys.get(ii)) == .gt) {
                        // add to the new daughter node
                        ii += 1;
                    }
                }
                const chosen_ptr = self.children.get(ii);
                var chosen_child = try chosen_ptr.swapIn(sa);
                defer chosen_ptr.swapOut(sa);
                return try chosen_child.insert(sa, cx, item);
            }

            fn splitChildIfFull(self: *@This(), sa: SwapAllocator, child_ii: usize) !bool {
                const child_ptr = self.children.get(child_ii);
                var child = try child_ptr.swapIn(sa);
                defer child_ptr.swapOut(sa);
                switch (child) {
                    .leaf => |node| {
                        if (node.keys.len == (2 * order) - 1) {
                            const right_ptr = try Leaf.fromSlicePtr(
                                sa,
                                node.keys.slice()[order..],
                            );
                            const mid = node.keys.get(order - 1);
                            node.keys.resize(order - 1) catch unreachable;
                            if (child_ii == self.children.len() - 1) {
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
                    .internal => |node| {
                        if (node.keys.len == (2 * order) - 1) {
                            const right_ptr = try Internal.fromSlicePtr(
                                sa,
                                node.keys.slice()[order..],
                                node.children.slice().subslice(order, null),
                            );
                            node.children.resize(order) catch unreachable;
                            const mid = node.keys.get(order - 1);
                            node.keys.resize(order - 1) catch unreachable;
                            if (child_ii == self.children.len() - 1) {
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

            // FIXME: this could be cleaner, I suspect we can move things around to minimize how many
            // nodes are swappend in at any point
            fn remove(
                self: *@This(),
                sa: SwapAllocator,
                cx: Ctx,
                needle: T,
            ) SwapAllocator.Error!?T {
                const child_ii = blk: {
                    for (self.keys.slice(), 0..) |key, ii| {
                        switch (cmp(cx, needle, key)) {
                            .eq => {
                                const child_ptr = self.children.get(ii);
                                var child = try child_ptr.swapIn(sa);
                                defer child_ptr.swapOut(sa);
                                // return node.keys.orderedRemove(ii);
                                if (child.len() >= order) {
                                    self.keys.set(ii, try child.removeMax(sa, cx));
                                    return key;
                                }

                                const child_b_ptr = self.children.get(ii + 1);
                                var child_b = try child_b_ptr.swapIn(sa);
                                if (child_b.len() >= order) {
                                    // NOTE: we only defer swapOut in this block
                                    defer child_b_ptr.swapOut(sa);
                                    self.keys.set(ii, try child_b.removeMin(sa, cx));
                                    return key;
                                }

                                // NOTE: we always swap out before merge since it'll free the mergee
                                child_b_ptr.swapOut(sa);
                                try child.merge(
                                    sa,
                                    self.keys.orderedRemove(ii),
                                    self.children.orderedRemove(ii + 1),
                                );
                                // FIXME: why does the book rec recursive remove here instead of just excluding needle from the merge
                                return try child.remove(sa, cx, needle);
                            },
                            .lt => break :blk ii,
                            .gt => {},
                        }
                    }
                    break :blk self.children.len() - 1;
                };

                const chosen_ptr = self.children.get(child_ii);
                var chosen_child = try chosen_ptr.swapIn(sa);
                var swap_out_child = true;
                defer if (swap_out_child) chosen_ptr.swapOut(sa);
                if (chosen_child.len() < order) {
                    const has_left = child_ii > 0;
                    const has_right = child_ii < self.children.len() - 1;
                    if (has_left) {
                        const left_ptr = self.children.get(child_ii - 1);
                        var left = try left_ptr.swapIn(sa);
                        defer left_ptr.swapOut(sa);
                        if (left.len() >= order) {
                            // give chosen key[chosen]
                            chosen_child.keys().insert(
                                0,
                                self.keys.get(child_ii - 1),
                            ) catch unreachable;
                            // replace key[chosen] with key[last] from left
                            self.keys.set(child_ii - 1, left.keys().pop());
                            switch (chosen_child) {
                                // give chosen last child from left
                                .internal => |chosen_internal| {
                                    chosen_internal.children.insert(
                                        0,
                                        left.internal.children.pop(),
                                    ) catch unreachable;
                                },
                                else => {},
                            }
                            return try chosen_child.remove(sa, cx, needle);
                        } else if (has_right) {
                            const right_ptr = self.children.get(child_ii + 1);
                            const right = try right_ptr.swapIn(sa);
                            defer right_ptr.swapOut(sa);
                            // if both siblings and chosen are under order
                            // TODO: what's the point of checking right here? it doesn't get modified
                            if (right.len() < order) {
                                chosen_ptr.swapOut(sa);
                                // we disable the defer block for chosen to avoid double
                                // swap out as `merge` will deinit it
                                swap_out_child = false;
                                try left.merge(
                                    sa,
                                    self.keys.orderedRemove(child_ii - 1),
                                    self.children.orderedRemove(child_ii),
                                );
                                return try left.remove(sa, cx, needle);
                            }
                        }
                    }
                    if (has_right) {
                        const right_ptr = self.children.get(child_ii + 1);
                        var right = try right_ptr.swapIn(sa);
                        defer right_ptr.swapOut(sa);
                        if (right.len() >= order) {
                            // give chosen key[chosen]
                            chosen_child.keys().append(self.keys.get(child_ii)) catch unreachable;
                            // replace key[chosen] with key[first] from right
                            self.keys.set(child_ii, right.keys().orderedRemove(0));
                            switch (chosen_child) {
                                // give chosen last child from right
                                .internal => |chosen_internal| {
                                    chosen_internal.children.append(
                                        right.internal.children.orderedRemove(0),
                                    ) catch unreachable;
                                },
                                else => {},
                            }
                            return try chosen_child.remove(sa, cx, needle);
                        }
                    }
                }
                return try chosen_child.remove(sa, cx, needle);
            }
        };

        const NodePtr = union(enum) {
            leaf: Ptr,
            internal: Ptr,

            pub fn swapIn(self: @This(), sa: SwapAllocator) !Node {
                return switch (self) {
                    .leaf => |ptr| .{ .leaf = try Leaf.swapIn(sa, ptr) },
                    .internal => |ptr| .{ .internal = try Internal.swapIn(sa, ptr) },
                };
            }

            fn swapOut(self: @This(), sa: SwapAllocator) void {
                switch (self) {
                    .leaf => |ptr| {
                        defer sa.swapOut(ptr);
                    },
                    .internal => |ptr| {
                        defer sa.swapOut(ptr);
                    },
                }
            }

            fn deinit(self: @This(), sa: SwapAllocator) !void {
                switch (self) {
                    .leaf => |ptr| {
                        defer sa.free(ptr);
                    },
                    .internal => |ptr| {
                        defer sa.free(ptr);
                        const int = try Internal.swapIn(sa, ptr);
                        defer sa.swapOut(ptr);
                        switch (int.children) {
                            .leaf => |arr| {
                                for (arr.slice()) |child| {
                                    var child_ptr = NodePtr{ .leaf = child };
                                    try child_ptr.deinit(sa);
                                }
                            },
                            .internal => |arr| {
                                for (arr.slice()) |child| {
                                    var child_ptr = NodePtr{ .internal = child };
                                    try child_ptr.deinit(sa);
                                }
                            },
                        }
                    },
                }
            }

            // This assumes that the Node at the ptr has not children
            fn deinitUnchecked(self: @This(), sa: SwapAllocator) void {
                switch (self) {
                    .leaf => |ptr| {
                        defer sa.free(ptr);
                    },
                    .internal => |ptr| {
                        defer sa.free(ptr);
                    },
                }
            }

            /// This returns the node containing the key and the index of the key
            fn find(self: NodePtr, sa: SwapAllocator, cx: Ctx, needle: T) !?struct {
                usize,
                Node,
                NodePtr,
            } {
                const in_mem = try self.swapIn(sa);
                var swap_out = true;
                defer if (swap_out) self.swapOut(sa);
                switch (in_mem) {
                    .leaf => |node| {
                        for (node.keys.slice(), 0..) |key, ii| {
                            switch (cmp(cx, needle, key)) {
                                .eq => {
                                    swap_out = false;
                                    return .{ ii, in_mem, self };
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
                                    return .{ ii, in_mem, self };
                                },
                                .lt => {
                                    const child_ptr = node.children.get(ii);
                                    return try child_ptr.find(sa, cx, needle);
                                },
                                .gt => {},
                            }
                        }
                        const child_ptr = node.children.get(node.children.len() - 1);
                        return try child_ptr.find(sa, cx, needle);
                    },
                }
            }
        };

        const Node = union(enum) {
            leaf: *Leaf,
            internal: *Internal,

            fn constKeys(self: *const @This()) *const KeyArray {
                return switch (self.*) {
                    .leaf => |node| &node.keys,
                    .internal => |node| &node.keys,
                };
            }

            fn keys(self: *@This()) *KeyArray {
                return switch (self.*) {
                    .leaf => |node| &node.keys,
                    .internal => |node| &node.keys,
                };
            }

            fn len(self: *const @This()) usize {
                return switch (self.*) {
                    .leaf => |node| node.keys.len,
                    .internal => |node| node.keys.len,
                };
            }

            fn remove(self: *@This(), sa: SwapAllocator, cx: Ctx, needle: T) !?T {
                return switch (self.*) {
                    .leaf => |node| node.remove(cx, needle),
                    .internal => |node| node.remove(sa, cx, needle),
                };
            }

            fn removeMin(self: *Node, sa: SwapAllocator, cx: Ctx) !T {
                switch (self.*) {
                    .leaf => |node| {
                        return node.keys.orderedRemove(0);
                    },
                    .internal => |node| {
                        const chosen_ptr = node.children.get(0);
                        var chosen_child = try chosen_ptr.swapIn(sa);
                        defer chosen_ptr.swapOut(sa);
                        if (chosen_child.len() < order) {
                            const right_ptr = node.children.get(1);
                            var right = try right_ptr.swapIn(
                                sa,
                            );
                            if (right.len() >= order) {
                                defer right_ptr.swapOut(sa);
                                // give chosen key[chosen]
                                chosen_child.keys().append(node.keys.get(0)) catch unreachable;
                                // replace self.key[chosen] with key[first] from right
                                node.keys.set(0, right.keys().orderedRemove(0));
                                switch (chosen_child) {
                                    // give chosen first child from right
                                    .internal => |chosen_internal| {
                                        chosen_internal.children.append(
                                            right.internal.children.orderedRemove(0),
                                        ) catch unreachable;
                                    },
                                    else => {},
                                }
                            } else {
                                right_ptr.swapOut(sa);
                                // both are under order, merge them
                                try chosen_child.merge(
                                    sa,
                                    node.keys.orderedRemove(0),
                                    node.children.orderedRemove(1),
                                );
                            }
                        }
                        return try chosen_child.removeMin(sa, cx);
                    },
                }
            }

            fn removeMax(self: *Node, sa: SwapAllocator, cx: Ctx) !T {
                switch (self.*) {
                    .leaf => |node| {
                        return node.keys.pop();
                    },
                    .internal => |node| {
                        const chosen_ptr = node.children.get(node.children.len() - 1);
                        var chosen_child = try chosen_ptr.swapIn(
                            sa,
                        );
                        var swap_out_child = true;
                        defer if (swap_out_child) chosen_ptr.swapOut(sa);
                        if (chosen_child.len() < order) {
                            const left_ptr = node.children.get(node.children.len() - 2);
                            var left = try left_ptr.swapIn(
                                sa,
                            );
                            defer left_ptr.swapOut(sa);
                            if (left.len() >= order) {
                                // give chosen key[chosen]
                                chosen_child.keys().insert(0, node.keys.pop()) catch unreachable;
                                // replace self.key[chosen] with key[last] from left
                                node.keys.append(left.keys().pop()) catch unreachable;
                                switch (chosen_child) {
                                    // give chosen last child from left
                                    .internal => |chosen_internal| {
                                        chosen_internal.children.insert(
                                            0,
                                            left.internal.children.pop(),
                                        ) catch unreachable;
                                    },
                                    else => {},
                                }
                            } else {
                                chosen_ptr.swapOut(sa);
                                swap_out_child = false;
                                // both are under order, merge them
                                try left.merge(sa, node.keys.pop(), node.children.pop());
                                // NOTE: we remove from left after the merge
                                return try left.removeMax(sa, cx);
                            }
                        }
                        return try chosen_child.removeMax(sa, cx);
                    },
                }
            }

            // this assumes both self and right are of the same Node type.
            fn merge(self: *Node, sa: SwapAllocator, median: T, right_node: NodePtr) !void {
                var right_ptr = right_node;
                switch (self.*) {
                    .leaf => |node| {
                        const right = try Leaf.swapIn(sa, right_ptr.leaf);
                        defer right_ptr.deinitUnchecked(sa);
                        defer right_ptr.swapOut(sa);

                        node.keys.append(median) catch unreachable;
                        node.keys.appendSlice(right.keys.slice()) catch unreachable;
                    },
                    .internal => |node| {
                        var right = try Internal.swapIn(sa, right_ptr.internal);
                        defer right_ptr.deinitUnchecked(sa);
                        defer right_ptr.swapOut(sa);
                        defer right.children.resize(0) catch unreachable;

                        node.keys.append(median) catch unreachable;
                        node.keys.appendSlice(right.keys.slice()) catch unreachable;

                        node.children.appendSlice(right.children.slice()) catch unreachable;
                    },
                }
            }

            fn insert(self: *Node, sa: SwapAllocator, cx: Ctx, needle: T) !?T {
                return switch (self.*) {
                    .leaf => |leaf| try leaf.insert(cx, needle),
                    .internal => |int| try int.insert(sa, cx, needle),
                };
            }

            fn size(self: *const Node, sa: SwapAllocator) !usize {
                switch (self.*) {
                    .leaf => |node| {
                        return node.keys.len;
                    },
                    .internal => |node| {
                        var total: usize = node.keys.len;
                        switch (node.children) {
                            .leaf => |arr| {
                                for (arr.slice()) |ptr| {
                                    var child_ptr = NodePtr{ .leaf = ptr };
                                    const child = try child_ptr.swapIn(sa);
                                    defer child_ptr.swapOut(sa);
                                    total += try child.size(sa);
                                }
                            },
                            .internal => |arr| {
                                for (arr.slice()) |ptr| {
                                    var child_ptr = NodePtr{ .internal = ptr };
                                    const child = try child_ptr.swapIn(sa);
                                    defer child_ptr.swapOut(sa);
                                    total += try child.size(sa);
                                }
                            },
                        }
                        return total;
                    },
                }
            }

            fn print(self: *const Node, sa: SwapAllocator, cur_depth: usize) !void {
                switch (self.*) {
                    .leaf => |node| {
                        for (node.keys.slice(), 0..) |key, kk| {
                            println(
                                "node {} on depth {} key[{}] = {}",
                                .{ @intFromPtr(self), cur_depth, kk, key },
                            );
                        }
                    },
                    .internal => |node| {
                        switch (node.children) {
                            .leaf => |arr| {
                                for (
                                    arr.slice()[0 .. arr.len - 1],
                                    node.keys.slice(),
                                    0..,
                                ) |ptr, key, kk| {
                                    var child_ptr = NodePtr{ .leaf = ptr };
                                    const child = try child_ptr.swapIn(sa);
                                    defer child_ptr.swapOut(sa);
                                    try child.print(sa, cur_depth + 1);
                                    println(
                                        "node {} on depth {} key[{}] = {}",
                                        .{ @intFromPtr(self), cur_depth, kk, key },
                                    );
                                }
                                var child_ptr = NodePtr{ .leaf = arr.get(arr.len - 1) };
                                const child = try child_ptr.swapIn(sa);
                                defer child_ptr.swapOut(sa);
                                try child.print(sa, cur_depth + 1);
                            },
                            .internal => |arr| {
                                for (
                                    arr.slice()[0 .. arr.len - 1],
                                    node.keys.slice(),
                                    0..,
                                ) |ptr, key, kk| {
                                    var child_ptr = NodePtr{ .internal = ptr };
                                    const child = try child_ptr.swapIn(sa);
                                    defer child_ptr.swapOut(sa);
                                    try child.print(sa, cur_depth + 1);
                                    println(
                                        "node {} on depth {} key[{}] = {}",
                                        .{ @intFromPtr(self), cur_depth, kk, key },
                                    );
                                }
                                var child_ptr = NodePtr{ .internal = arr.get(arr.len - 1) };
                                const child = try child_ptr.swapIn(sa);
                                defer child_ptr.swapOut(sa);
                                try child.print(sa, cur_depth + 1);
                            },
                        }
                    },
                }
            }
        };

        // pager: Pager,
        root: ?NodePtr = null,

        pub fn init() Self {
            return Self{};
        }

        pub fn deinit(self: *Self, sa: SwapAllocator) !void {
            if (self.root) |*root_ptr| {
                try root_ptr.deinit(sa);
                self.root = null;
            }
        }

        pub fn insert(self: *Self, sa: SwapAllocator, cx: Ctx, item: T) !?T {
            if (self.root) |root_ptr| {
                var node = try root_ptr.swapIn(sa);
                defer root_ptr.swapOut(sa);
                if (node.len() == (2 * order) - 1) {
                    var children = switch (root_ptr) {
                        .leaf => |ptr| blk: {
                            var slice = [_]Ptr{ptr};
                            break :blk Internal.ChildrenArray.fromSlice(
                                .{ .leaf = &slice },
                            ) catch unreachable;
                        },
                        .internal => |ptr| blk: {
                            var slice = [_]Ptr{ptr};
                            break :blk Internal.ChildrenArray.fromSlice(
                                .{ .internal = &slice },
                            ) catch unreachable;
                        },
                    };
                    var internal = Internal{
                        .keys = KeyArray.init(0) catch unreachable,
                        .children = children,
                    };
                    _ = try internal.splitChildIfFull(sa, 0);
                    const new_root_ptr = try sa.alloc(@sizeOf(Internal));
                    const new_root = try Internal.swapIn(sa, new_root_ptr);
                    defer sa.swapOut(new_root_ptr);
                    new_root.* = internal;
                    self.root = NodePtr{ .internal = new_root_ptr };
                    return try new_root.insert(sa, cx, item);
                } else {
                    return try node.insert(sa, cx, item);
                }
            } else {
                self.root = try Leaf.fromSlicePtr(sa, &[_]T{item});
                return null;
            }
        }

        pub fn find(self: *Self, sa: SwapAllocator, cx: Ctx, needle: T) !?T {
            if (self.root) |root_ptr| {
                if (try root_ptr.find(sa, cx, needle)) |found| {
                    defer found[2].swapOut(sa);
                    return switch (found[1]) {
                        .leaf => |node| node.keys.get(found[0]),
                        .internal => |node| node.keys.get(found[0]),
                    };
                }
            }
            return null;
        }

        pub fn remove(self: *Self, sa: SwapAllocator, cx: Ctx, needle: T) !?T {
            if (self.root) |root_ptr| {
                var deinit_root = false;
                defer if (deinit_root) root_ptr.deinitUnchecked(sa);
                var root = try root_ptr.swapIn(sa);
                defer root_ptr.swapOut(sa);
                if (try root.remove(sa, cx, needle)) |found| {
                    switch (root) {
                        .leaf => |node| if (node.keys.len == 0) {
                            deinit_root = true;
                            self.root = null;
                        },
                        .internal => |node| if (node.keys.len == 0) {
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

        pub fn size(self: *const Self, sa: SwapAllocator) !usize {
            if (self.root) |root_ptr| {
                const root = try root_ptr.swapIn(sa);
                defer root_ptr.swapOut(sa);
                return try root.size(sa);
            } else {
                return 0;
            }
        }

        pub fn depth(self: *const Self, sa: SwapAllocator) !usize {
            if (self.root) |rn| {
                var ctr: usize = 1;
                var ptr = rn;
                while (true) {
                    const node = try ptr.swapIn(sa);
                    defer ptr.swapOut(sa);
                    switch (node) {
                        .internal => |in| ptr = in.children.get(0),
                        else => break,
                    }
                    ctr += 1;
                }
                return ctr;
            } else {
                return 0;
            }
        }

        pub fn print(self: *Self, sa: SwapAllocator) !void {
            if (self.root) |ptr| {
                const node = try ptr.swapIn(sa);
                defer ptr.swapOut(sa);
                try node.print(sa, 0);
            }
        }

        pub const Iterator = struct {
            const NodeStack = std.ArrayList(struct { NodePtr, usize });
            tree: *const Self,
            sa: SwapAllocator,
            node_stack: NodeStack,
            cur_ptr: ?NodePtr = null,
            cur_node: ?Node = null,
            next_ii: usize = 0,

            fn new(ha: Allocator, sa: SwapAllocator, target: *const Self) !@This() {
                var stack = NodeStack.init(ha);
                if (target.root) |root_ptr| {
                    var ptr = root_ptr;
                    while (true) {
                        var node = try ptr.swapIn(sa);
                        switch (node) {
                            .leaf => {
                                return .{
                                    .tree = target,
                                    .node_stack = stack,
                                    .cur_node = node,
                                    .cur_ptr = ptr,
                                    .sa = sa,
                                };
                            },
                            .internal => |int| {
                                defer stack.getLast()[0].swapOut(sa);
                                try stack.append(.{ ptr, 0 });
                                ptr = int.children.get(0);
                            },
                        }
                    }
                } else {
                    return .{
                        .tree = target,
                        .node_stack = stack,
                        .sa = sa,
                    };
                }
            }

            pub fn deinit(self: *@This()) void {
                if (self.cur_node != null) {
                    (self.cur_ptr orelse unreachable).swapOut(self.sa);
                    self.cur_ptr = null;
                    self.cur_node = null;
                }
                self.node_stack.deinit();
            }

            fn goUp(self: *@This()) !?*T {
                (self.cur_ptr orelse unreachable).swapOut(self.sa);
                if (self.node_stack.popOrNull()) |parent| {
                    const par_ptr = parent[0];
                    self.cur_ptr = par_ptr;
                    const node = try par_ptr.swapIn(self.sa);
                    self.cur_node = node;
                    if (parent[1] < node.len()) {
                        self.next_ii = parent[1] + 1;
                        return &node.internal.keys.slice()[parent[1]];
                    } else {
                        return try self.goUp();
                    }
                }
                self.cur_node = null;
                self.cur_ptr = null;
                return null;
            }

            pub fn next(self: *@This()) !?*T {
                if (self.cur_node) |node| {
                    switch (node) {
                        .leaf => |leaf| if (self.next_ii < leaf.keys.len) {
                            defer self.next_ii += 1;
                            return &leaf.keys.slice()[self.next_ii];
                        } else return try self.goUp(),
                        .internal => |int| if (self.next_ii < int.children.len()) {
                            try self.node_stack.append(
                                .{ self.cur_ptr orelse unreachable, self.next_ii },
                            );
                            var ptr = int.children.get(self.next_ii);
                            (self.cur_ptr orelse unreachable).swapOut(self.sa);
                            while (true) {
                                var cur_node = try ptr.swapIn(self.sa);
                                switch (cur_node) {
                                    .leaf => {
                                        self.cur_node = cur_node;
                                        self.cur_ptr = ptr;
                                        self.next_ii = 0;
                                        return try self.next();
                                    },
                                    .internal => |int_int| {
                                        defer self.node_stack.getLast()[0].swapOut(self.sa);
                                        try self.node_stack.append(.{ ptr, 0 });
                                        ptr = int_int.children.get(0);
                                    },
                                }
                            }
                        } else return try self.goUp(),
                    }
                }
                return null;
            }
        };

        pub fn iterator(self: *const Self, ha: Allocator, sa: SwapAllocator) !Iterator {
            return try Iterator.new(ha, sa, self);
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

    var tree = MyBtree.init();
    defer tree.deinit(sa) catch unreachable;
    for (0..10_000) |ii| {
        _ = ii;
        _ = try tree.insert(sa, {}, std.crypto.random.uintAtMost(usize, 10_000));
    }
    const items = [_]usize{ 123, 53, 98823, 123, 534, 123, 54, 7264, 21, 0, 23 };
    for (items) |val| {
        _ = try tree.insert(sa, {}, val);
    }

    // tree.print();
    try std.testing.expect(try tree.find(sa, {}, 123) != null);
    try std.testing.expect(try tree.find(sa, {}, 0) != null);
    try std.testing.expect(try tree.find(sa, {}, 54) != null);
    const old_count = try tree.size(sa);
    try std.testing.expect(try tree.insert(sa, {}, 54) != null);
    try std.testing.expectEqual(old_count, try tree.size(sa));
    // try std.testing.expect(tree.find({}, 1223) == null);
}

test "SwapBTree.remove" {
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

    var tree = MyBtree.init();
    defer tree.deinit(sa) catch unreachable;
    const items = "CGMPTXABDEFJKLNOQRSUVYZ";
    for (items) |val| {
        _ = try tree.insert(sa, {}, val);
    }
    // tree.print();
    // try std.testing.expectEqual(@as(usize, 23), tree.size());
    // try std.testing.expectEqual(@as(usize, 3), tree.depth());
    try std.testing.expect(try tree.find(sa, {}, 'F') != null);
    try std.testing.expect(try tree.remove(sa, {}, 'F') != null);
    try std.testing.expect(try tree.find(sa, {}, 'F') == null);
    try std.testing.expectEqual(@as(usize, 22), try tree.size(sa));
    try std.testing.expect(try tree.remove(sa, {}, 'M') != null);
    try std.testing.expectEqual(@as(usize, 21), try tree.size(sa));
    try std.testing.expect(try tree.remove(sa, {}, 'G') != null);
    try std.testing.expectEqual(@as(usize, 20), try tree.size(sa));
    try std.testing.expect(try tree.remove(sa, {}, 'D') != null);
    try std.testing.expectEqual(@as(usize, 19), try tree.size(sa));
    try std.testing.expect(try tree.remove(sa, {}, 'B') != null);
    try std.testing.expectEqual(@as(usize, 18), try tree.size(sa));
    const del_items = "CPTXAEJKLNOQRSUVYZ";
    for (del_items) |val| {
        try std.testing.expect(try tree.remove(sa, {}, val) != null);
    }
    try std.testing.expect(try tree.depth(sa) == 0);
    // try std.testing.expect(tree.find({}, 1223) == null);
}

test "SwapBTree.iterator" {
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

    var tree = MyBtree.init();
    defer tree.deinit(sa) catch unreachable;
    const items = "CGMPTXABDEFJKLNOQRSUVYZ";
    for (items) |val| {
        _ = try tree.insert(sa, {}, val);
    }

    var it = try tree.iterator(ha, sa);
    defer it.deinit();
    // try tree.print();
    for ("ABCDEFGJKLMNOPQRSTUVXYZ") |char| {
        const item = try it.next() orelse unreachable;
        try std.testing.expectEqual(@as(usize, char), item.*);
    }
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

            fn remove(self: *Node, ha: Allocator, cx: Ctx, needle: T) ?T {
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
                                            node.keys.set(ii, node.children.get(ii).removeMax(ha, cx));
                                            return key;
                                        } else if (node.children.get(ii + 1).len() >= order) {
                                            node.keys.set(ii, node.children.get(ii + 1).removeMin(ha, cx));
                                            return key;
                                        } else {
                                            node.children.get(ii).merge(ha, node.keys.orderedRemove(ii), node.children.orderedRemove(ii + 1));
                                            // FIXME: why does the book rec recursive remove here instead of just excluding needle from the merge
                                            return node.children.get(ii).remove(ha, cx, needle);
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
                                    return chosen_child.remove(ha, cx, needle);
                                } else if (has_right) {
                                    const right = node.children.get(child_ii + 1);
                                    // if both siblings and chosen are under order
                                    if (right.len() < order) {
                                        // TODO: what's the point of checking right here? it doesn't get modified
                                        left.merge(ha, node.keys.orderedRemove(child_ii - 1), node.children.orderedRemove(child_ii));
                                        return left.remove(ha, cx, needle);
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
                                    return chosen_child.remove(ha, cx, needle);
                                }
                            }
                        }
                        return chosen_child.remove(ha, cx, needle);
                    },
                }
            }

            fn removeMin(self: *Node, ha: Allocator, cx: Ctx) T {
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
                        return chosen_child.removeMin(ha, cx);
                    },
                }
            }

            fn removeMax(self: *Node, ha: Allocator, cx: Ctx) T {
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
                                // NOTE: we remove from left after the merge
                                return left.removeMax(ha, cx);
                            }
                        }
                        return chosen_child.removeMax(ha, cx);
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

            fn size(self: *const Node) usize {
                switch (self.*) {
                    .leaf => |node| {
                        return node.keys.len;
                    },
                    .internal => |node| {
                        var total: usize = node.keys.len;
                        for (node.children.slice()) |child| {
                            total += child.size();
                        }
                        return total;
                    },
                }
            }

            fn print(self: *const Node, cur_depth: usize) void {
                for (self.constKeys().slice(), 0..) |key, kk| {
                    println("node {} on depth {} key[{}] = {}", .{ @intFromPtr(self), cur_depth, kk, key });
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

        pub fn remove(self: *Self, cx: Ctx, needle: T) ?T {
            if (self.root) |root| {
                if (root.remove(self.ha, cx, needle)) |found| {
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

        pub fn size(self: *const Self) usize {
            if (self.root) |node| {
                return node.size();
            } else {
                return 0;
            }
        }

        pub fn depth(self: *const Self) usize {
            if (self.root) |rn| {
                var ctr: usize = 1;
                var node = rn;
                while (true) {
                    switch (node.*) {
                        .internal => |in| node = in.children.get(0),
                        else => break,
                    }
                    ctr += 1;
                }
                return ctr;
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

test "BTree.remove" {
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
    );
    const ha = std.testing.allocator;

    var tree = MyBtree.init(ha);
    defer tree.deinit();
    const items = "CGMPTXABDEFJKLNOQRSUVYZ";
    for (items) |val| {
        try tree.insert({}, val);
    }
    // tree.print();
    try std.testing.expectEqual(@as(usize, 23), tree.size());
    try std.testing.expectEqual(@as(usize, 3), tree.depth());
    try std.testing.expect(tree.find({}, 'F') != null);
    try std.testing.expect(tree.remove({}, 'F') != null);
    try std.testing.expect(tree.find({}, 'F') == null);
    try std.testing.expectEqual(@as(usize, 22), tree.size());
    try std.testing.expect(tree.remove({}, 'M') != null);
    try std.testing.expectEqual(@as(usize, 21), tree.size());
    try std.testing.expect(tree.remove({}, 'G') != null);
    try std.testing.expectEqual(@as(usize, 20), tree.size());
    try std.testing.expect(tree.remove({}, 'D') != null);
    try std.testing.expectEqual(@as(usize, 19), tree.size());
    try std.testing.expect(tree.remove({}, 'B') != null);
    try std.testing.expectEqual(@as(usize, 18), tree.size());
    const del_items = "CPTXAEJKLNOQRSUVYZ";
    for (del_items) |val| {
        try std.testing.expect(tree.remove({}, val) != null);
    }
    try std.testing.expect(tree.depth() == 0);
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

        pub fn remove(self: *Self) ?*Self {
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
        try std.testing.expectEqual(@as(usize, 21), (find.remove() orelse unreachable).item);
    }
    {
        const find = tree;
        defer MyBst.deinitHeap(ha, find);
        tree = find.remove() orelse unreachable;
        try std.testing.expectEqual(@as(usize, 534), tree.item);
    }
    {
        const find = try MyBst.initHeap(ha, 500);
        defer MyBst.deinitHeap(ha, find);
        try std.testing.expect(find.remove() == null);
    }
}
