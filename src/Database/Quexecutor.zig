const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mod_utils = @import("../utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.dbg;

const mod_treewalking = @import("../treewalking.zig");
const FsEntry = mod_treewalking.FsEntry;

const mod_mmap = @import("../mmap.zig");
const SwapList = mod_mmap.SwapList;
const Ptr = mod_mmap.SwapAllocator.Ptr;
const Pager = mod_mmap.Pager;
const SwapAllocator = mod_mmap.SwapAllocator;

const Query = @import("../Query.zig");

const mod_gram = @import("../gram.zig");

const mod_plist = @import("../plist.zig");

const Database = @import("../Database.zig");
const Id = Database.Id;
const Entry = Database.Entry;
const gram_len = Database._gram_len;
const Gram = Database._Gram;

const mod_tracy = @import("../tracy.zig");

pub const Error = error{InvalidQuery};
const Plan = struct {
    const NodeId = usize;
    const NodeSlice = struct {
        start: usize,
        len: usize,
    };

    const Node = struct {
        const Payload = union(enum) {
            pub const Tag = @typeInfo(@This()).Union.tag_type orelse unreachable;
            ixion: NodeSlice,
            @"union": NodeSlice,
            complement: NodeId,
            gramMatch: Gram,
            childOf: NodeId,
            descendantOf: NodeId,
        };

        // parent: NodeId,
        load: Payload,
    };

    nodes: std.ArrayListUnmanaged(Node) = .{},

    pub fn clear(self: *@This()) void {
        self.nodes.clearRetainingCapacity();
    }

    pub fn deinit(self: *@This(), ha7r: Allocator) void {
        self.clear();
        self.nodes.deinit(ha7r);
    }

    fn build(self: *@This(), ha7r: Allocator, clause: *const Query.Filter.Clause) !void {
        const trace = mod_tracy.trace(@src());
        defer trace.end();
        self.nodes.clearRetainingCapacity();
        // don't rely on the return pointer since it might be
        // invalid when sub nodes resize the arraylist
        _ = try self.nodes.addOne(ha7r);
        var root = try self.buildQueryDAG(ha7r, clause);
        // separate addressing the array elt and making the root node
        // since the arraylist might resize/move when DAG is built
        self.nodes.items[0] = root;
    }

    fn buildQueryDAG(self: *@This(), ha7r: Allocator, clause: *const Query.Filter.Clause) anyerror!Node {
        return switch (clause.*) {
            .op => |load| switch (load) {
                .@"and" => |clauses| blk: {
                    if (clauses.len < 2) {
                        println("and clauses need 2 or more children: {any}", .{clause});
                        return Error.InvalidQuery;
                    }
                    var start_idx = self.nodes.items.len;
                    // allocate the children contigiously in `self.nodes`
                    // and just return a slice from that
                    try self.nodes.resize(ha7r, start_idx + clauses.len);
                    for (clauses, 0..) |subclause, ii| {
                        var node = try self.buildQueryDAG(ha7r, &subclause);
                        // `buildQueryDAG` might grow the nodes array
                        // thus moving the location so always address it
                        // through `self.nodes`
                        self.nodes.items[start_idx + ii] = node;
                    }
                    // TODO: consider using an arena allocator and pointers instead
                    // there's no advatage in this scheme if the NodeId's don't have
                    // generations
                    break :blk Node{
                        .load = Node.Payload{
                            .ixion = NodeSlice{ .start = start_idx, .len = clauses.len },
                        },
                    };
                },
                .@"or" => |clauses| blk: {
                    if (clauses.len < 2) {
                        println("or clauses need 2 or more children: {any}", .{clause});
                        return Error.InvalidQuery;
                    }
                    var start_idx = self.nodes.items.len;
                    try self.nodes.resize(ha7r, start_idx + clauses.len);
                    for (clauses, 0..) |subclause, ii| {
                        var node = try self.buildQueryDAG(ha7r, &subclause);
                        self.nodes.items[start_idx + ii] = node;
                    }
                    break :blk Node{
                        .load = Node.Payload{
                            .@"union" = NodeSlice{ .start = start_idx, .len = clauses.len },
                        },
                    };
                },
                .not => |subclause| blk: {
                    println("ERROR: we are here for some reason: {}", .{clause});
                    try self.nodes.append(ha7r, try self.buildQueryDAG(ha7r, subclause));
                    break :blk Node{
                        .load = Node.Payload{
                            .complement = self.nodes.items.len - 1,
                        },
                    };
                },
            },
            .param => |load| switch (load) {
                .nameMatch => |nameMatch| blk: {
                    const GramPos = mod_gram.GramPos(gram_len);
                    var start_idx = self.nodes.items.len;
                    const appenderImpl = struct {
                        a7r: Allocator,
                        list: *std.ArrayListUnmanaged(Node),
                        fn append(this: *const @This(), gram: GramPos) !void {
                            try this.list.append(this.a7r, Node{
                                .load = Node.Payload{
                                    .gramMatch = gram.gram,
                                },
                            });
                        }
                    };
                    // NOTE: should we use delimiters here?
                    try mod_gram.grammer(gram_len, nameMatch.string, nameMatch.exact, std.ascii.whitespace[0..], mod_utils.Appender(GramPos).new(&appenderImpl{ .a7r = ha7r, .list = &self.nodes }, appenderImpl.append));
                    break :blk Node{
                        .load = Node.Payload{
                            .ixion = NodeSlice{ .start = start_idx, .len = self.nodes.items.len - start_idx },
                        },
                    };
                },
                .childOf => |subclause| blk: {
                    try self.nodes.append(ha7r, try self.buildQueryDAG(ha7r, subclause));
                    break :blk Node{
                        .load = Node.Payload{
                            .childOf = self.nodes.items.len - 1,
                        },
                    };
                },
                .descendantOf => |subclause| blk: {
                    try self.nodes.append(ha7r, try self.buildQueryDAG(ha7r, subclause));
                    break :blk Node{
                        .load = Node.Payload{
                            .descendantOf = self.nodes.items.len - 1,
                        },
                    };
                },
            },
        };
    }
};

db: *Database,
name_matcher: Database.PlistNameMatcher = .{},
id_buf: std.ArrayListUnmanaged(Id) = .{},
check: std.AutoHashMapUnmanaged(Id, void) = .{},
plan: Plan = .{},
// timer: std.time.Timer,
// lists: std.ArrayListUnmanaged(Plan.Shortlist) = {},
// filters: std.ArrayListUnmanaged(Plan.Filter) = {},

pub fn init(db: *Database) @This() {
    return @This(){
        .db = db,
    };
}

pub fn deinit(self: *@This()) void {
    self.name_matcher.deinit(self.db);
    self.id_buf.deinit(self.db.ha7r);
    self.plan.deinit(self.db.ha7r);
    // self.lists.deinit(self.db.ha7r);
    // self.filters.deinit(self.db.ha7r);
}

const ExecErr = Allocator.Error || mod_mmap.Pager.SwapInError;

fn execNode(self: *@This(), node: *const Plan.Node, ha7r: Allocator) ExecErr![]Id {
    const trace = mod_tracy.trace(@src());
    defer trace.end();
    // const IdSet = std.AutoHashMapUnmanaged(Id, void);
    // const IdSet = std.ArrayListUnmanaged(Id, void);
    const IdSet = std.AutoHashMap(Id, void);
    var res_set = switch (node.load) {
        .ixion => |slice| blk: {
            var children = self.plan.nodes.items[slice.start..(slice.start + slice.len)];
            var check = IdSet.init(ha7r);
            defer check.deinit();
            // println("ixion: ", .{});
            var result = std.ArrayList(Id).fromOwnedSlice(
                ha7r,
                // FIXME: index out of bounds
                try self.execNode(&children[0], ha7r),
            );
            defer result.deinit();
            // println("-- {any}", .{ result.items });
            for (children[1..]) |subnode| {
                if (result.items.len == 0) break;
                check.clearRetainingCapacity();
                for (result.items) |item| {
                    try check.put(item, {});
                }
                result.clearRetainingCapacity();
                var subnode_result = try self.execNode(&subnode, ha7r);
                defer ha7r.free(subnode_result);

                // println("-- {any}", .{ subnode_result });

                for (subnode_result) |item| {
                    if (check.contains(item)) {
                        try result.append(item);
                    }
                }
            }
            // for some reason, we might have dupe items in `result` at this
            // point so clean those out
            // FIXME: this can be made more efficient
            check.clearRetainingCapacity();
            for (result.items) |item| {
                try check.put(item, {});
            }
            result.clearRetainingCapacity();
            var it = check.keyIterator();
            while (it.next()) |value| {
                try result.append(value.*);
            }
            break :blk result.toOwnedSlice();
        },
        .@"union" => |slice| blk: {
            var children = self.plan.nodes.items[slice.start..(slice.start + slice.len)];
            var result = std.ArrayList(Id).fromOwnedSlice(
                ha7r,
                try self.execNode(&children[0], ha7r),
            );
            defer result.deinit();
            var set = IdSet.init(ha7r);
            defer set.deinit();
            for (result.items) |item| {
                try set.put(item, {});
            }
            for (children) |subnode| {
                var subnode_result = try self.execNode(&subnode, ha7r);
                defer ha7r.free(subnode_result);
                for (subnode_result) |item| {
                    const res = try set.getOrPut(item);
                    if (!res.found_existing) {
                        // FIXME: is this neccessary
                        res.value_ptr.* = {};
                        try result.append(item);
                    }
                }
            }
            break :blk result.toOwnedSlice();
        },
        .complement => |child| {
            _ = child;
            @panic("fuck");
        },
        .gramMatch => |gram| blk: {
            // println("gramMatch: {s}", .{ gram });
            var out = std.ArrayList(Id).init(ha7r);
            defer out.deinit();
            try self.db.plist.gramItems(gram, self.db.sa7r, self.db.pager, mod_utils.Appender(Id).forList(&out));
            break :blk out.toOwnedSlice();
        },
        .childOf => |child_idx| blk: {
            var parents = try self.execNode(&self.plan.nodes.items[child_idx], ha7r);
            defer ha7r.free(parents);
            var result = std.ArrayList(Id).init(ha7r);
            defer result.deinit();
            for (parents) |id| {
                if (try self.db.childOfIndex.get(self.db.sa7r, {}, id)) |children| {
                    var it = try children.iterator(self.db.ha7r, self.db.sa7r);
                    defer it.deinit();
                    while (try it.next()) |child| {
                        try result.append(child.*);
                    }
                }
            }
            break :blk result.toOwnedSlice();
        },
        .descendantOf => |child_idx| blk: {
            if (true) @panic("descendant of index not in use");
            var ancestors = try self.execNode(&self.plan.nodes.items[child_idx], ha7r);
            defer ha7r.free(ancestors);
            var result = std.ArrayList(Id).init(ha7r);
            defer result.deinit();
            for (ancestors) |id| {
                if (try self.db.descendantOfIndex.get(self.db.sa7r, {}, id)) |descendants| {
                    var it = try descendants.iterator(self.db.ha7r, self.db.sa7r);
                    defer it.deinit();
                    while (try it.next()) |child| {
                        try result.append(child.*);
                    }
                }
            }
            break :blk result.toOwnedSlice();
        },
    };
    // println("res_set: {any} for node: {}", .{ res_set, node });
    return res_set;
}

pub fn query(self: *@This(), input: *const Query) ![]const Id {
    const trace = mod_tracy.trace(@src());
    defer trace.end();
    // println("querying: query={}", .{ input });
    if (input.filter) |filter| {
        defer self.plan.clear();
        try self.plan.build(self.db.ha7r, &filter.root);
        var arena = std.heap.ArenaAllocator.init(self.db.ha7r);
        defer arena.deinit();
        // println("plan: {any}", .{ self.plan.nodes.items, });
        var ha7r = arena.allocator();

        self.db.lock.lock();
        defer self.db.lock.unlock();

        var temp_result = try self.execNode(&self.plan.nodes.items[0], ha7r);
        defer ha7r.free(temp_result);
        self.id_buf.clearRetainingCapacity();
        try self.id_buf.appendSlice(self.db.ha7r, temp_result);
        return self.id_buf.items;
    } else {
        var out = &self.id_buf;
        out.clearRetainingCapacity();
        var it = self.db.meta.iterator(self.db.sa7r, self.db.pager);
        defer it.close();
        var ii: u24 = 0;
        while (try it.next()) |meta| {
            if (!meta.free) {
                try out.append(self.db.ha7r, Id{ .id = ii, .gen = meta.gen });
            }
            ii += 1;
        }
        return out.items;
    }
}
