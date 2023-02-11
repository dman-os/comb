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

pub const Parser = @import("Query/Parser.zig");

test {
    _ = Parser;
}

const Self = @This();

limit: u64 = 25,
offset: u64 = 0,
sorting_field: SortingField = .noSorting,
sorting_order: SortingOrder = .ascending,
filter: ?Filter = null,

pub fn deinit(self: *@This(), ha7r: Allocator) void {
    if (self.filter) |*filter| {
        filter.root.deinit(ha7r);
    }
}

pub const SortingField = enum {
    noSorting,
    name,
    dir,
    ctime,
    atime,
    mtime,
    entryType,
    size,
    inode,
    device,
    userId,
    groupId,
    mode,
    depth,
};

pub const SortingOrder = enum {
    ascending,
    descending,
};

pub const Filter = struct {
    pub const Param = union(enum) {
        nameMatch: struct {
            string: []const u8,
            exact: bool, // FIXME: find a better name and get rid of it
            pub fn deinit(self: *@This(), ha7r: Allocator) void {
                ha7r.free(self.string);
            }
        },
        childOf: *Clause,
        descendantOf: *Clause,

        pub fn deinit(self: *@This(), ha7r: Allocator) void {
            switch (self.*) {
                .nameMatch => |*load| load.deinit(ha7r),
                .childOf => |subclause| { 
                    subclause.deinit(ha7r);
                    ha7r.destroy(subclause); 
                },
                .descendantOf => |subclause| { 
                    subclause.deinit(ha7r);
                    ha7r.destroy(subclause); 
                },
            }
        }
    };
    pub const Clause = union(enum) {
        pub const Op = union(enum) {
            pub const Tag = @typeInfo(@This()).Union.tag_type orelse unreachable;

            @"and": []Clause,
            @"or": []Clause,
            not: *Clause,

            pub fn deinit(self: *@This(), ha7r: Allocator) void {
                switch (self.*) {
                    .@"and" => |clauses| {
                        for (clauses) |*clause| {
                            clause.deinit(ha7r);
                        }
                        ha7r.free(clauses);
                    },
                    .@"or" => |clauses| {
                        for (clauses) |*clause| {
                            clause.deinit(ha7r);
                        }
                        ha7r.free(clauses);
                    },
                    .not => |clause| { 
                        clause.deinit(ha7r);
                        ha7r.destroy(clause); 
                    },
                }
            }
        };
        op: Op,
        param: Param,

        pub fn deinit(self: *Clause, ha7r: Allocator) void {
            switch (self.*) {
                .op => |*load| load.deinit(ha7r),
                .param => |*load| load.deinit(ha7r),
                // else => unreachable, // FIXME:
            }
        }

        pub const Builder = struct {
            ha7r: Allocator,
            op: ?Op.Tag = null,
            sub_clauses: std.ArrayListUnmanaged(Clause) = .{},

            pub fn init(ha7r: Allocator) @This() {
                return @This(){ .ha7r = ha7r };
            }

            pub fn deinit(self: *@This()) void {
                self.sub_clauses.deinit(self.ha7r);
            }

            //pub inline fn withOperator(self: @This(), op: Op.Tag) @This() {
            //    self.setOperator(op);
            //    return self;
            //}

            pub inline fn setOperator(self: *@This(), op: Op.Tag) void {
                self.op = op;
            }

            //pub inline fn withSubClause(self: @This(), clause: Clause) !@This() {
            //    self.addSubClause(clause);
            //    return self;
            //}

            pub inline fn addSubClause(self: *@This(), clause: Clause) !void {
                try self.sub_clauses.append(self.ha7r, clause);
            }

            //pub inline fn withNameMatch(self: @This(), string: []const u8, exact: bool) !@This() {
            //    self.addNameMatch(string, exact);
            //    return self;
            //}

            pub inline fn addNameMatch(self: *@This(), string: []const u8, exact: bool) !void {
                if (string.len == 0) @panic("NameMatch string can not empty");
                try self.sub_clauses.append(
                    self.ha7r, 
                    Clause { 
                        .param = Param { 
                            .nameMatch = .{ 
                                .string = try self.ha7r.dupe(u8, string) ,
                                .exact = exact
                            } 
                        } 
                    }
                );
            }

            //pub inline fn withChildOf(self: @This(), parentFilter: Clause) !@This() {
            //    self.addChildOf(parentFilter);
            //    return self;
            //}

            pub inline fn addChildOf(self: *@This(), parentFilter: Clause) !void {
                var clause = try self.ha7r.create(Clause);
                clause.* = parentFilter;
                try self.sub_clauses.append(
                    self.ha7r, 
                    Clause { 
                        .param = Param { 
                            .childOf = clause
                        } 
                    }
                );
            }

            //pub inline fn withDescendantOf(self: @This(), ancestorFilter: Clause) !@This() {
            //    self.addChildOf(ancestorFilter);
            //    return self;
            //}

            pub inline fn addDescendantOf(self: *@This(), ancestorFilter: Clause) !void {
                var clause = try self.ha7r.create(Clause);
                clause.* = ancestorFilter;
                try self.sub_clauses.append(
                    self.ha7r, 
                    Clause { 
                        .param = Param { 
                            .descendantOf = clause
                        } 
                    }
                );
            }

            pub inline fn build(self: *@This()) !Clause {
                if (self.sub_clauses.items.len == 0) 
                    @panic(@typeName(@This()) ++ " need at least one sub clauses");
                if (self.op) |op| {
                    switch (op) {
                        .@"and" => {
                            if (self.sub_clauses.items.len == 1)
                                @panic(".and operator need more than one sub clause");
                            return Clause { 
                                .op = Op { 
                                    .@"and" = try self.sub_clauses.toOwnedSlice(self.ha7r),
                                } 
                            };
                        },
                        .@"or" => {
                            if (self.sub_clauses.items.len == 1)
                                @panic(".or operator need more than one sub clause");
                            return Clause { 
                                .op = Op { 
                                    .@"or" = try self.sub_clauses.toOwnedSlice(self.ha7r),
                                } 
                            };
                        },
                        .not => {
                            if (self.sub_clauses.items.len > 1)
                                @panic(".not operator can only have one sub clause");
                            var ptr = try self.ha7r.create(Clause);
                            ptr.* = self.sub_clauses.items[0];
                            return Clause { 
                                .op = Op { 
                                    .not = ptr ,
                                } 
                            };
                        },
                    }
                } else {
                    if (self.sub_clauses.items.len > 1)
                        @panic("operator is required if more than one sub clause added");
                    return self.sub_clauses.items[0];
                }
            }
        };

        pub fn format(
            value: @This(), 
            comptime fmt: []const u8, 
            options: std.fmt.FormatOptions, 
            writer: anytype
        ) !void {
            const T = @This();
            const info = @typeInfo(T).Union;
            if (fmt.len != 0) std.fmt.invalidFmtErr(fmt, value);
            try writer.writeAll(@typeName(T));
            if (info.tag_type) |UnionTagType| {
                try writer.writeAll("{ .");
                try writer.writeAll(@tagName(@as(UnionTagType, value)));
                try writer.writeAll(" = ");
                inline for (info.fields) |u_field| {
                    if (value == @field(UnionTagType, u_field.name)) {
                        try std.fmt.formatType(
                            @field(value, u_field.name), "any", options, writer, std.fmt.default_max_depth
                        );
                    }
                }
                try writer.writeAll(" }");
            } else {
                try std.fmt.format(writer, "@{x}", .{@ptrToInt(&value)});
            }
        }
    };
    root: Clause,
};

pub const Builder = struct {
    query: Self = .{},

    pub fn init() @This() {
        return .{};
    }

    pub fn discard(self: *Builder, ha7r: Allocator) void {
        self.query.deinit(ha7r);
    }

    pub inline fn withPagination(
        self: @This(),
        limit: u64,
        offset: u64,
    ) @This() {
        self.setPagination(limit, offset);
        return self;
    }
    pub inline fn setPagination(
        self: *@This(),
        limit: u64,
        offset: u64,
    ) void {
        self.query.limit = limit;
        self.query.offset = offset;
    }

    pub inline fn withSorting(
        self: @This(),
        field: SortingField,
        order: SortingOrder,
    ) @This() {
        self.setSorting(field, order);
        return self;
    }
    pub inline fn setSorting(
        self: *@This(),
        field: SortingField,
        order: SortingOrder,
    ) void {
        self.query.sorting_field = field;
        self.query.sorting_order = order;
    }

    pub inline fn withFilter(
        self: @This(),
        root_clause: Filter.Clause,
    ) @This() {
        self.setFilter(root_clause);
        return self;
    }
    pub inline fn setFilter(
        self: *@This(),
        root_clause: Filter.Clause,
    ) void {
        self.query.filter = Filter{ .root = root_clause };
    }

    pub inline fn build(self: @This()) Self {
        return self.query;
    }
};

//pub fn parse(ha7r: Allocator, string: [] const u8) !Self {
//    _ = string;
//    var builder = Builder.init();
//    var clause_builder = Filter.Clause.Builder.init(ha7r);
//    var it = std.mem.tokenize(u8, string, " ");
//    while (it.next()) |token| {
//        // println("{s}", .{ token });
//        try clause_builder.addNameMatch(token);
//    }
//    if(clause_builder.sub_clauses.items.len > 0) {
//        builder.setFilter(try clause_builder.build());
//    }
//    return builder.build();
//}
//
//test "Query.parse" {
//    if (true) return error.SkipZigTest;
//    var ha7r = std.testing.allocator;
//
//    var query = try parse(ha7r, "bye dye");
//    defer query.deinit(ha7r);
//    println("{!}", .{ query });
//}
