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

const Self = @This();

limit: u64 = 25,
offset: u64 = 0,
sorting_field: SortingField = .noSorting,
sorting_order: SortingOrder = .ascending,
filter: ?Filter = null,

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
        pub const NameMatch = struct {
            string: []const u8,
        };
        nameMatch: NameMatch,
    };
    pub const Clause = union(enum) {
        pub const Op = union(enum) {
            pub const Tag = @typeInfo(@This()).Union.tag_type;

            @"and": []Clause,
            @"or": []Clause,
            not: *Clause,

            pub fn deinit(self: *@This(), ha7r: Allocator) void {
                switch (self) {
                    .@"and" => |clauses| ha7r.free(clauses),
                    .@"or" => |clauses| ha7r.free(clauses),
                    .not => |clause| ha7r.destroy(clause),
                }
            }
        };
        op: Op,
        param: Param,

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

            pub inline fn withOperator(self: @This(), op: Op.Tag) @This() {
                self.setOperator(op);
                return self;
            }

            pub inline fn setOperator(self: *@This(), op: Op.Tag) void {
                self.op = op;
            }

            pub inline fn withSubClause(self: @This(), clause: Clause) !@This() {
                self.addSubClause(clause);
                return self;
            }

            pub inline fn addSubClause(self: *@This(), clause: Clause) !void {
                try self.sub_clauses.append(self.ha7r, clause);
            }

            pub inline fn withNameMatch(self: @This(), string: []const u8) !@This() {
                self.addNameMatch(string);
                return self;
            }

            pub inline fn addNameMatch(self: *@This(), string: []const u8) !void {
                if (string.len == 0) @panic("NameMatch string can not empty");
                try self.sub_clauses.append(
                    self.ha7r, 
                    Clause { 
                        .param = Param{ 
                            .nameMatch = Param.NameMatch{ 
                                .string = try self.ha7r.dupe(u8, string) 
                            } 
                        } 
                    }
                );
            }

            pub inline fn build(self: @This()) !Clause {
                defer self.deinit();
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
    };
    root: Clause,
};

pub const Builder = struct {
    query: Self = .{},

    pub fn init() @This() {
        return .{};
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
