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
                        .param = Param { 
                            .nameMatch = .{ 
                                .string = try self.ha7r.dupe(u8, string) 
                            } 
                        } 
                    }
                );
            }

            pub inline fn withChildOf(self: @This(), parentFilter: Clause) !@This() {
                self.addChildOf(parentFilter);
                return self;
            }

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
            pub inline fn withDescendantOf(self: @This(), ancestorFilter: Clause) !@This() {
                self.addChildOf(ancestorFilter);
                return self;
            }

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
                                    .@"and" = self.sub_clauses.toOwnedSlice(self.ha7r),
                                } 
                            };
                        },
                        .@"or" => {
                            if (self.sub_clauses.items.len == 1)
                                @panic(".or operator need more than one sub clause");
                            return Clause { 
                                .op = Op { 
                                    .@"or" = self.sub_clauses.toOwnedSlice(self.ha7r),
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


const Parser = struct {
    const param_val_delimiter = ':';
    const Token = union(enum) {
        lparen,
        rparen,
        andOp,
        orOp,
        notOp,
        param: []const u8,
        value: []const u8,
    };

    fn tokenize(raw: []const u8, appender: mod_utils.Appender(Token)) !void {
        var it1 = std.mem.tokenize(u8, raw, " ");
        while (it1.next()) |token_init| {
            var token = token_init;
            switch(token) {
                "(" => {
                    try appender.append(Token.lparen);
                },
                ")" => {
                    try appender.append(Token.rparen);
                },
                "&", "and", "AND" => {
                    try appender.append(Token.andOp);
                },
                "|", "or", "OR" => {
                    try appender.append(Token.orOp);
                },
                "not", "NOT" => {
                    try appender.append(Token.notOp);
                },
                else => {
                    var add_r_paren = false;
                    // handle lparen without whitespace
                    // eg. (dan | joe)
                    if (token[0] == '(') {
                        try appender.append(Token.lparen);
                        token = token[1..];
                    }
                    // handle not without whitespace seprating it 
                    // eg. ^joking
                    if (token[0] == '^') {
                        try appender.append(Token.notOp);
                        token = token[1..];
                    }
                    // handle rparen without whitespace
                    // eg. (dan | joe)
                    if (token[token.len - 1] == ')') {
                        add_r_paren = true;
                        token = token[1..(token.len - 1)];
                    }
                    var it2 = std.mem.split(u8, token, .{ param_val_delimiter });
                    var val = it2.next().?;
                    var rest = it2.rest();
                    if (rest.len == 0) {
                        try appender.append(Token{ .value = val });
                    } else {
                        try appender.append(Token{ .param = val });
                        try appender.append(Token{ .value = rest });
                    }
                    if (add_r_paren) {
                        try appender.append(Token.rparen);
                    }
                }
            }
        }
    }
    const Param = union(enum) {
        limit: u64,
        offset: u64,
        filter: Filter
    };

    const Error = error {
        UnexpectedToken,
        UnexpectedNonTerm,
        UnexpectedParam,
        InvalidValue
    };

    ha7r: Allocator,
    string: []const u8,
    tokens: std.ArrayListUnmanaged(Token) = .{},
    cur_token_idx: usize = 0,
    // cur_term: ?[]const u8,
    // utf8_iter: std.unicode.Utf8Iterator,

    fn init(ha7r: Allocator, raw: []const u8) !Parser {
        const str = std.mem.trim(u8, raw, " \n\t");
        // var iter = (try std.unicode.Utf8View.init(str)).iterator();
        // var uno = iter.nextCodepointSlice();
        var tokens = std.ArrayListUnmanaged(Token);
        try tokenize(str, mod_utils.Appender(Token).new(
            &tokens,
            mod_utils.Appender(Token).Curry.UnamanagedList
        ));
        return Parser {
            .string = str,
            .ha7r = ha7r,
            .tokens = tokens,
            // .cur_term = uno,
            // .utf8_iter = iter,
        };
    }

    fn deinit(self: *Parser) void {
        self.tokens.deinit(self.ha7r);
    }

    fn cur(self: *const Parser) ?Token {
        return if (self.cur_token_idx < self.tokens.len)
             self.tokens[self.cur_token_idx]
        else null;
    }

    fn advance(self: *Parser) void {
        self.cur_term = self.utf8_iter.nextCodepointSlice();
    }

    fn query(self: *Parser) !Self {
        var builder = Builder.init();
        var filters = std.ArrayList(Filter).init(self.ha7r);
        defer filters.deint();
        while (true) {
            switch(self.cur()) {
                null => break,
                else => {
                    switch (try self.param()) {
                        .limit => |limit| {
                            builder.setPagination(limit, builder.query.offset);
                        },
                        .offset => |offset| {
                            builder.setPagination(builder.query.limit,offset);
                        },
                        .filter => |filter| {
                            filters.append(filter);
                        }
                    }
                }
            }
            self.advance();
        }
        return builder.build();
    }

    fn param(self: *Parser) !Param {
        return switch(self.cur()) {
            Token.lparen, Token.value => Param { .filter = try self.clause() },
            Token.param => |name| switch (name) {
                "limit" => blk: { 
                    self.advance();
                    break :blk Param { .limit = try self.int() };
                },
                "offset" => blk: { 
                    self.advance();
                    break :blk Param { .offset = try self.int() };
                },
                else => Param { .filter = try self.clause() },
            },
            else => error.UnexpectedToken,
        };
    }

    fn clause(self: *Parser) !Filter.Clause {
        return switch(self.cur()) {
            Token.param => |name| switch (name) {
                else => error.UnexpectedParam,
            },
            Token.notOp => blk: {
                self.advance();
                var cb = Filter.Clause.Builder.init(self.ha7r);
                cb.setOperator(.not);
                cb.withSubClause(try self.clause());
                break :blk try cb.build();
            },
            Token.value => |value| blk: {
                var cb = Filter.Clause.Builder.init(self.ha7r);
                try cb.withNameMatch(value);
                break :blk try cb.build();
            },
            Token.lparen => blk: { 
                var cb = Filter.Clause.Builder.init(self.ha7r);
                self.advance();
                while (true) {
                    switch (self.cur()) {
                        Token.rparen => break,
                        Token.andOp => {
                            switch (cb.op) {
                                .@"and" => {}, 
                                .@"or" => {}, 
                            }
                        }
                    }
                }
                break :blk cb.build();
            },
        };
    }

    fn int(self: *Parser) !u64 {
        return switch(self.cur()) {
            Token.value => |value| std.fmt.parseUnsigned(u64, value, 0) catch error.InvalidValue,
            else => error.UnexpectedToken,
        };
    }
};

pub fn parse(ha7r: Allocator, string: [] const u8) !Self {
    _ = string;
    var builder = Builder.init();
    var clause_builder = Filter.Clause.Builder.init(ha7r);
    var it = std.mem.tokenize(u8, string, " ");
    while (it.next()) |token| {
        // println("{s}", .{ token });
        try clause_builder.addNameMatch(token);
    }
    if(clause_builder.sub_clauses.items.len > 0) {
        builder.setFilter(try clause_builder.build());
    }
    return builder.build();
}

test "Query.parse" {
    if (true) return error.SkipZigTest;
    var ha7r = std.testing.allocator;

    var query = try parse(ha7r, "bye dye");
    defer query.deinit(ha7r);
    println("{!}", .{ query });
}
