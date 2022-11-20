const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;

const mod_utils = @import("../utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.dbg;
const param_val_delimiter = ':';

const Query = @import("../Query.zig");
const Filter = Query.Filter;

const Self = @This();

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
        if (
            std.mem.eql(u8, token, "(")
        ) {
            try appender.append(Token.lparen);
        } else if (
            std.mem.eql(u8, token, ")")
        ) {
            try appender.append(Token.rparen);
        } else if (
            std.mem.eql(u8, token, "&") or
            std.mem.eql(u8, token, "and") or
            std.mem.eql(u8, token, "AND") 
        ) {
            try appender.append(Token.andOp);
        } else if (
            std.mem.eql(u8, token, "|") or
            std.mem.eql(u8, token, "or") or
            std.mem.eql(u8, token, "OR") 
        ) {
            try appender.append(Token.orOp);
        } else if (
            std.mem.eql(u8, token, "not") or
            std.mem.eql(u8, token, "NOT") 
        ) {
            try appender.append(Token.notOp);
        } else {
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
            var it2 = std.mem.split(u8, token, ([_]u8{ param_val_delimiter })[0..]);
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
const Param = union(enum) {
    limit: u64,
    offset: u64,
    // top level clause
    tlc: Filter.Clause
};

const Error = error {
    UnexpectedToken,
    UnexpectedNonTerm,
    UnexpectedParam,
    InvalidValue
} || std.mem.Allocator.Error;

tokens: std.ArrayListUnmanaged(Token) = .{},
cur_token_idx: usize = 0,
// cur_term: ?[]const u8,
// utf8_iter: std.unicode.Utf8Iterator,

pub fn deinit(self: *Self, ha7r: Allocator) void {
    self.tokens.deinit(ha7r);
}

fn cur(self: *const Self) ?Token {
    return if (self.cur_token_idx < self.tokens.items.len)
         self.tokens.items[self.cur_token_idx]
    else null;
}

fn peekNext(self: *const Self) ?Token {
    return if ((self.cur_token_idx + 1) < self.tokens.items.len)
         self.tokens.items[self.cur_token_idx + 1]
    else null;
}

fn advance(self: *Self) void {
    self.cur_token_idx += 1;
    // self.cur_term = self.utf8_iter.nextCodepointSlice();
}

pub fn parse(self: *Self, ha7r: Allocator, raw: []const u8) Error!Query {
    // var iter = (try std.unicode.Utf8View.init(str)).iterator();
    // var uno = iter.nextCodepointSlice();

    var builder = Query.Builder.init();
    errdefer {
        builder.discard(ha7r);
    }

    const str = std.mem.trim(u8, raw, " \n\t");

    if (str.len > 0) {
        self.tokens.clearRetainingCapacity();
        try tokenize(
            str, 
            mod_utils.Appender(Token).new(
                &mod_utils.Appender(Token).Curry.UnamanagedList{ .a7r = ha7r, .list = &self.tokens },
                mod_utils.Appender(Token).Curry.UnamanagedList.append
            )
        );

        // top level clauses
        var top_clauses = std.ArrayList(Filter.Clause).init(ha7r);
        defer top_clauses.deinit();
        errdefer {
            for (top_clauses.items) |*clause| {
                clause.deinit(ha7r);
            }
        }
        while (true) {
            if (self.cur() == null) {
                break;
            }         
            switch (try self.expect_param(ha7r)) {
                .limit => |limit| {
                    builder.setPagination(limit, builder.query.offset);
                },
                .offset => |offset| {
                    builder.setPagination(builder.query.limit, offset);
                },
                .tlc => |clause| {
                    try top_clauses.append(clause);
                }
            }
            self.advance();
        }
        if (top_clauses.items.len > 0) {
            if (top_clauses.items.len == 1) {
                builder.setFilter(top_clauses.items[0]);
            } else {
                var cb = Filter.Clause.Builder.init(ha7r);
                defer cb.deinit();
                cb.setOperator(.@"and");
                for (top_clauses.items) |clause| {
                    try cb.addSubClause(clause);
                }
                builder.setFilter(try cb.build());
            }
        }
    }
    return builder.build();
}

fn expect_param(self: *Self, ha7r: Allocator) !Param {
    return switch(self.cur() orelse return error.UnexpectedToken) {
        Token.lparen, Token.notOp, Token.value => Param { 
            .tlc = try self.expect_clause(ha7r)
        },
        Token.param => |name| if (std.mem.eql(u8, name, "limit")) blk: { 
            self.advance();
            break :blk Param { .limit = try self.expect_int() };
        } else if (std.mem.eql(u8, name, "offset")) blk: { 
            self.advance();
            break :blk Param { .offset = try self.expect_int() };
        } else Param { 
            .tlc = try self.expect_clause(ha7r)
        },
        else => error.UnexpectedToken,
    };
}

fn expect_clause(self: *Self, ha7r: Allocator) Error!Filter.Clause {
    return switch(self.cur() orelse return error.UnexpectedToken) {
        Token.param => error.UnexpectedParam,
        Token.notOp => blk: {
            self.advance();
            var cb = Filter.Clause.Builder.init(ha7r);
            defer cb.deinit();
            cb.setOperator(.not);
            try cb.addSubClause(try self.expect_clause(ha7r));
            break :blk try cb.build();
        },
        Token.value => |value| blk: {
            if (self.peekNext()) |next| {
                if (
                    next == Token.andOp or
                    next == Token.orOp
                ) {
                    break :blk try self.expect_binary_op_clause(ha7r);
                }
            }
            var path_it = std.mem.tokenize(u8, value, std.fs.path.sep_str);
            var str = path_it.next() orelse unreachable;
            if (path_it.peek() != null) {
                var clause = blk2: {
                    var cb = Query.Filter.Clause.Builder.init(ha7r);
                    defer cb.deinit();
                    try cb.addNameMatch(str);
                    break :blk2 try cb.build();
                };
                while (path_it.next()) |name| {
                    var cb = Query.Filter.Clause.Builder.init(ha7r);
                    defer cb.deinit();
                    cb.setOperator(.@"and");
                    try cb.addChildOf(clause);
                    try cb.addNameMatch(name);
                    clause = try cb.build();
                }
                break :blk clause;
            } else {
                var cb = Query.Filter.Clause.Builder.init(ha7r);
                defer cb.deinit();
                try cb.addNameMatch(str);
                break :blk try cb.build();
            }
        },
        Token.lparen => blk: { 
            var sub_clauses = std.ArrayList(Filter.Clause).init(ha7r);
            defer sub_clauses.deinit();
            while (true) {
                self.advance();
                switch (self.cur() orelse return error.UnexpectedToken) {
                    Token.rparen => break,
                    else => {
                        try sub_clauses.append(
                            try self.expect_clause(ha7r)
                        );
                    },
                }
            }
            if (sub_clauses.items.len > 0) {
                if (sub_clauses.items.len == 1) {
                    break :blk sub_clauses.items[0];
                } else {
                    var cb = Filter.Clause.Builder.init(ha7r);
                    defer cb.deinit();
                    cb.setOperator(.@"and");
                    for (sub_clauses.items) |clause| {
                        try cb.addSubClause(clause);
                    }
                    break :blk cb.build();
                }
            } else {
                // we got an empty parntheses
                break :blk error.UnexpectedToken;
            }
        },
        else => error.UnexpectedToken,
    };
}

fn expect_binary_op_clause(self: *Self, ha7r: Allocator) !Filter.Clause {
    var first_clause = try self.expect_clause(ha7r);
    self.advance();
    return switch (self.cur() orelse return error.UnexpectedToken) {
        Token.orOp, Token.andOp => blk: {
            var cb = Filter.Clause.Builder.init(ha7r);
            defer cb.deinit();
            cb.setOperator(
                if (self.cur().? == Token.andOp) .@"and" else .@"or"
            );
            self.advance();
            var second_clause = try self.expect_clause(ha7r);
            try cb.addSubClause(first_clause);
            try cb.addSubClause(second_clause);
            break :blk cb.build();
        },
        else => error.UnexpectedToken,
    };
}

fn expect_int(self: *Self) !u64 {
    return switch(self.cur() orelse return error.UnexpectedToken) {
        Token.value => |value| std.fmt.parseUnsigned(u64, value, 0) catch error.InvalidValue,
        else => error.UnexpectedToken,
    };
}