// TODO: rename Param to Query?

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
    quotedValue: []const u8,
    param: []const u8,
    value: []const u8,
};

// panics if passed the double quote or the colon
fn byteToToken(byte: u8) ?Token {
    std.debug.assert(byte != '"' and byte != param_val_delimiter);
    return switch (byte) {
        '(' => Token.lparen,
        ')' => Token.rparen,
        '^' => Token.notOp,
        '&' => Token.andOp,
        '|' => Token.orOp,
        else => null,
    };
}

// this panics if the segment is empty
fn segmentToToken(segment: []const u8) Token {
    std.debug.assert(segment.len > 0);
    if (std.mem.eql(u8, segment, "and") or
        std.mem.eql(u8, segment, "AND"))
    {
        return Token.andOp;
    } else if (std.mem.eql(u8, segment, "or") or
        std.mem.eql(u8, segment, "OR"))
    {
        return Token.orOp;
    } else if (std.mem.eql(u8, segment, "not") or
        std.mem.eql(u8, segment, "NOT"))
    {
        return Token.notOp;
    } else {
        return Token{ .value = segment };
    }
}

fn tokenize(raw: []const u8, appender: mod_utils.Appender(Token)) !void {
    std.debug.assert(raw.len > 0);
    // var it1 = std.mem.tokenizeSequence(u8, raw, " ");
    var seg_start: usize = 0;
    var idx: usize = 0;
    while (true) {
        const byte = raw[idx];
        // NOTE: this won't include current byte in segment
        const segment = raw[seg_start..idx];
        if (segment.len == 0) {
            switch (byte) {
                ' ', '\t', '\n', '\r', std.ascii.control_code.vt, std.ascii.control_code.ff, param_val_delimiter => {
                    // NOTE: reset segment length to start after the current byte
                    seg_start = idx + 1;
                },
                '"' => {
                    const quote_start = idx;
                    while (true) {
                        // enter the quoted section
                        idx += 1;
                        const closing_byte = raw[idx];
                        switch (closing_byte) {
                            // handle quote close
                            '"' => {
                                const value = raw[quote_start + 1 .. idx]; // note: don't include the opening, closing quotes
                                try appender.append(Token{ .quotedValue = value });
                                break;
                            },
                            // handle escapes
                            '\\' => {
                                idx += 1;
                            },
                            else => {},
                        }
                    }
                    // NOTE: reset segment length to start after the quote closer
                    seg_start = idx + 1;
                },
                else => if (byteToToken(byte)) |token| {
                    try appender.append(token);
                    // NOTE: we only start a new segment if the current byte was a valid
                    // token
                    seg_start = idx + 1;
                },
            }
        } else {
            // if the previous byte's a backslash
            if (raw[idx - 1] == '\\') {
                // we're escaping the current byte, let the segment keep growing
            } else {
                switch (byte) {
                    // handle params
                    // e.g.  limit:15
                    //       |----^
                    // NOTE: checking for params first allows using keywords like and
                    // as param names
                    param_val_delimiter => {
                        const segment_pre = raw[seg_start..idx];
                        std.debug.assert(segment_pre.len > 0);
                        try appender.append(Token{ .param = segment_pre });
                        seg_start = idx + 1;
                    },
                    // handle all operators that can appear after segments
                    // e.g.   `hey|hello`, `hey hello`, `hey&hello`, `^(hey)`,
                    //         |--^         |--^         |--^           |--^
                    //        `(hey(hello))`, `hey^(hello)`, 'hey"hello"'
                    //          |--^           |--^           |--^
                    '|',
                    ' ',
                    '\t',
                    '\n',
                    '\r',
                    std.ascii.control_code.vt,
                    std.ascii.control_code.ff,
                    '&',
                    ')',
                    '(',
                    '^',
                    '"',
                    => {
                        // get the segment before the operator
                        const segment_pre = raw[seg_start..idx];
                        try appender.append(segmentToToken(segment_pre));
                        // NOTE: we want the next segment to include the current byte
                        // so that the first if block above catches it
                        seg_start = idx;
                    },
                    else => {
                        // nothing interesting, keep growing segment
                    },
                }
            }
        }
        idx += 1;
        if (idx == raw.len) {
            if (seg_start < raw.len) {
                const last_segment = raw[seg_start..];
                try appender.append(segmentToToken(last_segment));
            }
            break;
        }
    }
}

const Param = union(enum) {
    limit: u64,
    offset: u64,
    // top level clause
    tlc: Filter.Clause,
};

const Error = error{ UnexpectedToken, UnexpectedNonTerm, UnexpectedParam, InvalidValue } || std.mem.Allocator.Error;

tokens: std.ArrayListUnmanaged(Token) = .{},
cur_token_idx: usize = 0,
// cur_term: ?[]const u8,
// utf8_iter: std.unicode.Utf8Iterator,

pub fn deinit(self: *Self, ha7r: Allocator) void {
    self.tokens.deinit(ha7r);
}

fn reset(self: *Self) void {
    self.tokens.clearRetainingCapacity();
    self.cur_token_idx = 0;
}

fn cur(self: *const Self) ?Token {
    return if (self.cur_token_idx < self.tokens.items.len)
        self.tokens.items[self.cur_token_idx]
    else
        null;
}

fn peekNext(self: *const Self) ?Token {
    return if ((self.cur_token_idx + 1) < self.tokens.items.len)
        self.tokens.items[self.cur_token_idx + 1]
    else
        null;
}

fn advance(self: *Self) void {
    self.cur_token_idx += 1;
    // self.cur_term = self.utf8_iter.nextCodepointSlice();
}

pub fn parse(self: *Self, ha7r: Allocator, raw: []const u8) Error!Query {
    // var iter = (try std.unicode.Utf8View.init(str)).iterator();
    // var uno = iter.nextCodepointSlice();
    self.reset();

    var builder = Query.Builder.init();
    errdefer {
        builder.discard(ha7r);
    }

    const str = std.mem.trim(u8, raw, " \n\t");

    if (str.len > 0) {
        try tokenize(str, mod_utils.Appender(Token).new(&mod_utils.Appender(Token).Curry.UnamanagedList{ .a7r = ha7r, .list = &self.tokens }, mod_utils.Appender(Token).Curry.UnamanagedList.append));
        // println("parsing query: raw={s} tokenized={any}", .{ raw, self.tokens.items });

        // top level clauses
        var top_clauses = std.ArrayList(Filter.Clause).init(ha7r);
        defer top_clauses.deinit();
        errdefer {
            for (top_clauses.items) |*clause| {
                clause.deinit(ha7r);
            }
        }
        while (self.cur() != null) {
            switch (try self.expect_param(ha7r)) {
                .limit => |limit| {
                    builder.setPagination(limit, builder.query.offset);
                },
                .offset => |offset| {
                    builder.setPagination(builder.query.limit, offset);
                },
                .tlc => |clause| {
                    try top_clauses.append(clause);
                },
            }
            self.advance();
        }
        if (top_clauses.items.len > 0) {
            if (top_clauses.items.len == 1) {
                builder.setFilter(top_clauses.items[0]);
            } else {
                // if tere are multiple tlcs, combine them in an `and` close
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
    return switch (self.cur() orelse return error.UnexpectedToken) {
        Token.lparen, Token.notOp, Token.quotedValue, Token.value => Param{ .tlc = try self.expect_clause(ha7r) },
        Token.param => |name| if (std.mem.eql(u8, name, "limit")) blk: {
            self.advance();
            break :blk Param{ .limit = try self.expect_int() };
        } else if (std.mem.eql(u8, name, "offset")) blk: {
            self.advance();
            break :blk Param{ .offset = try self.expect_int() };
        } else Param{ .tlc = try self.expect_clause(ha7r) },
        else => error.UnexpectedToken,
    };
}

fn expect_clause(self: *Self, ha7r: Allocator) Error!Filter.Clause {
    return switch (self.cur() orelse return error.UnexpectedToken) {
        Token.param => error.UnexpectedParam,
        Token.notOp => blk: {
            self.advance();
            var cb = Filter.Clause.Builder.init(ha7r);
            defer cb.deinit();
            cb.setOperator(.not);
            try cb.addSubClause(try self.expect_clause(ha7r));
            break :blk try cb.build();
        },
        Token.value, Token.quotedValue => blk: {
            if (self.peekNext()) |next| {
                if (next == Token.andOp or
                    next == Token.orOp)
                {
                    break :blk try self.expect_binary_op_clause(ha7r);
                }
            }
            break :blk try self.expect_name_match(ha7r);
        },
        Token.lparen => blk: {
            var sub_clauses = std.ArrayList(Filter.Clause).init(ha7r);
            defer sub_clauses.deinit();
            while (true) {
                self.advance();
                switch (self.cur() orelse return error.UnexpectedToken) {
                    Token.rparen => break,
                    else => {
                        try sub_clauses.append(try self.expect_clause(ha7r));
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
            cb.setOperator(if (self.cur().? == Token.andOp) .@"and" else .@"or");
            self.advance();
            var second_clause = try self.expect_clause(ha7r);
            try cb.addSubClause(first_clause);
            try cb.addSubClause(second_clause);
            break :blk cb.build();
        },
        else => error.UnexpectedToken,
    };
}

fn expect_name_match(self: *Self, ha7r: Allocator) !Filter.Clause {
    return switch (self.cur() orelse return error.UnexpectedToken) {
        Token.value => |value| blk: {
            var path_it = std.mem.tokenize(u8, value, std.fs.path.sep_str);
            var str = path_it.next() orelse unreachable;
            if (path_it.peek() != null) {
                var clause = blk2: {
                    var cb = Query.Filter.Clause.Builder.init(ha7r);
                    defer cb.deinit();
                    try cb.addNameMatch(str, true);
                    break :blk2 try cb.build();
                };
                while (path_it.next()) |name| {
                    var cb = Query.Filter.Clause.Builder.init(ha7r);
                    defer cb.deinit();
                    cb.setOperator(.@"and");
                    try cb.addChildOf(clause);
                    try cb.addNameMatch(name, path_it.peek() != null);
                    clause = try cb.build();
                }
                break :blk clause;
            } else {
                var cb = Query.Filter.Clause.Builder.init(ha7r);
                defer cb.deinit();
                try cb.addNameMatch(str, false);
                break :blk try cb.build();
            }
        },
        Token.quotedValue => |value| blk: {
            var path_it = std.mem.tokenize(u8, value, std.fs.path.sep_str);
            var str = path_it.next() orelse unreachable;
            if (path_it.peek() != null) {
                var clause = blk2: {
                    var cb = Query.Filter.Clause.Builder.init(ha7r);
                    defer cb.deinit();
                    try cb.addNameMatch(str, true);
                    break :blk2 try cb.build();
                };
                while (path_it.next()) |name| {
                    var cb = Query.Filter.Clause.Builder.init(ha7r);
                    defer cb.deinit();
                    cb.setOperator(.@"and");
                    try cb.addChildOf(clause);
                    try cb.addNameMatch(name, path_it.peek() != null);
                    clause = try cb.build();
                }
                break :blk clause;
            } else {
                var cb = Query.Filter.Clause.Builder.init(ha7r);
                defer cb.deinit();
                try cb.addNameMatch(str, false);
                break :blk try cb.build();
            }
        },
        else => error.UnexpectedToken,
    };
}

fn expect_int(self: *Self) !u64 {
    return switch (self.cur() orelse return error.UnexpectedToken) {
        Token.value => |value| std.fmt.parseUnsigned(u64, value, 0) catch error.InvalidValue,
        else => error.UnexpectedToken,
    };
}
