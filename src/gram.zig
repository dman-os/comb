const std = @import("std");

pub const mod_utils = @import("utils.zig");
const println = mod_utils.println;
const dbg = mod_utils.println;
const Appender = mod_utils.Appender;

pub const TEC: u8 = 0;

pub fn Gram(comptime gram_len: u4) type {
    return [gram_len]u8;
}

pub fn GramPos(comptime gram_len: u4) type {
    return struct {
        const Self = @This();
        pos: usize,
        gram: [gram_len]u8,

        fn new(pos: usize, gram: [gram_len]u8) Self {
            return Self{ .pos = pos, .gram = gram };
        }
    };
}

/// This will tokenize the string before gramming it according to the provided delimiter.
/// For example, provide std.ascii.whitespace to tokenize using whitespace.
pub fn grammer(comptime gram_len: u4, string: []const u8, boundary_grams: bool, delimiters: []const u8, out: Appender(GramPos(gram_len))) !void {
    if (gram_len == 0) {
        @compileError("gram_len is 0");
    }
    if (delimiters.len > 0) {
        var iter = std.mem.tokenize(u8, string, delimiters);
        while (iter.next()) |token| {
            try tokenGrammer(gram_len, token, @intFromPtr(token.ptr) - @intFromPtr(string.ptr), boundary_grams, out);
        }
    } else {
        try tokenGrammer(gram_len, string, 0, boundary_grams, out);
    }
}

fn tokenGrammer(comptime gram_len: u4, string: []const u8, offset: usize, boundary_grams: bool, out: Appender(GramPos(gram_len))) !void {
    if (gram_len == 0) {
        @compileError("gram_len is 0");
    }
    if (gram_len <= string.len) {
        if (boundary_grams) {
            try leftBoundaries(gram_len, string, offset, out);
        }
        var pos: usize = 0;
        while (pos + gram_len <= string.len) : ({
            pos += 1;
        }) {
            var gram: [gram_len]u8 = undefined;
            comptime var ii = 0;
            inline while (ii < gram_len) : ({
                ii += 1;
            }) {
                gram[ii] = string[pos + ii];
            }
            try out.append(GramPos(gram_len).new(offset + pos, gram));
        }
        if (boundary_grams) {
            try rightBoundaries(gram_len, string, offset, out);
        }
    } else {
        // left boundaries
        // we can't use the fn `leftBoundaries`, which's partially comtime
        // because we're constrained by the string.len now (which is shorter
        // than gram_len)
        if (boundary_grams) {
            var ii: usize = 1;
            // we'll emit it the `string.len - 1` times
            while (ii < string.len) : ({
                ii += 1;
            }) {
                // var gram = [1]u8{TEC} ** gram_len;
                // var gram_ii = gram_len - ii;
                // var str_ii = 0;
                // while (gram_ii < gram_len) : ({
                //     gram_ii += 1;
                //     str_ii += 1;
                // }) {
                //     gram[gram_ii] = string[str_ii];
                // }
                const gram = fillGram(gram_len, string[0..ii], gram_len - ii);
                try out.append(GramPos(gram_len).new(offset, gram));
            }
        }
        // fill it in from the right
        // i.e TECs on the left
        // this isn't a boundary since the string.len is shorter than gram_len
        {
            const gram = fillGram(gram_len, string, gram_len - string.len);
            try out.append(GramPos(gram_len).new(offset, gram));
        }
        // if it's short enough to have TECs on both side
        if (boundary_grams) {
            var ii = gram_len - string.len - 1;
            while (ii > 0) : ({
                ii -= 1;
            }) {
                const gram = fillGram(gram_len, string, ii);
                try out.append(GramPos(gram_len).new(offset, gram));
            }
        }
        // fill it in from the left
        // i.e. TECS on the right
        {
            const gram = fillGram(gram_len, string, 0);
            try out.append(GramPos(gram_len).new(offset, gram));
        }
        // right boundaries
        if (boundary_grams) {
            var start: usize = 1;
            // we'll emit it the `string.len - 1` times
            while (start < string.len) : ({
                start += 1;
            }) {
                const gram = fillGram(gram_len, string[start..], 0);
                try out.append(GramPos(gram_len).new(offset + start, gram));
            }
        }
    }
}

/// Panics if gram_len - start > string.len
inline fn fillGram(comptime gram_len: u4, string: []const u8, start: usize) Gram(gram_len) {
    var gram = [1]u8{TEC} ** gram_len;
    for (string, 0..) |char, ii| {
        gram[start + ii] = char;
    }
    // var ii = start;
    // while(ii < gram_len): ({ ii += 1; }){
    //     gram[ii] = string[ii - start];
    // }
    return gram;
}

inline fn leftBoundaries(comptime gram_len: u4, string: []const u8, offset: usize, out: Appender(GramPos(gram_len))) !void {
    // the following code will do something similar to what's shown below but for any gram_len
    // the commented out example is how it'd look if gram_len is 3
    // -- out.append(GramPos(2).new(0, .{ TEC, TEC, str[pos] }));
    // -- out.append(GramPos(2).new(0, .{ TEC, str[pos], str[pos + 1] }));

    // append `gram_len - 1` times where each gram is full of TEC
    // this won't enter if gram_len == 1. One length grams can't have boundary grams
    comptime var fill_count = 1; // i.e. the chars that are not TEC
    inline while (fill_count < gram_len) : ({
        fill_count += 1;
    }) {
        // create completely empty gram
        var gram = [1]u8{TEC} ** gram_len;
        // we start at an earlier index each iteration
        // meaning, we progressively fill more of the last few gram positions
        // from the string each iter
        comptime var gram_ii = gram_len - fill_count;
        comptime var str_ii = 0;
        inline while (gram_ii < gram_len) : ({
            gram_ii += 1;
            str_ii += 1;
        }) {
            gram[gram_ii] = string[str_ii];
        }
        try out.append(GramPos(gram_len).new(offset, gram));
    }
}

inline fn rightBoundaries(comptime gram_len: u4, string: []const u8, offset: usize, out: Appender(GramPos(gram_len))) !void {
    const pos = string.len - gram_len;
    // the following code will do something similar to what's shown below but for any gram_len
    // the commented out example is how it'd look if gram_len is 3
    // -- out.append(GramPos(2).new(pos + 1, .{ string[pos + 1], string[pos + 2], TEC }));
    // -- out.append(GramPos(2).new(pos + 2, .{ string[pos + 2], TEC, TEC }));

    // similar to the right boundaries gram. Read those comments
    comptime var tec_count = 1;
    inline while (tec_count < gram_len) : ({
        tec_count += 1;
    }) {
        var gram = [1]u8{TEC} ** gram_len;
        // this time, we fill progressively less from the string
        comptime var gram_ii = 0;
        inline while (gram_ii < (gram_len - tec_count)) : ({
            gram_ii += 1;
        }) {
            gram[gram_ii] = string[pos + tec_count + gram_ii];
        }
        try out.append(GramPos(gram_len).new(offset + pos + tec_count, gram));
    }
}

test "grammer.trigram" {
    // const Case = std.meta.Tuple(.{ []const u8, std.meta.Tuple(.{[]const u8, bool, []Gram(3)}) });
    comptime var table = .{
        .{ .name = "boundaries.3", .string = "etc", .boundary_grams = true, .expected = &.{
            GramPos(3).new(0, .{ TEC, TEC, 'e' }),
            GramPos(3).new(0, .{ TEC, 'e', 't' }),
            GramPos(3).new(0, .{ 'e', 't', 'c' }),
            GramPos(3).new(1, .{ 't', 'c', TEC }),
            GramPos(3).new(2, .{ 'c', TEC, TEC }),
        } },
        .{ .name = "no_boundaries.3", .string = "etc", .boundary_grams = false, .expected = &.{
            GramPos(3).new(0, .{ 'e', 't', 'c' }),
        } },
        .{ .name = "boundaries.2", .string = ".h", .boundary_grams = true, .expected = &.{
            GramPos(3).new(0, .{ TEC, TEC, '.' }),
            GramPos(3).new(0, .{ TEC, '.', 'h' }),
            GramPos(3).new(0, .{ '.', 'h', TEC }),
            GramPos(3).new(1, .{ 'h', TEC, TEC }),
        } },
        .{ .name = "no_boundaries.2", .string = ".h", .boundary_grams = false, .expected = &.{
            GramPos(3).new(0, .{ TEC, '.', 'h' }),
            GramPos(3).new(0, .{ '.', 'h', TEC }),
        } },
        .{ .name = "boundaries.1", .string = "h", .boundary_grams = true, .expected = &.{
            GramPos(3).new(0, .{ TEC, TEC, 'h' }),
            GramPos(3).new(0, .{ TEC, 'h', TEC }),
            GramPos(3).new(0, .{ 'h', TEC, TEC }),
        } },
        .{ .name = "no_boundaries.1", .string = "h", .boundary_grams = false, .expected = &.{
            GramPos(3).new(0, .{ TEC, TEC, 'h' }),
            GramPos(3).new(0, .{ 'h', TEC, TEC }),
        } },
        .{ .name = "boundaries.multi", .string = "homeuser", .boundary_grams = true, .expected = &.{
            GramPos(3).new(0, .{ TEC, TEC, 'h' }),
            GramPos(3).new(0, .{ TEC, 'h', 'o' }),
            GramPos(3).new(0, .{ 'h', 'o', 'm' }),
            GramPos(3).new(1, .{ 'o', 'm', 'e' }),
            GramPos(3).new(2, .{ 'm', 'e', 'u' }),
            GramPos(3).new(3, .{ 'e', 'u', 's' }),
            GramPos(3).new(4, .{ 'u', 's', 'e' }),
            GramPos(3).new(5, .{ 's', 'e', 'r' }),
            GramPos(3).new(6, .{ 'e', 'r', TEC }),
            GramPos(3).new(7, .{ 'r', TEC, TEC }),
        } },
        .{ .name = "no_boundaries.multi", .string = "homeuser", .boundary_grams = false, .expected = &.{
            GramPos(3).new(0, .{ 'h', 'o', 'm' }),
            GramPos(3).new(1, .{ 'o', 'm', 'e' }),
            GramPos(3).new(2, .{ 'm', 'e', 'u' }),
            GramPos(3).new(3, .{ 'e', 'u', 's' }),
            GramPos(3).new(4, .{ 'u', 's', 'e' }),
            GramPos(3).new(5, .{ 's', 'e', 'r' }),
        } },
        .{ .name = "whitespace_is_boundary.1", .string = " home user", .boundary_grams = true, .expected = &.{
            GramPos(3).new(1, .{ TEC, TEC, 'h' }),
            GramPos(3).new(1, .{ TEC, 'h', 'o' }),
            GramPos(3).new(1, .{ 'h', 'o', 'm' }),
            GramPos(3).new(2, .{ 'o', 'm', 'e' }),
            GramPos(3).new(3, .{ 'm', 'e', TEC }),
            GramPos(3).new(4, .{ 'e', TEC, TEC }),
            GramPos(3).new(6, .{ TEC, TEC, 'u' }),
            GramPos(3).new(6, .{ TEC, 'u', 's' }),
            GramPos(3).new(6, .{ 'u', 's', 'e' }),
            GramPos(3).new(7, .{ 's', 'e', 'r' }),
            GramPos(3).new(8, .{ 'e', 'r', TEC }),
            GramPos(3).new(9, .{ 'r', TEC, TEC }),
        } },
        .{ .name = "whitespace_is_boundary.2", .string = " home   user", .boundary_grams = true, .expected = &.{
            GramPos(3).new(1, .{ TEC, TEC, 'h' }),
            GramPos(3).new(1, .{ TEC, 'h', 'o' }),
            GramPos(3).new(1, .{ 'h', 'o', 'm' }),
            GramPos(3).new(2, .{ 'o', 'm', 'e' }),
            GramPos(3).new(3, .{ 'm', 'e', TEC }),
            GramPos(3).new(4, .{ 'e', TEC, TEC }),
            GramPos(3).new(8, .{ TEC, TEC, 'u' }),
            GramPos(3).new(8, .{ TEC, 'u', 's' }),
            GramPos(3).new(8, .{ 'u', 's', 'e' }),
            GramPos(3).new(9, .{ 's', 'e', 'r' }),
            GramPos(3).new(10, .{ 'e', 'r', TEC }),
            GramPos(3).new(11, .{ 'r', TEC, TEC }),
        } },
        .{ .name = "whitespace_is_boundary.3", .string = " home\tuser", .boundary_grams = true, .expected = &.{
            GramPos(3).new(1, .{ TEC, TEC, 'h' }),
            GramPos(3).new(1, .{ TEC, 'h', 'o' }),
            GramPos(3).new(1, .{ 'h', 'o', 'm' }),
            GramPos(3).new(2, .{ 'o', 'm', 'e' }),
            GramPos(3).new(3, .{ 'm', 'e', TEC }),
            GramPos(3).new(4, .{ 'e', TEC, TEC }),
            GramPos(3).new(6, .{ TEC, TEC, 'u' }),
            GramPos(3).new(6, .{ TEC, 'u', 's' }),
            GramPos(3).new(6, .{ 'u', 's', 'e' }),
            GramPos(3).new(7, .{ 's', 'e', 'r' }),
            GramPos(3).new(8, .{ 'e', 'r', TEC }),
            GramPos(3).new(9, .{ 'r', TEC, TEC }),
        } },
        .{ .name = "pure_delimiter.1", .string = " ", .boundary_grams = true, .expected = &.{} },
        .{ .name = "pure_delimiter.2", .string = "    ", .boundary_grams = true, .expected = &.{} },
        .{ .name = "pure_delimiter.3", .string = "", .boundary_grams = true, .expected = &.{} },
    };
    inline for (table) |case| {
        var list = std.ArrayList(GramPos(3)).init(std.testing.allocator);
        defer list.deinit();
        try grammer(3, case.string, case.boundary_grams, &std.ascii.whitespace, Appender(GramPos(3)).new(&list, std.ArrayList(GramPos(3)).append));
        std.testing.expectEqualSlices(GramPos(3), case.expected, list.items) catch |err| {
            std.debug.print("\nerror on {s}\n{any}\n !=\n {any}\n", .{ case.name, case.expected, list.items });
            return err;
        };
    }
}

test "grammer.quadgram" {
    // const Case = std.meta.Tuple(.{ []const u8, std.meta.Tuple(.{[]const u8, bool, []Gram(3)}) });
    comptime var table = .{
        .{ .name = "boundaries.4", .string = "root", .boundary_grams = true, .expected = &.{
            GramPos(4).new(0, .{ TEC, TEC, TEC, 'r' }),
            GramPos(4).new(0, .{ TEC, TEC, 'r', 'o' }),
            GramPos(4).new(0, .{ TEC, 'r', 'o', 'o' }),
            GramPos(4).new(0, .{ 'r', 'o', 'o', 't' }),
            GramPos(4).new(1, .{ 'o', 'o', 't', TEC }),
            GramPos(4).new(2, .{ 'o', 't', TEC, TEC }),
            GramPos(4).new(3, .{ 't', TEC, TEC, TEC }),
        } },
        .{ .name = "no_boundaries.4", .string = "root", .boundary_grams = false, .expected = &.{
            GramPos(4).new(0, .{ 'r', 'o', 'o', 't' }),
        } },
        .{ .name = "boundaries.3", .string = "etc", .boundary_grams = true, .expected = &.{
            GramPos(4).new(0, .{ TEC, TEC, TEC, 'e' }),
            GramPos(4).new(0, .{ TEC, TEC, 'e', 't' }),
            GramPos(4).new(0, .{ TEC, 'e', 't', 'c' }),
            GramPos(4).new(0, .{ 'e', 't', 'c', TEC }),
            GramPos(4).new(1, .{ 't', 'c', TEC, TEC }),
            GramPos(4).new(2, .{ 'c', TEC, TEC, TEC }),
        } },
        .{ .name = "no_boundaries.3", .string = "etc", .boundary_grams = false, .expected = &.{
            GramPos(4).new(0, .{ TEC, 'e', 't', 'c' }),
            GramPos(4).new(0, .{ 'e', 't', 'c', TEC }),
        } },
        .{ .name = "boundaries.2", .string = ".h", .boundary_grams = true, .expected = &.{
            GramPos(4).new(0, .{ TEC, TEC, TEC, '.' }),
            GramPos(4).new(0, .{ TEC, TEC, '.', 'h' }),
            GramPos(4).new(0, .{ TEC, '.', 'h', TEC }),
            GramPos(4).new(0, .{ '.', 'h', TEC, TEC }),
            GramPos(4).new(1, .{ 'h', TEC, TEC, TEC }),
        } },
        .{ .name = "no_boundaries.2", .string = ".h", .boundary_grams = false, .expected = &.{
            GramPos(4).new(0, .{ TEC, TEC, '.', 'h' }),
            GramPos(4).new(0, .{ '.', 'h', TEC, TEC }),
        } },
        .{ .name = "boundaries.1", .string = "h", .boundary_grams = true, .expected = &.{
            GramPos(4).new(0, .{ TEC, TEC, TEC, 'h' }),
            GramPos(4).new(0, .{ TEC, TEC, 'h', TEC }),
            GramPos(4).new(0, .{ TEC, 'h', TEC, TEC }),
            GramPos(4).new(0, .{ 'h', TEC, TEC, TEC }),
        } },
        .{ .name = "no_boundaries.1", .string = "h", .boundary_grams = false, .expected = &.{
            GramPos(4).new(0, .{ TEC, TEC, TEC, 'h' }),
            GramPos(4).new(0, .{ 'h', TEC, TEC, TEC }),
        } },
        .{ .name = "boundaries.multi", .string = "homeuser", .boundary_grams = true, .expected = &.{
            GramPos(4).new(0, .{ TEC, TEC, TEC, 'h' }),
            GramPos(4).new(0, .{ TEC, TEC, 'h', 'o' }),
            GramPos(4).new(0, .{ TEC, 'h', 'o', 'm' }),
            GramPos(4).new(0, .{ 'h', 'o', 'm', 'e' }),
            GramPos(4).new(1, .{ 'o', 'm', 'e', 'u' }),
            GramPos(4).new(2, .{ 'm', 'e', 'u', 's' }),
            GramPos(4).new(3, .{ 'e', 'u', 's', 'e' }),
            GramPos(4).new(4, .{ 'u', 's', 'e', 'r' }),
            GramPos(4).new(5, .{ 's', 'e', 'r', TEC }),
            GramPos(4).new(6, .{ 'e', 'r', TEC, TEC }),
            GramPos(4).new(7, .{ 'r', TEC, TEC, TEC }),
        } },
        .{ .name = "no_boundaries.multi", .string = "homeuser", .boundary_grams = false, .expected = &.{
            GramPos(4).new(0, .{ 'h', 'o', 'm', 'e' }),
            GramPos(4).new(1, .{ 'o', 'm', 'e', 'u' }),
            GramPos(4).new(2, .{ 'm', 'e', 'u', 's' }),
            GramPos(4).new(3, .{ 'e', 'u', 's', 'e' }),
            GramPos(4).new(4, .{ 'u', 's', 'e', 'r' }),
        } },
        .{ .name = "whitespace_is_boundary.1", .string = " home user", .boundary_grams = true, .expected = &.{
            GramPos(4).new(1, .{ TEC, TEC, TEC, 'h' }),
            GramPos(4).new(1, .{ TEC, TEC, 'h', 'o' }),
            GramPos(4).new(1, .{ TEC, 'h', 'o', 'm' }),
            GramPos(4).new(1, .{ 'h', 'o', 'm', 'e' }),
            GramPos(4).new(2, .{ 'o', 'm', 'e', TEC }),
            GramPos(4).new(3, .{ 'm', 'e', TEC, TEC }),
            GramPos(4).new(4, .{ 'e', TEC, TEC, TEC }),
            GramPos(4).new(6, .{ TEC, TEC, TEC, 'u' }),
            GramPos(4).new(6, .{ TEC, TEC, 'u', 's' }),
            GramPos(4).new(6, .{ TEC, 'u', 's', 'e' }),
            GramPos(4).new(6, .{ 'u', 's', 'e', 'r' }),
            GramPos(4).new(7, .{ 's', 'e', 'r', TEC }),
            GramPos(4).new(8, .{ 'e', 'r', TEC, TEC }),
            GramPos(4).new(9, .{ 'r', TEC, TEC, TEC }),
        } },
    };
    inline for (table) |case| {
        var list = std.ArrayList(GramPos(4)).init(std.testing.allocator);
        defer list.deinit();
        try grammer(4, case.string, case.boundary_grams, &std.ascii.whitespace, Appender(GramPos(4)).new(&list, std.ArrayList(GramPos(4)).append));
        std.testing.expectEqualSlices(GramPos(4), case.expected, list.items) catch |err| {
            std.debug.print("\nerror on {s}\n{any}\n !=\n {any}\n", .{ case.name, case.expected, list.items });
            return err;
        };
    }
}
