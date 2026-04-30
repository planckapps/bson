const std = @import("std");
const document = @import("document.zig");

const Allocator = std.mem.Allocator;
const BsonDocument = document.BsonDocument;
const Value = document.Value;
const Writer = std.Io.Writer;
const Stringify = std.json.Stringify;


pub fn writeJson(allocator: Allocator, writer: *Writer, data: []const u8) !void {
    var doc = try BsonDocument.init(allocator, data, false);
    defer doc.deinit();
    var js: Stringify = .{ .writer = writer };
    try writeDocument(allocator, &doc, &js);
}

pub fn writeJsonArray(allocator: Allocator, writer: *Writer, data: []const u8) !void {
    var js: Stringify = .{ .writer = writer };
    try js.beginArray();
    var pos: usize = 0;
    while (pos + 4 <= data.len) {
        const doc_size = std.mem.readInt(i32, data[pos..][0..4], .little);
        if (doc_size < 5) break;
        const size: usize = @intCast(doc_size);
        if (pos + size > data.len) break;
        var doc = try BsonDocument.init(allocator, data[pos .. pos + size], false);
        defer doc.deinit();
        try writeDocument(allocator, &doc, &js);
        pos += size;
    }
    try js.endArray();
}

pub fn toJson(allocator: Allocator, data: []const u8) ![]u8 {
    var aw: Writer.Allocating = .init(allocator);
    errdefer aw.deinit();
    try writeJson(allocator, &aw.writer, data);
    return try aw.toOwnedSlice();
}

pub fn toJsonArray(allocator: Allocator, data: []const u8) ![]u8 {
    var aw: Writer.Allocating = .init(allocator);
    errdefer aw.deinit();
    try writeJsonArray(allocator, &aw.writer, data);
    return try aw.toOwnedSlice();
}

fn writeDocument(allocator: Allocator, doc: *BsonDocument, js: *Stringify) anyerror!void {
    try js.beginObject();

    const names = try doc.getFieldNames(allocator);
    defer {
        for (names) |n| allocator.free(n);
        allocator.free(names);
    }

    for (names) |name| {
        try js.objectField(name);
        if (doc.getField(name) catch null) |field| {
            var val = field;
            defer val.deinit(allocator);
            try writeValue(allocator, val, js);
        } else {
            try js.write(null);
        }
    }

    try js.endObject();
}

fn writeValue(allocator: Allocator, value: Value, js: *Stringify) anyerror!void {
    switch (value) {
        .string => |s| try js.write(s),
        .int32 => |v| try js.write(v),
        .int64 => |v| try js.write(v),
        .double => |v| {
            if (std.math.isNan(v) or std.math.isInf(v)) {
                try js.write(null);
            } else {
                try js.write(v);
            }
        },
        .boolean => |v| try js.write(v),
        .null => try js.write(null),
        .datetime => |v| try js.write(v),
        .object_id => |oid| {
            // Emit as a hex string (24 chars). std.fmt.fmtSliceHexLower
            // has been removed from the stdlib; write the hex directly.
            var hex_buf: [24]u8 = undefined;
            for (oid.bytes, 0..) |b, i| {
                _ = std.fmt.bufPrint(hex_buf[i * 2 ..][0..2], "{x:0>2}", .{b}) catch unreachable;
            }
            try js.write(hex_buf[0..]);
        },
        .document => |d| {
            var nested = d;
            defer nested.deinit();
            try writeDocument(allocator, &nested, js);
        },
        .array => |a| {
            var arr = a;
            defer arr.deinit();
            try js.beginArray();
            var idx: usize = 0;
            while (true) {
                const elem = arr.get(idx) catch break;
                if (elem) |e| {
                    var v = e;
                    defer v.deinit(allocator);
                    writeValue(allocator, v, js) catch break;
                    idx += 1;
                } else break;
            }
            try js.endArray();
        },
        .binary => |bin| try js.print("\"<binary:{d}>\"", .{bin.data.len}),
        .regex => |r| try js.print("\"/{s}/{s}\"", .{ r.pattern, r.options }),
        .timestamp => |ts| try js.write(ts.toU64()),
        .decimal128 => try js.write("<decimal128>"),
    }
}


pub fn fromJson(allocator: Allocator, json_str: []const u8) ![]const u8 {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, json_str, .{});
    defer parsed.deinit();

    if (parsed.value != .object) return error.InvalidJson;

    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);

    try buf.appendNTimes(allocator, 0, 4);
    try writeBsonObject(allocator, &buf, parsed.value.object);
    try buf.append(allocator, 0);

    const size = @as(i32, @intCast(buf.items.len));
    std.mem.writeInt(i32, buf.items[0..4], size, .little);

    return try buf.toOwnedSlice(allocator);
}

const BsonWriteError = Allocator.Error;

fn writeBsonObject(allocator: Allocator, buf: *std.ArrayList(u8), obj: std.json.ObjectMap) BsonWriteError!void {
    var it = obj.iterator();
    while (it.next()) |entry| {
        try writeBsonValue(allocator, buf, entry.key_ptr.*, entry.value_ptr.*);
    }
}

fn writeBsonValue(allocator: Allocator, buf: *std.ArrayList(u8), name: []const u8, value: std.json.Value) BsonWriteError!void {
    switch (value) {
        .string => |s| {
            try buf.append(allocator, 0x02);
            try writeBsonCString(allocator, buf, name);
            const len = @as(i32, @intCast(s.len + 1));
            var len_bytes: [4]u8 = undefined;
            std.mem.writeInt(i32, &len_bytes, len, .little);
            try buf.appendSlice(allocator, &len_bytes);
            try buf.appendSlice(allocator, s);
            try buf.append(allocator, 0);
        },
        .integer => |v| {
            if (v >= std.math.minInt(i32) and v <= std.math.maxInt(i32)) {
                try buf.append(allocator, 0x10);
                try writeBsonCString(allocator, buf, name);
                var bytes: [4]u8 = undefined;
                std.mem.writeInt(i32, &bytes, @intCast(v), .little);
                try buf.appendSlice(allocator, &bytes);
            } else {
                try buf.append(allocator, 0x12);
                try writeBsonCString(allocator, buf, name);
                var bytes: [8]u8 = undefined;
                std.mem.writeInt(i64, &bytes, v, .little);
                try buf.appendSlice(allocator, &bytes);
            }
        },
        .float => |v| {
            try buf.append(allocator, 0x01);
            try writeBsonCString(allocator, buf, name);
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, @bitCast(v), .little);
            try buf.appendSlice(allocator, &bytes);
        },
        .bool => |v| {
            try buf.append(allocator, 0x08);
            try writeBsonCString(allocator, buf, name);
            try buf.append(allocator, if (v) 1 else 0);
        },
        .null => {
            try buf.append(allocator, 0x0A);
            try writeBsonCString(allocator, buf, name);
        },
        .object => |obj| {
            try buf.append(allocator, 0x03);
            try writeBsonCString(allocator, buf, name);
            const size_pos = buf.items.len;
            try buf.appendNTimes(allocator, 0, 4);
            try writeBsonObject(allocator, buf, obj);
            try buf.append(allocator, 0);
            const size = @as(i32, @intCast(buf.items.len - size_pos));
            std.mem.writeInt(i32, buf.items[size_pos..][0..4], size, .little);
        },
        .array => |arr| {
            try buf.append(allocator, 0x04);
            try writeBsonCString(allocator, buf, name);
            const size_pos = buf.items.len;
            try buf.appendNTimes(allocator, 0, 4);
            for (arr.items, 0..) |item, i| {
                var index_buf: [20]u8 = undefined;
                const index_str = std.fmt.bufPrint(&index_buf, "{d}", .{i}) catch unreachable;
                try writeBsonValue(allocator, buf, index_str, item);
            }
            try buf.append(allocator, 0);
            const size = @as(i32, @intCast(buf.items.len - size_pos));
            std.mem.writeInt(i32, buf.items[size_pos..][0..4], size, .little);
        },
        .number_string => {},
    }
}

fn writeBsonCString(allocator: Allocator, buf: *std.ArrayList(u8), s: []const u8) !void {
    try buf.appendSlice(allocator, s);
    try buf.append(allocator, 0);
}


const testing = std.testing;

test "toJson round-trip through fromJson" {
    const allocator = testing.allocator;
    const src = "{\"name\":\"Alice\",\"age\":30,\"active\":true,\"notes\":null}";
    const bson_bytes = try fromJson(allocator, src);
    defer allocator.free(bson_bytes);

    const round_trip = try toJson(allocator, bson_bytes);
    defer allocator.free(round_trip);

    // Field order is preserved by std.json.ObjectMap (insertion order).
    try testing.expectEqualStrings(src, round_trip);
}

test "toJsonArray over concatenated docs" {
    const allocator = testing.allocator;
    const doc_a = try fromJson(allocator, "{\"x\":1}");
    defer allocator.free(doc_a);
    const doc_b = try fromJson(allocator, "{\"x\":2}");
    defer allocator.free(doc_b);

    var concat: std.ArrayList(u8) = .empty;
    defer concat.deinit(allocator);
    try concat.appendSlice(allocator, doc_a);
    try concat.appendSlice(allocator, doc_b);

    const out = try toJsonArray(allocator, concat.items);
    defer allocator.free(out);
    try testing.expectEqualStrings("[{\"x\":1},{\"x\":2}]", out);
}

test "toJsonArray empty buffer" {
    const allocator = testing.allocator;
    const out = try toJsonArray(allocator, "");
    defer allocator.free(out);
    try testing.expectEqualStrings("[]", out);
}
