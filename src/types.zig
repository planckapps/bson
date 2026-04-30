const std = @import("std");

/// BSON type tags as defined in BSON 1.1 spec
pub const TypeTag = enum(u8) {
    double = 0x01,
    string = 0x02,
    document = 0x03,
    array = 0x04,
    binary = 0x05,
    undefined_deprecated = 0x06, // Deprecated
    object_id = 0x07,
    boolean = 0x08,
    datetime = 0x09,
    null = 0x0A,
    regex = 0x0B,
    db_pointer_deprecated = 0x0C, // Deprecated
    javascript = 0x0D,
    symbol_deprecated = 0x0E, // Deprecated
    javascript_with_scope = 0x0F,
    int32 = 0x10,
    timestamp = 0x11,
    int64 = 0x12,
    decimal128 = 0x13,
    min_key = 0xFF,
    max_key = 0x7F,

    pub fn toString(self: TypeTag) []const u8 {
        return switch (self) {
            .double => "double",
            .string => "string",
            .document => "document",
            .array => "array",
            .binary => "binary",
            .object_id => "objectId",
            .boolean => "boolean",
            .datetime => "datetime",
            .null => "null",
            .regex => "regex",
            .javascript => "javascript",
            .javascript_with_scope => "javascriptWithScope",
            .int32 => "int32",
            .timestamp => "timestamp",
            .int64 => "int64",
            .decimal128 => "decimal128",
            .min_key => "minKey",
            .max_key => "maxKey",
            else => "unknown",
        };
    }
};

/// Binary data subtypes
pub const BinarySubtype = enum(u8) {
    generic = 0x00,
    function = 0x01,
    binary_old = 0x02, // Deprecated
    uuid_old = 0x03, // Deprecated
    uuid = 0x04,
    md5 = 0x05,
    encrypted = 0x06,
    user_defined = 0x80,
};

/// Binary data with subtype
pub const Binary = struct {
    subtype: BinarySubtype,
    data: []const u8,
};

/// BSON ObjectId - 12 bytes
/// Format: [4-byte timestamp][5-byte random][3-byte counter]
pub const ObjectId = struct {
    bytes: [12]u8,

    /// Create ObjectId from raw 12 bytes
    pub fn fromBytes(bytes: [12]u8) ObjectId {
        return .{ .bytes = bytes };
    }

    /// Thread-local PRNG for ObjectId suffix bytes. Seeded lazily on
    /// first use from the wall-clock nanoseconds; advances from there.
    /// We need uniqueness (not crypto strength), and re-seeding on
    /// every call caused collisions when two generate() calls landed
    /// in the same nanosecond.
    threadlocal var prng: ?std.Random.DefaultPrng = null;

    /// Create new ObjectId with current timestamp.
    /// Takes an `Io` handle so the clock source is explicit —
    /// matches the rest of the codebase's std.Io convention (see
    /// `utils/src/time.zig`). The old POSIX `clock_gettime` path
    /// was removed in Zig 0.16 and wouldn't compile.
    pub fn generate(io: std.Io) ObjectId {
        var bytes: [12]u8 = undefined;

        // 4-byte timestamp (seconds since epoch).
        const ts_raw = std.Io.Clock.now(.real, io).toSeconds();
        const ts: u32 = if (ts_raw < 0) 0 else @intCast(@min(ts_raw, @as(i64, std.math.maxInt(u32))));
        std.mem.writeInt(u32, bytes[0..4], ts, .big);

        // 5-byte random value + 3-byte counter (uniqueness within the
        // same second). PRNG is seeded once per thread.
        if (prng == null) {
            const seed: u64 = @truncate(@as(u96, @bitCast(std.Io.Clock.now(.real, io).toNanoseconds())));
            prng = std.Random.DefaultPrng.init(seed);
        }
        prng.?.random().bytes(bytes[4..]);

        return .{ .bytes = bytes };
    }

    /// Get timestamp from ObjectId
    pub fn timestamp(self: ObjectId) u32 {
        return std.mem.readInt(u32, self.bytes[0..4], .big);
    }

    /// Convert to hex string (24 characters)
    pub fn toHexString(self: ObjectId, buf: *[24]u8) void {
        _ = std.fmt.bufPrint(buf, "{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}{x:0>2}", .{
            self.bytes[0],  self.bytes[1],  self.bytes[2],  self.bytes[3],
            self.bytes[4],  self.bytes[5],  self.bytes[6],  self.bytes[7],
            self.bytes[8],  self.bytes[9],  self.bytes[10], self.bytes[11],
        }) catch unreachable;
    }

    /// Parse from hex string (24 characters)
    pub fn fromHexString(hex: []const u8) !ObjectId {
        if (hex.len != 24) return error.InvalidObjectId;

        var bytes: [12]u8 = undefined;
        for (0..12) |i| {
            bytes[i] = try std.fmt.parseInt(u8, hex[i * 2 .. i * 2 + 2], 16);
        }

        return .{ .bytes = bytes };
    }
};

/// BSON Timestamp (internal MongoDB type)
pub const Timestamp = struct {
    increment: u32,
    timestamp: u32,

    pub fn fromU64(value: u64) Timestamp {
        return .{
            .increment = @as(u32, @truncate(value)),
            .timestamp = @as(u32, @truncate(value >> 32)),
        };
    }

    pub fn toU64(self: Timestamp) u64 {
        return (@as(u64, self.timestamp) << 32) | @as(u64, self.increment);
    }
};

/// Regular expression with options
pub const Regex = struct {
    pattern: []const u8,
    options: []const u8,
};

/// JavaScript code
pub const JavaScript = struct {
    code: []const u8,
};

/// JavaScript code with scope
pub const JavaScriptWithScope = struct {
    code: []const u8,
    scope: []const u8, // BSON document as bytes
};

/// Decimal128 (16 bytes)
pub const Decimal128 = struct {
    bytes: [16]u8,

    pub fn fromBytes(bytes: [16]u8) Decimal128 {
        return .{ .bytes = bytes };
    }
};

test "ObjectId generation" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const oid1 = ObjectId.generate(io);
    const oid2 = ObjectId.generate(io);

    // ObjectIds should be different
    try std.testing.expect(!std.mem.eql(u8, &oid1.bytes, &oid2.bytes));

    // Timestamp should be reasonable
    const ts = oid1.timestamp();
    try std.testing.expect(ts > 1700000000); // After Nov 2023
}

test "ObjectId hex conversion" {
    var threaded: std.Io.Threaded = .init(std.testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const oid = ObjectId.generate(io);

    var hex_buf: [24]u8 = undefined;
    oid.toHexString(&hex_buf);

    const oid2 = try ObjectId.fromHexString(&hex_buf);
    try std.testing.expectEqualSlices(u8, &oid.bytes, &oid2.bytes);
}

test "Timestamp conversion" {
    const ts = Timestamp{ .timestamp = 12345, .increment = 67890 };
    const value = ts.toU64();
    const ts2 = Timestamp.fromU64(value);

    try std.testing.expectEqual(ts.timestamp, ts2.timestamp);
    try std.testing.expectEqual(ts.increment, ts2.increment);
}
