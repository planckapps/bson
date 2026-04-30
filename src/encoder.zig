const std = @import("std");
const types = @import("types.zig");
const errors = @import("errors.zig");

const TypeTag = types.TypeTag;
const Binary = types.Binary;
const ObjectId = types.ObjectId;
const Timestamp = types.Timestamp;
const Regex = types.Regex;
const Decimal128 = types.Decimal128;

pub const Encoder = struct {
    allocator: std.mem.Allocator,
    buffer: []u8,
    pos: usize,
    skip_utf8_validation: bool = false,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .allocator = allocator,
            .buffer = &[_]u8{},
            .pos = 0,
            .skip_utf8_validation = false,
        };
    }

    /// Initialize with pre-allocated buffer for max document size
    /// Recommended for high-performance scenarios with known max doc size
    pub fn initWithCapacity(allocator: std.mem.Allocator, capacity: usize) !Self {
        const buffer = try allocator.alloc(u8, capacity);
        return .{
            .allocator = allocator,
            .buffer = buffer,
            .pos = 0,
            .skip_utf8_validation = false,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.buffer.len > 0) {
            self.allocator.free(self.buffer);
        }
    }

    /// Enable/disable UTF-8 validation (disable for ASCII-only data)
    pub fn setSkipUtf8Validation(self: *Self, skip: bool) void {
        self.skip_utf8_validation = skip;
    }

    /// Encode a Zig value to BSON
    /// Returns owned slice that must be freed by caller
    pub fn encode(self: *Self, value: anytype) ![]const u8 {
        self.pos = 0; // Reset position

        const T = @TypeOf(value);
        const type_info = @typeInfo(T);

        // Top-level BSON must be a document (struct)
        if (type_info != .@"struct") {
            @compileError("Top-level BSON value must be a struct");
        }

        // Allocate buffer if not already allocated
        if (self.buffer.len == 0) {
            const estimated_size = estimateDocSize(T);
            self.buffer = try self.allocator.alloc(u8, estimated_size);
        }

        // Reserve space for document size (4 bytes of zeros)
        try self.ensureCapacity(5); // 4 for size + 1 for null terminator at minimum
        @memset(self.buffer[0..4], 0);
        self.pos = 4;

        // Encode struct fields directly (no type tag or field name for top-level)
        inline for (type_info.@"struct".fields) |field| {
            try self.encodeValue(field.name, @field(value, field.name));
        }

        // Append null terminator
        try self.ensureCapacity(1);
        self.buffer[self.pos] = 0;
        self.pos += 1;

        // Write document size (including size field and null terminator)
        const size = @as(i32, @intCast(self.pos));
        std.mem.writeInt(i32, self.buffer[0..4], size, .little);

        return try self.allocator.dupe(u8, self.buffer[0..self.pos]);
    }

    /// Encode and transfer ownership of internal buffer (zero-copy)
    /// Returns owned slice that must be freed by caller
    /// Note: This is deprecated with raw buffer approach - use encode() instead
    pub fn encodeToOwned(self: *Self, value: anytype) ![]const u8 {
        return try self.encode(value);
    }

    /// Estimate document size at compile time
    fn estimateDocSize(comptime T: type) usize {
        const type_info = @typeInfo(T);
        if (type_info != .@"struct") return 0;

        var size: usize = 5; // 4 bytes size + 1 byte null terminator

        inline for (type_info.@"struct".fields) |field| {
            // Type tag (1) + field name + null (field.name.len + 1) + value
            size += 1 + field.name.len + 1;

            const field_type_info = @typeInfo(field.type);
            size += switch (field_type_info) {
                .int => |int_info| if (int_info.bits <= 32) @as(usize, 4) else @as(usize, 8),
                .float => 8,
                .bool => 1,
                .pointer => |ptr_info| if (ptr_info.size == .slice and ptr_info.child == u8) @as(usize, 128) else @as(usize, 64), // Estimate for strings/arrays
                .@"struct" => 256, // Conservative estimate for nested docs
                .optional => 128,
                else => 64,
            };
        }

        return size;
    }

    fn encodeValue(self: *Self, name: []const u8, value: anytype) errors.Error!void {
        const T = @TypeOf(value);
        const type_info = @typeInfo(T);

        switch (type_info) {
            .int => try self.encodeInt(name, value),
            .float => try self.encodeFloat(name, value),
            .bool => try self.encodeBool(name, value),
            .pointer => |ptr_info| {
                switch (ptr_info.size) {
                    .slice => {
                        if (ptr_info.child == u8) {
                            // Byte slice = string
                            try self.encodeString(name, value);
                        } else {
                            // Array of other types
                            try self.encodeArray(name, value);
                        }
                    },
                    .one => {
                        // Single pointer - dereference and encode
                        try self.encodeValue(name, value.*);
                    },
                    else => @compileError("Unsupported pointer type"),
                }
            },
            .@"struct" => |struct_info| {
                // Check for special BSON types
                if (T == ObjectId) {
                    try self.encodeObjectId(name, value);
                } else if (T == Binary) {
                    try self.encodeBinary(name, value);
                } else if (T == Timestamp) {
                    try self.encodeTimestamp(name, value);
                } else if (T == Regex) {
                    try self.encodeRegex(name, value);
                } else if (T == Decimal128) {
                    try self.encodeDecimal128(name, value);
                } else {
                    // Regular struct = BSON document
                    try self.encodeDocument(name, value);
                }
                _ = struct_info;
            },
            .optional => {
                if (value) |v| {
                    try self.encodeValue(name, v);
                } else {
                    try self.encodeNull(name);
                }
            },
            .null => try self.encodeNull(name),
            .@"enum" => try self.encodeString(name, @tagName(value)),
            else => @compileError("Unsupported type: " ++ @typeName(T)),
        }
    }

    fn encodeInt(self: *Self, name: []const u8, value: anytype) errors.Error!void {
        const T = @TypeOf(value);
        const type_info = @typeInfo(T).int;

        if (type_info.bits <= 32) {
            // i32
            try self.ensureCapacity(1);
            self.buffer[self.pos] = @intFromEnum(TypeTag.int32);
            self.pos += 1;
            try self.writeCString(name);
            const val = @as(i32, @intCast(value));
            try self.writeI32(val);
        } else {
            // i64
            try self.ensureCapacity(1);
            self.buffer[self.pos] = @intFromEnum(TypeTag.int64);
            self.pos += 1;
            try self.writeCString(name);
            const val = @as(i64, @intCast(value));
            try self.writeI64(val);
        }
    }

    fn encodeFloat(self: *Self, name: []const u8, value: anytype) errors.Error!void {
        try self.ensureCapacity(1);
        self.buffer[self.pos] = @intFromEnum(TypeTag.double);
        self.pos += 1;
        try self.writeCString(name);
        const val = @as(f64, value);
        try self.writeF64(val);
    }

    fn encodeBool(self: *Self, name: []const u8, value: bool) errors.Error!void {
        try self.ensureCapacity(1);
        self.buffer[self.pos] = @intFromEnum(TypeTag.boolean);
        self.pos += 1;
        try self.writeCString(name);
        try self.ensureCapacity(1);
        self.buffer[self.pos] = if (value) 1 else 0;
        self.pos += 1;
    }

    fn encodeString(self: *Self, name: []const u8, value: []const u8) errors.Error!void {
        // Validate UTF-8 (skip if disabled for performance)
        if (!self.skip_utf8_validation and !std.unicode.utf8ValidateSlice(value)) {
            return error.InvalidUtf8;
        }

        try self.ensureCapacity(1);
        self.buffer[self.pos] = @intFromEnum(TypeTag.string);
        self.pos += 1;
        try self.writeCString(name);

        // String length (including null terminator)
        const len = @as(i32, @intCast(value.len + 1));
        try self.writeI32(len);

        // String data + null terminator
        try self.ensureCapacity(value.len + 1);
        @memcpy(self.buffer[self.pos .. self.pos + value.len], value);
        self.pos += value.len;
        self.buffer[self.pos] = 0;
        self.pos += 1;
    }

    fn encodeNull(self: *Self, name: []const u8) errors.Error!void {
        try self.ensureCapacity(1);
        self.buffer[self.pos] = @intFromEnum(TypeTag.null);
        self.pos += 1;
        try self.writeCString(name);
    }

    fn encodeObjectId(self: *Self, name: []const u8, oid: ObjectId) errors.Error!void {
        try self.ensureCapacity(1);
        self.buffer[self.pos] = @intFromEnum(TypeTag.object_id);
        self.pos += 1;
        try self.writeCString(name);
        try self.ensureCapacity(12);
        @memcpy(self.buffer[self.pos .. self.pos + 12], &oid.bytes);
        self.pos += 12;
    }

    fn encodeBinary(self: *Self, name: []const u8, binary: Binary) errors.Error!void {
        try self.ensureCapacity(1);
        self.buffer[self.pos] = @intFromEnum(TypeTag.binary);
        self.pos += 1;
        try self.writeCString(name);

        // Binary length
        const len = @as(i32, @intCast(binary.data.len));
        try self.writeI32(len);

        // Subtype
        try self.ensureCapacity(1);
        self.buffer[self.pos] = @intFromEnum(binary.subtype);
        self.pos += 1;

        // Data
        try self.ensureCapacity(binary.data.len);
        @memcpy(self.buffer[self.pos .. self.pos + binary.data.len], binary.data);
        self.pos += binary.data.len;
    }

    fn encodeTimestamp(self: *Self, name: []const u8, ts: Timestamp) errors.Error!void {
        try self.ensureCapacity(1);
        self.buffer[self.pos] = @intFromEnum(TypeTag.timestamp);
        self.pos += 1;
        try self.writeCString(name);
        try self.writeI64(@as(i64, @bitCast(ts.toU64())));
    }

    fn encodeRegex(self: *Self, name: []const u8, regex: Regex) errors.Error!void {
        try self.ensureCapacity(1);
        self.buffer[self.pos] = @intFromEnum(TypeTag.regex);
        self.pos += 1;
        try self.writeCString(name);
        try self.writeCString(regex.pattern);
        try self.writeCString(regex.options);
    }

    fn encodeDecimal128(self: *Self, name: []const u8, decimal: Decimal128) errors.Error!void {
        try self.ensureCapacity(1);
        self.buffer[self.pos] = @intFromEnum(TypeTag.decimal128);
        self.pos += 1;
        try self.writeCString(name);
        try self.ensureCapacity(16);
        @memcpy(self.buffer[self.pos .. self.pos + 16], &decimal.bytes);
        self.pos += 16;
    }

    fn encodeDocument(self: *Self, name: []const u8, doc: anytype) errors.Error!void {
        try self.ensureCapacity(1);
        self.buffer[self.pos] = @intFromEnum(TypeTag.document);
        self.pos += 1;
        try self.writeCString(name);

        // Save position for size
        const size_pos = self.pos;
        try self.ensureCapacity(4);
        @memset(self.buffer[self.pos .. self.pos + 4], 0);
        self.pos += 4;

        // Encode struct fields
        const T = @TypeOf(doc);
        const type_info = @typeInfo(T).@"struct";

        inline for (type_info.fields) |field| {
            try self.encodeValue(field.name, @field(doc, field.name));
        }

        // Null terminator
        try self.ensureCapacity(1);
        self.buffer[self.pos] = 0;
        self.pos += 1;

        // Write document size
        const size = @as(i32, @intCast(self.pos - size_pos));
        std.mem.writeInt(i32, self.buffer[size_pos..][0..4], size, .little);
    }

    fn encodeArray(self: *Self, name: []const u8, array: anytype) errors.Error!void {
        try self.ensureCapacity(1);
        self.buffer[self.pos] = @intFromEnum(TypeTag.array);
        self.pos += 1;
        try self.writeCString(name);

        // Save position for size
        const size_pos = self.pos;
        try self.ensureCapacity(4);
        @memset(self.buffer[self.pos .. self.pos + 4], 0);
        self.pos += 4;

        // Encode array elements with numeric keys
        var index_buf: [20]u8 = undefined;
        for (array, 0..) |item, i| {
            const index_str = try std.fmt.bufPrint(&index_buf, "{d}", .{i});
            try self.encodeValue(index_str, item);
        }

        // Null terminator
        try self.ensureCapacity(1);
        self.buffer[self.pos] = 0;
        self.pos += 1;

        // Write array size
        const size = @as(i32, @intCast(self.pos - size_pos));
        std.mem.writeInt(i32, self.buffer[size_pos..][0..4], size, .little);
    }

    // Helper functions

    /// Ensure buffer has at least `needed` bytes available from current position.
    /// Grows the buffer by doubling (or more) if necessary.
    fn ensureCapacity(self: *Self, needed: usize) errors.Error!void {
        const required = self.pos + needed;
        if (required <= self.buffer.len) return;

        var new_cap = if (self.buffer.len == 0) @as(usize, 256) else self.buffer.len;
        while (new_cap < required) {
            new_cap *|= 2;
        }

        if (self.buffer.len == 0) {
            self.buffer = self.allocator.alloc(u8, new_cap) catch return error.OutOfMemory;
        } else {
            self.buffer = self.allocator.realloc(self.buffer, new_cap) catch return error.OutOfMemory;
        }
    }

    fn writeCString(self: *Self, str: []const u8) errors.Error!void {
        if (std.mem.indexOfScalar(u8, str, 0) != null) {
            return error.InvalidFieldName;
        }
        try self.ensureCapacity(str.len + 1);
        @memcpy(self.buffer[self.pos .. self.pos + str.len], str);
        self.pos += str.len;
        self.buffer[self.pos] = 0;
        self.pos += 1;
    }

    fn writeI32(self: *Self, value: i32) errors.Error!void {
        try self.ensureCapacity(4);
        std.mem.writeInt(i32, self.buffer[self.pos..][0..4], value, .little);
        self.pos += 4;
    }

    fn writeI64(self: *Self, value: i64) errors.Error!void {
        try self.ensureCapacity(8);
        std.mem.writeInt(i64, self.buffer[self.pos..][0..8], value, .little);
        self.pos += 8;
    }

    fn writeF64(self: *Self, value: f64) errors.Error!void {
        try self.ensureCapacity(8);
        std.mem.writeInt(u64, self.buffer[self.pos..][0..8], @as(u64, @bitCast(value)), .little);
        self.pos += 8;
    }
};
