const std = @import("std");
const types = @import("types.zig");
const errors = @import("errors.zig");

const TypeTag = types.TypeTag;
const Binary = types.Binary;
const ObjectId = types.ObjectId;
const Timestamp = types.Timestamp;
const Regex = types.Regex;
const Decimal128 = types.Decimal128;

pub const Decoder = struct {
    allocator: std.mem.Allocator,
    data: []const u8,
    pos: usize,
    skip_utf8_validation: bool = false,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, data: []const u8) Self {
        return .{
            .allocator = allocator,
            .data = data,
            .pos = 0,
            .skip_utf8_validation = false,
        };
    }

    /// Enable/disable UTF-8 validation (disable for ASCII-only data)
    pub fn setSkipUtf8Validation(self: *Self, skip: bool) void {
        self.skip_utf8_validation = skip;
    }

    /// Decode BSON data to a Zig type
    /// Caller owns returned data and must free it
    pub fn decode(self: *Self, comptime T: type) errors.Error!T {
        const type_info = @typeInfo(T);

        // Top-level BSON must be a document (struct)
        if (type_info != .@"struct") {
            @compileError("Top-level BSON value must be a struct");
        }

        // Read and validate document size
        if (self.data.len < 5) return error.UnexpectedEof;
        const doc_size = self.readI32();
        if (doc_size < 5 or doc_size > self.data.len) {
            return error.MalformedDocument;
        }

        const doc_end = @as(usize, @intCast(doc_size));
        // Prime the result with declared struct defaults + empty
        // slices for fields without defaults. Prior `= undefined`
        // caused UB when callers serialised the result - e.g.
        // `writeString` iterating a random-pointer slice from a BSON
        // field that wasn't actually present in the document.
        var result: T = undefined;
        primeStruct(&result);

        // Decode fields directly (no type tag or field name for top-level)
        while (self.pos < doc_end - 1) {
            const tag_byte = self.data[self.pos];
            if (tag_byte == 0) break;

            self.pos += 1;
            const field_name = try self.readCString();

            // Find matching struct field
            var found = false;
            inline for (type_info.@"struct".fields) |field| {
                if (std.mem.eql(u8, field.name, field_name)) {
                    try self.decodeFieldValue(@TypeOf(@field(result, field.name)), tag_byte, &@field(result, field.name));
                    found = true;
                    break;
                }
            }

            // Skip unknown fields
            if (!found) {
                try self.skipValue(tag_byte);
            }
        }

        // Validate null terminator
        if (self.pos >= self.data.len or self.data[self.pos] != 0) {
            return error.MalformedDocument;
        }

        return result;
    }

    fn decodeValue(self: *Self, comptime T: type, output: *T) errors.Error!void {
        const type_info = @typeInfo(T);

        switch (type_info) {
            .int => output.* = try self.decodeInt(T),
            .float => output.* = try self.decodeFloat(T),
            .bool => output.* = try self.decodeBool(),
            .pointer => |ptr_info| {
                switch (ptr_info.size) {
                    .slice => {
                        if (ptr_info.child == u8) {
                            output.* = try self.decodeString();
                        } else {
                            output.* = try self.decodeArray(ptr_info.child);
                        }
                    },
                    else => @compileError("Unsupported pointer type"),
                }
            },
            .@"struct" => {
                if (T == ObjectId) {
                    output.* = try self.decodeObjectId();
                } else if (T == Binary) {
                    output.* = try self.decodeBinary();
                } else if (T == Timestamp) {
                    output.* = try self.decodeTimestamp();
                } else if (T == Regex) {
                    output.* = try self.decodeRegex();
                } else if (T == Decimal128) {
                    output.* = try self.decodeDecimal128();
                } else {
                    try self.decodeDocument(T, output);
                }
            },
            .optional => |opt_info| {
                // Check next type tag
                if (self.pos < self.data.len) {
                    const tag_byte = self.data[self.pos];
                    if (tag_byte == @intFromEnum(TypeTag.null)) {
                        self.pos += 1;
                        // Skip field name
                        _ = try self.readCString();
                        output.* = null;
                    } else {
                        var value: opt_info.child = undefined;
                        try self.decodeValue(opt_info.child, &value);
                        output.* = value;
                    }
                } else {
                    output.* = null;
                }
            },
            .@"enum" => {
                const str = try self.decodeString();
                defer self.allocator.free(str);
                output.* = std.meta.stringToEnum(T, str) orelse return error.TypeMismatch;
            },
            else => @compileError("Unsupported type: " ++ @typeName(T)),
        }
    }

    fn decodeDocument(self: *Self, comptime T: type, output: *T) errors.Error!void {
        const type_info = @typeInfo(T).@"struct";

        // Same rationale as the top-level `decode`: prime defaults so
        // absent fields don't leak undefined memory to serialisers.
        primeStruct(output);

        // Read document size
        const doc_start = self.pos;
        const doc_size = self.readI32();
        const doc_end = doc_start + @as(usize, @intCast(doc_size));

        // Decode fields
        while (self.pos < doc_end - 1) {
            const tag_byte = self.data[self.pos];
            if (tag_byte == 0) break;

            self.pos += 1;
            const field_name = try self.readCString();

            // Find matching struct field
            var found = false;
            inline for (type_info.fields) |field| {
                if (std.mem.eql(u8, field.name, field_name)) {
                    try self.decodeFieldValue(@TypeOf(@field(output.*, field.name)), tag_byte, &@field(output.*, field.name));
                    found = true;
                    break;
                }
            }

            // Skip unknown fields
            if (!found) {
                try self.skipValue(tag_byte);
            }
        }

        // Skip null terminator
        if (self.pos < self.data.len and self.data[self.pos] == 0) {
            self.pos += 1;
        }
    }

    fn decodeFieldValue(self: *Self, comptime T: type, tag: u8, output: *T) errors.Error!void {
        const type_info = @typeInfo(T);

        // Handle optional types: decode child type directly since the tag
        // was already consumed by the caller's field loop.
        if (type_info == .optional) {
            if (tag == @intFromEnum(TypeTag.null)) {
                output.* = null;
                return;
            }
            var child_val: type_info.optional.child = undefined;
            try self.decodeFieldValue(type_info.optional.child, tag, &child_val);
            output.* = child_val;
            return;
        }

        const wire_is_int32 = tag == @intFromEnum(TypeTag.int32);
        const wire_is_int64 = tag == @intFromEnum(TypeTag.int64);
        const wire_is_double = tag == @intFromEnum(TypeTag.double);
        const wire_is_numeric = wire_is_int32 or wire_is_int64 or wire_is_double;

        const expected_tag = try self.expectedTag(T);

        // Fast path: wire tag matches expected type exactly
        if (tag == expected_tag) {
            try self.decodeValue(T, output);
            return;
        }

        // Numeric coercion: read based on wire tag, convert to target type
        if (wire_is_numeric) {
            // Read the raw value from the wire based on the actual BSON tag
            const raw_int: i64 = if (wire_is_int32) @as(i64, self.readI32())
                else if (wire_is_int64) self.readI64()
                else 0; // not used for double wire
            const raw_float: f64 = if (wire_is_double) @as(f64, @bitCast(self.readU64())) else 0;

            // Convert to target Zig type
            if (type_info == .int) {
                // Wire: int32/int64/double → Target: int
                if (wire_is_double) {
                    output.* = @intFromFloat(raw_float);
                } else {
                    output.* = @intCast(raw_int);
                }
                return;
            } else if (type_info == .float) {
                // Wire: int32/int64 → Target: float
                if (wire_is_double) {
                    output.* = @floatCast(raw_float);
                } else {
                    output.* = @floatFromInt(raw_int);
                }
                return;
            }
        }

        return error.TypeMismatch;
    }

    fn expectedTag(self: *Self, comptime T: type) !u8 {
        const type_info = @typeInfo(T);
        _ = self;

        return switch (type_info) {
            .int => |int_info| if (int_info.bits <= 32) @intFromEnum(TypeTag.int32) else @intFromEnum(TypeTag.int64),
            .float => @intFromEnum(TypeTag.double),
            .bool => @intFromEnum(TypeTag.boolean),
            .pointer => |ptr_info| if (ptr_info.child == u8) @intFromEnum(TypeTag.string) else @intFromEnum(TypeTag.array),
            .@"struct" => {
                if (T == ObjectId) return @intFromEnum(TypeTag.object_id);
                if (T == Binary) return @intFromEnum(TypeTag.binary);
                if (T == Timestamp) return @intFromEnum(TypeTag.timestamp);
                if (T == Regex) return @intFromEnum(TypeTag.regex);
                if (T == Decimal128) return @intFromEnum(TypeTag.decimal128);
                return @intFromEnum(TypeTag.document);
            },
            .optional => @intFromEnum(TypeTag.null),
            .@"enum" => @intFromEnum(TypeTag.string),
            else => error.TypeMismatch,
        };
    }

    fn decodeInt(self: *Self, comptime T: type) !T {
        const type_info = @typeInfo(T).int;

        if (type_info.bits <= 32) {
            return @as(T, @intCast(self.readI32()));
        } else {
            return @as(T, @intCast(self.readI64()));
        }
    }

    fn decodeFloat(self: *Self, comptime T: type) !T {
        const value = @as(f64, @bitCast(self.readU64()));
        return @as(T, @floatCast(value));
    }

    fn decodeBool(self: *Self) !bool {
        if (self.pos >= self.data.len) return error.UnexpectedEof;
        const value = self.data[self.pos];
        self.pos += 1;
        return value != 0;
    }

    fn decodeString(self: *Self) ![]const u8 {
        const len = self.readI32();
        if (len < 1) return error.MalformedDocument;

        const str_len = @as(usize, @intCast(len - 1)); // Exclude null terminator
        if (self.pos + str_len + 1 > self.data.len) return error.UnexpectedEof;

        const str = self.data[self.pos .. self.pos + str_len];

        // Validate UTF-8 (skip if disabled for performance)
        if (!self.skip_utf8_validation and !std.unicode.utf8ValidateSlice(str)) {
            return error.InvalidUtf8;
        }

        // Validate null terminator
        if (self.data[self.pos + str_len] != 0) {
            return error.MalformedDocument;
        }

        self.pos += str_len + 1;

        // Duplicate string (caller owns)
        return try self.allocator.dupe(u8, str);
    }

    fn decodeObjectId(self: *Self) !ObjectId {
        if (self.pos + 12 > self.data.len) return error.UnexpectedEof;

        var bytes: [12]u8 = undefined;
        @memcpy(&bytes, self.data[self.pos .. self.pos + 12]);
        self.pos += 12;

        return ObjectId.fromBytes(bytes);
    }

    fn decodeBinary(self: *Self) !Binary {
        const len = self.readI32();
        if (len < 0) return error.MalformedDocument;

        if (self.pos >= self.data.len) return error.UnexpectedEof;
        const subtype_byte = self.data[self.pos];
        self.pos += 1;

        const data_len = @as(usize, @intCast(len));
        if (self.pos + data_len > self.data.len) return error.UnexpectedEof;

        const data = try self.allocator.dupe(u8, self.data[self.pos .. self.pos + data_len]);
        self.pos += data_len;

        return Binary{
            .subtype = @enumFromInt(subtype_byte),
            .data = data,
        };
    }

    fn decodeTimestamp(self: *Self) !Timestamp {
        const value = @as(u64, @bitCast(self.readI64()));
        return Timestamp.fromU64(value);
    }

    fn decodeRegex(self: *Self) !Regex {
        const pattern = try self.readCStringAlloc();
        const options = try self.readCStringAlloc();

        return Regex{
            .pattern = pattern,
            .options = options,
        };
    }

    fn decodeDecimal128(self: *Self) !Decimal128 {
        if (self.pos + 16 > self.data.len) return error.UnexpectedEof;

        var bytes: [16]u8 = undefined;
        @memcpy(&bytes, self.data[self.pos .. self.pos + 16]);
        self.pos += 16;

        return Decimal128.fromBytes(bytes);
    }

    fn decodeArray(self: *Self, comptime Child: type) ![]Child {
        // Read array size
        const array_start = self.pos;
        const array_size = self.readI32();
        const array_end = array_start + @as(usize, @intCast(array_size));

        var items: std.ArrayList(Child) = .empty;
        errdefer items.deinit(self.allocator);

        // Decode elements
        while (self.pos < array_end - 1) {
            const tag_byte = self.data[self.pos];
            if (tag_byte == 0) break;

            self.pos += 1;

            // Read and ignore index (numeric key)
            _ = try self.readCString();

            var item: Child = undefined;
            try self.decodeFieldValue(Child, tag_byte, &item);
            try items.append(self.allocator, item);
        }

        // Skip null terminator
        if (self.pos < self.data.len and self.data[self.pos] == 0) {
            self.pos += 1;
        }

        return try items.toOwnedSlice(self.allocator);
    }

    fn skipValue(self: *Self, tag: u8) !void {
        switch (@as(TypeTag, @enumFromInt(tag))) {
            .double => self.pos += 8,
            .string => {
                const len = self.readI32();
                self.pos += @as(usize, @intCast(len));
            },
            .document, .array => {
                const size = self.readI32();
                self.pos += @as(usize, @intCast(size)) - 4;
            },
            .binary => {
                const len = self.readI32();
                self.pos += 1 + @as(usize, @intCast(len)); // subtype + data
            },
            .object_id => self.pos += 12,
            .boolean => self.pos += 1,
            .datetime => self.pos += 8,
            .null => {},
            .regex => {
                _ = try self.readCString(); // pattern
                _ = try self.readCString(); // options
            },
            .int32 => self.pos += 4,
            .timestamp, .int64 => self.pos += 8,
            .decimal128 => self.pos += 16,
            else => return error.InvalidType,
        }
    }

    // Helper functions
    fn readI32(self: *Self) i32 {
        const value = std.mem.readInt(i32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        return value;
    }

    fn readI64(self: *Self) i64 {
        const value = std.mem.readInt(i64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return value;
    }

    fn readU64(self: *Self) u64 {
        const value = std.mem.readInt(u64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return value;
    }

    fn readCString(self: *Self) ![]const u8 {
        const start = self.pos;
        while (self.pos < self.data.len and self.data[self.pos] != 0) {
            self.pos += 1;
        }

        if (self.pos >= self.data.len) return error.UnexpectedEof;

        const str = self.data[start..self.pos];
        self.pos += 1; // Skip null terminator

        return str;
    }

    fn readCStringAlloc(self: *Self) ![]const u8 {
        const str = try self.readCString();
        return try self.allocator.dupe(u8, str);
    }
};

/// Prime a struct so every field has a safe value before decoding:
/// declared defaults win, then slices become empty, ints/floats/bools
/// get zero equivalents. Enum fields without an explicit default are
/// left `undefined` - the decoder treats those as required and will
/// fail later with `TypeMismatch` if truly absent. This prevents
/// serialisers from tripping over undefined memory when a BSON doc is
/// missing a non-required field.
fn primeStruct(out: anytype) void {
    const T = @TypeOf(out.*);
    const info = @typeInfo(T);
    if (info != .@"struct") return;
    inline for (info.@"struct".fields) |f| {
        if (f.default_value_ptr) |dv| {
            @field(out.*, f.name) = @as(*const f.type, @ptrCast(@alignCast(dv))).*;
        } else {
            const fi = @typeInfo(f.type);
            switch (fi) {
                .pointer => |p| {
                    if (p.size == .slice) {
                        @field(out.*, f.name) = &[_]p.child{};
                    }
                },
                .int => @field(out.*, f.name) = 0,
                .float => @field(out.*, f.name) = 0.0,
                .bool => @field(out.*, f.name) = false,
                .optional => @field(out.*, f.name) = null,
                .@"struct" => primeStruct(&@field(out.*, f.name)),
                else => {},
            }
        }
    }
}
