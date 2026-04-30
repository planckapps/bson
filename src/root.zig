//! BSON (Binary JSON) encoder/decoder for Zig
//! Implements BSON 1.1 specification: http://bsonspec.org/spec.html
//!
//! ## Usage Example
//!
//! ```zig
//! const bson = @import("bson");
//!
//! const Person = struct {
//!     name: []const u8,
//!     age: i32,
//!     email: ?[]const u8,
//! };
//!
//! // Encoding
//! var encoder = bson.Encoder.init(allocator);
//! defer encoder.deinit();
//!
//! const person = Person{ .name = "Alice", .age = 30, .email = "alice@example.com" };
//! const bson_data = try encoder.encode(person);
//! defer allocator.free(bson_data);
//!
//! // Decoding
//! var decoder = bson.Decoder.init(allocator, bson_data);
//! const decoded = try decoder.decode(Person);
//! ```

const std = @import("std");

// Re-export public API
pub const types = @import("types.zig");
pub const errors = @import("errors.zig");
pub const Encoder = @import("encoder.zig").Encoder;
pub const Decoder = @import("decoder.zig").Decoder;
pub const document = @import("document.zig");
pub const json_interop = @import("json_interop.zig");

// JSON interop helpers (free functions; see json_interop.zig for the full surface).
pub const toJson = json_interop.toJson;
pub const toJsonArray = json_interop.toJsonArray;
pub const writeJson = json_interop.writeJson;
pub const writeJsonArray = json_interop.writeJsonArray;
pub const fromJson = json_interop.fromJson;

// Re-export document types
pub const BsonDocument = document.BsonDocument;
pub const BsonArray = document.BsonArray;
pub const Value = document.Value;

// Re-export commonly used types
pub const TypeTag = types.TypeTag;
pub const BinarySubtype = types.BinarySubtype;
pub const Binary = types.Binary;
pub const ObjectId = types.ObjectId;
pub const Timestamp = types.Timestamp;
pub const Regex = types.Regex;
pub const Decimal128 = types.Decimal128;
pub const BsonError = errors.BsonError;
pub const Error = errors.Error;

/// Convenience function to encode a value to BSON
/// Caller owns returned slice and must free it
pub fn encode(allocator: std.mem.Allocator, value: anytype) Error![]const u8 {
    var encoder = Encoder.init(allocator);
    defer encoder.deinit();
    return try encoder.encode(value);
}

/// Fast encode function that skips UTF-8 validation (for ASCII-only data)
/// Caller owns returned slice and must free it
pub fn encodeFast(allocator: std.mem.Allocator, value: anytype) Error![]const u8 {
    var encoder = Encoder.init(allocator);
    defer encoder.deinit();
    encoder.setSkipUtf8Validation(true);
    return try encoder.encode(value);
}

/// Convenience function to decode BSON data
/// Caller owns returned data and must free it (for slices/strings)
pub fn decode(allocator: std.mem.Allocator, comptime T: type, data: []const u8) Error!T {
    var decoder = Decoder.init(allocator, data);
    return try decoder.decode(T);
}

/// Decode a concatenated sequence of BSON documents into a slice of T.
/// Each BSON document starts with a 4-byte little-endian size prefix.
/// Caller owns the returned slice and must free it.
pub fn decodeSlice(allocator: std.mem.Allocator, comptime T: type, data: []const u8) Error![]const T {
    var results = std.ArrayList(T).empty;
    var pos: usize = 0;
    while (pos + 4 <= data.len) {
        const doc_size = std.mem.readInt(i32, data[pos..][0..4], .little);
        if (doc_size < 5) break; // minimum BSON doc is 5 bytes
        const size: usize = @intCast(doc_size);
        if (pos + size > data.len) break;
        const doc_data = data[pos .. pos + size];
        const item = try decode(allocator, T, doc_data);
        try results.append(allocator, item);
        pos += size;
    }
    return results.toOwnedSlice(allocator);
}

/// Fast decode function that skips UTF-8 validation (for ASCII-only data)
/// Caller owns returned data and must free it (for slices/strings)
pub fn decodeFast(allocator: std.mem.Allocator, comptime T: type, data: []const u8) Error!T {
    var decoder = Decoder.init(allocator, data);
    decoder.setSkipUtf8Validation(true);
    return try decoder.decode(T);
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "encode/decode simple struct" {
    const Person = struct {
        name: []const u8,
        age: i32,
        active: bool,
    };

    const person = Person{
        .name = "Alice",
        .age = 30,
        .active = true,
    };

    // Encode
    const bson_data = try encode(testing.allocator, person);
    defer testing.allocator.free(bson_data);

    // Decode
    const decoded = try decode(testing.allocator, Person, bson_data);
    defer testing.allocator.free(decoded.name);

    try testing.expectEqualStrings("Alice", decoded.name);
    try testing.expectEqual(@as(i32, 30), decoded.age);
    try testing.expectEqual(true, decoded.active);
}

test "encode/decode with optional fields" {
    const Person = struct {
        name: []const u8,
        email: ?[]const u8,
        age: ?i32,
    };

    // With values
    {
        const person = Person{
            .name = "Bob",
            .email = "bob@example.com",
            .age = 25,
        };

        const bson_data = try encode(testing.allocator, person);
        defer testing.allocator.free(bson_data);

        const decoded = try decode(testing.allocator, Person, bson_data);
        defer testing.allocator.free(decoded.name);
        defer if (decoded.email) |e| testing.allocator.free(e);

        try testing.expectEqualStrings("Bob", decoded.name);
        try testing.expectEqualStrings("bob@example.com", decoded.email.?);
        try testing.expectEqual(@as(i32, 25), decoded.age.?);
    }

    // With nulls
    {
        const person = Person{
            .name = "Charlie",
            .email = null,
            .age = null,
        };

        const bson_data = try encode(testing.allocator, person);
        defer testing.allocator.free(bson_data);

        const decoded = try decode(testing.allocator, Person, bson_data);
        defer testing.allocator.free(decoded.name);

        try testing.expectEqualStrings("Charlie", decoded.name);
        try testing.expectEqual(@as(?[]const u8, null), decoded.email);
        try testing.expectEqual(@as(?i32, null), decoded.age);
    }
}

test "encode/decode with ObjectId" {
    var threaded: std.Io.Threaded = .init(testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Doc = struct {
        id: ObjectId,
        name: []const u8,
    };

    const doc = Doc{
        .id = ObjectId.generate(io),
        .name = "test",
    };

    const bson_data = try encode(testing.allocator, doc);
    defer testing.allocator.free(bson_data);

    const decoded = try decode(testing.allocator, Doc, bson_data);
    defer testing.allocator.free(decoded.name);

    try testing.expectEqualSlices(u8, &doc.id.bytes, &decoded.id.bytes);
    try testing.expectEqualStrings("test", decoded.name);
}

test "encode/decode arrays" {
    const Doc = struct {
        tags: []const []const u8,
        numbers: []const i32,
    };

    const doc = Doc{
        .tags = &[_][]const u8{ "tag1", "tag2", "tag3" },
        .numbers = &[_]i32{ 1, 2, 3, 4, 5 },
    };

    const bson_data = try encode(testing.allocator, doc);
    defer testing.allocator.free(bson_data);

    const decoded = try decode(testing.allocator, Doc, bson_data);
    defer {
        for (decoded.tags) |tag| testing.allocator.free(tag);
        testing.allocator.free(decoded.tags);
        testing.allocator.free(decoded.numbers);
    }

    try testing.expectEqual(@as(usize, 3), decoded.tags.len);
    try testing.expectEqualStrings("tag1", decoded.tags[0]);
    try testing.expectEqualStrings("tag2", decoded.tags[1]);
    try testing.expectEqualStrings("tag3", decoded.tags[2]);

    try testing.expectEqual(@as(usize, 5), decoded.numbers.len);
    try testing.expectEqualSlices(i32, &[_]i32{ 1, 2, 3, 4, 5 }, decoded.numbers);
}

test "encode/decode nested documents" {
    const Address = struct {
        street: []const u8,
        city: []const u8,
        zip: i32,
    };

    const Person = struct {
        name: []const u8,
        address: Address,
    };

    const person = Person{
        .name = "Dave",
        .address = .{
            .street = "123 Main St",
            .city = "NYC",
            .zip = 10001,
        },
    };

    const bson_data = try encode(testing.allocator, person);
    defer testing.allocator.free(bson_data);

    const decoded = try decode(testing.allocator, Person, bson_data);
    defer {
        testing.allocator.free(decoded.name);
        testing.allocator.free(decoded.address.street);
        testing.allocator.free(decoded.address.city);
    }

    try testing.expectEqualStrings("Dave", decoded.name);
    try testing.expectEqualStrings("123 Main St", decoded.address.street);
    try testing.expectEqualStrings("NYC", decoded.address.city);
    try testing.expectEqual(@as(i32, 10001), decoded.address.zip);
}

test "encode/decode with binary data" {
    const Doc = struct {
        name: []const u8,
        data: Binary,
    };

    const binary_data = [_]u8{ 1, 2, 3, 4, 5 };
    const doc = Doc{
        .name = "test",
        .data = .{
            .subtype = .generic,
            .data = &binary_data,
        },
    };

    const bson_data = try encode(testing.allocator, doc);
    defer testing.allocator.free(bson_data);

    const decoded = try decode(testing.allocator, Doc, bson_data);
    defer {
        testing.allocator.free(decoded.name);
        testing.allocator.free(decoded.data.data);
    }

    try testing.expectEqualStrings("test", decoded.name);
    try testing.expectEqual(BinarySubtype.generic, decoded.data.subtype);
    try testing.expectEqualSlices(u8, &binary_data, decoded.data.data);
}

test "encode/decode with float" {
    const Doc = struct {
        price: f64,
        tax: f64,
    };

    const doc = Doc{
        .price = 99.99,
        .tax = 8.5,
    };

    const bson_data = try encode(testing.allocator, doc);
    defer testing.allocator.free(bson_data);

    const decoded = try decode(testing.allocator, Doc, bson_data);

    try testing.expectApproxEqAbs(@as(f64, 99.99), decoded.price, 0.001);
    try testing.expectApproxEqAbs(@as(f64, 8.5), decoded.tax, 0.001);
}

test "encode/decode with i64" {
    const Doc = struct {
        big_number: i64,
        small_number: i32,
    };

    const doc = Doc{
        .big_number = 9_223_372_036_854_775_000,
        .small_number = 42,
    };

    const bson_data = try encode(testing.allocator, doc);
    defer testing.allocator.free(bson_data);

    const decoded = try decode(testing.allocator, Doc, bson_data);

    try testing.expectEqual(@as(i64, 9_223_372_036_854_775_000), decoded.big_number);
    try testing.expectEqual(@as(i32, 42), decoded.small_number);
}

test "invalid UTF-8 in string" {
    const Doc = struct {
        name: []const u8,
    };

    const invalid_utf8 = [_]u8{ 0xFF, 0xFE, 0xFD };
    const doc = Doc{
        .name = &invalid_utf8,
    };

    var encoder = Encoder.init(testing.allocator);
    defer encoder.deinit();

    const result = encoder.encode(doc);
    try testing.expectError(error.InvalidUtf8, result);
}

test "document size validation" {
    const allocator = testing.allocator;

    // Create minimal valid BSON: [size: 5][null terminator: 0]
    const valid_bson = [_]u8{ 5, 0, 0, 0, 0 };
    var decoder = Decoder.init(allocator, &valid_bson);
    const EmptyDoc = struct {};
    _ = try decoder.decode(EmptyDoc);

    // Invalid: size too small
    const invalid_small = [_]u8{ 4, 0, 0, 0, 0 };
    decoder = Decoder.init(allocator, &invalid_small);
    try testing.expectError(error.MalformedDocument, decoder.decode(EmptyDoc));

    // Invalid: size larger than data
    const invalid_large = [_]u8{ 100, 0, 0, 0, 0 };
    decoder = Decoder.init(allocator, &invalid_large);
    try testing.expectError(error.MalformedDocument, decoder.decode(EmptyDoc));
}
