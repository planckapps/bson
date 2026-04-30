const std = @import("std");

/// BSON-specific errors
pub const BsonError = error{
    /// Document exceeds maximum size (16MB)
    DocumentTooLarge,

    /// Invalid BSON type tag
    InvalidType,

    /// Document structure is malformed
    MalformedDocument,

    /// String is not valid UTF-8
    InvalidUtf8,

    /// Unexpected end of data
    UnexpectedEof,

    /// Field name contains null byte
    InvalidFieldName,

    /// Array indices are not sequential
    InvalidArrayIndex,

    /// ObjectId must be exactly 12 bytes
    InvalidObjectId,

    /// Invalid binary subtype
    InvalidBinarySubtype,

    /// Type mismatch during decoding
    TypeMismatch,

    /// Missing required field
    MissingField,

    /// Allocation failed
    OutOfMemory,

    /// Integer overflow during conversion
    Overflow,

    /// Buffer has no space left for formatting
    NoSpaceLeft,
};

/// Combined error set for BSON operations
pub const Error = BsonError || std.mem.Allocator.Error;
