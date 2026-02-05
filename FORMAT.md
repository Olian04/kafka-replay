# Binary File Format Specification - Version 2

This document describes the binary file format (version 2) used by the Kafka Replay transcoder to store recorded Kafka messages.

**Note:** This is the current format. For the legacy version 1 format, see [legacy/FORMAT_v1.md](legacy/FORMAT_v1.md).

## Overview

The file format consists of:

1. A fixed-size file header containing protocol metadata
2. A series of message entries, each containing a timestamp, key size, message size, key (optional), and message data

**Protocol Versions:**

- **Version 1** (legacy): See [legacy/FORMAT_v1.md](legacy/FORMAT_v1.md) for details
- **Version 2** (current): Message entries contain timestamp, key size, message size, key, and message data

All new files are written in version 2 format. Version 1 files are still readable for backward compatibility.

## File Structure

```
[File Header (20 bytes)]
[Message Entry 1]
[Message Entry 2]
...
[Message Entry N]
```

## File Header

The file header is 20 bytes total and appears at the beginning of every file:

| Offset | Size | Type               | Description                               |
| ------ | ---- | ------------------ | ----------------------------------------- |
| 0      | 4    | int32 (big-endian) | Protocol version (2)                      |
| 4      | 16   | bytes              | Reserved space for future use (all zeros) |

### Protocol Version

The protocol version field is a 32-bit signed integer stored in big-endian byte order. Version 2 files use the value `2`. The decoder also supports reading version 1 files for backward compatibility.

### Reserved Space

The 16 bytes following the protocol version are reserved for future protocol extensions. Currently, these bytes are always set to zero.

## Message Entry Format

Each message entry follows this structure:

| Offset | Size     | Type               | Description                               |
| ------ | -------- | ------------------ | ----------------------------------------- |
| 0      | 8        | int64 (big-endian) | Unix timestamp (seconds since epoch, UTC) |
| 8      | 8        | int64 (big-endian) | Key size in bytes (0 if no key)           |
| 16     | 8        | int64 (big-endian) | Message data size in bytes                |
| 24     | variable | bytes              | Key data (if key size > 0)                |
| 24+N   | variable | bytes              | Message data (raw bytes)                  |

**Note:** In version 2, if the key size is 0, no key data is written and the message data starts immediately after the message size field (at offset 24).

**Design Rationale:** All fixed-size fields (timestamp, key size, message size) are placed before variable data (key, message). This ordering enables faster lookups by allowing readers to read all size information before seeking to or reading the actual data.

### Timestamp

The timestamp is stored as a Unix timestamp (seconds since January 1, 1970 UTC) as a 64-bit signed integer in big-endian byte order. This represents when the message was recorded.

**Example:** A timestamp value of `1706872530` represents `2024-02-02T10:15:30Z`.

### Key Size

The key size field indicates the length of the message key in bytes. It is stored as a 64-bit signed integer in big-endian byte order. A value of 0 indicates the message has no key. The maximum supported key size is 100 MB (104,857,600 bytes). Keys larger than this will cause an error when reading.

### Message Size

The message size field indicates the length of the message data in bytes. It is stored as a 64-bit signed integer in big-endian byte order. The maximum supported message size is 100 MB (104,857,600 bytes). Messages larger than this will cause an error when reading.

### Key Data

The key data follows after all fixed-size fields (timestamp, key size, message size), but only if the key size is greater than 0. It contains the raw bytes of the Kafka message key. The length of this field is determined by the key size field.

### Message Data

The message data follows after the key data (if present) or immediately after the message size field (if no key). It contains the raw bytes of the Kafka message value. The length of this field is determined by the message size field.

## Byte Order

All multi-byte integers (int32, int64) are stored in **big-endian** (network byte order) format. This ensures compatibility across different architectures.

## Examples

### Version 2 Example (With Key)

For a message with:

- Timestamp: `2024-02-02T10:15:30Z` (Unix timestamp: `1706872530`)
- Key: `"user-123"` (8 bytes)
- Data: `"Hello, World!"` (13 bytes)

The binary representation would be:

```
[File Header - 20 bytes]
[0x00 0x00 0x00 0x02]  # Protocol version 2
[0x00 ... 0x00]        # 16 reserved bytes

[Message Entry - 45 bytes]
[0x00 0x00 0x00 0x00 0x65 0x9C 0x5C 0x92]  # Timestamp: 1706872530
[0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x08]  # Key size: 8
[0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x0D]  # Message size: 13
[0x75 0x73 0x65 0x72 0x2D 0x31 0x32 0x33]  # Key: "user-123"
[0x48 0x65 0x6C 0x6C 0x6F 0x2C 0x20 0x57 0x6F 0x72 0x6C 0x64 0x21]  # "Hello, World!"
```

### Version 2 Example (No Key)

For a message with:

- Timestamp: `2024-02-02T10:15:30Z` (Unix timestamp: `1706872530`)
- Key: `nil` (no key)
- Data: `"Hello, World!"` (13 bytes)

The binary representation would be:

```
[File Header - 20 bytes]
[0x00 0x00 0x00 0x02]  # Protocol version 2
[0x00 ... 0x00]        # 16 reserved bytes

[Message Entry - 37 bytes]
[0x00 0x00 0x00 0x00 0x65 0x9C 0x5C 0x92]  # Timestamp: 1706872530
[0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x00]  # Key size: 0 (no key)
[0x00 0x00 0x00 0x00 0x00 0x00 0x00 0x0D]  # Message size: 13
[0x48 0x65 0x6C 0x6C 0x6F 0x2C 0x20 0x57 0x6F 0x72 0x6C 0x64 0x21]  # "Hello, World!"
```

## Reading Files

When reading files:

1. **Read the header** (20 bytes) and validate the protocol version (must be 1 or 2)
2. **For each message entry (version 2):**
   - Read 8 bytes for the timestamp
   - Read 8 bytes for the key size
   - Read 8 bytes for the message size
   - If key size > 0, read N bytes (where N is the key size) for the key data
   - Read M bytes (where M is the message size) for the message data
   - Parse the timestamp from Unix seconds to a time.Time value

**Backward Compatibility:** Version 1 files are automatically detected and read correctly. The decoder will return `nil` for the key when reading version 1 files. See [legacy/FORMAT_v1.md](legacy/FORMAT_v1.md) for version 1 reading instructions.

**Note:** The ordering of fixed-size fields (timestamp, key size, message size) before variable data (key, message) enables efficient lookups by allowing readers to determine all sizes before reading the actual data.

## Writing Files

When writing files:

1. **Write the header** (20 bytes) with protocol version 2 and zero-filled reserved bytes
2. **For each message:**
   - Convert the timestamp to Unix seconds (int64)
   - Write 8 bytes (big-endian) for the timestamp
   - Write 8 bytes (big-endian) for the key size (0 if no key)
   - Write 8 bytes (big-endian) for the message size
   - If key size > 0, write the key data bytes
   - Write the message data bytes

**Note:** All new files are written in version 2 format. Version 1 format is only used for reading legacy files. The ordering of all fixed-size fields (timestamp, key size, message size) before variable data (key, message) enables faster lookups.

## Constants

The format uses the following constants (defined in `pkg/transcoder/constants.go`):

- `ProtocolVersion = 2` (current version)
- `ProtocolVersion1 = 1` (legacy version, for backward compatibility)
- `HeaderVersionSize = 4` bytes
- `HeaderReservedSize = 16` bytes
- `HeaderSize = 20` bytes (HeaderVersionSize + HeaderReservedSize)
- `TimestampSize = 8` bytes
- `KeySizeFieldSize = 8` bytes
- `SizeFieldSize = 8` bytes
- Maximum message/key size: `100 * 1024 * 1024` bytes (100 MB)

## Implementation

The format is implemented in the `pkg/transcoder` package:

- **`EncodeWriter`**: Writes messages in version 2 format
- **`DecodeReader`**: Reads messages from version 2 format (and version 1 for backward compatibility)

Both types work with Go's standard `io.Writer` and `io.ReadSeeker` interfaces, making them flexible and testable.
