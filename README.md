# Kafka Replay

A utility tool for recording and replaying Kafka messages from and to Kafka topics. This tool enables you to capture messages from Kafka topics, store them in a structured binary format, and replay them later for testing or debugging.

## Introduction

### What is Kafka Replay?

Kafka Replay is a command-line tool that provides a simple way to:

- **Record** messages from Kafka topics to a binary log file
- **Replay** recorded messages back to Kafka topics (with rate limiting and timestamp preservation)
- **Inspect** recorded messages in a human-readable format

### Why does this project exist?

When working with Kafka, there are common scenarios where you need to:

- **Test and debug**: Capture real production messages to replay in a test environment
- **Reproduce issues**: Record problematic message sequences and replay them for debugging
- **Load testing**: Replay historical messages at different rates to test system performance

Kafka Replay provides a simple, efficient solution for these use cases with a structured binary format that enables fast lookups and efficient storage.

### Key Features

- **Efficient binary format**: Messages are stored in a structured binary format with fixed-size headers for fast lookups (see [FORMAT.md](FORMAT.md) for details)
- **Batch processing**: Replay uses batched writes for optimal performance
- **Rate limiting**: Control the speed of message replay
- **Timestamp preservation**: Optionally preserve original message timestamps
- **Context-aware**: Properly handles cancellation and cleanup
- **Protocol versioning**: File format includes version information for future compatibility

## Usage

### Prerequisites

- Go 1.25.6 or later
- Access to a Kafka/Redpanda cluster
- Docker and Docker Compose (for local development)

### Installation

#### Install using go install (Recommended)

```bash
go install github.com/lolocompany/kafka-replay/cmd/kafka-replay@latest
```

This will install the `kafka-replay` binary to `$GOPATH/bin` (or `$HOME/go/bin` if `GOPATH` is not set). Make sure this directory is in your `PATH`.

#### Download pre-built binaries

Pre-built binaries are available in the [Releases](https://github.com/lolocompany/kafka-replay/releases) section for Linux, macOS, and Windows.

**macOS Gatekeeper Note:**

If you download a macOS binary and encounter a Gatekeeper warning ("Apple could not verify..."), you can bypass it by running:

```bash
xattr -d com.apple.quarantine kafka-replay-darwin-arm64
```

Or for Intel Macs:

```bash
xattr -d com.apple.quarantine kafka-replay-darwin-amd64
```

#### Build from source

If you prefer to build from source:

```bash
make build
```

This will create a `kafka-replay` binary in the project root.

### Commands

#### Record

Record messages from a Kafka topic to a binary log file.

```bash
./kafka-replay record \
  --broker localhost:19092 \
  --topic test-topic \
  --output messages.log
```

**Options:**

- `--broker, -b`: Kafka broker address(es) (required, can be specified multiple times)
- `--topic, -t`: Kafka topic to record messages from (required)
- `--partition, -p`: Kafka partition to record from (default: 0)
- `--group-id, -g`: Consumer group ID (default: "kafka-replay-record")
- `--output, -o`: Output file path (default: "messages.log")
- `--offset, -O`: Start reading from a specific offset (-1 to use current position, 0 to start from beginning, default: -1)
- `--limit, -l`: Maximum number of messages to record (0 for unlimited, default: 0)

**Examples:**

Record all messages from the beginning of a topic:

```bash
./kafka-replay record \
  --broker localhost:19092 \
  --topic my-topic \
  --offset 0 \
  --output backup.log
```

Record a limited number of messages:

```bash
./kafka-replay record \
  --broker localhost:19092 \
  --topic my-topic \
  --output messages.log \
  --limit 100
```

Record 50 messages from the beginning:

```bash
./kafka-replay record \
  --broker localhost:19092 \
  --topic my-topic \
  --offset 0 \
  --output messages.log \
  --limit 50
```

Record from multiple brokers:

```bash
./kafka-replay record \
  --broker broker1:9092 \
  --broker broker2:9092 \
  --broker broker3:9092 \
  --topic my-topic \
  --output messages.log
```

#### Replay

Replay recorded messages from a log file back to a Kafka topic.

```bash
./kafka-replay replay \
  --broker localhost:19092 \
  --topic new-topic \
  --input messages.log
```

**Options:**

- `--broker, -b`: Kafka broker address(es) (required, can be specified multiple times)
- `--topic, -t`: Kafka topic to replay messages to (required)
- `--input, -i`: Input file path containing recorded messages (required)
- `--rate, -r`: Messages per second to replay (0 for maximum speed, default: 0)
- `--preserve-timestamps`: Preserve original message timestamps (default: false)
- `--create-topic`: Create the topic if it doesn't exist (default: false)
- `--loop`: Enable infinite looping - replay messages continuously until interrupted (default: false)

**Examples:**

Replay messages at a controlled rate:

```bash
./kafka-replay replay \
  --broker localhost:19092 \
  --topic test-topic \
  --input messages.log \
  --rate 100
```

Replay with original timestamps preserved:

```bash
./kafka-replay replay \
  --broker localhost:19092 \
  --topic test-topic \
  --input messages.log \
  --preserve-timestamps
```

#### Cat

Display recorded messages from a message file in human-readable format.

```bash
./kafka-replay cat --input messages.log
```

**Options:**

- `--input, -i`: Input file path containing recorded messages (required)
- `--raw, -r`: Output only the raw message data, excluding timestamps and JSON formatting
- `--find, -f`: Filter messages containing the specified byte sequence (string is converted to bytes)
- `--count, -c`: Only output the count of messages, don't display them

**Examples:**

Display messages as JSON (default):

```bash
./kafka-replay cat --input messages.log
```

Output format: Each message is displayed as a JSON object on a single line:

```json
{"timestamp":"2026-02-02T10:15:30.123456789Z","data":"{\"message\":\"test\"}"}
{"timestamp":"2026-02-02T10:15:31.234567890Z","data":"{\"message\":\"another\"}"}
```

The JSON output format includes:

- `timestamp`: ISO 8601 timestamp (RFC3339Nano format) when the message was recorded
- `data`: The message content as a string

Display raw message data only:

```bash
./kafka-replay cat --input messages.log --raw
```

When `--raw` is used, only the raw message bytes are written to stdout, with no timestamps or JSON formatting. This is useful for extracting message content for further processing or piping to other tools.

Filter messages containing a specific string:

```bash
./kafka-replay cat --input messages.log --find "error"
```

The `--find` flag filters messages to only show those containing the specified byte sequence. The string is converted to bytes for matching.

Count messages without displaying them:

```bash
./kafka-replay cat --input messages.log --count
```

The `--count` flag outputs only the total number of messages in the file, useful for quick statistics or scripting.

### File Format

Messages are stored in a structured binary format for efficiency. The format includes:

- **File header** (20 bytes): Protocol version and reserved space
- **Message entries**: Each entry contains a Unix timestamp (8 bytes), key size (8 bytes), message size (8 bytes), key (optional), and message data (variable)

For detailed information about the binary file format, including byte-level specifications and examples, see [FORMAT.md](FORMAT.md) (version 2, current format). For the legacy version 1 format, see [legacy/FORMAT_v1.md](legacy/FORMAT_v1.md).

This format enables:

- Fast lookups (fixed-size headers)
- Efficient storage
- Easy parsing
- Protocol versioning for future compatibility

## Development

### Getting Started

1. **Clone the repository:**

   ```bash
   git clone <repository-url>
   cd kafka-replay
   ```

2. **Install dependencies:**

   ```bash
   go mod download
   ```

3. **Build the project:**

   ```bash
   make build
   ```

### Local Development with Docker Compose

The project includes a `docker-compose.yml` file that sets up a local development environment:

- **Redpanda**: Kafka-compatible message broker (accessible on `localhost:19092`)
- **kafka-writer**: Continuously writes test messages to `test-topic`
- **Redpanda Console**: Web UI for managing Kafka topics (accessible on `http://localhost:8080`)

**Start the services:**

```bash
docker-compose up -d
```

**Stop the services:**

```bash
docker-compose down
```

**View logs:**

```bash
docker-compose logs -f
```

### Testing the Tool

1. **Start the local environment:**

   ```bash
   docker-compose up -d
   ```

2. **Record messages:**

   ```bash
   ./kafka-replay record \
     --broker localhost:19092 \
     --topic test-topic \
     --output messages.log \
     --offset 0
   ```

3. **View recorded messages:**

   ```bash
   ./kafka-replay cat --input messages.log
   ```

4. **Replay messages to a new topic:**

   ```bash
   ./kafka-replay replay \
     --broker localhost:19092 \
     --topic replayed-topic \
     --input messages.log \
     --rate 10
   ```

5. **Verify in Redpanda Console:**
   Open `http://localhost:8080` in your browser to view the topics and messages.

### Project Structure

```
kafka-replay/
├── cmd/                     # Entry points - contains code that relies on OS, IO, or global state
│   └── kafka-replay/        # CLI application entry point
├── pkg/                     # Reusable packages - pure, testable code usable as dependencies
│   ├── kafka/               # Kafka client abstractions
│   └── transcoder/          # Binary file format encoder/decoder
├── docker-compose.yml       # Local development environment
├── dockerfile               # Docker build configuration
├── go.mod                   # Go module definition
├── go.sum                   # Go module checksums
├── makefile                 # Build and test commands
├── LICENSE                  # License file
├── FORMAT.md                # Binary file format specification (version 2)
├── legacy/
│   └── FORMAT_v1.md         # Legacy format specification (version 1)
├── .gitignore               # Git ignore rules
└── README.md                # This file
```

**Directory Organization:**

- **`cmd/`**: Contains entry points and different ways of compiling the program. Code in `cmd` handles OS interactions, file I/O, and global state (CLI flags, environment variables).
- **`pkg/`**: Contains reusable packages that can be used by entry points or imported as dependencies by other projects. Code in `pkg` should be as close as possible to pure functions and testable code, avoiding direct OS/IO dependencies where possible.
  - **`pkg/transcoder/`**: Implements the binary file format using `EncodeWriter` and `DecodeReader` types that work with Go's standard `io.Writer` and `io.ReadSeeker` interfaces.

### Building and Running

**Build:**

```bash
make build
```

**Run with makefile shortcuts:**

```bash
make record    # Record messages
make replay    # Replay messages
make cat       # Display messages
```

**Clean build artifacts:**

```bash
make clean
```

### Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Ensure the code compiles: `go build ./...`
5. Test your changes with the local Docker Compose environment
6. Submit a pull request

### License

MIT License - see [LICENSE](LICENSE) file for details.
