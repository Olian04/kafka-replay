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

- **Efficient binary format**: Messages are stored in a structured binary format with fixed-size headers for fast lookups
- **Batch processing**: Replay uses batched writes for optimal performance
- **Rate limiting**: Control the speed of message replay
- **Timestamp preservation**: Optionally preserve original message timestamps
- **Context-aware**: Properly handles cancellation and cleanup

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
- `--output, -o`: Output file path (default: "kafka-record.jsonl")
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
- `--preserve-timestamps`: Preserve original message timestamps

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

**Example:**

```bash
./kafka-replay cat --input messages.log
```

Output format:

Each message is displayed as a JSON object on a single line:

```json
{"timestamp":"2026-02-02T10:15:30.123456789Z","data":"{\"message\":\"test\"}"}
{"timestamp":"2026-02-02T10:15:31.234567890Z","data":"{\"message\":\"another\"}"}
```

The output format includes:
- `timestamp`: ISO 8601 timestamp (RFC3339Nano format) when the message was recorded
- `data`: The message content as a string

### File Format

Messages are stored in a binary format for efficiency:

- **Timestamp** (27 bytes, fixed): ISO 8601 timestamp when the message was recorded
- **Size** (8 bytes, fixed): Message size in bytes (int64, big-endian)
- **Data** (variable): The actual message bytes

This format enables:

- Fast lookups (fixed-size headers)
- Efficient storage
- Easy parsing

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
├── cmd/
│   └── cli/
│       ├── main.go          # CLI entry point
│       ├── record.go        # Record command implementation
│       ├── replay.go        # Replay command implementation
│       └── cat.go           # Cat command implementation
├── pkg/
│   ├── kafka/
│   │   ├── consumer.go      # Kafka consumer implementation
│   │   └── producer.go      # Kafka producer implementation
│   ├── record.go            # Recording logic
│   ├── replay.go            # Replay logic with batching
│   ├── cat.go               # Message display logic
│   └── kafka_exports.go     # Package exports
├── docker-compose.yml       # Local development environment
├── dockerfile               # Docker build configuration
├── go.mod                   # Go module definition
├── go.sum                   # Go module checksums
├── makefile                 # Build and test commands
├── LICENSE                  # License file
├── .gitignore              # Git ignore rules
└── README.md               # This file
```

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
