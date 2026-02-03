package pkg

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"time"

	kafkapkg "github.com/lolocompany/kafka-replay/pkg/kafka"
	"github.com/schollz/progressbar/v3"
	"github.com/segmentio/kafka-go"
)

// LogFileReader reads messages from a binary log file
type LogFileReader struct {
	file               *os.File
	timestampBuf       []byte
	sizeBuf            []byte
	preserveTimestamps bool
}

// LogFileMessage represents a message read from the log file
type LogFileMessage struct {
	Data      []byte    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}

// NewLogFileReader creates a new reader for binary log files
func NewLogFileReader(input string, preserveTimestamps bool) (*LogFileReader, error) {
	file, err := os.Open(input)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file: %w", err)
	}

	return &LogFileReader{
		file:               file,
		timestampBuf:       make([]byte, TimestampSize),
		sizeBuf:            make([]byte, SizeFieldSize),
		preserveTimestamps: preserveTimestamps,
	}, nil
}

// ReadNextMessage reads the next complete message from the log file
// Returns the message data and timestamp, or an error if no message is available or EOF
func (r *LogFileReader) ReadNextMessage(ctx context.Context) (*LogFileMessage, error) {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Read timestamp (27 bytes)
	if _, err := io.ReadFull(r.file, r.timestampBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}

	// Read message size (8 bytes)
	if _, err := io.ReadFull(r.file, r.sizeBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read message size: %w", err)
	}

	messageSize := int64(binary.BigEndian.Uint64(r.sizeBuf))
	if messageSize < 0 || messageSize > 100*1024*1024 { // Sanity check: max 100MB
		return nil, fmt.Errorf("invalid message size: %d bytes", messageSize)
	}

	// Read message data
	messageData := make([]byte, messageSize)
	if _, err := io.ReadFull(r.file, messageData); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read message data: %w", err)
	}

	// Parse timestamp
	var msgTime time.Time
	if r.preserveTimestamps {
		timestampStr := string(r.timestampBuf)
		parsedTime, err := time.Parse(TimestampFormat, timestampStr)
		if err != nil {
			// If timestamp parsing fails, use current time
			msgTime = time.Now()
		} else {
			msgTime = parsedTime
		}
	} else {
		msgTime = time.Now()
	}

	return &LogFileMessage{
		Data:      messageData,
		Timestamp: msgTime,
	}, nil
}

// Close closes the underlying file
func (r *LogFileReader) Close() error {
	if r.file != nil {
		return r.file.Close()
	}
	return nil
}

// FileSize returns the size of the underlying file
func (r *LogFileReader) FileSize() (int64, error) {
	if r.file == nil {
		return 0, fmt.Errorf("file is nil")
	}
	stat, err := r.file.Stat()
	if err != nil {
		return 0, err
	}
	return stat.Size(), nil
}

const (
	// DefaultBatchSize is the default number of messages to batch before writing
	DefaultBatchSize = 100
	// DefaultBatchBytes is the default maximum bytes to batch before writing (10MB)
	DefaultBatchBytes = 10 * 1024 * 1024
)

func Replay(ctx context.Context, producer *kafkapkg.Producer, reader *LogFileReader, rate int) (int64, error) {
	// Get file size for progress bar
	fileSize, err := reader.FileSize()
	if err != nil {
		return 0, fmt.Errorf("failed to get file size: %w", err)
	}

	// Initialize progress bar based on file size
	bar := progressbar.DefaultBytes(fileSize, "Replaying messages")
	defer bar.Close()

	// Rate limiting setup
	var rateLimiter *time.Ticker
	if rate > 0 {
		interval := time.Second / time.Duration(rate)
		rateLimiter = time.NewTicker(interval)
		defer rateLimiter.Stop()
	}

	var messageCount int64
	var bytesRead int64 // Track total bytes read from file
	batch := make([]kafka.Message, 0, DefaultBatchSize)
	var batchBytes int64

	// Flush batch helper function
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := producer.WriteMessages(ctx, batch...); err != nil {
			return fmt.Errorf("failed to write batch to Kafka: %w", err)
		}
		batch = batch[:0] // Reset batch
		batchBytes = 0
		return nil
	}

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			// Flush any remaining messages before returning
			if err := flushBatch(); err != nil {
				return messageCount, err
			}
			return messageCount, ctx.Err()
		default:
		}

		// Read next complete message
		msg, err := reader.ReadNextMessage(ctx)
		if err != nil {
			if err == io.EOF {
				// End of file reached - flush remaining batch and exit
				if err := flushBatch(); err != nil {
					return messageCount, err
				}
				// Update progress bar to 100%
				bar.Set64(fileSize)
				break
			}
			// Check if context was canceled
			if ctx.Err() != nil {
				// Flush any remaining messages before returning
				if err := flushBatch(); err != nil {
					return messageCount, err
				}
				return messageCount, ctx.Err()
			}
			return messageCount, err
		}

		// Calculate bytes read for this message:
		// TimestampSize (27) + SizeFieldSize (8) + messageData size
		messageBytesRead := TimestampSize + SizeFieldSize + int64(len(msg.Data))
		bytesRead += messageBytesRead

		// Update progress bar
		if err := bar.Set64(bytesRead); err != nil {
			// Ignore progress bar errors, continue replaying
		}

		// Rate limiting - if enabled, wait before adding to batch
		if rateLimiter != nil {
			select {
			case <-ctx.Done():
				// Flush any remaining messages before returning
				if err := flushBatch(); err != nil {
					return messageCount, err
				}
				return messageCount, ctx.Err()
			case <-rateLimiter.C:
				// Rate limit tick received, proceed
			}
		}

		// Add message to batch
		kafkaMsg := kafka.Message{
			Value: msg.Data,
			Time:  msg.Timestamp,
		}
		batch = append(batch, kafkaMsg)
		batchBytes += int64(len(msg.Data))

		messageCount++

		// Flush batch if it reaches size or byte limit
		if len(batch) >= DefaultBatchSize || batchBytes >= DefaultBatchBytes {
			if err := flushBatch(); err != nil {
				return messageCount, err
			}
		}
	}

	return messageCount, nil
}
