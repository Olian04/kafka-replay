package pkg

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	kafkapkg "github.com/lolocompany/kafka-replay/pkg/kafka"
	"github.com/segmentio/kafka-go"
)

// MessageFileReader reads recorded Kafka messages from a binary file
type MessageFileReader struct {
	reader             io.ReadSeeker
	timestampBuf       []byte
	sizeBuf            []byte
	preserveTimestamps bool
	timeProvider       TimeProvider
}

// RecordedMessage represents a message read from the recorded messages file
type RecordedMessage struct {
	Data      []byte    `json:"data"`
	Timestamp time.Time `json:"timestamp"`
}

// NewMessageFileReader creates a new reader for binary message files
func NewMessageFileReader(reader io.ReadSeeker, preserveTimestamps bool, timeProvider TimeProvider) *MessageFileReader {
	return &MessageFileReader{
		reader:             reader,
		timestampBuf:       make([]byte, TimestampSize),
		sizeBuf:            make([]byte, SizeFieldSize),
		preserveTimestamps: preserveTimestamps,
		timeProvider:       timeProvider,
	}
}

// ReadNextMessage reads the next complete message from the recorded messages file
// Returns the message data and timestamp, or an error if no message is available or EOF
func (r *MessageFileReader) ReadNextMessage(ctx context.Context) (*RecordedMessage, error) {
	// Check context cancellation
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	// Read timestamp (27 bytes)
	if _, err := io.ReadFull(r.reader, r.timestampBuf); err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, io.EOF
		}
		return nil, fmt.Errorf("failed to read timestamp: %w", err)
	}

	// Read message size (8 bytes)
	if _, err := io.ReadFull(r.reader, r.sizeBuf); err != nil {
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
	if _, err := io.ReadFull(r.reader, messageData); err != nil {
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
			msgTime = r.timeProvider.Now()
		} else {
			msgTime = parsedTime
		}
	} else {
		msgTime = r.timeProvider.Now()
	}

	return &RecordedMessage{
		Data:      messageData,
		Timestamp: msgTime,
	}, nil
}

// Close closes the underlying reader if it implements io.Closer
func (r *MessageFileReader) Close() error {
	if closer, ok := r.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// FileSize returns the size of the underlying reader if it implements Size() method,
// otherwise returns 0. For files, use Seek to determine size.
func (r *MessageFileReader) FileSize() (int64, error) {
	// Try to get size using Seek
	currentPos, err := r.reader.Seek(0, io.SeekCurrent)
	if err != nil {
		return 0, fmt.Errorf("failed to get current position: %w", err)
	}

	endPos, err := r.reader.Seek(0, io.SeekEnd)
	if err != nil {
		return 0, fmt.Errorf("failed to seek to end: %w", err)
	}

	// Restore original position
	if _, err := r.reader.Seek(currentPos, io.SeekStart); err != nil {
		return 0, fmt.Errorf("failed to restore position: %w", err)
	}

	return endPos, nil
}

// Reset seeks back to the beginning of the reader
func (r *MessageFileReader) Reset() error {
	_, err := r.reader.Seek(0, io.SeekStart)
	return err
}

const (
	// DefaultBatchSize is the default number of messages to batch before writing
	DefaultBatchSize = 100
	// DefaultBatchBytes is the default maximum bytes to batch before writing (10MB)
	DefaultBatchBytes = 10 * 1024 * 1024
)

// ReplayConfig holds configuration for the Replay function
type ReplayConfig struct {
	Producer         *kafkapkg.Producer
	Reader           *MessageFileReader
	Rate             int
	Loop             bool
	LogWriter        io.Writer
	ProgressReporter ProgressReporter
	MaxBatchSize     int
	MaxBatchBytes    int64
}

func Replay(ctx context.Context, cfg ReplayConfig) (int64, error) {
	if cfg.MaxBatchSize == 0 {
		cfg.MaxBatchSize = DefaultBatchSize
	}
	if cfg.MaxBatchBytes == 0 {
		cfg.MaxBatchBytes = DefaultBatchBytes
	}

	// Get file size for progress reporter if provided
	var fileSize int64
	if cfg.ProgressReporter != nil {
		var err error
		fileSize, err = cfg.Reader.FileSize()
		if err != nil {
			return 0, fmt.Errorf("failed to get file size: %w", err)
		}
		cfg.ProgressReporter.SetTotal(fileSize)
		defer cfg.ProgressReporter.Close()
	}

	// Rate limiting setup
	var rateLimiter *time.Ticker
	if cfg.Rate > 0 {
		interval := time.Second / time.Duration(cfg.Rate)
		rateLimiter = time.NewTicker(interval)
		defer rateLimiter.Stop()
	}

	var messageCount int64
	var bytesRead int64   // Track total bytes read from file
	var loopIteration int // Track loop iteration for display
	batch := make([]kafka.Message, 0, cfg.MaxBatchSize)
	var batchBytes int64

	// Flush batch helper function
	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}
		if err := cfg.Producer.WriteMessages(ctx, batch...); err != nil {
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
		msg, err := cfg.Reader.ReadNextMessage(ctx)
		if err != nil {
			if err == io.EOF {
				// End of file reached - flush remaining batch
				if err := flushBatch(); err != nil {
					return messageCount, err
				}
				// Update progress reporter to 100% if provided
				if cfg.ProgressReporter != nil && fileSize > 0 {
					cfg.ProgressReporter.Set(fileSize)
				}

				// Check if we should loop
				if cfg.Loop {
					// Reset to beginning of file
					if err := cfg.Reader.Reset(); err != nil {
						return messageCount, fmt.Errorf("failed to reset file: %w", err)
					}
					loopIteration++
					bytesRead = 0 // Reset bytes read counter
					if cfg.ProgressReporter != nil {
						cfg.ProgressReporter.Set(0)
					}
					if cfg.LogWriter != nil {
						fmt.Fprintf(cfg.LogWriter, "Looping: restarting from beginning (iteration %d)\n", loopIteration+1)
					}
					continue // Continue the loop to read from beginning
				}

				// No more looping, exit
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

		// Update progress reporter if provided
		if cfg.ProgressReporter != nil {
			cfg.ProgressReporter.Set(bytesRead)
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
		if len(batch) >= cfg.MaxBatchSize || batchBytes >= cfg.MaxBatchBytes {
			if err := flushBatch(); err != nil {
				return messageCount, err
			}
		}
	}

	return messageCount, nil
}
