package pkg

import (
	"context"
	"encoding/binary"
	"io"

	kafka "github.com/lolocompany/kafka-replay/pkg/kafka"
)

const (
	// TimestampFormat is a fixed-size ISO 8601 timestamp format
	// Format: "2006-01-02T15:04:05.000000Z" (27 bytes)
	TimestampFormat = "2006-01-02T15:04:05.000000Z"
	TimestampSize   = 27
	SizeFieldSize   = 8 // int64 = 8 bytes
)

// RecordConfig holds configuration for the Record function
type RecordConfig struct {
	Consumer         *kafka.Consumer
	Offset           *int64
	Output           io.WriteCloser
	Limit            int
	TimeProvider     TimeProvider
	ProgressReporter ProgressReporter
}

func Record(ctx context.Context, cfg RecordConfig) (int64, int64, error) {
	// Set offset if specified
	if cfg.Offset != nil {
		if err := cfg.Consumer.SetOffset(*cfg.Offset); err != nil {
			return 0, 0, err
		}
	}

	// Initialize progress reporter if provided
	if cfg.ProgressReporter != nil {
		if cfg.Limit > 0 {
			cfg.ProgressReporter.SetTotal(int64(cfg.Limit))
		}
		defer cfg.ProgressReporter.Close()
	}

	var totalBytes int64
	var messageCount int64
	timestampBuf := make([]byte, TimestampSize)
	sizeBuf := make([]byte, SizeFieldSize)

	for {
		// Check if we've reached the message limit
		if cfg.Limit > 0 && messageCount >= int64(cfg.Limit) {
			break
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			return totalBytes, messageCount, ctx.Err()
		default:
		}

		// Read next complete message
		messageData, err := cfg.Consumer.ReadNextMessage(ctx)
		if err != nil {
			if err == io.EOF {
				// End of batch, continue to read next batch
				continue
			}
			// Check if context was canceled
			if ctx.Err() != nil {
				return totalBytes, messageCount, ctx.Err()
			}
			return totalBytes, messageCount, err
		}

		messageSize := int64(len(messageData))
		recordTime := cfg.TimeProvider.Now().UTC()

		// Write timestamp (fixed size: 27 bytes)
		timestampStr := recordTime.Format(TimestampFormat)
		copy(timestampBuf, timestampStr)
		if _, err := cfg.Output.Write(timestampBuf); err != nil {
			return totalBytes, messageCount, err
		}
		totalBytes += TimestampSize

		// Write message size (fixed size: 8 bytes, big-endian)
		binary.BigEndian.PutUint64(sizeBuf, uint64(messageSize))
		if _, err := cfg.Output.Write(sizeBuf); err != nil {
			return totalBytes, messageCount, err
		}
		totalBytes += SizeFieldSize

		// Write message data
		if _, err := cfg.Output.Write(messageData); err != nil {
			return totalBytes, messageCount, err
		}
		totalBytes += messageSize

		messageCount++

		// Update progress reporter if provided
		if cfg.ProgressReporter != nil {
			cfg.ProgressReporter.Add(1)
		}
	}

	return totalBytes, messageCount, nil
}
