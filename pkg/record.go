package pkg

import (
	"context"
	"encoding/binary"
	"io"
	"time"

	kafka "github.com/lolocompany/kafka-replay/pkg/kafka"
	"github.com/schollz/progressbar/v3"
)

const (
	// TimestampFormat is a fixed-size ISO 8601 timestamp format
	// Format: "2006-01-02T15:04:05.000000Z" (27 bytes)
	TimestampFormat = "2006-01-02T15:04:05.000000Z"
	TimestampSize   = 27
	SizeFieldSize   = 8 // int64 = 8 bytes
)

func Record(ctx context.Context, consumer *kafka.Consumer, fromBeginning bool, output io.WriteCloser, limit int) (int64, int64, error) {
	// Set offset if we want to read from the beginning
	if fromBeginning {
		if err := consumer.SetOffsetFromBeginning(); err != nil {
			return 0, 0, err
		}
	}

	// Initialize progress bar
	// If limit is 0 (unlimited), use -1 to create a spinner
	// Otherwise, use limit as the total
	var bar *progressbar.ProgressBar
	if limit > 0 {
		bar = progressbar.Default(int64(limit), "Recording messages")
	} else {
		bar = progressbar.Default(-1, "Recording messages")
	}
	defer bar.Close()

	var totalBytes int64
	var messageCount int64
	timestampBuf := make([]byte, TimestampSize)
	sizeBuf := make([]byte, SizeFieldSize)

	for {
		// Check if we've reached the message limit
		if limit > 0 && messageCount >= int64(limit) {
			break
		}

		// Check context cancellation
		select {
		case <-ctx.Done():
			return totalBytes, messageCount, ctx.Err()
		default:
		}

		// Read next complete message
		messageData, err := consumer.ReadNextMessage(ctx)
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
		recordTime := time.Now().UTC()

		// Write timestamp (fixed size: 27 bytes)
		timestampStr := recordTime.Format(TimestampFormat)
		copy(timestampBuf, timestampStr)
		if _, err := output.Write(timestampBuf); err != nil {
			return totalBytes, messageCount, err
		}
		totalBytes += TimestampSize

		// Write message size (fixed size: 8 bytes, big-endian)
		binary.BigEndian.PutUint64(sizeBuf, uint64(messageSize))
		if _, err := output.Write(sizeBuf); err != nil {
			return totalBytes, messageCount, err
		}
		totalBytes += SizeFieldSize

		// Write message data
		if _, err := output.Write(messageData); err != nil {
			return totalBytes, messageCount, err
		}
		totalBytes += messageSize

		messageCount++

		// Update progress bar
		if err := bar.Add(1); err != nil {
			// Ignore progress bar errors, continue recording
		}
	}

	return totalBytes, messageCount, nil
}
