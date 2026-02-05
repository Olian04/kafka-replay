package pkg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	kafkapkg "github.com/lolocompany/kafka-replay/pkg/kafka"
	"github.com/lolocompany/kafka-replay/pkg/transcoder"
	"github.com/segmentio/kafka-go"
)

const (
	// DefaultBatchSize is the default number of messages to batch before writing
	DefaultBatchSize = 100
	// DefaultBatchBytes is the default maximum bytes to batch before writing (10MB)
	DefaultBatchBytes = 10 * 1024 * 1024
)

// ReplayConfig holds configuration for the Replay function
type ReplayConfig struct {
	Producer  *kafkapkg.Producer
	Decoder   *transcoder.DecodeReader
	Rate      int
	Loop      bool
	Partition *int // Optional partition to write to (nil for auto-assignment)
	LogWriter io.Writer
	DryRun    bool // If true, validate messages without actually sending to Kafka
}

func Replay(ctx context.Context, cfg ReplayConfig) (int64, error) {
	if cfg.Producer == nil {
		return 0, errors.New("producer is required")
	}
	if cfg.Decoder == nil {
		return 0, errors.New("decoder is required")
	}
	if cfg.LogWriter == nil {
		cfg.LogWriter = os.Stderr
	}

	// Rate limiting setup
	var rateLimiter *time.Ticker
	if cfg.Rate > 0 {
		interval := time.Second / time.Duration(cfg.Rate)
		rateLimiter = time.NewTicker(interval)
		defer rateLimiter.Stop()
	}

	var messageCount int64
	batch := make([]kafka.Message, 0, DefaultBatchSize)
	var batchBytes int64

	for {
		// Check context cancellation
		select {
		case <-ctx.Done():
			if err := flushReplayBatch(ctx, cfg, batch); err != nil {
				return messageCount, err
			}
			return messageCount, ctx.Err()
		default:
		}

		// Read next complete message
		timestamp, data, err := cfg.Decoder.Read()
		if err != nil {
			if err == io.EOF {
				// End of file reached - flush remaining batch
				if err := flushReplayBatch(ctx, cfg, batch); err != nil {
					return messageCount, err
				}

				// Check if we should loop
				if cfg.Loop {
					if err := cfg.Decoder.Reset(); err != nil {
						return messageCount, err
					}
					continue
				}

				// No more looping, exit
				break
			}
			// Check if context was canceled
			if ctx.Err() != nil {
				if err := flushReplayBatch(ctx, cfg, batch); err != nil {
					return messageCount, err
				}
				return messageCount, ctx.Err()
			}
			return messageCount, err
		}

		// Rate limiting - if enabled, wait before adding to batch
		if rateLimiter != nil {
			select {
			case <-ctx.Done():
				if err := flushReplayBatch(ctx, cfg, batch); err != nil {
					return messageCount, err
				}
				return messageCount, ctx.Err()
			case <-rateLimiter.C:
				// Rate limit tick received, proceed
			}
		}

		// Add message to batch
		kafkaMsg := kafka.Message{
			Value: data,
			Time:  timestamp,
		}
		// Set partition if specified in config
		if cfg.Partition != nil {
			kafkaMsg.Partition = *cfg.Partition
		}
		batch = append(batch, kafkaMsg)
		batchBytes += int64(len(data))

		messageCount++

		// Flush batch if it reaches size or byte limit
		if len(batch) >= DefaultBatchSize || batchBytes >= DefaultBatchBytes {
			if err := flushReplayBatch(ctx, cfg, batch); err != nil {
				return messageCount, err
			}
			batch = batch[:0]
			batchBytes = 0
		}
	}

	return messageCount, nil
}

// flushReplayBatch writes the current batch to Kafka (or validates in dry-run mode)
func flushReplayBatch(ctx context.Context, cfg ReplayConfig, batch []kafka.Message) error {
	if len(batch) == 0 {
		return nil
	}
	// In dry-run mode, skip actual writing but still validate the batch
	if cfg.DryRun {
		// Validate that messages are properly formed
		// The fact that we got here means decoding succeeded, so validation passes
		return nil
	}
	if err := cfg.Producer.WriteMessages(ctx, batch...); err != nil {
		return fmt.Errorf("failed to write batch to Kafka: %w", err)
	}
	return nil
}
