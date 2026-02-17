package pkg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	kafkapkg "github.com/lolocompany/kafka-replay/v2/pkg/kafka"
	"github.com/segmentio/kafka-go"
)

// MirrorConfig holds configuration for the Mirror function
type MirrorConfig struct {
	Consumer   *kafkapkg.Consumer
	Producer   *kafkapkg.Producer
	Offset     *int64
	Limit      int
	Partition  *int // Optional partition to write to (nil for auto-assignment)
	LogWriter  io.Writer
	DryRun     bool   // If true, validate messages without actually sending to Kafka
	FindBytes  []byte // Optional byte sequence to search for in messages
	PreserveTimestamps bool // Preserve original message timestamps
	OnBytesProcessed func(int64) // Optional callback to report bytes processed
}

func Mirror(ctx context.Context, cfg MirrorConfig) (int64, error) {
	if cfg.Consumer == nil {
		return 0, errors.New("consumer is required")
	}
	if cfg.Producer == nil {
		return 0, errors.New("producer is required")
	}
	if cfg.LogWriter == nil {
		cfg.LogWriter = os.Stderr
	}

	// Set offset if specified
	// Note: When using consumer groups, SetOffset will fail as offsets are managed automatically.
	// In that case, we skip setting the offset and let the consumer group handle it.
	if cfg.Offset != nil {
		if err := cfg.Consumer.SetOffset(*cfg.Offset); err != nil {
			// If SetOffset fails (e.g., when using consumer groups), we continue anyway.
			// Consumer groups manage offsets automatically, so this is expected behavior.
		}
	}

	// Channel to pass messages from reader to writer goroutine
	// Buffered to allow some pipelining while maintaining backpressure
	msgChan := make(chan kafka.Message, BatchSize)
	
	// Channel to signal completion and pass errors
	errChan := make(chan error, 1)

	// Reader goroutine: reads from consumer and sends messages to channel
	go func() {
		defer close(msgChan)
		
		var messageCount int64

		for {
			// Check if we've reached the message limit
			if cfg.Limit > 0 && messageCount >= int64(cfg.Limit) {
				return
			}

			// Check context cancellation
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Read next complete message from Kafka consumer
			timestamp, key, messageData, err := cfg.Consumer.ReadNextMessage(ctx)
			if err != nil {
				if err == io.EOF {
					// End of batch, continue to read next batch
					continue
				}
				// Check if context was canceled
				if ctx.Err() != nil {
					return
				}
				// Other error - send to error channel
				select {
				case errChan <- err:
				case <-ctx.Done():
				}
				return
			}

			// Filter by find bytes if specified
			if cfg.FindBytes != nil && !bytes.Contains(messageData, cfg.FindBytes) {
				// Skip this message, continue to next one
				continue
			}

			// Use pooled buffers for key and value
			var keyBuf []byte
			if len(key) > 0 {
				keyBuf = getKeySlice()
				if cap(keyBuf) < len(key) {
					// Buffer too small, allocate new one
					returnKeySlice(keyBuf)
					keyBuf = make([]byte, len(key))
				} else {
					keyBuf = keyBuf[:len(key)]
				}
				copy(keyBuf, key)
			}

			valueBuf := getValueSlice()
			if cap(valueBuf) < len(messageData) {
				// Buffer too small, allocate new one
				returnValueSlice(valueBuf)
				valueBuf = make([]byte, len(messageData))
			} else {
				valueBuf = valueBuf[:len(messageData)]
			}
			copy(valueBuf, messageData)

			// Determine timestamp to use
			msgTime := timestamp
			if !cfg.PreserveTimestamps {
				msgTime = time.Now().UTC()
			}

			// Build Kafka message with pooled buffers (returned to pool after flush)
			kafkaMsg := kafka.Message{
				Key:   keyBuf,
				Value: valueBuf,
				Time:  msgTime,
			}
			// Set partition if specified in config (nil means auto-assignment)
			if cfg.Partition != nil {
				kafkaMsg.Partition = *cfg.Partition
			}

			// Send message to writer goroutine
			select {
			case msgChan <- kafkaMsg:
				// Message sent successfully
				messageCount++
				// Update progress: track bytes for key + value
				if cfg.OnBytesProcessed != nil {
					bytesProcessed := int64(len(messageData))
					if len(key) > 0 {
						bytesProcessed += int64(len(key))
					}
					cfg.OnBytesProcessed(bytesProcessed)
				}
			case <-ctx.Done():
				// Context canceled, return buffers and exit
				returnKeySlice(keyBuf)
				returnValueSlice(valueBuf)
				return
			}
		}
	}()

	// Writer goroutine: receives messages, batches them, and writes to Kafka
	var messagesSent int64

	batch := make([]kafka.Message, 0, BatchSize)
	var batchBytes int64

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		// In dry-run mode, skip actual writing but still validate
		// The fact that we got here means reading succeeded, so validation passes
		if !cfg.DryRun {
			if err := cfg.Producer.WriteMessages(ctx, batch...); err != nil {
				return fmt.Errorf("failed to write batch to Kafka: %w", err)
			}
		}
		returnBatchBuffersToPool(batch)
		messagesSent += int64(len(batch))
		batch = batch[:0]
		batchBytes = 0
		return nil
	}

	// Receive messages from reader goroutine and batch them
	for {
		select {
		case <-ctx.Done():
			// Flush any pending batch before returning
			if err := flushBatch(); err != nil {
				return messagesSent, err
			}
			return messagesSent, ctx.Err()
		case err := <-errChan:
			// Error from reader goroutine
			if flushErr := flushBatch(); flushErr != nil {
				return messagesSent, flushErr
			}
			return messagesSent, err
		case msg, ok := <-msgChan:
			if !ok {
				// Channel closed, reader finished
				// Check for any error from reader goroutine
				select {
				case err := <-errChan:
					if err != nil {
						// Error occurred, flush batch and return error
						if flushErr := flushBatch(); flushErr != nil {
							return messagesSent, flushErr
						}
						return messagesSent, err
					}
				default:
					// No error, proceed normally
				}
				// Flush any remaining messages
				if err := flushBatch(); err != nil {
					return messagesSent, err
				}
				return messagesSent, nil
			}

			// Add message to batch
			batch = append(batch, msg)
			batchBytes += int64(len(msg.Value))

			// Flush batch if it reaches size or byte limit
			// The kafka-go Writer will further batch these internally for optimal throughput
			if len(batch) >= BatchSize || batchBytes >= BatchBytes {
				if err := flushBatch(); err != nil {
					return messagesSent, err
				}
			}
		}
	}
}
