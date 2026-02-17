package pkg

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"sync"
	"time"

	kafkapkg "github.com/lolocompany/kafka-replay/v2/pkg/kafka"
	"github.com/lolocompany/kafka-replay/v2/pkg/transcoder"
	"github.com/segmentio/kafka-go"
)

const (
	// EnvKeyPoolBufBytes configures the default capacity of buffers in the key pool.
	EnvKeyPoolBufBytes = "KAFKA_REPLAY_KEY_POOL_BUFFER_BYTES"
	// EnvValuePoolBufBytes configures the default capacity of buffers in the value pool.
	EnvValuePoolBufBytes = "KAFKA_REPLAY_VALUE_POOL_BUFFER_BYTES"
)

func envPoolCapBytes(name string, def int) int {
	v, ok := os.LookupEnv(name)
	if !ok || v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}

var (
	keyPoolDefaultCapBytes   = envPoolCapBytes(EnvKeyPoolBufBytes, 4*1024)    // 4KB
	valuePoolDefaultCapBytes = envPoolCapBytes(EnvValuePoolBufBytes, 64*1024) // 64KB
)

// keyBufPool holds []byte buffers for Kafka message keys.
var keyBufPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, keyPoolDefaultCapBytes)
	},
}

// valueBufPool holds []byte buffers for Kafka message values.
var valueBufPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, valuePoolDefaultCapBytes)
	},
}

const (
	// BatchSize is the number of messages to batch before writing to Kafka
	// Matches kafka-go Writer's BatchSize (10000) to maximize throughput
	BatchSize = 10000
	// BatchBytes is the maximum bytes to batch before writing
	// Matches kafka-go Writer's BatchBytes (50MB) to maximize throughput
	BatchBytes = 50 * 1024 * 1024 // 50MB
)

// ReplayConfig holds configuration for the Replay function
type ReplayConfig struct {
	Producer  *kafkapkg.Producer
	Decoder   *transcoder.DecodeReader
	Rate      int
	Loop      bool
	Partition *int // Optional partition to write to (nil for auto-assignment)
	LogWriter io.Writer
	DryRun    bool   // If true, validate messages without actually sending to Kafka
	FindBytes []byte // Optional byte sequence to search for in messages
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

	// Channel to pass messages from reader to writer goroutine
	// Buffered to allow some pipelining while maintaining backpressure
	msgChan := make(chan kafka.Message, BatchSize)
	
	// Channel to signal completion and pass errors
	errChan := make(chan error, 1)

	// Reader goroutine: reads from decoder and sends messages to channel
	go func() {
		defer close(msgChan)
		
		for {
			// Check context cancellation
			select {
			case <-ctx.Done():
				return
			default:
			}

			// Read next complete message into per-message pooled buffers.
			// DecodeReader is no-grow: if these buffers are too small, it returns ErrBufferTooSmall.
			keyBuf := getKeySlice()
			dataBuf := getValueSlice()

			timestamp, keyLen, dataLen, err := cfg.Decoder.Read(keyBuf, dataBuf)
			if err != nil {
				// We won't be using these buffers
				returnKeySlice(keyBuf)
				returnValueSlice(dataBuf)

				if err == io.EOF {
					// End of file reached
					if cfg.Loop {
						// In loop mode: reset and continue without flushing.
						// This allows batches to accumulate across loop iterations for better throughput.
						if err := cfg.Decoder.Reset(); err != nil {
							select {
							case errChan <- err:
							case <-ctx.Done():
							}
							return
						}
						continue
					}
					// No more looping, exit normally
					return
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

			// Limit buffers to the valid decoded lengths
			if keyLen > 0 {
				keyBuf = keyBuf[:keyLen]
			} else {
				// Return unused key buffer immediately
				returnKeySlice(keyBuf)
				keyBuf = nil
			}
			dataBuf = dataBuf[:dataLen]

			// Filter by find bytes if specified
			if cfg.FindBytes != nil && !bytes.Contains(dataBuf, cfg.FindBytes) {
				// Return buffers for skipped message
				returnKeySlice(keyBuf)
				returnValueSlice(dataBuf)
				continue
			}

			// Build Kafka message with pooled buffers (returned to pool after flush)
			kafkaMsg := kafka.Message{
				Key:   keyBuf,
				Value: dataBuf,
				Time:  timestamp,
			}
			// Set partition if specified in config (nil means auto-assignment)
			if cfg.Partition != nil {
				kafkaMsg.Partition = *cfg.Partition
			}

			// Send message to writer goroutine
			select {
			case msgChan <- kafkaMsg:
				// Message sent successfully
			case <-ctx.Done():
				// Context canceled, return buffers and exit
				returnKeySlice(keyBuf)
				returnValueSlice(dataBuf)
				return
			}
		}
	}()

	// Writer goroutine: receives messages, batches them, and writes to Kafka
	var messagesSent int64
	var rateStartTime time.Time
	if cfg.Rate > 0 {
		rateStartTime = time.Now()
	}

	batch := make([]kafka.Message, 0, BatchSize)
	var batchBytes int64

	flushBatch := func() error {
		if len(batch) == 0 {
			return nil
		}

		// Rate limiting: ensure we don't exceed the rate limit
		if cfg.Rate > 0 {
			batchSize := int64(len(batch))
			elapsed := time.Since(rateStartTime)

			// Calculate how many messages we should have sent by now
			expectedMessages := int64(float64(cfg.Rate) * elapsed.Seconds())

			// If we're about to exceed the rate, wait
			if messagesSent+batchSize > expectedMessages {
				// Calculate how long to wait
				// We want: messagesSent + batchSize <= rate * (elapsed + waitTime)
				// So: waitTime >= (messagesSent + batchSize) / rate - elapsed
				requiredTime := time.Duration(float64(messagesSent+batchSize) / float64(cfg.Rate) * float64(time.Second))
				waitTime := requiredTime - elapsed

				if waitTime > 0 {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-time.After(waitTime):
						// Wait complete, proceed
					}
					// Update elapsed time after waiting
					elapsed = time.Since(rateStartTime)
				}
			}
		}

		// In dry-run mode, skip actual writing but still validate
		// The fact that we got here means decoding succeeded, so validation passes
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

// getKeySlice returns a key buffer slice from the pool.
func getKeySlice() []byte {
	return keyBufPool.Get().([]byte)
}

// getValueSlice returns a value buffer slice from the pool.
func getValueSlice() []byte {
	return valueBufPool.Get().([]byte)
}

func returnKeySlice(key []byte) {
	if key == nil {
		return
	}
	// Put the slice back into the pool using the full capacity
	keyBufPool.Put(key[:cap(key)])
}

func returnValueSlice(value []byte) {
	if value == nil {
		return
	}
	// Put the slice back into the pool using the full capacity
	valueBufPool.Put(value[:cap(value)])
}

// returnBatchBuffersToPool returns Key and Value buffers from batch messages to their pools.
// Call after the producer has finished with the batch (after WriteMessages returns).
func returnBatchBuffersToPool(batch []kafka.Message) {
	for i := range batch {
		returnKeySlice(batch[i].Key)
		returnValueSlice(batch[i].Value)
	}
}
