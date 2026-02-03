package kafka

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	conn  *kafka.Conn
	batch *kafka.Batch
	mu    sync.Mutex
}

func (c *Consumer) Consume(ctx context.Context, fromBeginning bool) (io.ReadCloser, error) {
	// Note: topic and groupID are accepted for API compatibility but not used here
	// since the connection is already established for a specific topic in NewKafkaConsumer

	// Set offset if we want to read from the beginning
	if fromBeginning {
		firstOffset, _, err := c.conn.ReadOffsets()
		if err != nil {
			return nil, err
		}
		if _, err := c.conn.Seek(firstOffset, kafka.SeekStart); err != nil {
			return nil, err
		}
	}

	// Create a message reader that reads continuously
	return &messageReader{
		conn: c.conn,
		ctx:  ctx,
	}, nil
}

// SetOffsetFromBeginning sets the offset to the beginning of the partition
func (c *Consumer) SetOffsetFromBeginning() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	firstOffset, _, err := c.conn.ReadOffsets()
	if err != nil {
		return err
	}
	_, err = c.conn.Seek(firstOffset, kafka.SeekStart)
	return err
}

// ReadNextMessage reads the next complete message from Kafka
// Returns the message value bytes, or an error if no message is available or context is canceled
func (c *Consumer) ReadNextMessage(ctx context.Context) ([]byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If we don't have a batch or it's exhausted, read a new one
	if c.batch == nil {
		// Set up batch reading with context cancellation support
		batchChan := make(chan *kafka.Batch, 1)
		errChan := make(chan error, 1)

		go func() {
			batch := c.conn.ReadBatch(1, 10*1024*1024) // minBytes=1, maxBytes=10MB
			select {
			case <-ctx.Done():
				batch.Close()
				errChan <- ctx.Err()
			case batchChan <- batch:
			}
		}()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case err := <-errChan:
			return nil, err
		case b := <-batchChan:
			c.batch = b
		}
	}

	// Read the next message from the batch
	msg, err := c.batch.ReadMessage()
	if err != nil {
		// If batch is exhausted, close it and return EOF
		// The next call will read a new batch
		if err == io.EOF {
			c.batch.Close()
			c.batch = nil
		}
		return nil, err
	}

	// Return a copy of the message value
	value := make([]byte, len(msg.Value))
	copy(value, msg.Value)
	return value, nil
}

// messageReader is a custom io.Reader that reads Kafka messages continuously
type messageReader struct {
	conn  *kafka.Conn
	ctx   context.Context
	batch *kafka.Batch
	buf   []byte // buffer for current message that wasn't fully read
	mu    sync.Mutex
}

func (r *messageReader) Read(p []byte) (n int, err error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Check if context is canceled
	select {
	case <-r.ctx.Done():
		return 0, r.ctx.Err()
	default:
	}

	// First, try to read from buffer if we have leftover data
	if len(r.buf) > 0 {
		copied := copy(p, r.buf)
		r.buf = r.buf[copied:]
		return copied, nil
	}

	// If we don't have a batch, read a new one
	if r.batch == nil {
		// ReadBatch blocks until messages are available, so we need to handle context cancellation
		// by running it in a goroutine and using a channel
		batchChan := make(chan *kafka.Batch, 1)
		errChan := make(chan error, 1)

		go func() {
			// ReadBatch blocks until at least minBytes are available or maxBytes is reached
			// minBytes=1 means read at least 1 byte, maxBytes=10MB for reasonable batch size
			batch := r.conn.ReadBatch(1, 10*1024*1024)
			select {
			case <-r.ctx.Done():
				batch.Close()
				errChan <- r.ctx.Err()
			case batchChan <- batch:
			}
		}()

		select {
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		case err := <-errChan:
			return 0, err
		case batch := <-batchChan:
			r.batch = batch
		}
	}

	// Read the next message from the batch
	msg, err := r.batch.ReadMessage()
	if err != nil {
		// If we got an error, close the batch
		r.batch.Close()
		r.batch = nil

		// Check if it's EOF (end of batch) or context cancellation
		if err == io.EOF {
			// End of batch, try reading a new batch on next call
			// But first check if we have buffered data
			if len(r.buf) > 0 {
				copied := copy(p, r.buf)
				r.buf = r.buf[copied:]
				return copied, nil
			}
			return 0, nil
		}
		if r.ctx.Err() != nil {
			return 0, r.ctx.Err()
		}
		return 0, err
	}

	// Copy message value to the buffer
	copied := copy(p, msg.Value)

	// If we couldn't copy everything, store the remainder in buffer
	// Make sure to copy the data, not just create a slice reference
	if copied < len(msg.Value) {
		// Ensure we have enough capacity, then copy the remainder
		remainder := msg.Value[copied:]
		if cap(r.buf) < len(remainder) {
			r.buf = make([]byte, len(remainder))
		} else {
			r.buf = r.buf[:len(remainder)]
		}
		copy(r.buf, remainder)
	}

	return copied, nil
}

func (r *messageReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.batch != nil {
		r.batch.Close()
		r.batch = nil
	}
	return nil // Don't close the connection here, let the caller manage it
}

func NewKafkaConsumer(ctx context.Context, brokers []string, topic string, partition int) (*Consumer, error) {
	// DialLeader expects a single broker address - it will discover the leader from metadata
	// Try each broker until one works
	var conn *kafka.Conn
	var err error
	for _, broker := range brokers {
		conn, err = kafka.DialLeader(ctx, "tcp", broker, topic, partition)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to any broker: %w", err)
	}

	return &Consumer{
		conn: conn,
	}, nil
}
