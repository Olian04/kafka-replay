package kafka

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	kafkago "github.com/segmentio/kafka-go"
)

// Consumer wraps either a kafka.Reader (for consumer groups) or a kafka.Conn (for direct partition reads).
// When a groupID is provided, it uses kafka.Reader which automatically manages consumer group membership
// and offset commits. When no groupID is provided, it uses kafka.DialLeader for direct partition access.
type Consumer struct {
	// reader is used when groupID is provided (consumer group mode)
	reader *kafkago.Reader
	// conn and batch are used when no groupID is provided (direct partition mode)
	conn  *kafkago.Conn
	batch *kafkago.Batch
	mu    sync.Mutex
	// usingGroup indicates whether we're using consumer group mode
	usingGroup bool
}

// SetOffset sets the offset to a specific value.
// Note: This only works in direct partition mode (no consumer group).
// When using consumer groups, offsets are managed automatically by Kafka.
func (c *Consumer) SetOffset(offset int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.usingGroup {
		return fmt.Errorf("SetOffset is not supported when using consumer groups; offsets are managed automatically")
	}

	switch offset {
	case -1:
		// -1 means high watermark
		_, err := c.conn.Seek(0, kafkago.SeekEnd)
		return err
	case 0:
		// 0 means low watermark
		_, err := c.conn.Seek(0, kafkago.SeekStart)
		return err
	default:
		_, err := c.conn.Seek(offset, kafkago.SeekAbsolute)
		return err
	}
}

func (c *Consumer) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.usingGroup {
		if c.reader != nil {
			if err := c.reader.Close(); err != nil {
				return fmt.Errorf("failed to close reader: %w", err)
			}
		}
		return nil
	}

	if c.conn != nil {
		if err := c.conn.Close(); err != nil {
			return fmt.Errorf("failed to close consumer: %w", err)
		}
	}
	return nil
}

// ReadNextMessage reads the next complete message from Kafka
// Returns the message timestamp, key, value, and error
func (c *Consumer) ReadNextMessage(ctx context.Context) (time.Time, []byte, []byte, error) {
	if c.usingGroup {
		// Use Reader for consumer group mode
		msg, err := c.reader.ReadMessage(ctx)
		if err != nil {
			return time.Time{}, nil, nil, err
		}

		// Return the message timestamp, key, and value
		var key []byte
		if len(msg.Key) > 0 {
			key = make([]byte, len(msg.Key))
			copy(key, msg.Key)
		}
		value := make([]byte, len(msg.Value))
		copy(value, msg.Value)
		return msg.Time, key, value, nil
	}

	// Use direct partition mode (Conn + Batch)
	c.mu.Lock()
	defer c.mu.Unlock()

	// If we don't have a batch or it's exhausted, read a new one
	if c.batch == nil {
		// Set up batch reading with context cancellation support
		batchChan := make(chan *kafkago.Batch, 1)
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
			return time.Time{}, nil, nil, ctx.Err()
		case err := <-errChan:
			return time.Time{}, nil, nil, err
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
		return time.Time{}, nil, nil, err
	}

	// Return the message timestamp, key, and value
	var key []byte
	if len(msg.Key) > 0 {
		key = make([]byte, len(msg.Key))
		copy(key, msg.Key)
	}
	value := make([]byte, len(msg.Value))
	copy(value, msg.Value)
	return msg.Time, key, value, nil
}

// NewConsumer creates a new Consumer. If groupID is provided and non-empty, it uses
// kafka.Reader with consumer group support. Otherwise, it uses kafka.DialLeader for
// direct partition access.
func NewConsumer(ctx context.Context, brokers []string, topic string, partition int, groupID string) (*Consumer, error) {
	if groupID != "" {
		// Use kafka.Reader for consumer group mode
		readerConfig := kafkago.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			GroupID:  groupID,
			MinBytes: 1,
			MaxBytes: 10 * 1024 * 1024, // 10MB
		}

		// If partition is specified (>= 0), set it; otherwise Reader will handle partition assignment
		if partition >= 0 {
			readerConfig.Partition = partition
		}

		reader := kafkago.NewReader(readerConfig)

		return &Consumer{
			reader:     reader,
			usingGroup: true,
		}, nil
	}

	// Use direct partition mode (kafka.DialLeader)
	// DialLeader expects a single broker address - it will discover the leader from metadata
	// Try each broker until one works
	var conn *kafkago.Conn
	var err error
	for _, broker := range brokers {
		conn, err = kafkago.DialLeader(ctx, "tcp", broker, topic, partition)
		if err == nil {
			break
		}
	}
	if err != nil {
		// The error may show a different broker address than the one provided because
		// Kafka returns the leader broker's advertised address in metadata, and the
		// client tries to connect to that address. If the advertised address is
		// unreachable, the error will show the leader's address, not the initial broker.
		return nil, fmt.Errorf("failed to connect to any broker (tried: %v). Note: Kafka may return a different broker address (leader) in metadata that must also be reachable: %w", brokers, err)
	}

	return &Consumer{
		conn:       conn,
		usingGroup: false,
	}, nil
}
