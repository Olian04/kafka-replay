package kafka

import (
	"context"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer  *kafka.Writer
	brokers []string
	topic   string
}

func NewProducer(brokers []string, topic string, allowAutoTopicCreation bool) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			AllowAutoTopicCreation: allowAutoTopicCreation,
		},
		brokers: brokers,
		topic:   topic,
	}
}

// WriteMessage writes a single message to Kafka
func (p *Producer) WriteMessage(ctx context.Context, value []byte, timestamp time.Time) error {
	msg := kafka.Message{
		Value: value,
		Time:  timestamp,
	}
	return p.writer.WriteMessages(ctx, msg)
}

// WriteMessages writes multiple messages to Kafka
func (p *Producer) WriteMessages(ctx context.Context, messages ...kafka.Message) error {
	return p.writer.WriteMessages(ctx, messages...)
}

// Close closes the underlying writer
func (p *Producer) Close() error {
	if p.writer != nil {
		return p.writer.Close()
	}
	return nil
}
