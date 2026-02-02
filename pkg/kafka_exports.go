package pkg

import (
	"context"

	kafkapkg "github.com/lolocompany/kafka-replay/pkg/kafka"
)

// NewKafkaConsumer creates a new Kafka consumer
func NewKafkaConsumer(ctx context.Context, brokers []string, topic string, partition int) (*kafkapkg.Consumer, error) {
	return kafkapkg.NewKafkaConsumer(ctx, brokers, topic, partition)
}

// NewProducer creates a new Kafka producer
func NewProducer(brokers []string, topic string, allowAutoTopicCreation bool) *kafkapkg.Producer {
	return kafkapkg.NewProducer(brokers, topic, allowAutoTopicCreation)
}
