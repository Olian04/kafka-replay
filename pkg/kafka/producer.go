package kafka

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer  *kafka.Writer
	brokers []string
	topic   string
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: topic,
		},
		brokers: brokers,
		topic:   topic,
	}
}

// CreateTopicIfNotExists creates the topic if it doesn't exist
func (p *Producer) CreateTopicIfNotExists(ctx context.Context, numPartitions int, replicationFactor int) error {
	// Connect to any broker
	conn, err := kafka.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	// Get the controller
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("failed to get controller: %w", err)
	}

	// Connect to the controller
	controllerConn, err := kafka.DialContext(ctx, "tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("failed to connect to controller: %w", err)
	}
	defer controllerConn.Close()

	// Create topic configuration
	topicConfig := kafka.TopicConfig{
		Topic:             p.topic,
		NumPartitions:     numPartitions,
		ReplicationFactor: replicationFactor,
	}

	// Create the topic
	err = controllerConn.CreateTopics(topicConfig)
	if err != nil {
		// Check if topic already exists (error code 36)
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "TopicExistsException") {
			// Topic already exists, which is fine
			return nil
		}
		return fmt.Errorf("failed to create topic: %w", err)
	}

	return nil
}

// EnsureTopicExists checks if the topic exists, and if not, returns an error
func (p *Producer) EnsureTopicExists(ctx context.Context) error {
	// Connect to any broker
	conn, err := kafka.DialContext(ctx, "tcp", p.brokers[0])
	if err != nil {
		return fmt.Errorf("failed to connect to broker: %w", err)
	}
	defer conn.Close()

	// Read partitions to check if topic exists
	partitions, err := conn.ReadPartitions(p.topic)
	if err != nil {
		return err
	}

	// Check if we found the topic
	found := false
	for _, partition := range partitions {
		if partition.Topic == p.topic {
			found = true
			break
		}
	}

	if !found {
		return fmt.Errorf("topic '%s' does not exist", p.topic)
	}

	return nil
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
