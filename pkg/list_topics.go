package pkg

import (
	"context"
	"sort"

	"github.com/lolocompany/kafka-replay/pkg/kafka"
)

// TopicOutput represents a topic in the list output
type TopicOutput struct {
	Name              string `json:"name"`
	PartitionCount    int    `json:"partitionCount"`
	ReplicationFactor int    `json:"replicationFactor"`
}

// ListTopics lists all topics with partition count and replication factor
func ListTopics(ctx context.Context, brokers []string) ([]TopicOutput, error) {
	conn, err := kafka.ConnectToAnyBroker(ctx, brokers)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	partitions, err := kafka.ReadAllPartitions(conn)
	if err != nil {
		return nil, err
	}

	// Aggregate by topic name
	type topicInfo struct {
		count    int
		replFact int
	}
	byTopic := make(map[string]topicInfo)
	for _, p := range partitions {
		info := byTopic[p.Topic]
		info.count++
		if info.replFact == 0 && len(p.Replicas) > 0 {
			info.replFact = len(p.Replicas)
		}
		byTopic[p.Topic] = info
	}

	names := make([]string, 0, len(byTopic))
	for name := range byTopic {
		names = append(names, name)
	}
	sort.Strings(names)

	result := make([]TopicOutput, 0, len(names))
	for _, name := range names {
		info := byTopic[name]
		result = append(result, TopicOutput{
			Name:              name,
			PartitionCount:    info.count,
			ReplicationFactor: info.replFact,
		})
	}
	return result, nil
}
