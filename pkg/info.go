package pkg

import (
	"context"
	"fmt"
	"sort"

	"github.com/segmentio/kafka-go"
)

// BrokerInfo contains information about a Kafka broker
type BrokerInfo struct {
	ID      int    `json:"id"`
	Address string `json:"address"`
	IsLeader bool  `json:"is_leader,omitempty"`
}

// PartitionInfo contains information about a partition
type PartitionInfo struct {
	ID             int   `json:"id"`
	Leader         int   `json:"leader"`
	Replicas       []int `json:"replicas"`
	InSyncReplicas []int `json:"in_sync_replicas,omitempty"`
}

// TopicInfo contains information about a topic
type TopicInfo struct {
	Name       string                  `json:"name"`
	Partitions map[int]*PartitionInfo `json:"partitions"`
}

// ClusterInfo contains all information about the Kafka cluster
type ClusterInfo struct {
	Brokers []*BrokerInfo           `json:"brokers"`
	Topics  map[string]*TopicInfo   `json:"topics"`
}

// InfoConfig contains configuration for collecting cluster information
type InfoConfig struct {
	Brokers []string
}

// CollectInfo collects information about the Kafka cluster
func CollectInfo(ctx context.Context, cfg InfoConfig) (*ClusterInfo, error) {
	if len(cfg.Brokers) == 0 {
		return nil, fmt.Errorf("at least one broker address is required")
	}

	// Connect to any broker to get metadata
	var conn *kafka.Conn
	var err error
	for _, broker := range cfg.Brokers {
		conn, err = kafka.DialContext(ctx, "tcp", broker)
		if err == nil {
			break
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to connect to any broker (tried: %v): %w", cfg.Brokers, err)
	}
	defer conn.Close()

	// Get broker list
	brokers, err := conn.Brokers()
	if err != nil {
		return nil, fmt.Errorf("failed to get broker list: %w", err)
	}

	// Get partitions for all topics (empty slice means all topics)
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return nil, fmt.Errorf("failed to get partitions: %w", err)
	}

	// Build broker info map
	brokerMap := make(map[int]*BrokerInfo)
	for _, broker := range brokers {
		brokerMap[broker.ID] = &BrokerInfo{
			ID:      broker.ID,
			Address: broker.Host + ":" + fmt.Sprintf("%d", broker.Port),
		}
	}

	// Build cluster info
	clusterInfo := &ClusterInfo{
		Brokers: make([]*BrokerInfo, 0, len(brokerMap)),
		Topics:  make(map[string]*TopicInfo),
	}

	// Add brokers to list (sorted by ID)
	brokerIDs := make([]int, 0, len(brokerMap))
	for id := range brokerMap {
		brokerIDs = append(brokerIDs, id)
	}
	sort.Ints(brokerIDs)
	for _, id := range brokerIDs {
		clusterInfo.Brokers = append(clusterInfo.Brokers, brokerMap[id])
	}

	// Process topics and partitions
	leaderBrokerIDs := make(map[int]bool)
	for _, partition := range partitions {
		// Get or create topic info
		topicInfo, exists := clusterInfo.Topics[partition.Topic]
		if !exists {
			topicInfo = &TopicInfo{
				Name:       partition.Topic,
				Partitions: make(map[int]*PartitionInfo),
			}
			clusterInfo.Topics[partition.Topic] = topicInfo
		}

		// Create partition info
		partitionInfo := &PartitionInfo{
			ID:             partition.ID,
			Leader:         partition.Leader.ID,
			Replicas:       make([]int, 0, len(partition.Replicas)),
			InSyncReplicas: make([]int, 0, len(partition.Isr)),
		}

		// Track which brokers are leaders
		leaderBrokerIDs[partition.Leader.ID] = true

		// Add replicas
		for _, replica := range partition.Replicas {
			partitionInfo.Replicas = append(partitionInfo.Replicas, replica.ID)
		}
		sort.Ints(partitionInfo.Replicas)

		// Add In-Sync Replicas
		for _, isr := range partition.Isr {
			partitionInfo.InSyncReplicas = append(partitionInfo.InSyncReplicas, isr.ID)
		}
		sort.Ints(partitionInfo.InSyncReplicas)

		topicInfo.Partitions[partition.ID] = partitionInfo
	}

	// Mark brokers that are leaders
	for _, broker := range clusterInfo.Brokers {
		if leaderBrokerIDs[broker.ID] {
			broker.IsLeader = true
		}
	}

	return clusterInfo, nil
}
