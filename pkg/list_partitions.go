package pkg

import (
	"context"
	"sort"

	"github.com/lolocompany/kafka-replay/pkg/kafka"
)

// PartitionOutput represents a partition in the list output
type PartitionOutput struct {
	Topic          string   `json:"topic"`
	Partition      int      `json:"partition"`
	Leader         string   `json:"leader"`
	Followers      []string `json:"followers,omitempty"`
	EarliestOffset *int64   `json:"earliestOffset,omitempty"`
	LatestOffset   *int64   `json:"latestOffset,omitempty"`
	Replicas       []string `json:"replicas,omitempty"`
	InSyncReplicas []string `json:"inSyncReplicas,omitempty"`
}

// ListPartitions lists all partitions with optional offsets and replicas
func ListPartitions(ctx context.Context, brokers []string, includeOffsets bool, includeReplicas bool) ([]PartitionOutput, error) {
	conn, err := kafka.ConnectToAnyBroker(ctx, brokers)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// Get broker map for replica address lookup
	brokerList, err := kafka.GetBrokerList(conn)
	if err != nil {
		return nil, err
	}
	brokerMap := kafka.CreateBrokerIDToAddressMap(brokerList)

	partitions, err := kafka.ReadAllPartitions(conn)
	if err != nil {
		return nil, err
	}

	// Sort partitions by topic name and partition ID for consistent output
	sort.Slice(partitions, func(i, j int) bool {
		if partitions[i].Topic != partitions[j].Topic {
			return partitions[i].Topic < partitions[j].Topic
		}
		return partitions[i].ID < partitions[j].ID
	})

	result := make([]PartitionOutput, 0, len(partitions))
	for _, partition := range partitions {
		// Followers: all replicas except the leader
		followers := make([]string, 0)
		for _, replica := range partition.Replicas {
			if replica.ID != partition.Leader.ID {
				if addr, ok := brokerMap[replica.ID]; ok {
					followers = append(followers, addr)
				}
			}
		}

		output := PartitionOutput{
			Topic:     partition.Topic,
			Partition: partition.ID,
			Leader:    partition.Leader.Address,
			Followers: followers,
		}

		if includeOffsets {
			// Get offsets for this partition
			leaderConn, err := kafka.DialLeader(ctx, "tcp", partition.Leader.Address, partition.Topic, partition.ID)
			if err == nil {
				firstOffset, lastOffset, err := leaderConn.ReadOffsets()
				leaderConn.Close()
				if err == nil {
					output.EarliestOffset = &firstOffset
					output.LatestOffset = &lastOffset
				}
			}
		}

		if includeReplicas {
			// Convert replica brokers to addresses
			replicaAddresses := make([]string, 0, len(partition.Replicas))
			for _, replica := range partition.Replicas {
				if addr, ok := brokerMap[replica.ID]; ok {
					replicaAddresses = append(replicaAddresses, addr)
				}
			}

			// Convert ISR brokers to addresses
			inSyncReplicaAddresses := make([]string, 0, len(partition.Isr))
			for _, isr := range partition.Isr {
				if addr, ok := brokerMap[isr.ID]; ok {
					inSyncReplicaAddresses = append(inSyncReplicaAddresses, addr)
				}
			}

			output.Replicas = replicaAddresses
			output.InSyncReplicas = inSyncReplicaAddresses
		}

		result = append(result, output)
	}

	return result, nil
}
