package pkg

import (
	"context"
	"fmt"

	"github.com/lolocompany/kafka-replay/pkg/kafka/admin"
)

// ConsumerGroupOutput represents a consumer group in the list output
type ConsumerGroupOutput struct {
	GroupID      string                   `json:"groupId"`
	State        string                   `json:"state,omitempty"`
	ProtocolType string                   `json:"protocolType,omitempty"`
	Members      []ConsumerGroupMember    `json:"members,omitempty"`
	Offsets      []ConsumerGroupOffset    `json:"offsets,omitempty"`
}

// ConsumerGroupMember represents a consumer group member
type ConsumerGroupMember struct {
	MemberID          string            `json:"memberId"`
	ClientID          string            `json:"clientId"`
	ClientHost        string            `json:"clientHost"`
	AssignedPartitions map[string][]int `json:"assignedPartitions,omitempty"`
}

// ConsumerGroupOffset represents offset information for a partition
type ConsumerGroupOffset struct {
	Topic     string `json:"topic"`
	Partition int    `json:"partition"`
	Offset    int64  `json:"offset"`
	Metadata  string `json:"metadata"`
}

// ListConsumerGroups lists all consumer groups
func ListConsumerGroups(ctx context.Context, brokers []string, includeOffsets bool, includeMembers bool) ([]ConsumerGroupOutput, error) {
	if len(brokers) == 0 {
		return nil, fmt.Errorf("at least one broker address is required")
	}

	// List all consumer groups
	groups, err := admin.ListConsumerGroups(ctx, brokers)
	if err != nil {
		return nil, fmt.Errorf("failed to list consumer groups: %w", err)
	}

	// For each group, always fetch base information (State, ProtocolType)
	// and optionally include offsets and members if requested
	result := make([]ConsumerGroupOutput, 0, len(groups))
	for _, g := range groups {
		// Always describe each group to get base information (State, ProtocolType)
		info, err := admin.DescribeConsumerGroup(ctx, brokers, g, includeOffsets, includeMembers)
		if err != nil {
			// If we can't describe a group, still include it but without details
			result = append(result, ConsumerGroupOutput{
				GroupID: g,
			})
			continue
		}

		output := ConsumerGroupOutput{
			GroupID:      info.GroupID,
			State:        info.State,
			ProtocolType: info.ProtocolType,
		}

		if includeMembers {
			members := make([]ConsumerGroupMember, 0, len(info.Members))
			for _, member := range info.Members {
				members = append(members, ConsumerGroupMember{
					MemberID:          member.MemberID,
					ClientID:          member.ClientID,
					ClientHost:        member.ClientHost,
					AssignedPartitions: member.AssignedTopics,
				})
			}
			output.Members = members
		}

		if includeOffsets {
			offsets := make([]ConsumerGroupOffset, 0, len(info.Offsets))
			for _, offset := range info.Offsets {
				offsets = append(offsets, ConsumerGroupOffset{
					Topic:     offset.Topic,
					Partition: offset.Partition,
					Offset:    offset.Offset,
					Metadata:  offset.Metadata,
				})
			}
			output.Offsets = offsets
		}

		result = append(result, output)
	}
	return result, nil
}
