package pkg

import (
	"context"
	"sort"

	"github.com/lolocompany/kafka-replay/pkg/kafka"
)

// BrokerOutput represents a broker in the list output
type BrokerOutput struct {
	ID        int    `json:"id"`
	Address   string `json:"address"`
	Reachable bool   `json:"reachable"`
	Rack      string `json:"rack,omitempty"`
}

// ListBrokers lists all brokers with their reachability status
func ListBrokers(ctx context.Context, brokers []string) ([]BrokerOutput, error) {
	conn, err := kafka.ConnectToAnyBroker(ctx, brokers)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	brokerList, err := kafka.GetBrokerList(conn)
	if err != nil {
		return nil, err
	}

	// Sort broker IDs for consistent output
	brokerIDs := make([]int, 0, len(brokerList))
	for _, broker := range brokerList {
		brokerIDs = append(brokerIDs, broker.ID)
	}
	sort.Ints(brokerIDs)

	result := make([]BrokerOutput, 0, len(brokerIDs))
	for _, id := range brokerIDs {
		// Find broker by ID
		var broker kafka.Broker
		for _, b := range brokerList {
			if b.ID == id {
				broker = b
				break
			}
		}

		brokerAddress := broker.Address
		reachable := kafka.IsBrokerReachable(ctx, brokerAddress)

		output := BrokerOutput{
			ID:        broker.ID,
			Address:   brokerAddress,
			Reachable: reachable,
			// Rack not provided by current kafka.Broker; leave empty
		}
		result = append(result, output)
	}

	return result, nil
}
