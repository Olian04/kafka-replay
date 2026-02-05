package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"

	"github.com/lolocompany/kafka-replay/cmd/kafka-replay/util"
	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/urfave/cli/v3"
)

func InfoCommand() *cli.Command {
	return &cli.Command{
		Name:        "info",
		Usage:       "Display information about Kafka brokers and topics",
		Description: "Collect and display information about Kafka brokers, topics, partitions, and their relationships.",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:    "broker",
				Aliases: []string{"b"},
				Usage:   "Kafka broker address(es) (can be specified multiple times). Defaults to KAFKA_BROKERS env var if not provided.",
			},
			&cli.BoolFlag{
				Name:    "json",
				Aliases: []string{"j"},
				Usage:   "Output information in JSON format",
				Value:   false,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers, err := util.ResolveBrokers(cmd.StringSlice("broker"))
			if err != nil {
				return err
			}
			outputJSON := cmd.Bool("json")

			clusterInfo, err := pkg.CollectInfo(ctx, pkg.InfoConfig{
				Brokers: brokers,
			})
			if err != nil {
				return err
			}

			if outputJSON {
				return outputJSONFormat(clusterInfo)
			}
			return outputHumanReadable(clusterInfo)
		},
	}
}

func outputJSONFormat(clusterInfo *pkg.ClusterInfo) error {
	jsonBytes, err := json.MarshalIndent(clusterInfo, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}
	_, err = fmt.Fprintf(os.Stdout, "%s\n", jsonBytes)
	return err
}

func outputHumanReadable(clusterInfo *pkg.ClusterInfo) error {
	// Output brokers
	fmt.Fprintf(os.Stdout, "Brokers:\n")
	for _, broker := range clusterInfo.Brokers {
		leaderMark := ""
		if broker.IsLeader {
			leaderMark = " (leader)"
		}
		fmt.Fprintf(os.Stdout, "  Broker %d: %s%s\n", broker.ID, broker.Address, leaderMark)
	}
	fmt.Fprintf(os.Stdout, "\n")

	// Output topics
	if len(clusterInfo.Topics) == 0 {
		fmt.Fprintf(os.Stdout, "No topics found.\n")
		return nil
	}

	fmt.Fprintf(os.Stdout, "Topics:\n")
	
	// Sort topic names for consistent output
	topicNames := make([]string, 0, len(clusterInfo.Topics))
	for name := range clusterInfo.Topics {
		topicNames = append(topicNames, name)
	}
	sort.Strings(topicNames)

	for _, topicName := range topicNames {
		topic := clusterInfo.Topics[topicName]
		fmt.Fprintf(os.Stdout, "  Topic: %s\n", topicName)
		
		// Sort partition IDs for consistent output
		partitionIDs := make([]int, 0, len(topic.Partitions))
		for id := range topic.Partitions {
			partitionIDs = append(partitionIDs, id)
		}
		sort.Ints(partitionIDs)

		for _, partitionID := range partitionIDs {
			partition := topic.Partitions[partitionID]
			replicasStr := intSliceToString(partition.Replicas)
			inSyncReplicasStr := intSliceToString(partition.InSyncReplicas)
			
			fmt.Fprintf(os.Stdout, "    Partition %d:\n", partitionID)
			fmt.Fprintf(os.Stdout, "      Leader: Broker %d\n", partition.Leader)
			fmt.Fprintf(os.Stdout, "      Replicas: %s\n", replicasStr)
			if len(partition.InSyncReplicas) > 0 && len(partition.InSyncReplicas) != len(partition.Replicas) {
				fmt.Fprintf(os.Stdout, "      In-Sync Replicas: %s\n", inSyncReplicasStr)
			}
		}
		fmt.Fprintf(os.Stdout, "\n")
	}

	// Output broker-to-topic mapping
	fmt.Fprintf(os.Stdout, "Broker-to-Topic Mapping:\n")
	brokerTopics := make(map[int][]string)
	for topicName, topic := range clusterInfo.Topics {
		for _, partition := range topic.Partitions {
			// Add topic to leader broker
			if _, exists := brokerTopics[partition.Leader]; !exists {
				brokerTopics[partition.Leader] = make([]string, 0)
			}
			// Check if topic already added for this broker
			found := false
			for _, existingTopic := range brokerTopics[partition.Leader] {
				if existingTopic == topicName {
					found = true
					break
				}
			}
			if !found {
				brokerTopics[partition.Leader] = append(brokerTopics[partition.Leader], topicName)
			}

			// Add topic to replica brokers
			for _, replicaID := range partition.Replicas {
				if _, exists := brokerTopics[replicaID]; !exists {
					brokerTopics[replicaID] = make([]string, 0)
				}
				found = false
				for _, existingTopic := range brokerTopics[replicaID] {
					if existingTopic == topicName {
						found = true
						break
					}
				}
				if !found {
					brokerTopics[replicaID] = append(brokerTopics[replicaID], topicName)
				}
			}
		}
	}

	// Sort broker IDs
	brokerIDs := make([]int, 0, len(brokerTopics))
	for id := range brokerTopics {
		brokerIDs = append(brokerIDs, id)
	}
	sort.Ints(brokerIDs)

	for _, brokerID := range brokerIDs {
		topics := brokerTopics[brokerID]
		sort.Strings(topics)
		fmt.Fprintf(os.Stdout, "  Broker %d: %s\n", brokerID, strings.Join(topics, ", "))
	}

	return nil
}

func intSliceToString(ints []int) string {
	if len(ints) == 0 {
		return "[]"
	}
	strs := make([]string, len(ints))
	for i, v := range ints {
		strs[i] = fmt.Sprintf("%d", v)
	}
	return "[" + strings.Join(strs, ", ") + "]"
}
