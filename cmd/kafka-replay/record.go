package main

import (
	"context"
	"fmt"
	"os"

	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/urfave/cli/v3"
)

func recordCommand() *cli.Command {
	return &cli.Command{
		Name:        "record",
		Usage:       "Record messages from a Kafka topic",
		Description: "Record messages from a Kafka topic and save them to a file or output location.",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:    "broker",
				Aliases: []string{"b"},
				Usage:   "Kafka broker address(es) (can be specified multiple times). Defaults to KAFKA_BROKERS env var if not provided.",
			},
			&cli.StringFlag{
				Name:     "topic",
				Aliases:  []string{"t"},
				Usage:    "Kafka topic to record messages from",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "group-id",
				Aliases: []string{"g"},
				Usage:   "Consumer group ID",
				Value:   "kafka-replay-record",
			},
			&cli.IntFlag{
				Name:    "partition",
				Aliases: []string{"p"},
				Usage:   "Kafka partition to record messages from",
				Value:   0,
			},
			&cli.StringFlag{
				Name:    "output",
				Aliases: []string{"o"},
				Usage:   "Output file path for recorded messages",
				Value:   "messages.log",
			},
			&cli.Int64Flag{
				Name:    "offset",
				Aliases: []string{"O"},
				Usage:   "Start reading from a specific offset (-1 to use current position, 0 to start from beginning)",
				Value:   -1,
			},
			&cli.IntFlag{
				Name:    "limit",
				Aliases: []string{"l"},
				Usage:   "Maximum number of messages to record (0 for unlimited)",
				Value:   0,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers, err := resolveBrokers(cmd.StringSlice("broker"))
			if err != nil {
				return err
			}
			topic := cmd.String("topic")
			groupID := cmd.String("group-id")
			partition := cmd.Int("partition")
			output := cmd.String("output")
			offsetFlag := cmd.Int64("offset")
			limit := cmd.Int("limit")

			// Determine the offset to use
			// If --offset is explicitly set (>= 0), use it
			// Otherwise, use nil (start from current position)
			var offset *int64
			if offsetFlag >= 0 {
				offset = &offsetFlag
			}

			fmt.Printf("Recording messages from topic '%s' on brokers %v\n", topic, brokers)
			fmt.Printf("Consumer group: %s\n", groupID)
			fmt.Printf("Output file: %s\n", output)
			if offset != nil {
				fmt.Printf("Starting from offset: %d\n", *offset)
			} else {
				fmt.Println("Starting from current position")
			}
			if limit > 0 {
				fmt.Printf("Message limit: %d\n", limit)
			}
			consumer, err := pkg.NewKafkaConsumer(ctx, brokers, topic, partition)
			if err != nil {
				return err
			}
			writer, err := os.Create(output)
			if err != nil {
				return err
			}
			defer writer.Close()

			// Create progress reporter
			progressReporter := NewRecordProgressReporter(limit)

			read, messageCount, err := pkg.Record(ctx, pkg.RecordConfig{
				Consumer:        consumer,
				Offset:          offset,
				Output:          writer,
				Limit:           limit,
				TimeProvider:    pkg.RealTimeProvider{},
				ProgressReporter: progressReporter,
			})
			if err != nil {
				return err
			}
			fmt.Printf("Recorded %d messages (%d bytes)\n", messageCount, read)
			return nil
		},
	}
}
