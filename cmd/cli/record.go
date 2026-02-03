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
			&cli.BoolFlag{
				Name:  "from-beginning",
				Usage: "Start reading from the beginning of the topic",
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
			fromBeginning := cmd.Bool("from-beginning")
			limit := cmd.Int("limit")

			fmt.Printf("Recording messages from topic '%s' on brokers %v\n", topic, brokers)
			fmt.Printf("Consumer group: %s\n", groupID)
			fmt.Printf("Output file: %s\n", output)
			if fromBeginning {
				fmt.Println("Starting from beginning of topic")
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
			read, messageCount, err := pkg.Record(ctx, consumer, fromBeginning, writer, limit)
			if err != nil {
				return err
			}
			fmt.Printf("Recorded %d messages (%d bytes)\n", messageCount, read)
			return nil
		},
	}
}
