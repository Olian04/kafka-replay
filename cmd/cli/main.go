package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:        "kafka-replay",
		Usage:       "A utility tool for recording and replaying Kafka messages",
		Description: "Record messages from Kafka topics or replay previously recorded messages back to Kafka topics.",
		Commands: []*cli.Command{
			{
				Name:        "record",
				Usage:       "Record messages from a Kafka topic",
				Description: "Record messages from a Kafka topic and save them to a file or output location.",
				Flags: []cli.Flag{
					&cli.StringSliceFlag{
						Name:     "broker",
						Aliases:  []string{"b"},
						Usage:    "Kafka broker address(es) (can be specified multiple times)",
						Required: true,
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
					brokers := cmd.StringSlice("broker")
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
			},
			{
				Name:        "replay",
				Usage:       "Replay recorded messages to a Kafka topic",
				Description: "Replay previously recorded messages from a file back to a Kafka topic.",
				Flags: []cli.Flag{
					&cli.StringSliceFlag{
						Name:     "broker",
						Aliases:  []string{"b"},
						Usage:    "Kafka broker address(es) (can be specified multiple times)",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "topic",
						Aliases:  []string{"t"},
						Usage:    "Kafka topic to replay messages to",
						Required: true,
					},
					&cli.StringFlag{
						Name:     "input",
						Aliases:  []string{"i"},
						Usage:    "Input file path containing recorded messages",
						Required: true,
					},
					&cli.IntFlag{
						Name:    "rate",
						Aliases: []string{"r"},
						Usage:   "Messages per second to replay (0 for maximum speed)",
						Value:   0,
					},
					&cli.BoolFlag{
						Name:  "preserve-timestamps",
						Usage: "Preserve original message timestamps",
					},
					&cli.BoolFlag{
						Name:  "create-topic",
						Usage: "Create the topic if it doesn't exist",
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					brokers := cmd.StringSlice("broker")
					topic := cmd.String("topic")
					input := cmd.String("input")
					rate := cmd.Int("rate")
					preserveTimestamps := cmd.Bool("preserve-timestamps")
					createTopic := cmd.Bool("create-topic")

					fmt.Printf("Replaying messages to topic '%s' on brokers %v\n", topic, brokers)
					fmt.Printf("Input file: %s\n", input)
					if rate > 0 {
						fmt.Printf("Rate limit: %d messages/second\n", rate)
					} else {
						fmt.Println("Rate limit: maximum speed")
					}
					if preserveTimestamps {
						fmt.Println("Preserving original timestamps")
					}

					// Create log file reader
					reader, err := pkg.NewLogFileReader(input, preserveTimestamps)
					if err != nil {
						return err
					}
					defer reader.Close()

					// Create Kafka producer
					producer := pkg.NewProducer(brokers, topic)
					defer producer.Close()

					// Check if topic exists or create it
					if createTopic {
						fmt.Printf("Creating topic '%s' if it doesn't exist...\n", topic)
						if err := producer.CreateTopicIfNotExists(ctx, 1, 1); err != nil {
							return fmt.Errorf("failed to create topic: %w", err)
						}
						fmt.Printf("Topic '%s' is ready\n", topic)
					} else {
						// Check if topic exists
						if err := producer.EnsureTopicExists(ctx); err != nil {
							panic(fmt.Sprintf(
								"ERROR: Topic '%s' does not exist on brokers %v.\n"+
									"The replay command requires the topic to exist before writing messages.\n"+
									"To automatically create the topic, use the --create-topic flag:",
								topic, brokers,
							))
						}
					}

					messageCount, err := pkg.Replay(ctx, producer, reader, rate)
					if err != nil {
						return err
					}
					fmt.Printf("Successfully replayed %d messages to topic '%s'\n", messageCount, topic)
					return nil
				},
			},
			{
				Name:        "cat",
				Usage:       "Display recorded messages from a log file",
				Description: "Read and display messages from a binary log file in human-readable format.",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:     "input",
						Aliases:  []string{"i"},
						Usage:    "Input file path containing recorded messages",
						Required: true,
					},
				},
				Action: func(ctx context.Context, cmd *cli.Command) error {
					input := cmd.String("input")

					fmt.Printf("Reading messages from: %s\n", input)
					if err := pkg.Cat(ctx, input); err != nil {
						return err
					}
					return nil
				},
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			// Show help when no command is provided
			return cli.ShowAppHelp(cmd)
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
