package main

import (
	"context"
	"fmt"
	"os"

	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/urfave/cli/v3"
)

func replayCommand() *cli.Command {
	return &cli.Command{
		Name:        "replay",
		Usage:       "Replay recorded messages to a Kafka topic",
		Description: "Replay previously recorded messages from a file back to a Kafka topic.",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:    "broker",
				Aliases: []string{"b"},
				Usage:   "Kafka broker address(es) (can be specified multiple times). Defaults to KAFKA_BROKERS env var if not provided.",
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
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "create-topic",
				Usage: "Create the topic if it doesn't exist",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "loop",
				Usage: "Enable infinite looping - replay messages continuously until interrupted",
				Value: false,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers, err := resolveBrokers(cmd.StringSlice("broker"))
			if err != nil {
				return err
			}
			topic := cmd.String("topic")
			input := cmd.String("input")
			rate := cmd.Int("rate")
			preserveTimestamps := cmd.Bool("preserve-timestamps")
			createTopic := cmd.Bool("create-topic")
			loop := cmd.Bool("loop")

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
			if loop {
				fmt.Println("Looping: infinite")
			}

			// Open input file
			file, err := os.Open(input)
			if err != nil {
				return fmt.Errorf("failed to open input file: %w", err)
			}
			defer file.Close()

			// Create message file reader
			reader := pkg.NewMessageFileReader(file, preserveTimestamps, pkg.RealTimeProvider{})

			// Get file size for progress bar
			fileSize, err := reader.FileSize()
			if err != nil {
				return fmt.Errorf("failed to get file size: %w", err)
			}

			// Create progress reporter
			progressReporter := NewReplayProgressReporter(fileSize, loop)

			// Create Kafka producer
			producer := pkg.NewProducer(brokers, topic, createTopic)
			defer producer.Close()

			messageCount, err := pkg.Replay(ctx, pkg.ReplayConfig{
				Producer:        producer,
				Reader:           reader,
				Rate:             rate,
				Loop:             loop,
				LogWriter:        os.Stderr,
				ProgressReporter: progressReporter,
			})
			if err != nil {
				return err
			}
			fmt.Printf("Successfully replayed %d messages to topic '%s'\n", messageCount, topic)
			return nil
		},
	}
}
