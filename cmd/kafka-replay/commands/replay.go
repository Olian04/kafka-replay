package commands

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/lolocompany/kafka-replay/cmd/kafka-replay/util"
	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/lolocompany/kafka-replay/pkg/kafka"
	"github.com/lolocompany/kafka-replay/pkg/transcoder"
	"github.com/urfave/cli/v3"
)

func ReplayCommand() *cli.Command {
	return &cli.Command{
		Name:        "replay",
		Usage:       "Replay recorded messages to a Kafka topic",
		Description: "Replay previously recorded messages from a file back to a Kafka topic.",
		Flags: append(util.GlobalFlags(),
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
				Name:  "rate",
				Usage: "Messages per second to replay (0 for maximum speed)",
				Value: 0,
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
			&cli.IntFlag{
				Name:    "partition",
				Aliases: []string{"p"},
				Usage:   "Target partition to write messages to (default: auto-assign)",
				Value:   -1,
			},
			&cli.BoolFlag{
				Name:  "dry-run",
				Usage: "Validate configuration, messages and connectivity without actually sending to Kafka",
				Value: false,
			},
		),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers, err := util.ResolveBrokers(cmd)
			if err != nil {
				return err
			}
			topic := cmd.String("topic")
			input := cmd.String("input")
			rate := cmd.Int("rate")
			preserveTimestamps := cmd.Bool("preserve-timestamps")
			createTopic := cmd.Bool("create-topic")
			loop := cmd.Bool("loop")
			partitionFlag := cmd.Int("partition")
			dryRun := cmd.Bool("dry-run")

			var partition *int
			if partitionFlag >= 0 {
				partition = &partitionFlag
			}

			quiet := util.Quiet(cmd)
			if !quiet {
				if dryRun {
					fmt.Fprintln(os.Stderr, "DRY RUN MODE: No messages will be sent to Kafka")
				}
				fmt.Fprintf(os.Stderr, "Replaying messages to topic '%s' on brokers %v\n", topic, brokers)
				fmt.Fprintf(os.Stderr, "Input file: %s\n", input)
				if rate > 0 {
					fmt.Fprintf(os.Stderr, "Rate limit: %d messages/second\n", rate)
				} else {
					fmt.Fprintln(os.Stderr, "Rate limit: maximum speed")
				}
				if preserveTimestamps {
					fmt.Fprintln(os.Stderr, "Preserving original timestamps")
				}
				if loop {
					fmt.Fprintln(os.Stderr, "Looping: infinite")
				}
				if partition != nil {
					fmt.Fprintf(os.Stderr, "Target partition: %d\n", *partition)
				}
			}

			// Open input file
			file, err := os.Open(input)
			if err != nil {
				return fmt.Errorf("failed to open input file: %w", err)
			}
			defer file.Close()

			var spinner *util.ProgressSpinner
			if !quiet {
				spinner = util.NewProgressSpinner("Replaying messages")
			}
			countingReader := util.CountingReadSeeker(file, spinner)

			// Create message decoder
			decoder, err := transcoder.NewDecodeReader(countingReader, preserveTimestamps)
			if err != nil {
				return fmt.Errorf("failed to create message decoder: %w", err)
			}

			// Create Kafka producer
			producer := kafka.NewProducer(brokers, topic, createTopic)
			defer producer.Close()

			logWriter := io.Writer(os.Stderr)
			if quiet {
				logWriter = io.Discard
			}
			messageCount, err := pkg.Replay(ctx, pkg.ReplayConfig{
				Producer:  producer,
				Decoder:   decoder,
				Rate:      rate,
				Loop:      loop,
				Partition: partition,
				LogWriter: logWriter,
				DryRun:    dryRun,
			})

			if err != nil {
				return err
			}

			if spinner != nil {
				spinner.Close()
			}
			if !quiet {
				if dryRun {
					fmt.Fprintf(os.Stderr, "Dry run completed: validated %d messages (no messages were sent)\n", messageCount)
				} else {
					fmt.Fprintf(os.Stderr, "Successfully replayed %d messages to topic '%s'\n", messageCount, topic)
				}
			}
			return nil
		},
	}
}
