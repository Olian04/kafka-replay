package commands

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/lolocompany/kafka-replay/v2/cmd/kafka-replay/config"
	"github.com/lolocompany/kafka-replay/v2/cmd/kafka-replay/util"
	"github.com/lolocompany/kafka-replay/v2/pkg"
	"github.com/lolocompany/kafka-replay/v2/pkg/kafka"
	"github.com/urfave/cli/v3"
)

func MirrorCommand() *cli.Command {
	return &cli.Command{
		Name:        "mirror",
		Usage:       "Mirror messages from one Kafka topic to another",
		Description: "Read messages from a source Kafka topic and write them directly to a destination topic without writing to disk.",
		Flags: append(util.GlobalFlags(),
			&cli.StringFlag{
				Name:     "from-topic",
				Aliases:  []string{"s"},
				Usage:    "Source Kafka topic to read messages from",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "to-topic",
				Aliases:  []string{"t"},
				Usage:    "Target Kafka topic to write messages to",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "from-profile",
				Usage:    "Profile name to use for source brokers (from configuration)",
				Required: true,
			},
			&cli.StringFlag{
				Name:     "to-profile",
				Usage:    "Profile name to use for target brokers (from configuration)",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "from-group",
				Aliases: []string{"g"},
				Usage:   "Consumer group ID for source topic (empty by default, uses direct partition access). Cannot be used together with --from-offset.",
				Value:   "",
			},
			&cli.IntFlag{
				Name:  "from-partition",
				Usage: "Source partition to read messages from (only used with direct partition access, not consumer groups)",
				Value: 0,
			},
			&cli.Int64Flag{
				Name:    "from-offset",
				Aliases: []string{"O"},
				Usage:   "Start reading from a specific offset (-1 to use current position, 0 to start from beginning). Cannot be used together with --from-group.",
				Value:   -1,
			},
			&cli.IntFlag{
				Name:    "limit",
				Aliases: []string{"l"},
				Usage:   "Maximum number of messages to mirror (0 for unlimited)",
				Value:   0,
			},
			&cli.DurationFlag{
				Name:    "timeout",
				Aliases: []string{"T"},
				Usage:   "Timeout for the mirror operation (e.g., 5m, 30s). 0 means no timeout",
				Value:   0,
			},
			&cli.StringFlag{
				Name:  "find",
				Usage: "Only mirror messages containing the specified byte sequence (string is converted to bytes)",
			},
			&cli.IntFlag{
				Name:    "to-partition",
				Aliases: []string{"p"},
				Usage:   "Target partition to write messages to (default: auto-assign)",
				Value:   -1,
			},
			&cli.BoolFlag{
				Name:  "preserve-timestamps",
				Usage: "Preserve original message timestamps",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "to-create-topic",
				Usage: "Create the target topic if it doesn't exist",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "no-ack",
				Usage: "Don't wait for broker acknowledgment (faster but less reliable - messages may be lost if broker fails immediately)",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "dry-run",
				Usage: "Validate configuration, messages and connectivity without actually sending to Kafka",
				Value: false,
			},
		),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			// Load config
			cfg, err := util.LoadConfigForCmd(cmd)
			if err != nil {
				return err
			}

			// Resolve brokers for source (from)
			fromProfile := cmd.String("from-profile")
			fromBrokers, err := config.ResolveBrokers(nil, fromProfile, cfg) // Don't use global --brokers flag
			if err != nil {
				return fmt.Errorf("failed to resolve source brokers: %w", err)
			}

			// Resolve brokers for target (to)
			toProfile := cmd.String("to-profile")
			toBrokers, err := config.ResolveBrokers(nil, toProfile, cfg) // Don't use global --brokers flag
			if err != nil {
				return fmt.Errorf("failed to resolve target brokers: %w", err)
			}

			fromTopic := cmd.String("from-topic")
			toTopic := cmd.String("to-topic")
			groupID := cmd.String("from-group")
			fromPartition := cmd.Int("from-partition")
			partitionFlag := cmd.Int("to-partition")
			offsetFlag := cmd.Int64("from-offset")
			limit := cmd.Int("limit")
			timeout := cmd.Duration("timeout")
			findStr := cmd.String("find")
			preserveTimestamps := cmd.Bool("preserve-timestamps")
			createTopic := cmd.Bool("to-create-topic")
			dryRun := cmd.Bool("dry-run")
			noAck := cmd.Bool("no-ack")

			// Validate that --from-group and --from-offset are not used together
			// offsetFlag >= 0 means an explicit offset was provided (not the default -1)
			if groupID != "" && offsetFlag >= 0 {
				return fmt.Errorf("--from-group and --from-offset cannot be used together: consumer groups manage offsets automatically, while --from-offset requires direct partition access")
			}

			// Convert find string to byte slice if provided
			var findBytes []byte
			if findStr != "" {
				findBytes = []byte(findStr)
			}

			// Apply timeout if specified
			if timeout > 0 {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, timeout)
				defer cancel()
			}

			// Determine the offset to use
			// If --offset is explicitly set (>= 0), use it
			// Otherwise, use nil (start from current position)
			var offset *int64
			if offsetFlag >= 0 {
				offset = &offsetFlag
			}

			// Determine target partition
			var partition *int
			if partitionFlag >= 0 {
				partition = &partitionFlag
			}

			quiet := util.Quiet(cmd)
			if !quiet {
				if dryRun {
					fmt.Fprintln(os.Stderr, "DRY RUN MODE: No messages will be sent to Kafka")
				}
				fmt.Fprintf(os.Stderr, "Mirroring messages from topic '%s' to topic '%s'\n", fromTopic, toTopic)
				fmt.Fprintf(os.Stderr, "Source brokers: %v\n", fromBrokers)
				fmt.Fprintf(os.Stderr, "Target brokers: %v\n", toBrokers)
				if groupID != "" {
					fmt.Fprintf(os.Stderr, "Consumer group: %s\n", groupID)
				} else {
					fmt.Fprintf(os.Stderr, "Using direct partition access (no consumer group), source partition: %d\n", fromPartition)
				}
				if offset != nil {
					fmt.Fprintf(os.Stderr, "Starting from offset: %d\n", *offset)
				} else {
					fmt.Fprintln(os.Stderr, "Starting from current position")
				}
				if limit > 0 {
					fmt.Fprintf(os.Stderr, "Message limit: %d\n", limit)
				}
				if timeout > 0 {
					fmt.Fprintf(os.Stderr, "Timeout: %v\n", timeout)
				}
				if findStr != "" {
					fmt.Fprintf(os.Stderr, "Find filter: %s\n", findStr)
				}
				if preserveTimestamps {
					fmt.Fprintln(os.Stderr, "Preserving original timestamps")
				}
				if partition != nil {
					fmt.Fprintf(os.Stderr, "To partition: %d\n", *partition)
				}
				if noAck {
					fmt.Fprintln(os.Stderr, "No acknowledgment: enabled (faster but less reliable)")
				}
			}

			// Create consumer for source topic (using from brokers)
			// Consumer groups handle partition assignment automatically
			consumer, err := kafka.NewConsumer(ctx, fromBrokers, fromTopic, fromPartition, groupID)
			if err != nil {
				return err
			}
			defer consumer.Close()

			// Create producer for target topic (using to brokers)
			producer := kafka.NewProducer(toBrokers, toTopic, createTopic, noAck)
			defer producer.Close()

			var spinner *util.ProgressSpinner
			if !quiet {
				spinner = util.NewProgressSpinner("Mirroring messages")
			}

			logWriter := io.Writer(os.Stderr)
			if quiet {
				logWriter = io.Discard
			}

			var onBytesProcessed func(int64)
			if spinner != nil {
				onBytesProcessed = func(bytes int64) {
					spinner.AddBytes(bytes)
				}
			}

			messageCount, err := pkg.Mirror(ctx, pkg.MirrorConfig{
				Consumer:           consumer,
				Producer:           producer,
				Offset:             offset,
				Limit:              limit,
				Partition:          partition,
				LogWriter:          logWriter,
				DryRun:             dryRun,
				FindBytes:          findBytes,
				PreserveTimestamps: preserveTimestamps,
				OnBytesProcessed:   onBytesProcessed,
			})

			if spinner != nil {
				spinner.Close()
			}

			if err != nil {
				return err
			}

			if !quiet {
				if dryRun {
					fmt.Fprintf(os.Stderr, "Dry run completed: validated %d messages (no messages were sent)\n", messageCount)
				} else {
					fmt.Fprintf(os.Stderr, "Successfully mirrored %d messages from topic '%s' to topic '%s'\n", messageCount, fromTopic, toTopic)
				}
			}
			return nil
		},
	}
}
