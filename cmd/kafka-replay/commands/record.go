package commands

import (
	"context"
	"fmt"
	"os"

	"github.com/lolocompany/kafka-replay/cmd/kafka-replay/util"
	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/lolocompany/kafka-replay/pkg/kafka"
	"github.com/urfave/cli/v3"
)

func RecordCommand() *cli.Command {
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
			&cli.DurationFlag{
				Name:    "timeout",
				Aliases: []string{"T"},
				Usage:   "Timeout for the recording operation (e.g., 5m, 30s). 0 means no timeout",
				Value:   0,
			},
			&cli.StringFlag{
				Name:    "find",
				Aliases: []string{"f"},
				Usage:   "Only record messages containing the specified byte sequence (string is converted to bytes). When combined with --limit, keeps consuming until the limit of matching messages is found",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers, err := util.ResolveBrokers(cmd.StringSlice("broker"))
			if err != nil {
				return err
			}
			topic := cmd.String("topic")
			groupID := cmd.String("group-id")
			partition := cmd.Int("partition")
			output := cmd.String("output")
			offsetFlag := cmd.Int64("offset")
			limit := cmd.Int("limit")
			timeout := cmd.Duration("timeout")
			findStr := cmd.String("find")

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

			fmt.Fprintf(os.Stderr, "Recording messages from topic '%s' on brokers %v\n", topic, brokers)
			fmt.Fprintf(os.Stderr, "Consumer group: %s\n", groupID)
			fmt.Fprintf(os.Stderr, "Output file: %s\n", output)
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
			consumer, err := kafka.NewConsumer(ctx, brokers, topic, partition)
			if err != nil {
				return err
			}
			defer consumer.Close()
			fileWriter, err := os.Create(output)
			if err != nil {
				return err
			}
			defer fileWriter.Close()

			// Create progress spinner
			spinner := util.NewProgressSpinner("Recording messages")

			// Wrap writer to count bytes for spinner
			writer := util.CountingWriter(fileWriter, spinner)

			read, messageCount, err := pkg.Record(ctx, pkg.RecordConfig{
				Consumer:  consumer,
				Offset:    offset,
				Output:    writer,
				Limit:     limit,
				FindBytes: findBytes,
			})

			if err != nil {
				return err
			}

			// Close spinner before printing final message to avoid double display
			spinner.Close()

			fmt.Fprintf(os.Stderr, "Recorded %d messages (%d bytes)\n", messageCount, read)
			return nil
		},
	}
}
