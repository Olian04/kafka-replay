package commands

import (
	"context"
	"fmt"
	"os"

	"github.com/lolocompany/kafka-replay/cmd/kafka-replay/util"
	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/lolocompany/kafka-replay/cmd/kafka-replay/output"
	"github.com/urfave/cli/v3"
)

func listPartitionsCommand() *cli.Command {
	return &cli.Command{
		Name:        "partitions",
		Aliases:     []string{"partition"},
		Usage:       "List partitions with their leaders",
		Description: "Display topic-partition pairs with their leader brokers (table or json).",
		Flags: append(util.GlobalFlags(),
			&cli.BoolFlag{
				Name:  "offsets",
				Usage: "Include earliest and latest offsets for each partition",
				Value: false,
			},
			&cli.BoolFlag{
				Name:  "replicas",
				Usage: "Include replica assignment details (replicas and in-sync-replicas)",
				Value: false,
			},
		),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers, err := util.ResolveBrokers(cmd)
			if err != nil {
				return err
			}

			includeOffsets := cmd.Bool("offsets")
			includeReplicas := cmd.Bool("replicas")

			partitions, err := pkg.ListPartitions(ctx, brokers, includeOffsets, includeReplicas)
			if err != nil {
				return err
			}

			format, err := output.ParseFormat(util.GetFormat(cmd), output.IsTTY(os.Stdout))
			if err != nil {
				return err
			}
			if format == output.FormatRaw {
				return fmt.Errorf("format 'raw' is only supported by the 'cat' command")
			}
			enc := output.NewEncoder(format, os.Stdout)
			if format == output.FormatTable {
				headers := []string{"TOPIC", "PARTITION", "LEADER"}
				rows := make([][]string, 0, len(partitions))
				for _, p := range partitions {
					row := []string{p.Topic, fmt.Sprintf("%d", p.Partition), p.Leader}
					rows = append(rows, row)
				}
				return enc.EncodeTable(headers, rows)
			}
			return output.EncodeSlice(enc, partitions)
		},
	}
}
