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

func listTopicsCommand() *cli.Command {
	return &cli.Command{
		Name:        "topics",
		Aliases:     []string{"topic"},
		Usage:       "List topics with partition counts",
		Description: "Display topic names with partition count and replication factor (table or json).",
		Flags:       util.GlobalFlags(),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers, err := util.ResolveBrokers(cmd)
			if err != nil {
				return err
			}

			topics, err := pkg.ListTopics(ctx, brokers)
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
				headers := []string{"NAME", "PARTITIONS", "REPLICATION_FACTOR"}
				rows := make([][]string, 0, len(topics))
				for _, t := range topics {
					rows = append(rows, []string{t.Name, fmt.Sprintf("%d", t.PartitionCount), fmt.Sprintf("%d", t.ReplicationFactor)})
				}
				return enc.EncodeTable(headers, rows)
			}
			return output.EncodeSlice(enc, topics)
		},
	}
}
