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

func InspectCommand() *cli.Command {
	return &cli.Command{
		Name:        "inspect",
		Usage:       "Inspect a single resource in detail",
		Description: "Show detailed information for a topic or consumer group. Subcommands: topic, consumer-group.",
		Commands: []*cli.Command{
			inspectTopicCommand(),
			inspectConsumerGroupCommand(),
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return cli.ShowSubcommandHelp(cmd)
		},
	}
}

func inspectTopicCommand() *cli.Command {
	return &cli.Command{
		Name:        "topic",
		Aliases:     []string{"topics"},
		Usage:       "Inspect a topic",
		Description: "List partitions and details for a single topic.",
		ArgsUsage:   "TOPIC",
		Flags:       util.GlobalFlags(),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			args := cmd.Args().Slice()
			if len(args) < 1 {
				return fmt.Errorf("topic name required")
			}
			topicName := args[0]
			brokers, err := util.ResolveBrokers(cmd)
			if err != nil {
				return err
			}
			partitions, err := pkg.ListPartitions(ctx, brokers, true, true)
			if err != nil {
				return err
			}
			var filtered []pkg.PartitionOutput
			for _, p := range partitions {
				if p.Topic == topicName {
					filtered = append(filtered, p)
				}
			}
			if len(filtered) == 0 {
				return fmt.Errorf("topic %q not found", topicName)
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
				rows := make([][]string, 0, len(filtered))
				for _, p := range filtered {
					rows = append(rows, []string{p.Topic, fmt.Sprintf("%d", p.Partition), p.Leader})
				}
				return enc.EncodeTable(headers, rows)
			}
			return output.EncodeSlice(enc, filtered)
		},
	}
}

func inspectConsumerGroupCommand() *cli.Command {
	return &cli.Command{
		Name:        "consumer-group",
		Aliases:     []string{"consumer-groups", "group"},
		Usage:       "Inspect a consumer group",
		Description: "Show details for a single consumer group (members and offsets).",
		ArgsUsage:   "GROUP_ID",
		Flags:       util.GlobalFlags(),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			args := cmd.Args().Slice()
			if len(args) < 1 {
				return fmt.Errorf("consumer group ID required")
			}
			groupID := args[0]
			brokers, err := util.ResolveBrokers(cmd)
			if err != nil {
				return err
			}
			groups, err := pkg.ListConsumerGroups(ctx, brokers, true, true)
			if err != nil {
				return err
			}
			for _, g := range groups {
				if g.GroupID == groupID {
					format, _ := output.ParseFormat(util.GetFormat(cmd), output.IsTTY(os.Stdout))
					enc := output.NewEncoder(format, os.Stdout)
					return output.EncodeSlice(enc, []pkg.ConsumerGroupOutput{g})
				}
			}
			return fmt.Errorf("consumer group %q not found", groupID)
		},
	}
}
