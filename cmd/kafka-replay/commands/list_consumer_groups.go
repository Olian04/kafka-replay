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

func listConsumerGroupsCommand() *cli.Command {
	return &cli.Command{
		Name:        "consumer-groups",
		Aliases:     []string{"groups", "consumer-group"},
		Usage:       "List consumer groups",
		Description: "Display consumer groups (table or json).",
		Flags: append(util.GlobalFlags(),
			&cli.BoolFlag{
				Name:  "offsets",
				Usage: "Include offset information for each partition",
				Value: false,
			},
			&cli.BoolFlag{
				Name:    "members",
				Aliases: []string{"m"},
				Usage:   "Include member information for each group",
				Value:   false,
			},
		),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			brokers, err := util.ResolveBrokers(cmd)
			if err != nil {
				return err
			}

			includeOffsets := cmd.Bool("offsets")
			includeMembers := cmd.Bool("members")

			groups, err := pkg.ListConsumerGroups(ctx, brokers, includeOffsets, includeMembers)
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
				headers := []string{"GROUP_ID", "STATE", "PROTOCOL_TYPE"}
				rows := make([][]string, 0, len(groups))
				for _, g := range groups {
					rows = append(rows, []string{g.GroupID, g.State, g.ProtocolType})
				}
				return enc.EncodeTable(headers, rows)
			}
			return output.EncodeSlice(enc, groups)
		},
	}
}
