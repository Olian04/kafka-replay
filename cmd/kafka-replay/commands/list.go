package commands

import (
	"context"

	"github.com/urfave/cli/v3"
)

func ListCommand() *cli.Command {
	return &cli.Command{
		Name:        "list",
		Aliases:     []string{"ls"},
		Usage:       "List Kafka resources",
		Description: "List Kafka resources with their details. Subcommands: brokers, topics, partitions, consumer-groups.",
		Commands: []*cli.Command{
			listBrokersCommand(),
			listTopicsCommand(),
			listPartitionsCommand(),
			listConsumerGroupsCommand(),
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return cli.ShowSubcommandHelp(cmd)
		},
	}
}
