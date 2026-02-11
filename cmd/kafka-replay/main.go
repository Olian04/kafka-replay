package main

import (
	"context"
	"fmt"
	"os"

	"github.com/lolocompany/kafka-replay/cmd/kafka-replay/commands"
	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:        "kafka-replay",
		Usage:       "A utility tool for recording and replaying Kafka messages",
		Description: "Record messages from Kafka topics or replay previously recorded messages back to Kafka topics.",
		Commands: []*cli.Command{
			commands.ListCommand(),
			commands.RecordCommand(),
			commands.ReplayCommand(),
			commands.CatCommand(),
			commands.InspectCommand(),
			commands.DebugCommand(),
			commands.VersionCommand(),
		},
		Action: func(c context.Context, cmd *cli.Command) error {
			return cli.ShowAppHelp(cmd)
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
