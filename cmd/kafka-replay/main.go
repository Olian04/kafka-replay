package main

import (
	"context"
	
	"log"
	"os"
	

	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:        "kafka-replay",
		Usage:       "A utility tool for recording and replaying Kafka messages",
		Description: "Record messages from Kafka topics or replay previously recorded messages back to Kafka topics.",
		Commands: []*cli.Command{
			recordCommand(),
			replayCommand(),
			catCommand(),
			versionCommand(),
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			// Show help when no command is provided
			return cli.ShowAppHelp(cmd)
		},
	}

	if err := app.Run(context.Background(), os.Args); err != nil {
		log.Fatal(err)
	}
}
