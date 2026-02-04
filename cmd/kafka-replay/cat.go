package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/urfave/cli/v3"
)

type catMessage struct {
	Timestamp string `json:"timestamp"`
	Data      string `json:"data"`
}

func catCommand() *cli.Command {
	return &cli.Command{
		Name:        "cat",
		Usage:       "Display recorded messages from a message file",
		Description: "Read and display messages from a binary message file in human-readable format.",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "input",
				Aliases:  []string{"i"},
				Usage:    "Input file path containing recorded messages",
				Required: true,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			input := cmd.String("input")

			formatter := func(msg *pkg.RecordedMessage) string {
				catMessage := catMessage{
					Timestamp: msg.Timestamp.Format(time.RFC3339Nano),
					Data:      string(msg.Data),
				}
				jsonMessage, err := json.Marshal(catMessage)
				if err != nil {
					return fmt.Sprintf("{\"error\":\"%s\"}", err.Error())
				}
				return string(jsonMessage)
			}

			// Open input file
			file, err := os.Open(input)
			if err != nil {
				return fmt.Errorf("failed to open input file: %w", err)
			}
			defer file.Close()

			fmt.Printf("Reading messages from: %s\n", input)
			if err := pkg.Cat(ctx, pkg.CatConfig{
				Reader:       file,
				TimeProvider: pkg.RealTimeProvider{},
				Formatter:    formatter,
				Output:       os.Stdout,
			}); err != nil {
				return err
			}
			return nil
		},
	}
}
