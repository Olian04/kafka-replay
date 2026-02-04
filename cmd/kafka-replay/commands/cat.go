package commands

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

func CatCommand() *cli.Command {
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
			&cli.BoolFlag{
				Name:    "raw",
				Aliases: []string{"r"},
				Usage:   "Output only the raw message data, excluding timestamps and JSON formatting",
				Value:   false,
			},
			&cli.StringFlag{
				Name:    "find",
				Aliases: []string{"f"},
				Usage:   "Filter messages containing the specified byte sequence (string is converted to bytes)",
			},
			&cli.BoolFlag{
				Name:    "count",
				Aliases: []string{"c"},
				Usage:   "Only output the count of messages, don't display them",
				Value:   false,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			input := cmd.String("input")
			raw := cmd.Bool("raw")
			findStr := cmd.String("find")
			countOnly := cmd.Bool("count")

			// Convert find string to byte slice if provided
			var findBytes []byte
			if findStr != "" {
				findBytes = []byte(findStr)
			}

			// Open input file
			file, err := os.Open(input)
			if err != nil {
				return fmt.Errorf("failed to open input file: %w", err)
			}
			defer file.Close()

			if !countOnly {
				fmt.Fprintf(os.Stderr, "Reading messages from: %s\n", input)
			}

			// Create formatter (only used if not count-only mode)
			formatter := jsonFormatter
			if raw {
				formatter = rawFormatter
			}

			count, err := pkg.Cat(ctx, pkg.CatConfig{
				Reader:    file,
				Formatter: formatter,
				Output:    os.Stdout,
				FindBytes: findBytes,
				CountOnly: countOnly,
			})
			if err != nil {
				return err
			}

			// Output count if count-only mode
			if countOnly {
				_, err := fmt.Fprintf(os.Stdout, "%d\n", count)
				if err != nil {
					return err
				}
			}

			return nil
		},
	}
}

func rawFormatter(timestamp time.Time, data []byte) []byte {
	return data
}

func jsonFormatter(timestamp time.Time, data []byte) []byte {
	catMessage := catMessage{
		Timestamp: timestamp.Format(time.RFC3339Nano),
		Data:      string(data),
	}
	jsonMessage, err := json.Marshal(catMessage)
	if err != nil {
		return []byte(fmt.Sprintf("{\"error\":\"%s\"}", err.Error()))
	}
	return jsonMessage
}
