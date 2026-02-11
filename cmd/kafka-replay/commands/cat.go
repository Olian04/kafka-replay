package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/lolocompany/kafka-replay/cmd/kafka-replay/util"
	"github.com/lolocompany/kafka-replay/pkg"
	"github.com/lolocompany/kafka-replay/cmd/kafka-replay/output"
	"github.com/urfave/cli/v3"
)

var globalFlags = util.GlobalFlags()

type catMessage struct {
	Timestamp string `json:"timestamp"`
	Key       string `json:"key"`
	Data      string `json:"data"`
}

func CatCommand() *cli.Command {
	return &cli.Command{
		Name:        "cat",
		Usage:       "Display recorded messages from a message file",
		Description: "Read and display messages from a binary message file. Uses global --format flag (json, raw).",
		Flags: append(globalFlags,
			&cli.StringFlag{
				Name:     "input",
				Aliases:  []string{"i"},
				Usage:    "Input file path containing recorded messages",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "find",
				Aliases: []string{"f"},
				Usage:   "Filter messages containing the specified literal byte sequence, case-sensitive (string converted to bytes)",
			},
			&cli.BoolFlag{
				Name:    "count",
				Usage:   "Only output the count of messages to stdout, do not display them",
				Value:   false,
			},
		),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			input := cmd.String("input")
			findStr := cmd.String("find")
			countOnly := cmd.Bool("count")

			var findBytes []byte
			if findStr != "" {
				findBytes = []byte(findStr)
			}

			file, err := os.Open(input)
			if err != nil {
				return fmt.Errorf("failed to open input file: %w", err)
			}
			defer file.Close()

			// For cat, default to json when --format is not set
			formatStr := util.GetFormat(cmd)
			if formatStr == "" {
				formatStr = "json"
			}
			format, err := output.ParseFormat(formatStr, false)
			if err != nil {
				return err
			}
			formatter, err := catFormatter(format)
			if err != nil {
				return err
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

			if countOnly {
				_, err = fmt.Fprintf(os.Stdout, "%d\n", count)
				return err
			}
			return nil
		},
	}
}

// catFormatter returns a formatter for the given output format.
func catFormatter(format output.Format) (func(time.Time, []byte, []byte) []byte, error) {
	switch format {
	case output.FormatJSON:
		return jsonFormatter, nil
	case output.FormatRaw:
		return rawFormatter, nil
	default:
		return nil, fmt.Errorf("cat command only supports formats: json, raw (got %q)", format)
	}
}

func rawFormatter(timestamp time.Time, key []byte, data []byte) []byte {
	return data
}

func jsonFormatter(timestamp time.Time, key []byte, data []byte) []byte {
	msg := catMessage{
		Timestamp: timestamp.Format(time.RFC3339Nano),
		Key:       string(key),
		Data:      string(data),
	}
	b, err := json.Marshal(msg)
	if err != nil {
		return []byte(fmt.Sprintf("{\"error\":\"%s\"}\n", err.Error()))
	}
	return append(b, '\n')
}

