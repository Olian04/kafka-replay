package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/urfave/cli/v3"
)

// resolveBrokers resolves broker addresses from command flags or environment variable.
// Returns the broker addresses and an error if neither is provided.
func resolveBrokers(flagBrokers []string) ([]string, error) {
	if len(flagBrokers) > 0 {
		return flagBrokers, nil
	}

	envBrokers := os.Getenv("KAFKA_BROKERS")
	if envBrokers == "" {
		return nil, fmt.Errorf("broker address(es) must be provided via --broker flag or KAFKA_BROKERS environment variable")
	}

	brokers := strings.Split(envBrokers, ",")
	// Trim whitespace from each broker
	for i, broker := range brokers {
		brokers[i] = strings.TrimSpace(broker)
	}
	return brokers, nil
}

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
