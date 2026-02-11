package util

import (
	"github.com/urfave/cli/v3"
)

// GlobalFlags returns the global flags that should be available on all commands.
// These flags are added to each command so they can be used after the command name.
func GlobalFlags() []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:    "config",
			Usage:   "Path to configuration file (defaults to ~/.kafka-replay/config.yaml)",
			Sources: cli.EnvVars("KAFKA_REPLAY_CONFIG"),
			Local:   false,
		},
		&cli.StringFlag{
			Name:    "profile",
			Usage:   "Profile name to use from configuration",
			Sources: cli.EnvVars("KAFKA_REPLAY_PROFILE"),
			Local:   false,
		},
		&cli.StringSliceFlag{
			Name:    "brokers",
			Usage:   "Kafka broker address(es) (comma-separated or repeated)",
			Sources: cli.EnvVars("KAFKA_REPLAY_BROKERS"),
			Local:   false,
		},
		&cli.StringFlag{
			Name:    "format",
			Aliases: []string{"f"},
			Usage:   "Global output format: table (default), json (one object per line), or raw (cat only)",
			Local:   false,
		},
		&cli.BoolFlag{
			Name:  "quiet",
			Usage: "Suppress all status logging (progress, \"Recording...\", \"Replaying...\", etc.)",
			Value: false,
			Local: false,
		},
	}
}
