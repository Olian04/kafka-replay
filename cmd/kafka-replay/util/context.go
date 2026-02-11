package util

import (
	"github.com/lolocompany/kafka-replay/cmd/kafka-replay/config"
	"github.com/urfave/cli/v3"
)

// Quiet returns true if the global --quiet flag is set. When true, commands
// should suppress all status logging (progress, "Recording...", etc.).
func Quiet(cmd *cli.Command) bool {
	return cmd.Bool("quiet")
}

// ResolveBrokers returns the broker list for the current invocation by reading
// --config, --profile, and --brokers from the command.
func ResolveBrokers(cmd *cli.Command) ([]string, error) {
	c, err := config.LoadConfig(cmd.String("config"))
	if err != nil {
		return nil, err
	}
	return config.ResolveBrokers(cmd.StringSlice("brokers"), cmd.String("profile"), c)
}

// GetFormat returns the global --format flag value from the command.
// It may be empty if not set; callers should use output.ParseFormat with a
// default (e.g. from TTY detection).
func GetFormat(cmd *cli.Command) string {
	return cmd.String("format")
}

// LoadConfigForCmd loads the config file using the --config path from the command.
func LoadConfigForCmd(cmd *cli.Command) (config.Config, error) {
	return config.LoadConfig(cmd.String("config"))
}
