package commands

import (
	"context"

	"github.com/urfave/cli/v3"
)

// DebugCommand returns the debug top-level command. Subcommands here are
// unstable and not part of the scriptable CLI contract.
func DebugCommand() *cli.Command {
	return &cli.Command{
		Name:        "debug",
		Usage:       "Debug and internal commands (unstable)",
		Description: "Commands for debugging and internal use. Behavior may change without notice.",
		Commands: []*cli.Command{
			ConfigCommand(),
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return cli.ShowSubcommandHelp(cmd)
		},
	}
}
