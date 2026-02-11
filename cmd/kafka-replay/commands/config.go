package commands

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/lolocompany/kafka-replay/cmd/kafka-replay/config"
	"github.com/lolocompany/kafka-replay/cmd/kafka-replay/util"
	"github.com/urfave/cli/v3"
)

func ConfigCommand() *cli.Command {
	return &cli.Command{
		Name:        "config",
		Aliases:     []string{"cfg", "conf"},
		Usage:       "Show resolved configuration and where each value comes from",
		Description: "Resolves and displays the config currently in use: config file path, profile, and brokers. Shows the source of each value and whether it is overridden by a higher-priority source.",
		Flags:       util.GlobalFlags(),
		Action:      runConfig,
	}
}

func runConfig(ctx context.Context, cmd *cli.Command) error {
	configPathFlag := cmd.String("config")
	actualPath := configPathFlag
	sourceNote := ""
	if actualPath == "" {
		resolved, err := config.ResolveConfigPath()
		if err != nil {
			actualPath = "(could not resolve path: " + err.Error() + ")"
		} else {
			actualPath = resolved
			if actualPath == config.DefaultConfigPath() {
				sourceNote = " (default)"
			} else {
				sourceNote = " (current directory)"
			}
		}
	}
	profileFlag := cmd.String("profile")
	brokersFlag := cmd.StringSlice("brokers")
	envBrokers := os.Getenv("KAFKA_BROKERS")

	cfg, err := config.LoadConfig(configPathFlag)
	if err != nil {
		return err
	}

	configFileLine := actualPath + sourceNote
	if actualPath != "(could not resolve default path)" {
		if _, err := os.Stat(actualPath); err == nil {
			configFileLine += " [file exists]"
		} else {
			configFileLine += " [file not found; using empty config]"
		}
	}
	fmt.Fprintf(os.Stdout, "config file:   %s\n", configFileLine)

	// Resolve profile: --profile > config default_profile
	var profileName, profileSource string
	if profileFlag != "" {
		profileName = profileFlag
		profileSource = "from --profile (overrides config default)"
	} else if cfg.DefaultProfile != "" {
		profileName = cfg.DefaultProfile
		profileSource = "from config default_profile"
	} else {
		profileName = "(none)"
		profileSource = ""
	}
	if profileSource != "" {
		fmt.Fprintf(os.Stdout, "profile:       %s  [%s]\n", profileName, profileSource)
	} else {
		fmt.Fprintf(os.Stdout, "profile:       %s\n", profileName)
	}

	// Resolve brokers: --brokers > profile from config > KAFKA_BROKERS
	var brokers []string
	var brokersSource string
	var overrides []string

	if len(brokersFlag) > 0 {
		brokers = brokersFlag
		brokersSource = "from --brokers"
		if cfg.Profiles != nil && profileName != "(none)" && cfg.Profiles[profileName].Brokers != nil {
			overrides = append(overrides, "config profile \""+profileName+"\"")
		}
		if envBrokers != "" {
			overrides = append(overrides, "env KAFKA_BROKERS")
		}
	} else if cfg.Profiles != nil && profileName != "(none)" {
		if p, ok := cfg.Profiles[profileName]; ok && len(p.Brokers) > 0 {
			brokers = p.Brokers
			brokersSource = "from config profile \"" + profileName + "\""
			if envBrokers != "" {
				overrides = append(overrides, "env KAFKA_BROKERS")
			}
		}
	}
	if brokers == nil && envBrokers != "" {
		parts := strings.Split(envBrokers, ",")
		for _, p := range parts {
			if t := strings.TrimSpace(p); t != "" {
				brokers = append(brokers, t)
			}
		}
		brokersSource = "from env KAFKA_BROKERS"
	}

	if brokers == nil {
		fmt.Fprintf(os.Stdout, "brokers:       (none)  [set --brokers, a profile with brokers, or KAFKA_BROKERS]\n")
	} else {
		fmt.Fprintf(os.Stdout, "brokers:       %s  [%s", strings.Join(brokers, ", "), brokersSource)
		if len(overrides) > 0 {
			fmt.Fprintf(os.Stdout, "; overrides: %s", strings.Join(overrides, ", "))
		}
		fmt.Fprintf(os.Stdout, "]\n")
	}

	return nil
}
