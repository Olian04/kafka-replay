package config

import (
	"fmt"
	"os"
	"strings"
)

// ResolveProfile returns the profile identified by profileName from cfg.
// If profileName is empty, DefaultProfile is used when set.
func ResolveProfile(cfg Config, profileName string) (Profile, error) {
	if profileName == "" {
		profileName = cfg.DefaultProfile
	}
	if profileName == "" {
		return Profile{}, fmt.Errorf("no profile specified and no default_profile configured")
	}

	p, ok := cfg.Profiles[profileName]
	if !ok {
		return Profile{}, fmt.Errorf("profile %q not found in config", profileName)
	}
	return p, nil
}

// ResolveBrokers determines the list of brokers to use for a command invocation.
// Precedence (highest to lowest): --brokers flag, profile from config, KAFKA_BROKERS env.
func ResolveBrokers(globalBrokers []string, profileName string, cfg Config) ([]string, error) {
	if len(globalBrokers) > 0 {
		return globalBrokers, nil
	}

	if cfg.Profiles != nil {
		profile, err := ResolveProfile(cfg, profileName)
		if err == nil && len(profile.Brokers) > 0 {
			return profile.Brokers, nil
		}
	}

	if env := os.Getenv("KAFKA_BROKERS"); env != "" {
		parts := strings.Split(env, ",")
		var brokers []string
		for _, p := range parts {
			if trimmed := strings.TrimSpace(p); trimmed != "" {
				brokers = append(brokers, trimmed)
			}
		}
		if len(brokers) > 0 {
			return brokers, nil
		}
	}

	return nil, fmt.Errorf("no brokers configured; set --brokers, a profile, or KAFKA_BROKERS")
}
