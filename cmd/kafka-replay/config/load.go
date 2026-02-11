package config

import (
	"fmt"
	"os"
	"path/filepath"

	"gopkg.in/yaml.v3"
)

// ConfigFileNameInCurrentDir is the config file name looked for in the current directory (higher priority than default path).
const ConfigFileNameInCurrentDir = "kafka-replay.yaml"

// DefaultConfigPath returns the default config file path (~/.kafka-replay/config.yaml).
// Returns empty string if UserHomeDir fails.
func DefaultConfigPath() string {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return ""
	}
	return filepath.Join(homeDir, ".kafka-replay", "config.yaml")
}

// ResolveConfigPath returns the path used when path is empty: first kafka-replay.yaml in the current directory (if present), then default path.
func ResolveConfigPath() (string, error) {
	wd, err := os.Getwd()
	if err != nil {
		return "", fmt.Errorf("unable to get current directory: %w", err)
	}
	currentDirConfig := filepath.Join(wd, ConfigFileNameInCurrentDir)
	if _, err := os.Stat(currentDirConfig); err == nil {
		return currentDirConfig, nil
	}
	p := DefaultConfigPath()
	if p == "" {
		return "", fmt.Errorf("unable to determine home directory for default config path: %w", os.ErrInvalid)
	}
	return p, nil
}

// LoadConfig loads configuration from the given path.
// If the path is empty, the path is resolved: first kafka-replay.yaml in the current directory (if present), then ~/.kafka-replay/config.yaml.
// If the resolved file does not exist, an empty config is returned.
func LoadConfig(path string) (Config, error) {
	if path == "" {
		var err error
		path, err = ResolveConfigPath()
		if err != nil {
			return Config{}, err
		}
	}

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return Config{}, nil
		}
		return Config{}, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return Config{}, fmt.Errorf("failed to parse config file %s: %w", path, err)
	}
	return cfg, nil
}
