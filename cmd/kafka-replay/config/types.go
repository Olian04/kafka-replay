package config

// Profile represents a named connection/profile configuration.
// For now this only contains brokers, but it can be extended with
// SASL/TLS and other options without changing the CLI surface.
type Profile struct {
	Brokers []string `json:"brokers" yaml:"brokers"`
}

// Config is the top-level configuration structure loaded from disk.
type Config struct {
	Profiles       map[string]Profile `json:"profiles" yaml:"profiles"`
	DefaultProfile string             `json:"default_profile" yaml:"default_profile"`
}
