package main

import (
	"fmt"
	"os"
	"strings"
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
