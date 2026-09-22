package main

import (
	"strings"
	"testing"
	"time"
)

func TestParseLoadConfigRequiresExplicitAcknowledgement(t *testing.T) {
	_, err := parseLoadConfig(nil)
	if err == nil || !strings.Contains(err.Error(), "--acknowledge-local-load") {
		t.Fatalf("parseLoadConfig() error = %v", err)
	}
}

func TestParseLoadConfigDefaultsToRequestedBoundedTopology(t *testing.T) {
	config, err := parseLoadConfig([]string{"--acknowledge-local-load"})
	if err != nil {
		t.Fatal(err)
	}
	if config.Bots != 3000 || config.Callbacks != 15000 || config.Command != "echo" || config.StateBackend != "sqlite" ||
		config.PollInterval != 60*time.Second || config.JitterPercent != 20 ||
		config.Settle != 5*time.Second || config.Timeout != 30*time.Minute {
		t.Fatalf("parseLoadConfig() = %#v", config)
	}
}

func TestParseLoadConfigRejectsUnboundedValues(t *testing.T) {
	for _, arguments := range [][]string{
		{"--acknowledge-local-load", "--bots", "4097"},
		{"--acknowledge-local-load", "--callbacks", "100001"},
		{"--acknowledge-local-load", "--callbacks", "0"},
		{"--acknowledge-local-load", "--command", "shell"},
		{"--acknowledge-local-load", "--state-backend", "unknown"},
		{"--acknowledge-local-load", "--poll-interval", "99ms"},
		{"--acknowledge-local-load", "--poll-interval", "121s"},
		{"--acknowledge-local-load", "--jitter-percent", "51"},
		{"--acknowledge-local-load", "--timeout", "9s"},
		{"--acknowledge-local-load", "--bots", "1", "--callbacks", "10001"},
	} {
		if _, err := parseLoadConfig(arguments); err == nil {
			t.Errorf("parseLoadConfig(%q) unexpectedly succeeded", arguments)
		}
	}
}
