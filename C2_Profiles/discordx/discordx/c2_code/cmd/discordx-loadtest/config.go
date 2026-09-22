package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"time"
)

const (
	maximumLoadBots             = 4096
	maximumLoadCallbacks        = 100000
	maximumCallbacksPerListener = 10000
)

type loadConfig struct {
	Bots          int
	Callbacks     int
	Command       string
	StateBackend  string
	PollInterval  time.Duration
	JitterPercent int
	Settle        time.Duration
	Timeout       time.Duration
}

func parseLoadConfig(arguments []string) (loadConfig, error) {
	flags := flag.NewFlagSet("discordx-loadtest", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	acknowledge := flags.Bool("acknowledge-local-load", false, "acknowledge the local resource-intensive load test")
	bots := flags.Int("bots", 3000, "number of synthetic bot workers and listeners")
	callbacks := flags.Int("callbacks", 15000, "number of synthetic callbacks that each execute one command")
	command := flags.String("command", syntheticCommandEcho, "allow-listed synthetic command executed by every callback")
	stateBackend := flags.String("state-backend", "sqlite", "callback journal backend: sqlite or memory")
	pollInterval := flags.Duration("poll-interval", 60*time.Second, "base interval for staggered callback tasking polls")
	jitterPercent := flags.Int("jitter-percent", 20, "deterministic callback poll jitter percentage")
	settle := flags.Duration("settle", 5*time.Second, "time to hold the fully loaded topology")
	timeout := flags.Duration("timeout", 30*time.Minute, "absolute test timeout")
	if err := flags.Parse(arguments); err != nil {
		return loadConfig{}, err
	}
	if flags.NArg() != 0 {
		return loadConfig{}, errors.New("discordx load test does not accept positional arguments")
	}
	if !*acknowledge {
		return loadConfig{}, errors.New("refusing to start: pass --acknowledge-local-load to run the local resource-intensive test")
	}
	if *bots < 1 || *bots > maximumLoadBots {
		return loadConfig{}, fmt.Errorf("bots must be between 1 and %d", maximumLoadBots)
	}
	if *callbacks < 1 || *callbacks > maximumLoadCallbacks || *callbacks > *bots*maximumCallbacksPerListener {
		return loadConfig{}, fmt.Errorf("callbacks must be between 1 and %d and no more than %d per listener", maximumLoadCallbacks, maximumCallbacksPerListener)
	}
	if *command != syntheticCommandEcho {
		return loadConfig{}, fmt.Errorf("command must be %q; arbitrary operating-system commands are not supported", syntheticCommandEcho)
	}
	if *stateBackend != "memory" && *stateBackend != "sqlite" {
		return loadConfig{}, errors.New("state-backend must be memory or sqlite")
	}
	if *pollInterval < time.Second || *pollInterval > 2*time.Minute {
		return loadConfig{}, errors.New("poll-interval must be between 1s and 2m")
	}
	if *jitterPercent < 0 || *jitterPercent > 50 {
		return loadConfig{}, errors.New("jitter-percent must be between 0 and 50")
	}
	if *settle < 0 || *settle > 10*time.Minute {
		return loadConfig{}, errors.New("settle must be between 0 and 10m")
	}
	if *timeout < 10*time.Second || *timeout > 30*time.Minute || *settle >= *timeout {
		return loadConfig{}, errors.New("timeout must be between 10s and 30m and greater than settle")
	}
	return loadConfig{
		Bots: *bots, Callbacks: *callbacks, Command: *command,
		StateBackend: *stateBackend, PollInterval: *pollInterval,
		JitterPercent: *jitterPercent, Settle: *settle, Timeout: *timeout,
	}, nil
}
