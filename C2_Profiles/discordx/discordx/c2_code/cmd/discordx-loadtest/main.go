package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	load, err := parseLoadConfig(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	ctx, cancel := context.WithTimeout(context.Background(), load.Timeout)
	defer cancel()
	report, runErr := runLoadTest(ctx, load)
	if runErr != nil {
		report.Passed = false
		report.ErrorClass = "load_test_failed"
		fmt.Fprintf(os.Stderr, "discordx load test failed: %v\n", runErr)
	}
	encoded, encodeErr := json.MarshalIndent(report, "", "  ")
	if encodeErr != nil || bytes.Contains(encoded, []byte("synthetic-load-token-")) {
		fmt.Fprintln(os.Stderr, "discordx load test report generation failed")
		os.Exit(1)
	}
	fmt.Println(string(encoded))
	if runErr != nil {
		os.Exit(1)
	}
}
