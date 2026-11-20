// Command teamscache reads the local Microsoft Teams (v2) message cache.
package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/stn1slv/teamscache/internal/cli"
)

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := cli.Execute(ctx, version, os.Args[1:])
	stop()
	os.Exit(code)
}
