package main

import (
	"os"

	"github.com/antonioborgerees/agentwrap/internal/cli"
)

var version = "dev"

func main() {
	os.Exit(cli.Run(cli.Config{
		Args:    os.Args[1:],
		Stdin:   os.Stdin,
		Stdout:  os.Stdout,
		Stderr:  os.Stderr,
		Version: version,
	}))
}
