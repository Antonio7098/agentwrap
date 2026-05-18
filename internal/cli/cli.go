package cli

import (
	"fmt"
	"io"
)

// Config carries process-level dependencies into the testable CLI runner.
type Config struct {
	Args    []string
	Stdin   io.Reader
	Stdout  io.Writer
	Stderr  io.Writer
	Version string
}

// Run executes the skeleton CLI and returns a process exit code.
func Run(cfg Config) int {
	stdout := cfg.Stdout
	if stdout == nil {
		stdout = io.Discard
	}
	stderr := cfg.Stderr
	if stderr == nil {
		stderr = io.Discard
	}

	args := append([]string(nil), cfg.Args...)
	if len(args) == 0 || args[0] == "help" || args[0] == "--help" || args[0] == "-h" {
		printHelp(stdout)
		return 0
	}
	if args[0] == "version" || args[0] == "--version" || args[0] == "-v" {
		version := cfg.Version
		if version == "" {
			version = "dev"
		}
		fmt.Fprintf(stdout, "agentwrap %s\n", version)
		return 0
	}

	fmt.Fprintf(stderr, "agentwrap: unknown command %q\n", args[0])
	fmt.Fprintln(stderr, "Run 'agentwrap --help' for usage.")
	return 2
}

func printHelp(w io.Writer) {
	fmt.Fprintln(w, "agentwrap")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Usage:")
	fmt.Fprintln(w, "  agentwrap [--help]")
	fmt.Fprintln(w, "  agentwrap [--version]")
}
