package main

import (
	"context"
	"errors"
	"flag"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"syscall"

	"github.com/iryzhkov/t3-steward/internal/providercontainment"
)

// Operator qualification entry point. The normal worker remains fail-closed
// until supervisor identity/recovery and scoped T3 control are integrated.
func cmdContainedExec(args []string) error {
	flags := flag.NewFlagSet("worker contained-exec", flag.ContinueOnError)
	path := flags.String("spec", "", "operator-owned containment launch JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" || flags.NArg() != 0 {
		return errors.New("contained-exec requires --spec FILE")
	}
	var spec providercontainment.Spec
	if err := readDirectoryJSON(*path, &spec); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return providercontainment.Run(ctx, spec, providercontainment.Streams{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr})
}

func cmdContainedChild(args []string) error {
	if len(args) < 2 || args[0] != "--" {
		return errors.New("contained-child requires -- COMMAND")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	listener, err := net.Listen("tcp", "127.0.0.1:18080")
	if err != nil {
		return err
	}
	defer listener.Close()
	go func() { _ = providercontainment.Bridge(ctx, listener, "/run/provider-egress.sock") }()
	cmd := exec.CommandContext(ctx, args[1], args[2:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	return cmd.Run()
}
