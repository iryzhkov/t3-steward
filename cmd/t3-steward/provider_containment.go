package main

import (
	"context"
	"encoding/json"
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

// The state directory must be provisioned privately by the worker/operator and
// must survive worker restarts. These commands never infer a stop from expiry.
func cmdContainedSupervisor(action string, args []string) error {
	flags := flag.NewFlagSet("worker contained-"+action, flag.ContinueOnError)
	path := flags.String("spec", "", "operator-owned containment launch JSON")
	root := flags.String("state-dir", "", "existing private supervisor state directory")
	execution := flags.String("execution", "", "stable execution identity")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *path == "" || *root == "" || *execution == "" || flags.NArg() != 0 {
		return errors.New("contained supervisor requires --spec FILE --state-dir DIR --execution ID")
	}
	var spec providercontainment.Spec
	if err := readDirectoryJSON(*path, &spec); err != nil {
		return err
	}
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	manager := providercontainment.Supervisor{Root: *root, Executable: executable}
	launch := providercontainment.Launch{ExecutionID: *execution, Spec: spec}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var observation providercontainment.SupervisorObservation
	switch action {
	case "start":
		observation, err = manager.Start(ctx, launch)
	case "show":
		observation, err = manager.Observe(ctx, launch)
	case "stop":
		observation, err = manager.Stop(ctx, launch)
	default:
		return errors.New("unknown containment supervisor action")
	}
	if outputErr := json.NewEncoder(os.Stdout).Encode(observation); outputErr != nil {
		return outputErr
	}
	return err
}

func cmdContainedT3(args []string) error {
	flags := flag.NewFlagSet("contained-t3", flag.ContinueOnError)
	node := flags.String("node", "", "mounted Node executable")
	entry := flags.String("entry", "", "mounted T3 entry point")
	port := flags.Int("port", 0, "namespace-local API port")
	opencode := flags.String("opencode-binary", "", "mounted OpenCode binary; requires --opencode-model")
	model := flags.String("opencode-model", "", "explicit contained OpenCode model")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return errors.New("unexpected contained-t3 arguments")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	return providercontainment.RunT3(ctx, providercontainment.T3Spec{Node: *node, Entry: *entry, Port: *port, OpenCodeBinary: *opencode, OpenCodeModel: *model}, providercontainment.Streams{Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr})
}

func cmdContainedChild(args []string) error {
	flags := flag.NewFlagSet("contained-child", flag.ContinueOnError)
	egress := flags.Bool("egress", false, "enable constrained provider gateway")
	port := flags.Int("control-port", 0, "scoped namespace API port")
	if err := flags.Parse(args); err != nil {
		return err
	}
	command := flags.Args()
	if len(command) == 0 {
		return errors.New("contained-child requires -- COMMAND")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *egress {
		listener, err := net.Listen("tcp", "127.0.0.1:18080")
		if err != nil {
			return err
		}
		defer listener.Close()
		go func() {
			if err := providercontainment.Bridge(ctx, listener, "/run/provider-egress.sock"); err != nil {
				stop()
			}
		}()
	}
	if *port != 0 {
		listener, err := providercontainment.ListenControl("/control/api.sock")
		if err != nil {
			return err
		}
		defer listener.Close()
		go func() {
			if err := providercontainment.ControlBridge(ctx, listener, *port); err != nil {
				stop()
			}
		}()
	}
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = os.Environ()
	return cmd.Run()
}
