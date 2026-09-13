package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/iryzhkov/t3-steward/internal/directoryresource"
)

// cmdInspectDirectory is a read-only host diagnostic, not resource enrollment.
func cmdInspectDirectory(args []string) error {
	flags := flag.NewFlagSet("worker inspect-directory", flag.ContinueOnError)
	registrationPath := flags.String("registration", "", "operator candidate registration JSON")
	expectedPath := flags.String("expected", "", "previous identity JSON to revalidate")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *registrationPath == "" || flags.NArg() != 0 {
		return errors.New("worker inspect-directory requires --registration FILE [--expected FILE]")
	}
	var registration directoryresource.Registration
	if err := readDirectoryJSON(*registrationPath, &registration); err != nil {
		return err
	}
	if *expectedPath != "" {
		var expected directoryresource.Identity
		if err := readDirectoryJSON(*expectedPath, &expected); err != nil {
			return err
		}
		f, err := directoryresource.Reopen(expected, registration)
		if err != nil {
			return err
		}
		f.Close()
		return json.NewEncoder(os.Stdout).Encode(expected)
	}
	f, identity, err := directoryresource.Open(registration)
	if err != nil {
		return err
	}
	defer f.Close()
	return json.NewEncoder(os.Stdout).Encode(identity)
}

func readDirectoryJSON(path string, target any) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	raw, err := io.ReadAll(io.LimitReader(f, (1<<20)+1))
	if err != nil {
		return err
	}
	if len(raw) > 1<<20 {
		return errors.New("directory evidence exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("directory evidence %s: %w", path, err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("directory evidence %s: trailing or oversized JSON", path)
	}
	return nil
}
