//go:build !linux && !darwin

package main

// exchangeDirectories has no single-rename exchange on this operating system,
// so the result directory is replaced in two renames.
func exchangeDirectories(from, to string) error {
	return errExchangeUnsupported
}
