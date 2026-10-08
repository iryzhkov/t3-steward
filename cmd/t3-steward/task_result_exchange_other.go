//go:build !linux && !darwin

package main

// exchangeDirectories has no single-rename exchange on this operating system,
// so nonempty replacement is refused; initial publication can rename atomically.
func exchangeDirectories(from, to string) error {
	return errExchangeUnsupported
}
