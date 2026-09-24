//go:build !unix

package backlogadmin

// PrepareControlDir disables connection reuse where ssh multiplexing over Unix
// sockets is unavailable.
func PrepareControlDir(string) (string, error) {
	return "", nil
}
