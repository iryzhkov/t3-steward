//go:build !unix

package main

import "io/fs"

// taskIdentityFileIsOwned has no portable ownership check outside unix. The
// mode and regular-file checks still apply; this platform relies on them.
func taskIdentityFileIsOwned(fs.FileInfo) error { return nil }
