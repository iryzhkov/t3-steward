//go:build !unix

package quotatelemetry

// noFollow is unavailable here; the store path is still checked with Lstat.
const noFollow = 0
