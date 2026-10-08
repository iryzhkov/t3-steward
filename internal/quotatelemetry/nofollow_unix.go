//go:build unix

package quotatelemetry

import "syscall"

// noFollow makes creating the store file fail on a symlink.
const noFollow = syscall.O_NOFOLLOW
