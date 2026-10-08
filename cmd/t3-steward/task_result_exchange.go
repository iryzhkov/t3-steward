package main

import "errors"

// errExchangeUnsupported reports that the filesystem or operating system
// cannot exchange two directories in one rename.
var errExchangeUnsupported = errors.New("atomic directory exchange is not supported")
