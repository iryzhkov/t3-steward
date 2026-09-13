//go:build !linux

package t3api

import (
	"errors"
	"github.com/iryzhkov/t3-steward/internal/directoryresource"
	"time"
)

func NewContained(directoryresource.Identity, time.Duration) (*Client, error) {
	return nil, errors.New("contained T3 identity transport requires Linux")
}
