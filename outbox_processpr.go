package msnger

import (
	"context"
)

type OutboxProcessor interface {
	RunProcess(ctx context.Context, processID string)
	StopProcess(ctx context.Context, processID string) error
}
