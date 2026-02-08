package msnger

import (
	"context"
)

type OutboxWorker interface {
	Run(ctx context.Context)
	Shutdown(ctx context.Context) error
}
