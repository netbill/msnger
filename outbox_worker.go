package msnger

import (
	"context"
)

type OutboxWorker interface {
	Run(ctx context.Context, id string)
	Shutdown(ctx context.Context) error
}
