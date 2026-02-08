package msnger

import (
	"context"
)

type InboxWorker interface {
	Run(ctx context.Context)
	Shutdown(ctx context.Context) error

	Route(eventType string, handler InHandlerFunc)
}
