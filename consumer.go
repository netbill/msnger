package msnger

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type InHandlerFunc func(ctx context.Context, msg kafka.Message) error

type Consumer interface {
	Run(ctx context.Context)
	Shutdown(ctx context.Context) error

	Route(eventType string, handler InHandlerFunc)
}
