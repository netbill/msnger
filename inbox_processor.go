package msnger

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type InboxHandlerFunc func(ctx context.Context, msg kafka.Message) error

type InboxProcessor interface {
	RunProcess(ctx context.Context, processID string)
	StopProcess(ctx context.Context, processID string) error

	Route(eventType string, handler InboxHandlerFunc)
}
