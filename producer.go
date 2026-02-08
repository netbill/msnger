package msnger

import (
	"context"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

type Producer interface {
	Publish(ctx context.Context, msg kafka.Message) error

	WriteToOutbox(ctx context.Context, msg kafka.Message) (uuid.UUID, error)
	WriteToOutboxAndReserve(
		ctx context.Context,
		msg kafka.Message,
	) (eventID uuid.UUID, reserve bool, err error)

	TrySendFromOutbox(ctx context.Context, messageID uuid.UUID) error

	Shutdown(ctx context.Context) error
}
