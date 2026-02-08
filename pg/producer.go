package pg

import (
	"context"
	"errors"

	"github.com/google/uuid"
	"github.com/netbill/logium"
	"github.com/netbill/msnger"
	"github.com/segmentio/kafka-go"
)

type producer struct {
	id     string
	log    *logium.Logger
	writer *kafka.Writer
	outbox outbox
}

func NewProducer(
	id string,
	log *logium.Logger,
	writer *kafka.Writer,
	outbox outbox,
) msnger.Producer {
	p := &producer{
		id:     id,
		log:    log,
		writer: writer,
		outbox: outbox,
	}

	return p
}

func (p *producer) Publish(
	ctx context.Context,
	msg kafka.Message,
) error {
	return p.writer.WriteMessages(ctx, msg)
}

func (p *producer) WriteToOutbox(
	ctx context.Context,
	msg kafka.Message,
) (uuid.UUID, error) {
	event, err := p.outbox.WriteAndReserveOutboxEvent(ctx, msg, p.id)
	if err != nil {
		return uuid.Nil, err
	}

	return event.EventID, nil
}

func (p *producer) WriteToOutboxAndReserve(
	ctx context.Context,
	msg kafka.Message,
) (uuid.UUID, error) {
	event, err := p.outbox.WriteAndReserveOutboxEvent(ctx, msg, p.id)
	if err != nil {
		return uuid.Nil, err
	}

	return event.EventID, nil
}

func (p *producer) SendFromOutbox(
	ctx context.Context,
	messageID uuid.UUID,
) error {
	_, err := p.outbox.CommitOutboxEvent(ctx, messageID, p.id)
	if err != nil {
		return err
	}

	return nil
}

func (p *producer) Shutdown(ctx context.Context) error {
	var errs []error

	if err := p.writer.Close(); err != nil {
		p.log.WithError(err).Error("failed to close kafka writer")
		errs = append(errs, err)
	}

	if err := p.outbox.CleanProcessingOutboxEventForWorker(ctx, p.id); err != nil {
		p.log.WithError(err).Error("failed to clean processing outbox events for producer")
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
