package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

// Publish sends message directly to kafka topic without using outbox.
// Use with caution,if event doesn't really matter, and you admit the risk of losing it in case of failure.
func (p *producer) Publish(
	ctx context.Context,
	msg kafka.Message,
) error {
	return p.writer.WriteMessages(ctx, msg)
}

// WriteToOutbox writes message to outbox, but doesn't reserve it for sending.
// Use it if you want to write event to outbox, but reserve it later in the same transaction with other events.
// This method should be used with handler function in transaction
func (p *producer) WriteToOutbox(
	ctx context.Context,
	msg kafka.Message,
) (uuid.UUID, error) {
	event, err := p.outbox.WriteOutboxEvent(ctx, msg)
	if err != nil {
		return uuid.Nil, err
	}

	return event.EventID, nil
}

// WriteToOutboxAndReserve this method it's similar to WriteToOutbox, but this method reserve event,
// so this method also should be used with handler function in transaction,
// it's also assumed that you can call the TrySendFromOutbox method to process the event, otherwise it will hang,
// because reserved event will not be processed by worker, and if you don't call TrySendFromOutbox,
// the event will be reserved until "reserved_by" becomes null (i.e. reserved_by is set to NULL by Delay/Commit or
// by a cleaner on restart/shutdown). For more info see TrySendFromOutbox
func (p *producer) WriteToOutboxAndReserve(
	ctx context.Context,
	msg kafka.Message,
) (eventID uuid.UUID, reserve bool, err error) {
	event, reserve, err := p.outbox.WriteAndReserveOutboxEvent(ctx, msg, p.id)
	if err != nil {
		return uuid.Nil, false, fmt.Errorf("failed to write and reserve outbox event: %w", err)
	}

	return event.EventID, reserve, nil
}

// TrySendFromOutbox sends message from outbox to kafka topic, this method should be used for processing events from outbox,
// assumed this method should because after WriteToOutboxAndReserve,
// and not in transaction with WriteToOutboxAndReserve and handleEvent function
// because this method will try to reserve event for sending, and if reservation is successful, it will send message to kafka topic,
// so if send event to kafka topic is successful, it will commit event in outbox,
// otherwise it will delay event for future processing, and if reservation is not successful,
// it will do nothing, because it's assumed that another worker will process this event,
// and if reservation is not successful because of some error, it will return error
// be careful with this method, because try to send message to kafka will fail
// and try to delay event for future processing also can fail,
// this can create a situation when event will be reserved for processing,
// but message will not be sent to kafka topic, and event will not be delayed for future processing,
// so this event will be stuck in outbox with status processing until "reserved_by" becomes null
// (i.e. reserved_by is set to NULL by Delay/Commit or by a cleaner on restart/shutdown),
// and if you have a lot of such events, it can create a problem with outbox,
// because you will have a lot of events with status processing, that can create a problem with performance of outbox,
// so it's recommended to use this method with caution, and monitor outbox for such events
func (p *producer) TrySendFromOutbox(
	ctx context.Context,
	messageID uuid.UUID,
) error {
	event, err := p.outbox.GetOutboxEventByID(ctx, messageID)
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("failed to get outbox event by id: %w", err)
	}

	var errs []error
	err = p.Publish(ctx, event.ToKafkaMessage())
	if err != nil {
		errs = append(errs, err)

		err = p.outbox.DelayOutboxEvent(ctx, p.id, messageID, DelayOutboxEventData{
			NextAttemptAt: time.Now().Add(1 * time.Minute).UTC(),
			Reason:        fmt.Sprintf("failed to publish message: %v", err),
		})
		if err != nil {
			errs = append(errs, err)
			return fmt.Errorf("publish+delay failed: %w", errors.Join(errs...))
		}

		return nil
	}

	_, err = p.outbox.CommitOutboxEvent(ctx, messageID, p.id, CommitOutboxEventParams{
		SentAt: time.Now().UTC(),
	})
	if err != nil {
		return fmt.Errorf("failed to commit outbox event: %w", err)
	}

	return nil
}

// Shutdown closes kafka writer and clean processing outbox events for producer,
// use it when you want to gracefully shutdown producer for example when you want to shut down application,
// and you want to make sure that all events that are in processing will be cleaned,
func (p *producer) Shutdown(ctx context.Context) error {
	var errs []error

	if err := p.writer.Close(); err != nil {
		p.log.WithError(err).Error("failed to close kafka writer")
		errs = append(errs, fmt.Errorf("failed to close kafka writer: %w", err))
	}

	if err := p.outbox.CleanProcessingOutboxEventForWorker(ctx, p.id); err != nil {
		p.log.WithError(err).Error("failed to clean processing outbox events for producer")
		errs = append(errs, fmt.Errorf("failed to clean processing outbox events for producer: %w", err))
	}

	return errors.Join(errs...)
}
