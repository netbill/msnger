package pg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/netbill/eventbox"
	"github.com/netbill/eventbox/pg/sqlc"
	"github.com/netbill/pgdbx"
)

type Outbox struct {
	db *pgdbx.DB
}

func NewOutbox(db *pgdbx.DB) eventbox.Outbox {
	return &Outbox{db: db}
}

func (b *Outbox) queries() *sqlc.Queries {
	return sqlc.New(b.db)
}

// WriteOutboxEvent writes a Kafka message to the Outbox.
// It extracts the required headers from the message and inserts a new Outbox event into the database.
// If an event with the same ID already exists, it returns an error.
func (b *Outbox) WriteOutboxEvent(
	ctx context.Context,
	event eventbox.Message,
) (eventbox.OutboxEvent, error) {
	row, err := b.queries().InsertOutboxEvent(ctx, sqlc.InsertOutboxEventParams{
		EventID: pgtype.UUID{Bytes: event.ID, Valid: true},

		Topic:    event.Topic,
		Key:      event.Key,
		Type:     event.Type,
		Version:  event.Version,
		Producer: event.Producer,
		Payload:  event.Payload,

		Status:        eventbox.OutboxEventStatusPending,
		Attempts:      0,
		NextAttemptAt: pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
	})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return eventbox.OutboxEvent{}, fmt.Errorf("outbox event with the same ID already exists: %w", eventbox.ErrOutboxEventAlreadyExists)
		}

		return eventbox.OutboxEvent{}, fmt.Errorf("insert outbox event: %w", err)
	}

	return parseOutboxEventFromSqlcRow(row), nil
}

// GetOutboxEventByID retrieves an Outbox event by its ID.
func (b *Outbox) GetOutboxEventByID(
	ctx context.Context,
	id uuid.UUID,
) (eventbox.OutboxEvent, error) {
	row, err := b.queries().GetOutboxEventByID(ctx, pgtype.UUID{Bytes: id, Valid: true})
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return eventbox.OutboxEvent{}, fmt.Errorf("outbox event not found: %w", eventbox.ErrOutboxEventNotFound)
		}

		return eventbox.OutboxEvent{}, fmt.Errorf("get Outbox event by id: %w", err)
	}

	return parseOutboxEventFromSqlcRow(row), nil
}

// ReserveOutboxEvents reserves a batch of Outbox events for processing by a specific processor.
// How it works:
// select batch of pending events ordered by created_at with limit = limit*10 + 100
// after that mark as reserved batch of events with limit = limit where id in (ids of selected events) and reserved_by is null,
// but important don't mark events which key+topic is already reserved by another processor,
// because it can cause deadlocks between processors
func (b *Outbox) ReserveOutboxEvents(
	ctx context.Context,
	workerID string,
	limit int,
) ([]eventbox.OutboxEvent, error) {
	rows, err := b.queries().ReserveOutboxEvents(ctx, sqlc.ReserveOutboxEventsParams{
		WorkerID:   pgtype.Text{String: workerID, Valid: true},
		BatchLimit: int32(limit),
		SortLimit:  int32(limit*10 + 100),
	})
	if err != nil {
		return nil, fmt.Errorf("reserve Outbox events: %w", err)
	}

	result := make([]eventbox.OutboxEvent, 0, len(rows))
	for _, row := range rows {
		result = append(result, parseOutboxEventFromSqlcRow(row))
	}

	return result, nil
}

// CommitOutboxEvents marks a batch of Outbox events as sent by a specific processor.
func (b *Outbox) CommitOutboxEvents(
	ctx context.Context,
	workerID string,
	events map[uuid.UUID]eventbox.CommitOutboxEventParams,
) error {
	eventIDs := make([]pgtype.UUID, 0, len(events))
	SentAts := make([]pgtype.Timestamptz, 0, len(events))
	for eventID, params := range events {
		eventIDs = append(eventIDs, pgtype.UUID{Bytes: eventID, Valid: true})
		SentAts = append(SentAts, pgtype.Timestamptz{Time: params.SentAt, Valid: true})
	}

	err := b.queries().MarkOutboxEventsAsSent(ctx, sqlc.MarkOutboxEventsAsSentParams{
		WorkerID: pgtype.Text{String: workerID, Valid: true},
		EventIds: eventIDs,
		SentAts:  SentAts,
	})
	if err != nil {
		return fmt.Errorf("mark Outbox events as sent: %w", err)
	}

	return nil
}

// DelayOutboxEvents marks a batch of Outbox events as pending for retry by a specific processor.
func (b *Outbox) DelayOutboxEvents(
	ctx context.Context,
	workerID string,
	events map[uuid.UUID]eventbox.DelayOutboxEventData,
) error {
	eventIDs := make([]pgtype.UUID, 0, len(events))
	nextAttemptAts := make([]pgtype.Timestamptz, 0, len(events))
	lastAttemptAts := make([]pgtype.Timestamptz, 0, len(events))
	lastErrors := make([]string, 0, len(events))
	for eventID, params := range events {
		eventIDs = append(eventIDs, pgtype.UUID{Bytes: eventID, Valid: true})
		nextAttemptAts = append(nextAttemptAts, pgtype.Timestamptz{Time: params.NextAttemptAt, Valid: true})
		lastAttemptAts = append(lastAttemptAts, pgtype.Timestamptz{Time: params.LastAttemptAt, Valid: true})
		lastErrors = append(lastErrors, params.Reason)
	}

	err := b.queries().MarkOutboxEventsAsPending(ctx, sqlc.MarkOutboxEventsAsPendingParams{
		WorkerID:       pgtype.Text{String: workerID, Valid: true},
		EventIds:       eventIDs,
		NextAttemptAts: nextAttemptAts,
		LastAttemptAts: lastAttemptAts,
		LastErrors:     lastErrors,
	})
	if err != nil {
		return fmt.Errorf("mark Outbox events as pending: %w", err)
	}

	return nil
}

// FailedOutboxEvents marks a batch of Outbox events as failed by a specific processor.
func (b *Outbox) FailedOutboxEvents(
	ctx context.Context,
	workerID string,
	events map[uuid.UUID]eventbox.FailedOutboxEventData,
) error {
	eventIDs := make([]pgtype.UUID, 0, len(events))
	lastAttemptAts := make([]pgtype.Timestamptz, 0, len(events))
	lastErrors := make([]string, 0, len(events))
	for eventID, params := range events {
		eventIDs = append(eventIDs, pgtype.UUID{Bytes: eventID, Valid: true})
		lastAttemptAts = append(lastAttemptAts, pgtype.Timestamptz{Time: params.LastAttemptAt, Valid: true})
		lastErrors = append(lastErrors, params.Reason)
	}

	err := b.queries().MarkOutboxEventsAsFailed(ctx, sqlc.MarkOutboxEventsAsFailedParams{
		WorkerID:   pgtype.Text{String: workerID, Valid: true},
		EventIds:   eventIDs,
		LastErrors: lastErrors,
	})
	if err != nil {
		return fmt.Errorf("mark Outbox events as failed: %w", err)
	}

	return nil
}

// CleanProcessingOutboxEvent cleans up Outbox events which status is processing
func (b *Outbox) CleanProcessingOutboxEvents(ctx context.Context, workerIDs ...string) error {
	if len(workerIDs) == 0 {
		err := b.queries().CleanProcessingOutboxEvents(ctx)
		if err != nil {
			return fmt.Errorf("clean processing Outbox events: %w", err)
		}
	} else {
		err := b.queries().CleanReservedProcessingOutboxEvents(ctx, workerIDs)
		if err != nil {
			return fmt.Errorf("clean processing Outbox events for processor: %w", err)
		}
	}

	return nil
}

// CleanFailedOutboxEvent cleans up Outbox events which status is failed
func (b *Outbox) CleanFailedOutboxEvents(ctx context.Context) error {
	err := b.queries().CleanFailedOutboxEvents(ctx)
	if err != nil {
		return fmt.Errorf("clean failed Outbox events: %w", err)
	}

	return nil
}

func parseOutboxEventFromSqlcRow(row sqlc.OutboxEvent) eventbox.OutboxEvent {
	event := eventbox.OutboxEvent{
		EventID: row.EventID.Bytes,
		Seq:     row.Seq,

		Topic:    row.Topic,
		Key:      row.Key,
		Type:     row.Type,
		Version:  row.Version,
		Producer: row.Producer,
		Payload:  row.Payload,

		Status:   string(row.Status),
		Attempts: row.Attempts,

		CreatedAt: pgtype.Timestamptz{Time: row.CreatedAt.Time, Valid: true}.Time,
	}

	if row.ReservedBy.Valid {
		event.ReservedBy = &row.ReservedBy.String
	}
	if row.NextAttemptAt.Valid {
		event.NextAttemptAt = row.NextAttemptAt.Time
	}
	if row.LastAttemptAt.Valid {
		event.LastAttemptAt = &row.LastAttemptAt.Time
	}
	if row.LastError.Valid {
		event.LastError = &row.LastError.String
	}
	if row.SentAt.Valid {
		event.SentAt = &row.SentAt.Time
	}

	return event
}
