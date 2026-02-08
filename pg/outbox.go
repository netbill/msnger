package pg

import (
	"context"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/msnger/headers"
	"github.com/segmentio/kafka-go"
)

const (
	// OutboxEventStatusPending defines pending event status
	OutboxEventStatusPending = "pending"
	// OutboxEventStatusProcessing  indicates this is event already processing by some worker
	OutboxEventStatusProcessing = "processing"
	// OutboxEventStatusSent defines sent event status
	OutboxEventStatusSent = "sent"
	// OutboxEventStatusFailed defines failed event status
	OutboxEventStatusFailed = "failed"
)

type OutboxEvent struct {
	EventID uuid.UUID `json:"event_id"`
	Seq     uint      `json:"seq"`

	Topic    string `json:"topic"`
	Key      string `json:"key"`
	Type     string `json:"type"`
	Version  uint   `json:"version"`
	Producer string `json:"producer"`
	Payload  []byte `json:"payload"`

	ReservedBy *string `json:"reserved_by"`

	Status        string     `json:"status"`
	Attempts      uint       `json:"attempts"`
	NextAttemptAt time.Time  `json:"next_attempt_at"`
	LastAttemptAt *time.Time `json:"last_attempt_at"`
	LastError     *string    `json:"last_error"`

	SentAt    *time.Time `json:"sent_at"`
	CreatedAt time.Time  `json:"created_at"`
}

func (e *OutboxEvent) ToKafkaMessage() kafka.Message {
	return kafka.Message{
		Topic: e.Topic,
		Key:   []byte(e.Key),
		Value: e.Payload,
		Headers: []kafka.Header{
			{
				Key:   headers.EventID,
				Value: []byte(e.EventID.String()),
			},
			{
				Key:   headers.EventType,
				Value: []byte(e.Type),
			},
			{
				Key:   headers.EventVersion,
				Value: []byte(strconv.FormatUint(uint64(e.Version), 10)),
			},
			{
				Key:   headers.Producer,
				Value: []byte(e.Producer),
			},
			{
				Key:   headers.ContentType,
				Value: []byte("application/json"),
			},
		},
	}
}

type DelayOutboxEventData struct {
	NextAttemptAt time.Time // this data for field "next_attempt_at" in outbox_events table
	Reason        string    // this data for field "last_error" in outbox_events table
}

type CommitOutboxEventParams struct {
	SentAt time.Time // this data for field "sent_at" in outbox_events table
}

type outbox interface {
	// WriteOutboxEvent writes new event to outbox, fields "event_id", "type", "version", "producer"
	// are taken from kafka message headers, "topic", "key", "payload" - from kafka message fields.
	//
	// This method sets:
	// - "status"         to OutboxEventStatusPending
	// - "attempts"       to 0
	// - "next_attempt_at" to current time
	WriteOutboxEvent(
		ctx context.Context,
		message kafka.Message,
	) (OutboxEvent, error)

	// WriteAndReserveOutboxEvent writes new event to outbox and reserves it for processing.
	// This method is similar to WriteOutboxEvent, but also sets:
	// - "status"      to OutboxEventStatusProcessing
	// - "reserved_by" to workerID
	//
	// If event already exists and:
	// - "status"      is OutboxEventStatusPending
	// - "reserved_by" IS NULL
	// this method reserves existing event for processing by updating:
	// - "status"      to OutboxEventStatusProcessing
	// - "reserved_by" to workerID
	// and returns reserved = true.
	//
	// Otherwise this method does not reserve event and returns reserved = false with existing event.
	WriteAndReserveOutboxEvent(
		ctx context.Context,
		message kafka.Message,
		workerID string,
	) (OutboxEvent, bool, error)

	// GetOutboxEventByID retrieves event by ID.
	// Returns typed error if event not found.
	GetOutboxEventByID(ctx context.Context, id uuid.UUID) (OutboxEvent, error)

	// ReserveOutboxEvents reserves events for processing.
	// This method selects events with:
	// - "status"          = OutboxEventStatusPending
	// - "next_attempt_at" <= current time
	// Orders by "seq" ascending and limits by "limit" parameter.
	//
	// This method updates selected events:
	// - "status"      to OutboxEventStatusProcessing
	// - "reserved_by" to workerID
	ReserveOutboxEvents(
		ctx context.Context,
		workerID string,
		limit uint,
	) ([]OutboxEvent, error)

	// CommitOutboxEvents marks events as sent.
	// This method updates events with ids from "events" map and sets:
	// - "status"          to OutboxEventStatusSent
	// - "sent_at"         to events[eventID].SentAt
	// - "last_attempt_at" to events[eventID].SentAt
	// - clears "reserved_by"
	//
	// This method updates only events with:
	// - "status"      = OutboxEventStatusProcessing
	// - "reserved_by" = workerID
	CommitOutboxEvents(
		ctx context.Context,
		workerID string,
		events map[uuid.UUID]CommitOutboxEventParams,
	) error

	// CommitOutboxEvent marks event as sent.
	// This method does the same thing as CommitOutboxEvents, but for one event.
	// Returns updated event.
	CommitOutboxEvent(
		ctx context.Context,
		eventID uuid.UUID,
		workerID string,
		data CommitOutboxEventParams,
	) (OutboxEvent, error)

	// DelayOutboxEvents delays events for future processing.
	// This method updates events with ids from "events" map and sets:
	// - "status"          to OutboxEventStatusPending
	// - increments "attempts" by 1
	// - sets "last_attempt_at" to current time
	// - sets "next_attempt_at" to events[eventID].NextAttemptAt
	// - sets "last_error"      to events[eventID].Reason
	// - clears "reserved_by"
	//
	// This method updates only events with:
	// - "status"      = OutboxEventStatusProcessing
	// - "reserved_by" = workerID
	DelayOutboxEvents(
		ctx context.Context,
		workerID string,
		events map[uuid.UUID]DelayOutboxEventData,
	) error

	// DelayOutboxEvent delays event processing.
	// This method does the same thing as DelayOutboxEvents, but for one event.
	DelayOutboxEvent(
		ctx context.Context,
		workerID string,
		eventID uuid.UUID,
		data DelayOutboxEventData,
	) error

	// CleanProcessingOutboxEvent cleans events with "status" OutboxEventStatusProcessing.
	// This method updates events and sets:
	// - "status"          to OutboxEventStatusPending
	// - clears "reserved_by"
	// - sets "next_attempt_at" to current time
	CleanProcessingOutboxEvent(ctx context.Context) error

	// CleanProcessingOutboxEventForWorker is similar to CleanProcessingOutboxEvent,
	// but only for events with "reserved_by" equal to workerID.
	CleanProcessingOutboxEventForWorker(ctx context.Context, workerID string) error

	// CleanFailedOutboxEvent cleans events with "status" OutboxEventStatusFailed.
	// This method updates events and sets:
	// - "status"          to OutboxEventStatusPending
	// - clears "reserved_by"
	// - sets "next_attempt_at" to current time
	CleanFailedOutboxEvent(ctx context.Context) error

	// CleanFailedOutboxEventForWorker is similar to CleanFailedOutboxEvent,
	// but only for events with "reserved_by" equal to workerID.
	CleanFailedOutboxEventForWorker(ctx context.Context, workerID string) error
}
