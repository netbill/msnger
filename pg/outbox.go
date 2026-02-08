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
	// WriteOutboxEvent writes new event to outbox, fields "event_id", "type", "version", "producer",
	// take from kafka message headers, "topic", "key", "payload" - from kafka message fields,
	// also field "next_attempt_at" will be set to current time, and "status" - OutboxEventStatusPending
	WriteOutboxEvent(
		ctx context.Context,
		message kafka.Message,
	) (OutboxEvent, error)

	// WriteAndReserveOutboxEvent writes new event to outbox and reserves it for processing,
	// does the same thing as WriteOutboxEvent, but set "status" to OutboxEventStatusProcessing and
	// set "reserved_by" to workerID,
	// if event its already exists, and its "status" is OutboxEventStatusPending and reserved_by is null,
	// this method will update "status" to OutboxEventStatusProcessing and set "reserved_by" to workerID and
	// return this event with reserved = true, otherwise - return existing event with reserved = false
	WriteAndReserveOutboxEvent(
		ctx context.Context,
		message kafka.Message,
		workerID string,
	) (event OutboxEvent, reserved bool, err error)

	// GetOutboxEventByID get event by ID
	GetOutboxEventByID(ctx context.Context, id uuid.UUID) (OutboxEvent, error)

	// ReserveOutboxEvents reserves a batch of events for
	// with status OutboxEventStatusPending and "next_attempt_at" less than current time,
	// updates their "status" to OutboxEventStatusProcessing, sets "reserved_by" to workerID
	ReserveOutboxEvents(
		ctx context.Context,
		workerID string,
		limit uint,
	) ([]OutboxEvent, error)

	// CommitOutboxEvents set field "status" to OutboxEventStatusSent, "sent_at" to SentAt field from map value,
	// this method updates events with ids from events map, sets "status" to sent, "sent_at" to SentAt field from map value,
	// and "reserved_by" to null, but only if "reserved_by" is equal to workerID and "status" is OutboxEventStatusProcessing
	CommitOutboxEvents(
		ctx context.Context,
		workerID string,
		events map[uuid.UUID]CommitOutboxEventParams,
	) error

	// CommitOutboxEvent marks event as sent this method does the same thing as CommitOutboxEvents, but for one event
	CommitOutboxEvent(
		ctx context.Context,
		eventID uuid.UUID,
		workerID string,
		data CommitOutboxEventParams,
	) (OutboxEvent, error)

	// DelayOutboxEvents delays events for future processing
	// this method updates events with ids from events map, sets "status" to OutboxEventStatusPending,
	// "next_attempt_at" to NextAttemptAt field from map value, "last_error" to Reason field from map value,
	// "reserved_by" to null, but only if "reserved_by" is equal to workerID and "status" is OutboxEventStatusProcessing
	DelayOutboxEvents(
		ctx context.Context,
		workerID string,
		events map[uuid.UUID]DelayOutboxEventData,
	) error

	// DelayOutboxEvent delays event processing this method does the same thing as DelayOutboxEvents, but for one event
	DelayOutboxEvent(
		ctx context.Context,
		workerID string,
		eventID uuid.UUID,
		data DelayOutboxEventData,
	) error

	// CleanProcessingOutboxEvent this method updates events with status OutboxEventStatusProcessing
	// to OutboxEventStatusPending and set "reserved_by" to null and set "next_attempt_at" to current time
	CleanProcessingOutboxEvent(ctx context.Context) error

	// CleanProcessingOutboxEventForWorker this method similar to CleanProcessingOutboxEvent,
	// but updates only events with "reserved_by" equal to workerID and set "next_attempt_at" to current time
	CleanProcessingOutboxEventForWorker(ctx context.Context, workerID string) error

	// CleanFailedOutboxEvent this method updates events with status OutboxEventStatusFailed
	// to OutboxEventStatusPending and set "reserved_by" to null and set "next_attempt_at" to current time
	CleanFailedOutboxEvent(ctx context.Context) error

	// CleanFailedOutboxEventForWorker  this method similar to CleanFailedOutboxEvent,
	// but updates only events with "reserved_by" equal to workerID and set "next_attempt_at" to current time
	CleanFailedOutboxEventForWorker(ctx context.Context, workerID string) error
}
