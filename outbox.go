package eventbox

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/eventbox/headers"
	"github.com/segmentio/kafka-go"
)

const (
	// OutboxEventStatusPending defines pending event status
	OutboxEventStatusPending = "pending"
	// OutboxEventStatusProcessing  indicates this is event already processing by some processor
	OutboxEventStatusProcessing = "processing"
	// OutboxEventStatusSent defines sent event status
	OutboxEventStatusSent = "sent"
	// OutboxEventStatusFailed defines failed event status
	OutboxEventStatusFailed = "failed"
)

// OutboxEvent represents an event stored in the outbox for later processing and sending to Kafka.
type OutboxEvent struct {
	EventID uuid.UUID `json:"event_id"`
	Seq     int64     `json:"seq"`

	Topic    string `json:"topic"`
	Key      string `json:"key"`
	Type     string `json:"type"`
	Version  int32  `json:"version"`
	Producer string `json:"producer"`
	Payload  []byte `json:"payload"`

	ReservedBy *string `json:"reserved_by"`

	Status        string     `json:"status"`
	Attempts      int32      `json:"attempts"`
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
			{Key: headers.EventID, Value: []byte(e.EventID.String())},
			{Key: headers.EventType, Value: []byte(e.Type)},
			{Key: headers.EventVersion, Value: []byte(strconv.FormatInt(int64(e.Version), 10))},
			{Key: headers.Producer, Value: []byte(e.Producer)},
			{Key: headers.ContentType, Value: []byte("application/json")},
		},
	}
}

var (
	// ErrOutboxEventAlreadyExists TODO maybe its useless
	ErrOutboxEventAlreadyExists = errors.New("outbox event with the same ID already exists")
	ErrOutboxEventNotFound      = errors.New("outbox event not found")
)

type DelayOutboxEventData struct {
	NextAttemptAt time.Time // this data for field "next_attempt_at" in outbox_events table
	LastAttemptAt time.Time // this data for field "last_attempt_at" in outbox_events table
	Reason        string    // this data for field "last_error" in outbox_events table
}

type CommitOutboxEventParams struct {
	SentAt time.Time // this data for field "sent_at" in outbox_events table
}

type FailedOutboxEventData struct {
	LastAttemptAt time.Time // this data for field "last_attempt_at" in outbox_events table
	Reason        string    // this data for field "last_error" in outbox_events table
}

type Outbox interface {
	// WriteOutboxEvent writes a new event to the outbox. It returns an error if an event with the same ID already exists.
	WriteOutboxEvent(
		ctx context.Context,
		message Message,
	) (OutboxEvent, error)

	// GetOutboxEventByID retrieves an outbox event by its ID. It returns an error if the event is not found.
	GetOutboxEventByID(
		ctx context.Context,
		id uuid.UUID,
	) (OutboxEvent, error)

	// ReserveOutboxEvents reserves a batch of outbox events for processing by a worker.
	// where workerId is nil and next_attempt_at is in the past.
	ReserveOutboxEvents(
		ctx context.Context,
		workerID string,
		limit int,
	) ([]OutboxEvent, error)

	// CommitOutboxEvents commits the processing of a batch of outbox events by a worker, marking them as sent.
	CommitOutboxEvents(
		ctx context.Context,
		workerID string,
		events map[uuid.UUID]CommitOutboxEventParams,
	) error

	// DelayOutboxEvents delays the processing of a batch of outbox events by a worker, updating their next attempt time and last error.
	DelayOutboxEvents(
		ctx context.Context,
		workerID string,
		events map[uuid.UUID]DelayOutboxEventData,
	) error

	// FailedOutboxEvents marks a batch of outbox events as failed by a worker, updating their last attempt time and last error.
	FailedOutboxEvents(
		ctx context.Context,
		workerID string,
		events map[uuid.UUID]FailedOutboxEventData,
	) error

	// CleanProcessingOutboxEvents cleans up outbox events that have been reserved by workers but not processed within a reasonable time frame, making them available for reservation again.
	CleanProcessingOutboxEvents(
		ctx context.Context,
		workerIDs ...string,
	) error

	// CleanFailedOutboxEvents cleans up outbox events that have been marked as failed but not processed within a reasonable time frame, making them available for reservation again.
	CleanFailedOutboxEvents(ctx context.Context) error
}
