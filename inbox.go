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
	// InboxEventStatusPending indicates that the event is pending processing
	InboxEventStatusPending = "pending"
	// InboxEventStatusProcessing indicates this is event already processing by some processor
	InboxEventStatusProcessing = "processing"
	// InboxEventStatusProcessed indicates that the event has been successfully processed
	InboxEventStatusProcessed = "processed"
	// InboxEventStatusFailed indicates that all retry attempts have been exhausted
	InboxEventStatusFailed = "failed"
)

// InboxEvent represents an event stored in the inbox for later processing by workers.
type InboxEvent struct {
	EventID uuid.UUID `json:"event_id"`
	Seq     int64     `json:"seq"`

	Topic     string `json:"topic"`
	Key       string `json:"key"`
	Type      string `json:"type"`
	Version   int32  `json:"version"`
	Producer  string `json:"producer"`
	Payload   []byte `json:"payload"`
	Partition int32  `json:"partition"`
	Offset    int64  `json:"offset"`

	ReservedBy *string `json:"reserved_by"`

	Status        string     `json:"status"`
	Attempts      int32      `json:"attempts"`
	NextAttemptAt time.Time  `json:"next_attempt_at"`
	LastAttemptAt *time.Time `json:"last_attempt_at"`
	LastError     *string    `json:"last_error"`

	ProcessedAt *time.Time `json:"processed_at"`
	ProducedAt  time.Time  `json:"produced_at"`
	CreatedAt   time.Time  `json:"created_at"`
}

func (e *InboxEvent) ToKafkaMessage() kafka.Message {
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
				Value: []byte(strconv.FormatInt(int64(e.Version), 10)),
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

var (
	ErrInboxEventAlreadyExists = errors.New("inbox event with the same ID already exists")
	ErrInboxEventNotFound      = errors.New("inbox event not found")
)

type Inbox interface {
	// WriteInboxEvent writes a new event to the inbox. It returns an error if an event with the same ID already exists.
	WriteInboxEvent(
		ctx context.Context,
		message kafka.Message,
	) (InboxEvent, error)

	// GetInboxEventByID retrieves an inbox event by its ID. It returns an error if the event is not found.
	GetInboxEventByID(
		ctx context.Context,
		id uuid.UUID,
	) (InboxEvent, error)

	// ReserveInboxEvents reserves a batch of pending inbox events for processing by a worker.
	// reserved events will have their status updated to "processing" and will be associated with the worker ID.
	ReserveInboxEvents(
		ctx context.Context,
		workerID string,
		limit int,
	) ([]InboxEvent, error)

	// CommitInboxEvent marks the specified inbox event as successfully processed by the worker.
	// It updates the event's status to "processed" and records the processing time.
	CommitInboxEvent(
		ctx context.Context,
		workerID string,
		eventID uuid.UUID,
	) (InboxEvent, error)

	// DelayInboxEvent marks the specified inbox event as failed and schedules it for a retry at the specified time.
	// It updates the event's status to "pending", increments the attempt count, and records the reason for the delay.
	DelayInboxEvent(
		ctx context.Context,
		workerID string,
		eventID uuid.UUID,
		reason string,
		nextAttemptAt time.Time,
	) (InboxEvent, error)

	// FailedInboxEvent marks the specified inbox event as failed after exhausting all retry attempts.
	// It updates the event's status to "failed", increments the attempt count, and records the reason for the failure.
	FailedInboxEvent(
		ctx context.Context,
		workerID string,
		eventID uuid.UUID,
		reason string,
	) (InboxEvent, error)

	// CleanProcessingInboxEvents releases any inbox events that are currently reserved by the specified worker IDs,
	CleanProcessingInboxEvents(
		ctx context.Context,
		workerIDs ...string,
	) error

	// CleanFailedInboxEvents removes all inbox events that are marked as failed and have exhausted all retry attempts.
	CleanFailedInboxEvents(ctx context.Context) error
}
