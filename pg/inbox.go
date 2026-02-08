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
	// InboxEventStatusPending indicates that the event is pending processing
	InboxEventStatusPending = "pending"
	// InboxEventStatusProcessing indicates this is event already processing by some worker
	InboxEventStatusProcessing = "processing"
	// InboxEventStatusProcessed indicates that the event has been successfully processed
	InboxEventStatusProcessed = "processed"
	// InboxEventStatusFailed indicates that all retry attempts have been exhausted
	InboxEventStatusFailed = "failed"
)

type InboxEvent struct {
	EventID uuid.UUID `json:"event_id"`
	Seq     uint      `json:"seq"`

	Topic     string `json:"topic"`
	Key       string `json:"key"`
	Type      string `json:"type"`
	Version   uint   `json:"version"`
	Producer  string `json:"producer"`
	Payload   []byte `json:"payload"`
	Partition uint   `json:"partition"`
	Offset    uint   `json:"offset"`

	ReservedBy *string `json:"reserved_by"`

	Status        string     `json:"status"`
	Attempts      uint       `json:"attempts"`
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

type inbox interface {
	// GetInboxEventByID retrieves event by ID
	GetInboxEventByID(ctx context.Context, id uuid.UUID) (InboxEvent, error)

	// WriteInboxEvent writes new event to inbox
	WriteInboxEvent(
		ctx context.Context,
		message kafka.Message,
	) (InboxEvent, error)

	// WriteAndReserveInboxEvent writes new event to inbox and reserves it for processing
	WriteAndReserveInboxEvent(
		ctx context.Context,
		message kafka.Message,
		workerID string,
	) (event InboxEvent, reserved bool, err error)

	// ReserveInboxEvents reserves event for processing
	ReserveInboxEvents(
		ctx context.Context,
		workerID string,
		limit uint,
	) ([]InboxEvent, error)

	// CommitInboxEvent marks event as processed
	CommitInboxEvent(ctx context.Context, workerID string, eventID uuid.UUID) error

	// DelayInboxEvent delays event processing
	DelayInboxEvent(ctx context.Context, workerID string, eventID uuid.UUID, nextAttemptAt time.Duration, reason string) error

	// FailedInboxEvent use if all retry attempts are exhausted
	FailedInboxEvent(ctx context.Context, workerID string, eventID uuid.UUID, reason string) error

	// CleanProcessingInboxEvent use for cleaning inbox events with status processing
	CleanProcessingInboxEvent(ctx context.Context) error

	// CleanProcessingInboxEventForWorker use cleaning inbox events with status processing for worker
	CleanProcessingInboxEventForWorker(ctx context.Context, workerID string) error

	// CleanFailedInboxEvent use for cleaning failed inbox events
	CleanFailedInboxEvent(ctx context.Context) error

	// CleanFailedInboxEventForWorker use cleaning inbox events with status failed for this worker
	CleanFailedInboxEventForWorker(ctx context.Context, workerID string) error

	Transaction(ctx context.Context, fn func(ctx context.Context) error) error
}
