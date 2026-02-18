package eventbox

import (
	"context"
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

	ProcessedAt *time.Time `json:"processedAt"`
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

// TopicConsumerConfig holds the configuration for the Consumer.
type TopicConsumerConfig struct {
	// Instances defines the number of consumer instances (Kafka readers)
	// that will subscribe to the same topic within the same group.
	Instances int

	//GroupID is the Kafka consumer group ID to use when subscribing to topics.
	GroupID string

	// Brokers is the list of Kafka broker addresses to connect to.
	Brokers []string

	// MinBytes is the minimum number of bytes to fetch in a single request to Kafka.
	MinBytes int
	// MaxBytes is the maximum number of bytes to fetch in a single request to Kafka.
	MaxBytes int

	// MaxWait is the maximum amount of time to wait for new messages from Kafka before returning an empty batch.
	MaxWait time.Duration

	// CommitInterval is the interval at which to commit messages in Kafka.
	CommitInterval time.Duration

	// StartOffset is the offset from which to start consuming messages in Kafka.
	StartOffset string

	// QueueCapacity is the capacity of the internal queue used by the Kafka reader.
	QueueCapacity int
}

func (c *TopicConsumerConfig) CalculateStartOffset() int64 {
	switch c.StartOffset {
	case "", "last", "latest":
		return kafka.LastOffset
	case "first", "earliest":
		return kafka.FirstOffset
	default:
		return kafka.LastOffset
	}
}

type Consumer interface {
	Run(ctx context.Context)
	AddTopic(topic string, config TopicConsumerConfig)
}

type InboxHandlerFunc func(ctx context.Context, msg kafka.Message) error

type InboxWorker interface {
	Run(ctx context.Context)
	Stop(ctx context.Context)

	Route(eventType string, handler InboxHandlerFunc)
}

type InboxCleaner interface {
	CleanInboxProcessing(ctx context.Context, processIDs ...string) error

	CleanInboxFailed(ctx context.Context) error
}
