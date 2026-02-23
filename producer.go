package eventbox

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/eventbox/headers"
	"github.com/netbill/logium"
	"github.com/segmentio/kafka-go"
)

// WriterTopicConfig defines the configuration for a Kafka writer associated with a specific topic.
type WriterTopicConfig struct {
	// Balancer specifies the partitioning strategy for messages sent to Kafka.
	// Valid values are:
	// - "hash": Partitions messages based on the hash of the message key (default).
	// - "leastbytes": Partitions messages to the partition with the least amount of data.
	// - "roundrobin": Partitions messages in a round-robin fashion across all partitions.
	//
	// ATTENTION: FOR OUTBOX MUST BE "hash", OTHERWISE YOU CAN LOSE ORDER OF MESSAGES WITH THE SAME KEY
	Balancer string

	// MaxAttempts specifies the maximum number of attempts to send a message before giving up.
	MaxAttempts int

	// WriteBackoffMin is the minimum duration to wait before retrying to send a message after a failure.
	WriteBackoffMin time.Duration
	// WriteBackoffMax is the maximum duration to wait before retrying to send a message after a failure.
	WriteBackoffMax time.Duration

	// BatchSize is the number of messages to batch together before sending to Kafka.
	BatchSize int
	// BatchBytes is the maximum total size of messages to batch together before sending to Kafka.
	BatchBytes int64
	// BatchTimeout is the maximum duration to wait before sending a batch of messages to Kafka,
	// even if the batch size or batch bytes thresholds have not been reached.
	BatchTimeout time.Duration

	// ReadTimeout is the maximum duration to wait for a response from the Kafka broker when sending messages.
	ReadTimeout time.Duration
	// WriteTimeout is the maximum duration to wait for a response from the Kafka broker when sending messages.
	WriteTimeout time.Duration

	// RequiredAcks specifies the acknowledgment level required from the Kafka broker for a message to be considered successfully sent.
	// Valid values are:
	// - "all" or "-1": Wait for acknowledgment from all in-sync replicas (strongest guarantee).
	// - "one" or "1": Wait for acknowledgment from the leader only (weaker guarantee).
	// - "none" or "0": Do not wait for any acknowledgment (no guarantee).
	//
	// ATTENTION FOR OUTBOX MUST BE "all" OR "-1", OTHERWISE YOU CAN LOSE MESSAGES IN CASE OF PRODUCER CRASH
	RequiredAcks string

	// Async determines whether the writer should operate in asynchronous mode.
	// If true, WriteMessages will return immediately after queuing the messages for sending,
	// without waiting for acknowledgments from the Kafka broker. If false, WriteMessages will block
	// until the messages have been acknowledged according to the RequiredAcks setting.
	//
	// ATTENTION: FOR OUTBOX MUST BE FALSE, OTHERWISE YOU CAN LOSE MESSAGES IN CASE OF PRODUCER CRASH
	Async bool

	// Compression specifies the compression algorithm to use for messages sent to Kafka.
	// Valid values are:
	// - "none": No compression (default).
	// - "gzip": Use gzip compression.
	// - "snappy": Use snappy compression.
	// - "lz4": Use lz4 compression.
	// - "zstd": Use zstd compression.
	Compression string
}

// parseRequiredAcks converts a string representation of required acknowledgments
// into the corresponding kafka.RequiredAcks value.
func parseRequiredAcks(v string) (kafka.RequiredAcks, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "all", "-1":
		return kafka.RequireAll, nil
	case "none", "0":
		return kafka.RequireNone, nil
	case "one", "1":
		return kafka.RequireOne, nil
	default:
		return 0, fmt.Errorf("invalid required_acks: %q", v)
	}
}

// parseBalancer converts a string representation of a balancer into the corresponding kafka.Balancer instance.
func parseBalancer(v string) (kafka.Balancer, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "hash":
		return &kafka.Hash{}, nil
	case "leastbytes", "least_bytes":
		return &kafka.LeastBytes{}, nil
	case "roundrobin", "round_robin":
		return &kafka.RoundRobin{}, nil
	default:
		return nil, fmt.Errorf("invalid balancer: %q", v)
	}
}

// parseCompression converts a string representation of a compression algorithm
// into the corresponding kafka.Compression value.
func parseCompression(v string) (kafka.Compression, error) {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "", "none":
		return kafka.Snappy, nil
	case "gzip":
		return kafka.Gzip, nil
	case "snappy":
		return kafka.Snappy, nil
	case "lz4":
		return kafka.Lz4, nil
	case "zstd":
		return kafka.Zstd, nil
	default:
		return 0, fmt.Errorf("invalid compression: %q", v)
	}
}

// Producer is responsible for sending messages to Kafka topics using configured writers.
type Producer struct {
	log *Logger

	addr    net.Addr
	writers map[string]*kafka.Writer
}

// NewProducer creates a new Producer instance with the given Kafka broker addresses.
func NewProducer(logger logium.Logger, addr ...string) *Producer {
	return &Producer{
		log:     NewLogger(logger),
		addr:    kafka.TCP(addr...),
		writers: make(map[string]*kafka.Writer),
	}
}

// AddWriter adds a new Kafka writer for the specified topic with the given configuration.
func (p *Producer) AddWriter(topic string, cfg WriterTopicConfig) error {
	if topic == "" {
		return fmt.Errorf("empty topic for writer")
	}

	if _, ok := p.writers[topic]; ok {
		return fmt.Errorf("writer for topic %s already exists", topic)
	}

	requiredAcks, err := parseRequiredAcks(cfg.RequiredAcks)
	if err != nil {
		return fmt.Errorf("failed to parse required_acks for topic %s: %w", topic, err)
	}

	balancer, err := parseBalancer(cfg.Balancer)
	if err != nil {
		return fmt.Errorf("failed to parse balancer for topic %s: %w", topic, err)
	}

	compression, err := parseCompression(cfg.Compression)
	if err != nil {
		return fmt.Errorf("failed to parse compression for topic %s: %w", topic, err)
	}

	p.log.WithTopic(topic).Debug("adding Kafka writer for topic")

	p.writers[topic] = &kafka.Writer{
		Addr:         p.addr,
		Topic:        topic,
		Balancer:     balancer,
		RequiredAcks: requiredAcks,
		Async:        cfg.Async,
		Compression:  compression,

		MaxAttempts:     cfg.MaxAttempts,
		WriteBackoffMin: cfg.WriteBackoffMin,
		WriteBackoffMax: cfg.WriteBackoffMax,

		BatchSize:    cfg.BatchSize,
		BatchBytes:   cfg.BatchBytes,
		BatchTimeout: cfg.BatchTimeout,

		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
	}

	return nil
}

// Message represents an event message to be sent to Kafka.
type Message struct {
	ID       uuid.UUID `json:"event_id"`
	Type     string    `json:"type"`
	Version  int32     `json:"version"`
	Topic    string    `json:"topic"`
	Key      string    `json:"key"`
	Producer string    `json:"producer"`
	Payload  []byte    `json:"payload"`
}

// WriteToKafka writes the given message to the appropriate Kafka topic using the configured writer.
func (p *Producer) WriteToKafka(ctx context.Context, msg Message) error {
	writer, ok := p.writers[msg.Topic]
	if !ok {
		return fmt.Errorf("no writer for topic %s", msg.Topic)
	}

	m := kafka.Message{
		Key:   []byte(msg.Key),
		Value: msg.Payload,
		Headers: []kafka.Header{
			{Key: headers.EventID, Value: []byte(msg.ID.String())},
			{Key: headers.EventType, Value: []byte(msg.Type)},
			{Key: headers.EventVersion, Value: []byte(strconv.FormatInt(int64(msg.Version), 10))},
			{Key: headers.Producer, Value: []byte(msg.Producer)},
			{Key: headers.ContentType, Value: []byte("application/json")},
		},
	}

	err := writer.WriteMessages(ctx, m)
	if err != nil {
		return fmt.Errorf("write message to kafka: %w", err)
	}

	p.log.WithMessage(m).Debug("writing message to Kafka")

	return nil
}

// Close closes all Kafka writers managed by the producer.
func (p *Producer) Close() error {
	var errs []error

	for topic, writer := range p.writers {
		if err := writer.Close(); err != nil {
			errs = append(errs, fmt.Errorf("close writer for topic %s: %w", topic, err))
		}
	}

	if len(errs) != 0 {
		p.log.WithError(errors.Join(errs...)).Error("failed to close kafka producer")
		return errors.Join(errs...)
	}

	p.log.Info("kafka producer closed successfully")
	return nil
}
