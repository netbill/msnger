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
	"github.com/segmentio/kafka-go"
)

// WriterTopicConfig defines the configuration for a Kafka writer associated with a specific topic.
type WriterTopicConfig struct {
	Balancer string

	MaxAttempts int

	WriteBackoffMin time.Duration
	WriteBackoffMax time.Duration

	BatchSize    int
	BatchBytes   int64
	BatchTimeout time.Duration

	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	RequiredAcks string

	Async bool

	Compression string
}

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
	addr    net.Addr
	writers map[string]*kafka.Writer
}

// NewProducer creates a new Producer instance with the given Kafka broker addresses.
func NewProducer(addr ...string) *Producer {
	return &Producer{
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

	err := writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.Key),
		Value: msg.Payload,
		Headers: []kafka.Header{
			{Key: headers.EventID, Value: []byte(msg.ID.String())},
			{Key: headers.EventType, Value: []byte(msg.Type)},
			{Key: headers.EventVersion, Value: []byte(strconv.FormatInt(int64(msg.Version), 10))},
			{Key: headers.Producer, Value: []byte(msg.Producer)},
			{Key: headers.ContentType, Value: []byte("application/json")},
		},
	})
	if err != nil {
		return fmt.Errorf("write message to kafka: %w", err)
	}

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

	if len(errs) == 0 {
		return nil
	}

	return errors.Join(errs...)
}
