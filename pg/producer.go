package pg

import (
	"context"
	"fmt"
	"time"

	"github.com/netbill/eventbox"
	"github.com/netbill/pgdbx"
	"github.com/segmentio/kafka-go"
)

type ProducerConfig struct {
	//Brokers is a list of Kafka broker addresses in the format "host:port".
	Brokers []string

	// RequiredAcks specifies the acknowledgment level for produced messages.
	RequiredAcks string

	// Compression specifies the compression algorithm to use for messages.
	Compression string

	// Balancer specifies the partitioning strategy for messages.
	Balancer string

	// BatchSize is the maximum number of messages to batch together before sending to Kafka.
	BatchSize int

	// BatchTimeout is the maximum duration to wait before sending a batch of messages to Kafka.
	BatchTimeout time.Duration

	// DialTimeout is the maximum duration to wait when establishing a connection to Kafka.
	DialTimeout time.Duration

	// IdleTimeout is the maximum duration to keep idle connections to Kafka before closing them.
	IdleTimeout time.Duration
}

type producer struct {
	writer *kafka.Writer
	outbox outbox
}

func NewProducer(
	db *pgdbx.DB,
	config ProducerConfig,
) eventbox.Producer {
	writer := &kafka.Writer{
		Addr:         kafka.TCP(config.Brokers...),
		RequiredAcks: parseRequiredAcks(config.RequiredAcks),
		Compression:  parseCompression(config.Compression),
		Balancer:     parseBalancer(config.Balancer),
		BatchSize:    config.BatchSize,
		BatchTimeout: config.BatchTimeout,
	}

	if config.DialTimeout > 0 || config.IdleTimeout > 0 {
		writer.Transport = &kafka.Transport{
			DialTimeout: config.DialTimeout,
			IdleTimeout: config.IdleTimeout,
		}
	}

	return &producer{
		writer: writer,
		outbox: outbox{db: db},
	}
}

// WriteToKafka writes the message directly to Kafka.
// It should be used when the message is not critical and can be lost in case of failure.
func (p *producer) WriteToKafka(
	ctx context.Context,
	params eventbox.Message,
) error {
	err := p.writer.WriteMessages(ctx, kafka.Message{
		Topic:   params.Topic,
		Key:     params.Key,
		Value:   params.Payload,
		Headers: params.Headers.ToKafka(),
	})
	if err != nil {
		return fmt.Errorf("write message to kafka: %w", err)
	}

	return nil
}

// WriteToOutbox writes the message to the outbox table.
func (p *producer) WriteToOutbox(
	ctx context.Context,
	params eventbox.Message,
) error {
	_, err := p.outbox.WriteOutboxEvent(ctx, kafka.Message{
		Topic:   params.Topic,
		Key:     params.Key,
		Value:   params.Payload,
		Headers: params.Headers.ToKafka(),
	})
	if err != nil {
		return fmt.Errorf("write outbox event: %w", err)
	}

	return nil
}

// Close closes the Kafka writer.
func (p *producer) Close() error {
	if err := p.writer.Close(); err != nil {
		return fmt.Errorf("close writer: %w", err)
	}

	return nil
}

func parseRequiredAcks(v string) kafka.RequiredAcks {
	switch v {
	case "none":
		return kafka.RequireNone
	case "one":
		return kafka.RequireOne
	default:
		return kafka.RequireAll
	}
}

func parseCompression(v string) kafka.Compression {
	switch v {
	case "gzip":
		return kafka.Gzip
	case "lz4":
		return kafka.Lz4
	case "zstd":
		return kafka.Zstd
	case "none":
		return 0
	default:
		return kafka.Snappy
	}
}

func parseBalancer(v string) kafka.Balancer {
	switch v {
	case "round_robin":
		return &kafka.RoundRobin{}
	case "hash":
		return &kafka.Hash{}
	default:
		return &kafka.LeastBytes{}
	}
}
