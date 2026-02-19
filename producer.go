package eventbox

import (
	"context"
	"errors"
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/netbill/eventbox/headers"
	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writers map[string]*kafka.Writer
}

func NewProducer() *Producer {
	return &Producer{
		writers: make(map[string]*kafka.Writer),
	}
}

// AddTopic adds a Kafka writer for the specified topic.
func (p *Producer) AddTopic(topic string, w *kafka.Writer) error {
	if w == nil {
		return fmt.Errorf("nil writer for topic %s", topic)
	}
	if topic == "" {
		return fmt.Errorf("empty topic")
	}
	if w.Topic != "" && w.Topic != topic {
		return fmt.Errorf("writer topic mismatch: map=%q writer=%q", topic, w.Topic)
	}
	if _, ok := p.writers[topic]; ok {
		return fmt.Errorf("writer for topic %s already exists", topic)
	}
	if w.Topic == "" {
		w.Topic = topic
	}
	p.writers[topic] = w
	return nil
}

type Event struct {
	ID       uuid.UUID `json:"event_id"`
	Type     string    `json:"type"`
	Version  int32     `json:"version"`
	Topic    string    `json:"topic"`
	Key      string    `json:"key"`
	Producer string    `json:"producer"`
	Payload  []byte    `json:"payload"`
}

// WriteToKafka writes the message directly to Kafka.
// It should be used when the message is not critical and can be lost in case of failure.
func (p *Producer) WriteToKafka(
	ctx context.Context,
	msg Event,
) error {
	writer := p.writers[msg.Topic]
	if writer == nil {
		return fmt.Errorf("writer for topic %s not found", msg.Topic)
	}

	err := writer.WriteMessages(ctx, kafka.Message{
		Topic: msg.Topic,
		Key:   []byte(msg.Key),
		Value: msg.Payload,
		Headers: []kafka.Header{
			{
				Key:   headers.EventID,
				Value: []byte(msg.ID.String()),
			},
			{
				Key:   headers.EventType,
				Value: []byte(msg.Type),
			},
			{
				Key:   headers.EventVersion,
				Value: []byte(strconv.FormatInt(int64(msg.Version), 10)),
			},
			{
				Key:   headers.Producer,
				Value: []byte(msg.Producer),
			},
			{
				Key:   headers.ContentType,
				Value: []byte("application/json"),
			},
		},
	})
	if err != nil {
		return fmt.Errorf("write message to kafka: %w", err)
	}

	return nil
}

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
