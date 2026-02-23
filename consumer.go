package eventbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/netbill/logium"
	"github.com/segmentio/kafka-go"
)

const (
	DefaultMinConsumerBackoff = 200 * time.Millisecond
	DefaultMaxConsumerBackoff = 5 * time.Second
)

type ConsumerConfig struct {
	// MinBackoff is the minimum duration to wait before retrying after a failure.
	// If not set, it defaults to DefaultMinConsumerBackoff.
	MinBackoff time.Duration
	// MaxBackoff is the maximum duration to wait before retrying after a failure.
	// If not set, it defaults to DefaultMaxConsumerBackoff.
	MaxBackoff time.Duration
}

// ReaderConfig defines the configuration for the Kafka reader used by the Consumer.
type ReaderConfig struct {
	Brokers   []string
	GroupID   string
	Topic     string
	Instances int

	MaxWait       time.Duration
	QueueCapacity int
	MinBytes      int
	MaxBytes      int
}

// Consumer is responsible for consuming messages from Kafka, writing them to the inbox, and committing them.
type Consumer struct {
	log     *Logger
	inbox   Inbox
	config  ConsumerConfig
	readers []*kafka.Reader
}

// NewConsumer creates a new Consumer instance with the provided logger, inbox, and configuration.
func NewConsumer(log logium.Logger, inbox Inbox, config ConsumerConfig) *Consumer {
	if config.MinBackoff <= 0 {
		config.MinBackoff = DefaultMinConsumerBackoff
	}
	if config.MaxBackoff <= 0 {
		config.MaxBackoff = DefaultMaxConsumerBackoff
	}
	if config.MaxBackoff < config.MinBackoff {
		config.MaxBackoff = config.MinBackoff
	}

	return &Consumer{
		log:    NewLogger(log),
		inbox:  inbox,
		config: config,
	}
}

func (c *Consumer) AddReader(cfg ReaderConfig) {
	for i := 0; i < cfg.Instances; i++ {
		r := kafka.NewReader(kafka.ReaderConfig{
			Brokers:       cfg.Brokers,
			GroupID:       cfg.GroupID,
			Topic:         cfg.Topic,
			MaxWait:       cfg.MaxWait,
			QueueCapacity: cfg.QueueCapacity,
			MinBytes:      cfg.MinBytes,
			MaxBytes:      cfg.MaxBytes,
		})
		c.readers = append(c.readers, r)
	}
}

func (c *Consumer) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, r := range c.readers {
		wg.Add(1)
		go func(reader *kafka.Reader) {
			defer wg.Done()
			c.subscribe(ctx, reader)
		}(r)
	}

	wg.Wait()
}

// subscribe starts the consumer to consume messages from Kafka for all configured topics.
func (c *Consumer) subscribe(ctx context.Context, reader *kafka.Reader) {
	backoff := c.config.MinBackoff

	for {
		if ctx.Err() != nil {
			return
		}

		m, err := c.fetchMessage(ctx, reader)
		if err != nil {
			if !c.backoffOrStop(ctx, &backoff) {
				return
			}
			continue
		}

		if err = c.writeInbox(ctx, m); err != nil {
			if !c.backoffOrStop(ctx, &backoff) {
				return
			}
			continue
		}

		if err = c.commitMessage(ctx, reader, m); err != nil {
			if !c.backoffOrStop(ctx, &backoff) {
				return
			}
			continue
		}
		backoff = c.config.MinBackoff
	}
}

// fetchMessage attempts to fetch a message from the Kafka reader.
// It logs and returns an error if fetching fails.
func (c *Consumer) fetchMessage(ctx context.Context, r *kafka.Reader) (kafka.Message, error) {
	m, err := r.FetchMessage(ctx)
	switch {
	case ctx.Err() != nil:
		return kafka.Message{}, ctx.Err()
	case err != nil:
		c.log.WithTopic(r.Config().Topic).WithError(err).Error("failed to fetch message from Kafka")
		return kafka.Message{}, fmt.Errorf("fetch message: %w", err)
	default:
		c.log.WithTopic(r.Config().Topic).WithMessage(m).Debug("message fetched from Kafka")
		return m, nil
	}
}

// writeInbox attempts to write the fetched message to the inbox.
// It handles the case where the inbox event already exists and logs appropriately.
func (c *Consumer) writeInbox(ctx context.Context, m kafka.Message) error {
	_, err := c.inbox.WriteInboxEvent(ctx, m)
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.Is(err, ErrInboxEventAlreadyExists):
		c.log.WithMessage(m).WithError(err).Info("inbox event already exists, skipping")
		return nil
	case err != nil:
		c.log.WithMessage(m).WithError(err).Error("failed to write inbox event")
		return fmt.Errorf("write inbox event: %w", err)
	default:
		c.log.WithMessage(m).Debug("inbox event written successfully")
		return nil
	}
}

// commitMessage attempts to commit the processed message in Kafka.
// It logs and returns an error if committing fails.
func (c *Consumer) commitMessage(ctx context.Context, r *kafka.Reader, m kafka.Message) error {
	err := r.CommitMessages(ctx, m)
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case err != nil:
		c.log.WithMessage(m).WithError(err).Error("failed to commit message in Kafka")
		return fmt.Errorf("commit message: %w", err)
	default:
		c.log.WithMessage(m).Debug("message committed in Kafka successfully")
		return nil
	}
}

// backoffOrStop implements an exponential backoff strategy for retrying operations.
// It waits for the specified backoff duration before allowing a retry.
// If the context is canceled during the wait, it returns false to indicate that the operation should stop.
func (c *Consumer) backoffOrStop(ctx context.Context, backoff *time.Duration) bool {
	t := time.NewTimer(*backoff)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-t.C:
	}

	next := *backoff * 2
	if next > c.config.MaxBackoff {
		next = c.config.MaxBackoff
	}
	*backoff = next
	return true
}

// Close gracefully shuts down the consumer by closing all Kafka readers.
func (c *Consumer) Close() error {
	var (
		wg   sync.WaitGroup
		mu   sync.Mutex
		errs []error
	)

	for _, r := range c.readers {
		wg.Add(1)
		go func(rr *kafka.Reader) {
			defer wg.Done()
			if err := rr.Close(); err != nil {
				mu.Lock()
				errs = append(errs, fmt.Errorf("close reader for topic %s: %w", rr.Config().Topic, err))
				mu.Unlock()
			}
		}(r)
	}

	wg.Wait()

	if len(errs) != 0 {
		c.log.WithError(errors.Join(errs...)).Error("failed to close kafka consumer")
		return errors.Join(errs...)
	}

	c.log.Info("kafka consumer closed successfully")
	return nil
}
