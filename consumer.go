package eventbox

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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

// Consumer is responsible for consuming messages from Kafka, writing them to the inbox, and committing them.
type Consumer struct {
	log    Logger
	inbox  Inbox
	config ConsumerConfig
}

// NewConsumer creates a new Consumer instance with the provided logger, inbox, and configuration.
func NewConsumer(log Logger, inbox Inbox, config ConsumerConfig) *Consumer {
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
		log:    log,
		inbox:  inbox,
		config: config,
	}
}

// Subscribe starts consuming messages from Kafka using the provided reader configuration and number of instances.
func (c *Consumer) Subscribe(ctx context.Context, config kafka.ReaderConfig, instances int) {
	var wg sync.WaitGroup

	for i := 0; i < instances; i++ {
		reader := kafka.NewReader(config)

		wg.Add(1)
		go func(r *kafka.Reader) {
			defer wg.Done()
			defer r.Close()
			c.consumeLoop(ctx, r)
		}(reader)
	}

	wg.Wait()
}

// consumeLoop continuously fetches messages from the Kafka reader, writes them to the inbox, and commits them.
// It implements an exponential backoff strategy for handling errors and respects context cancellation.
func (c *Consumer) consumeLoop(ctx context.Context, reader *kafka.Reader) {
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
	log := c.log.WithTopic(r.Config().Topic)

	m, err := r.FetchMessage(ctx)
	switch {
	case ctx.Err() != nil:
		return kafka.Message{}, ctx.Err()
	case err != nil:
		log.WithError(err).Errorf("failed to fetch message from Kafka")
		return kafka.Message{}, fmt.Errorf("fetch message: %w", err)
	default:
		log.WithMessage(m).Debug("message fetched from Kafka")
		return m, nil
	}
}

// writeInbox attempts to write the fetched message to the inbox.
// It handles the case where the inbox event already exists and logs appropriately.
func (c *Consumer) writeInbox(ctx context.Context, m kafka.Message) error {
	log := c.log.WithMessage(m)

	_, err := c.inbox.WriteInboxEvent(ctx, m)
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case errors.Is(err, ErrInboxEventAlreadyExists):
		log.WithError(err).Warnf("inbox event already exists, skipping")
		return nil
	case err != nil:
		log.WithError(err).Errorf("failed to write inbox event")
		return fmt.Errorf("write inbox event: %w", err)
	default:
		log.Debug("inbox event written successfully")
		return nil
	}
}

// commitMessage attempts to commit the processed message in Kafka.
// It logs and returns an error if committing fails.
func (c *Consumer) commitMessage(ctx context.Context, r *kafka.Reader, m kafka.Message) error {
	log := c.log.WithMessage(m)

	err := r.CommitMessages(ctx, m)
	switch {
	case ctx.Err() != nil:
		return ctx.Err()
	case err != nil:
		log.WithError(err).Errorf("failed to commit message in Kafka")
		return fmt.Errorf("commit message: %w", err)
	default:
		log.Debug("message committed in Kafka successfully")
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
