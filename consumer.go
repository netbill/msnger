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

type TopicReaderConfig struct {
	Instances int
	Reader    kafka.ReaderConfig
}

// Consumer is responsible for consuming messages from Kafka, writing them to the inbox, and committing them.
type Consumer struct {
	log    Logger
	inbox  Inbox
	topics []TopicReaderConfig
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

func (c *Consumer) AddTopic(config TopicReaderConfig) error {
	for _, existing := range c.topics {
		if existing.Reader.Topic == config.Reader.Topic && existing.Reader.GroupID == config.Reader.GroupID {
			return fmt.Errorf(
				"topic %s with group %s already exists in consumer configuration",
				config.Reader.Topic, config.Reader.GroupID,
			)
		}
	}

	if config.Instances <= 0 {
		return fmt.Errorf("instances must be greater than 0 for topic %s", config.Reader.Topic)
	}

	if config.Reader.Topic == "" {
		return fmt.Errorf("topic name cannot be empty")
	}

	if config.Reader.GroupID == "" {
		return fmt.Errorf("group ID cannot be empty for topic %s", config.Reader.Topic)
	}

	if len(config.Reader.Brokers) == 0 {
		return fmt.Errorf("brokers cannot be empty for topic %s", config.Reader.Topic)
	}

	c.topics = append(c.topics, TopicReaderConfig{
		Instances: config.Instances,
		Reader:    config.Reader,
	})

	return nil
}

// Run starts the consumer to consume messages from Kafka for all configured topics.
func (c *Consumer) Run(ctx context.Context) {
	var wg sync.WaitGroup

	for _, cfg := range c.topics {
		cfg := cfg
		for i := 0; i < cfg.Instances; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()

				reader := kafka.NewReader(cfg.Reader)
				defer reader.Close()

				c.consumeLoop(ctx, reader)
			}()
		}
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
