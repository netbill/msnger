package pg

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/logium"
	"github.com/segmentio/kafka-go"
)

const (
	DefaultOutboxWorkerMaxRutin = 10

	DefaultOutboxWorkerMinSleep = 100 * time.Millisecond
	DefaultOutboxWorkerMaxSleep = 5 * time.Second

	DefaultOutboxWorkerMinBatchSize = 10
	DefaultOutboxWorkerMaxBatchSize = 100

	DefaultOutboxWorkerMinNextAttempt = time.Minute
	DefaultOutboxWorkerMaxNextAttempt = 10 * time.Minute
)

// OutboxWorkerConfig configures OutboxWorker behavior.
type OutboxWorkerConfig struct {
	// MaxRutin is the maximum number of parallel send loops.
	// If 0, it defaults to DefaultOutboxWorkerMaxRutin.
	MaxRutin int

	// MinSleep is the minimum delay between iterations when there are few/no events.
	// If 0, it defaults to DefaultOutboxWorkerMinSleep.
	MinSleep time.Duration
	// MaxSleep is the maximum delay between iterations during long idle periods.
	// If 0, it defaults to DefaultOutboxWorkerMaxSleep.
	MaxSleep time.Duration

	// MinBatchSize is the minimum number of events reserved per batch.
	// If 0, it defaults to DefaultOutboxWorkerMinBatchSize.
	MinBatchSize int
	// MaxBatchSize is the maximum number of events reserved per batch.
	// If 0, it defaults to DefaultOutboxWorkerMaxBatchSize.
	MaxBatchSize int

	// MinNextAttempt is the minimum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultOutboxWorkerMinNextAttempt.
	MinNextAttempt time.Duration
	// MaxNextAttempt is the maximum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultOutboxWorkerMaxNextAttempt.
	MaxNextAttempt time.Duration
}

// OutboxWorker reads pending events from outbox storage and publishes them to Kafka.
// It uses at-least-once delivery semantics: duplicates are possible and must be handled by consumers.
type OutboxWorker struct {
	log    *logium.Logger
	config OutboxWorkerConfig

	// box provides outbox storage operations (reserve/commit/delay/cleanup).
	box outbox

	// writer publishes messages to Kafka.
	writer *kafka.Writer
}

// NewOutboxWorker creates a new OutboxWorker.
func NewOutboxWorker(
	log *logium.Logger,
	cfg OutboxWorkerConfig,
	box outbox,
	writer *kafka.Writer,
) *OutboxWorker {
	if cfg.MaxRutin < 0 {
		cfg.MaxRutin = DefaultOutboxWorkerMaxRutin
	}
	if cfg.MinSleep <= 0 {
		cfg.MinSleep = DefaultOutboxWorkerMinSleep
	}
	if cfg.MaxSleep <= 0 {
		cfg.MaxSleep = DefaultOutboxWorkerMaxSleep
	}
	if cfg.MinBatchSize <= 0 {
		cfg.MinBatchSize = DefaultOutboxWorkerMinBatchSize
	}
	if cfg.MaxBatchSize < cfg.MinBatchSize {
		cfg.MaxBatchSize = DefaultOutboxWorkerMaxBatchSize
	}
	if cfg.MinNextAttempt <= 0 {
		cfg.MinNextAttempt = DefaultOutboxWorkerMinNextAttempt
	}
	if cfg.MaxNextAttempt < cfg.MinNextAttempt {
		cfg.MaxNextAttempt = DefaultOutboxWorkerMaxNextAttempt
	}

	w := &OutboxWorker{
		log:    log,
		config: cfg,
		box:    box,
		writer: writer,
	}

	return w
}

// outboxWorkerJob defines job for sending outbox event to kafka.
type outboxWorkerJob struct {
	ev OutboxEvent
}

// outboxWorkerRes defines result of sending outbox event to kafka.
type outboxWorkerRes struct {
	eventID     uuid.UUID
	err         error
	nextAttempt time.Time
	processedAt time.Time
}

// Run starts the worker loop and blocks until ctx is cancelled.
//
// Flow:
//  1. Start MaxRutin send loops reading jobs from a channel.
//  2. In a loop: reserve a batch of pending events, send them, then commit or delay.
//  3. On exit: attempts to release events reserved by this worker ( the best effort).
//
// The worker does not guarantee exactly-once delivery.
// Consumers must be idempotent.
func (w *OutboxWorker) Run(ctx context.Context, id string) {
	defer func() {
		if err := w.CleanOwnProcessingEvents(context.Background(), id); err != nil {
			w.log.WithError(err).Error("failed to clean processing events for worker")
		}
	}()

	BatchSize := w.config.MinBatchSize
	sleep := w.config.MinSleep

	maxParallel := w.config.MaxRutin
	if maxParallel <= 0 {
		maxParallel = 1
	}

	// create to channels for sending jobs to send loops and receiving results back from them
	jobs := make(chan outboxWorkerJob, maxParallel)
	results := make(chan outboxWorkerRes, maxParallel)

	var wg sync.WaitGroup
	wg.Add(maxParallel)

	for i := 0; i < maxParallel; i++ {
		// start send loops that will read reserved events from jobs channel,
		// publish them to Kafka and report results back to results channel
		go w.sendLoop(ctx, &wg, jobs, results)
	}

	defer func() {
		close(jobs)
		wg.Wait()
		close(results)
	}()

	for {
		if ctx.Err() != nil {
			return
		}

		// reserve a batch of pending events for this worker, send them via send loops,
		// and then commit (sent) or delay (retry later) each event
		numEvents := w.processBatch(ctx, id, BatchSize, jobs, results)

		BatchSize = w.calculateBatch(numEvents)
		sleep = w.calculateSleep(numEvents, sleep)

		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

// sendLoop reads reserved events from jobs channel, publishes them to Kafka and reports results.
func (w *OutboxWorker) sendLoop(
	ctx context.Context,
	wg *sync.WaitGroup,
	jobs <-chan outboxWorkerJob,
	results chan<- outboxWorkerRes,
) {
	defer wg.Done()

	// read jobs until the channel is closed or context is cancelled
	for job := range jobs {
		if ctx.Err() != nil {
			return
		}

		ev := job.ev
		msg := ev.ToKafkaMessage()

		// try to send message to kafka topic
		sendErr := w.writer.WriteMessages(ctx, msg)
		if sendErr != nil {
			w.log.WithError(sendErr).Errorf(
				"failed to send event id=%s, topic=%s, event_type%s, attempts+%v",
				ev.EventID, ev.Topic, ev.Type, ev.Attempts,
			)
			// if sending failed, report error back to results channel, so event will be delayed for future processing
			results <- outboxWorkerRes{
				eventID:     ev.EventID,
				err:         sendErr,
				nextAttempt: w.nextAttemptAt(ev.Attempts),
				processedAt: time.Now().UTC(),
			}
			continue
		}

		// if sending succeeded, report success back to results channel, so event will be committed as sent
		w.log.Debugf("sent event id=%s", ev.EventID)
		results <- outboxWorkerRes{
			eventID:     ev.EventID,
			processedAt: time.Now().UTC()}
	}
}

// processBatch reserves up to batchSize pending events for this worker,
// sends them via send loops, and then commits (sent) or delays (retry later) each event.
func (w *OutboxWorker) processBatch(
	ctx context.Context,
	id string,
	BatchSize int,
	jobs chan<- outboxWorkerJob,
	results <-chan outboxWorkerRes,
) int {
	// reserve a batch of pending events for this worker
	events, err := w.box.ReserveOutboxEvents(ctx, id, BatchSize)
	if err != nil {
		w.log.WithError(err).Error("failed to reserve events")
		return 0
	}
	if len(events) == 0 {
		return 0
	}

	for _, ev := range events {
		select {
		case <-ctx.Done():
			return len(events)
			// send reserved events to send loops via jobs channel, so
			// they will be processed and then committed or delayed
		case jobs <- outboxWorkerJob{ev: ev}:
		}
	}

	commit := make(map[uuid.UUID]CommitOutboxEventParams, len(events))
	pending := make(map[uuid.UUID]DelayOutboxEventData, len(events))

	for i := 0; i < len(events); i++ {
		select {
		case <-ctx.Done():
			return len(events)
		case r := <-results:
			if r.err != nil {
				pending[r.eventID] = DelayOutboxEventData{
					NextAttemptAt: r.nextAttempt,
					Reason:        r.err.Error(),
				}
			} else {
				commit[r.eventID] = CommitOutboxEventParams{SentAt: r.processedAt}
			}
		}
	}

	if len(commit) > 0 {
		// if sending succeeded, commit events as sent, so they won't be processed again
		if err := w.box.CommitOutboxEvents(ctx, id, commit); err != nil {
			w.log.WithError(err).Error("failed to mark events as sent")
		}
	}

	if len(pending) > 0 {
		// if sending failed, delay events for future processing, so they will be retried later
		if err := w.box.DelayOutboxEvents(ctx, id, pending); err != nil {
			w.log.WithError(err).Error("failed to delay events")
		}
	}

	return len(events)
}

// nextAttemptAt calculates when next attempt to process a failed event based on the number of attempts.
func (w *OutboxWorker) nextAttemptAt(attempts int) time.Time {
	res := time.Second * time.Duration(30*attempts)
	if res < w.config.MinNextAttempt {
		return time.Now().UTC().Add(w.config.MinNextAttempt)
	}
	if res > w.config.MaxNextAttempt {
		return time.Now().UTC().Add(w.config.MaxNextAttempt)
	}

	return time.Now().UTC().Add(res)
}

// calculateBatch adjusts the next batch size based on how many events were processed last time.
func (w *OutboxWorker) calculateBatch(
	numEvents int,
) int {
	minBatch := w.config.MinBatchSize
	maxBatch := w.config.MaxBatchSize
	if maxBatch == 0 {
		maxBatch = 100
	}

	var batch int
	switch {
	case numEvents == 0:
		batch = minBatch
	case numEvents >= maxBatch:
		batch = maxBatch
	default:
		batch = numEvents * 2
	}

	if batch < minBatch {
		batch = minBatch
	}
	if batch > maxBatch {
		batch = maxBatch
	}

	return batch
}

// calculateSleep adjusts the delay before the next iteration based on how many events were processed.
func (w *OutboxWorker) calculateSleep(
	numEvents int,
	lastSleep time.Duration,
) time.Duration {
	minSleep := w.config.MinSleep
	maxSleep := w.config.MaxSleep

	var sleep time.Duration

	switch {
	case numEvents == 0:
		if lastSleep == 0 {
			sleep = minSleep
		} else {
			sleep = lastSleep * 2
		}

	case numEvents >= w.config.MaxBatchSize:
		sleep = 0

	default:
		fill := float64(numEvents) / float64(w.config.MaxBatchSize)

		switch {
		case fill >= 0.75:
			sleep = 0
		case fill >= 0.5:
			sleep = minSleep
		case fill >= 0.25:
			sleep = minSleep * 2
		default:
			sleep = minSleep * 4
		}
	}

	if sleep < minSleep {
		sleep = minSleep
	}
	if sleep > maxSleep {
		sleep = maxSleep
	}

	return sleep
}

// CleanOwnFailedEvents moves failed events reserved by this worker back to pending.
func (w *OutboxWorker) CleanOwnFailedEvents(
	ctx context.Context,
	id string,
) error {
	return w.box.CleanFailedOutboxEventForWorker(ctx, id)
}

// CleanOwnProcessingEvents releases processing events reserved by this worker back to pending.
func (w *OutboxWorker) CleanOwnProcessingEvents(
	ctx context.Context,
	id string,
) error {
	return w.box.CleanProcessingOutboxEventForWorker(ctx, id)
}

// Shutdown closes Kafka writer and performs a best-effort cleanup.
// Note: CleanProcessingOutboxEvent affects all processing events, not only this worker.
// Use with caution in multi-worker setups.
func (w *OutboxWorker) Shutdown(ctx context.Context) error {
	var errs []error

	err := w.writer.Close()
	if err != nil {
		w.log.WithError(err).Error("failed to close kafka writer")
		errs = append(errs, err)
	}

	err = w.box.CleanProcessingOutboxEvent(ctx)
	if err != nil {
		w.log.WithError(err).Error("failed to clean processing events for worker")
		errs = append(errs, err)
	}

	return errors.Join(errs...)
}
