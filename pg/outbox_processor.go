package pg

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/logium"
	"github.com/netbill/msnger"
	"github.com/netbill/msnger/logfields"
	"github.com/netbill/pgdbx"
	"github.com/segmentio/kafka-go"
)

const (
	DefaultOutboxProcessorRoutines = 10

	DefaultOutboxProcessorMinSleep = 100 * time.Millisecond
	DefaultOutboxProcessorMaxSleep = 5 * time.Second

	DefaultOutboxProcessorMinBatchSize = 10
	DefaultOutboxProcessorMaxBatchSize = 100

	DefaultOutboxProcessorMinNextAttempt = time.Minute
	DefaultOutboxProcessorMaxNextAttempt = 10 * time.Minute
)

// OutboxProcessorConfig configures OutboxProcessor behavior.
type OutboxProcessorConfig struct {
	// Routines is the maximum number of parallel send loops.
	// If 0, it defaults to DefaultOutboxProcessorRoutines.
	Routines int

	// MinSleep is the minimum delay between iterations when there are few/no events.
	// If 0, it defaults to DefaultOutboxProcessorMinSleep.
	MinSleep time.Duration
	// MaxSleep is the maximum delay between iterations during long idle periods.
	// If 0, it defaults to DefaultOutboxProcessorMaxSleep.
	MaxSleep time.Duration

	// MinBatch is the minimum number of events reserved per batch.
	// If 0, it defaults to DefaultOutboxProcessorMinBatchSize.
	MinBatch int
	// MaxBatch is the maximum number of events reserved per batch.
	// If 0, it defaults to DefaultOutboxProcessorMaxBatchSize.
	MaxBatch int

	// MinNextAttempt is the minimum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultOutboxProcessorMinNextAttempt.
	MinNextAttempt time.Duration
	// MaxNextAttempt is the maximum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultOutboxProcessorMaxNextAttempt.
	MaxNextAttempt time.Duration

	//MaxAttempts is the maximum number of attempts to process an event before it is marked as failed.
	// If 0, it defaults to 5.
	MaxAttempts int32
}

type OutboxProcessor struct {
	log    *logium.Entry
	config OutboxProcessorConfig

	box outbox

	writer *kafka.Writer
}

func NewOutboxProcessor(
	log *logium.Entry,
	cfg OutboxProcessorConfig,
	db *pgdbx.DB,
	writer *kafka.Writer,
) *OutboxProcessor {
	if cfg.Routines < 0 {
		cfg.Routines = DefaultOutboxProcessorRoutines
	}
	if cfg.MinSleep <= 0 {
		cfg.MinSleep = DefaultOutboxProcessorMinSleep
	}
	if cfg.MaxSleep <= 0 {
		cfg.MaxSleep = DefaultOutboxProcessorMaxSleep
	}
	if cfg.MinBatch <= 0 {
		cfg.MinBatch = DefaultOutboxProcessorMinBatchSize
	}
	if cfg.MaxBatch < cfg.MinBatch {
		cfg.MaxBatch = DefaultOutboxProcessorMaxBatchSize
	}
	if cfg.MinNextAttempt <= 0 {
		cfg.MinNextAttempt = DefaultOutboxProcessorMinNextAttempt
	}
	if cfg.MaxNextAttempt < cfg.MinNextAttempt {
		cfg.MaxNextAttempt = DefaultOutboxProcessorMaxNextAttempt
	}

	w := &OutboxProcessor{
		log:    log,
		config: cfg,
		box:    outbox{db: db},
		writer: writer,
	}

	return w
}

type outboxProcessorJob struct {
	ev msnger.OutboxEvent
}

func sendOutboxJob(ctx context.Context, jobs chan<- outboxProcessorJob, job outboxProcessorJob) bool {
	select {
	case <-ctx.Done():
		return false
	case jobs <- job:
		return true
	}
}

type outboxProcessorRes struct {
	eventID     uuid.UUID
	topic       string
	key         string
	eType       string
	err         error
	attempts    int32
	nextAttempt time.Time
	processedAt time.Time
}

func sendOutboxResult(ctx context.Context, results chan<- outboxProcessorRes, res outboxProcessorRes) bool {
	select {
	case <-ctx.Done():
		return false
	case results <- res:
		return true
	}
}

func getOutboxResult(ctx context.Context, results <-chan outboxProcessorRes) (outboxProcessorRes, bool) {
	select {
	case <-ctx.Done():
		return outboxProcessorRes{}, false
	case r, ok := <-results:
		return r, ok
	}
}

func (p *OutboxProcessor) RunProcess(ctx context.Context, processID string) {
	p.log.WithField(logfields.ProcessID, processID).Info("starting outbox processor")

	batch := p.config.MinBatch
	sleep := p.config.MinSleep

	maxParallel := p.config.Routines
	if maxParallel <= 0 {
		maxParallel = 1
	}

	jobs := make(chan outboxProcessorJob, maxParallel)
	results := make(chan outboxProcessorRes, maxParallel)

	var wg sync.WaitGroup
	wg.Add(maxParallel)

	for i := 0; i < maxParallel; i++ {
		go p.sendLoop(ctx, processID, &wg, jobs, results)
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

		numEvents := p.processBatch(ctx, processID, batch, jobs, results)

		batch = calculateBatch(numEvents, p.config.MinBatch, p.config.MaxBatch)

		sleep = p.sleep(ctx, numEvents, sleep)
	}
}

func (p *OutboxProcessor) sendLoop(
	ctx context.Context,
	processID string,
	wg *sync.WaitGroup,
	jobs <-chan outboxProcessorJob,
	results chan<- outboxProcessorRes,
) {
	defer wg.Done()

	for job := range jobs {
		ev := job.ev
		msg := ev.ToKafkaMessage()

		err := p.writer.WriteMessages(ctx, msg)
		if err != nil {
			p.log.WithError(err).
				WithField(logfields.ProcessID, processID).
				WithFields(logfields.FromOutboxEvent(ev)).
				Errorf("failed to send outbox event to Kafka")

			_ = sendOutboxResult(ctx, results, outboxProcessorRes{
				eventID:     ev.EventID,
				topic:       ev.Topic,
				key:         ev.Key,
				eType:       ev.Type,
				err:         err,
				attempts:    ev.Attempts + 1,
				nextAttempt: p.nextAttemptAt(ev.Attempts),
				processedAt: time.Now().UTC(),
			})
			continue
		}

		_ = sendOutboxResult(ctx, results, outboxProcessorRes{
			eventID:     ev.EventID,
			topic:       ev.Topic,
			key:         ev.Key,
			eType:       ev.Type,
			err:         nil,
			attempts:    ev.Attempts + 1,
			processedAt: time.Now().UTC(),
		})
	}
}

func (p *OutboxProcessor) processBatch(
	ctx context.Context,
	processID string,
	BatchSize int,
	jobs chan<- outboxProcessorJob,
	results <-chan outboxProcessorRes,
) int {
	events, err := p.box.ReserveOutboxEvents(ctx, processID, BatchSize)
	if err != nil {
		p.log.WithError(err).
			WithField(logfields.ProcessID, processID).
			Error("failed to reserve outbox events")

		return 0
	}
	if len(events) == 0 {
		return 0
	}

	for _, ev := range events {
		if !sendOutboxJob(ctx, jobs, outboxProcessorJob{ev: ev}) {
			return len(events)
		}
	}

	commit := make(map[uuid.UUID]CommitOutboxEventParams, len(events))
	pending := make(map[uuid.UUID]DelayOutboxEventData, len(events))
	failed := make(map[uuid.UUID]FailedOutboxEventData, len(events))

	for i := 0; i < len(events); i++ {
		r, ok := getOutboxResult(ctx, results)
		if !ok {
			return len(events)
		}

		entry := p.log.WithFields(logium.Fields{
			logfields.ProcessID:         processID,
			logfields.EventIDFiled:      r.eventID,
			logfields.EventTopicFiled:   r.topic,
			logfields.EventTypeFiled:    r.eType,
			logfields.EventAttemptFiled: r.attempts,
		})

		if r.err != nil {
			if p.config.MaxAttempts != 0 && r.attempts >= p.config.MaxAttempts {
				failed[r.eventID] = FailedOutboxEventData{
					LastAttemptAt: r.processedAt,
					Reason:        r.err.Error(),
				}

				entry.Errorf("event marked as failed after reaching max attempts")
				continue
			}

			pending[r.eventID] = DelayOutboxEventData{
				NextAttemptAt: r.nextAttempt,
				Reason:        r.err.Error(),
			}

			entry.Warnf("event will be delayed for future processing after failed attempt")
			continue
		}

		commit[r.eventID] = CommitOutboxEventParams{SentAt: r.processedAt}
	}

	if len(commit) > 0 {
		if err = p.box.CommitOutboxEvents(ctx, processID, commit); err != nil {
			p.log.WithError(err).
				WithField(logfields.ProcessID, processID).
				Error("failed to mark events as sent")
		}
	}

	if len(pending) > 0 {
		if err = p.box.DelayOutboxEvents(ctx, processID, pending); err != nil {
			p.log.WithError(err).
				WithField(logfields.ProcessID, processID).
				Error("failed to delay events")
		}
	}

	if len(failed) > 0 {
		if err = p.box.FailedOutboxEvents(ctx, processID, failed); err != nil {
			p.log.WithError(err).
				WithField(logfields.ProcessID, processID).
				Error("failed to mark events as failed")
		}
	}

	return len(events)
}

func (p *OutboxProcessor) nextAttemptAt(attempts int32) time.Time {
	res := time.Second * time.Duration(30*attempts)
	if res < p.config.MinNextAttempt {
		return time.Now().UTC().Add(p.config.MinNextAttempt)
	}
	if res > p.config.MaxNextAttempt {
		return time.Now().UTC().Add(p.config.MaxNextAttempt)
	}

	return time.Now().UTC().Add(res)
}

func (p *OutboxProcessor) sleep(
	ctx context.Context,
	numEvents int,
	lastSleep time.Duration,
) time.Duration {
	sleep := calculateSleep(numEvents, p.config.MaxBatch, lastSleep, p.config.MinSleep, p.config.MaxSleep)

	t := time.NewTimer(sleep)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return sleep
	case <-t.C:
		return sleep
	}
}

func (p *OutboxProcessor) StopProcess(ctx context.Context, processID string) error {
	err := p.box.CleanProcessingOutboxEvent(ctx, processID)
	if err != nil {
		p.log.WithError(err).
			WithField(logfields.ProcessID, processID).
			Error("failed to clean processing events for processor")
		return err
	}

	return nil
}
