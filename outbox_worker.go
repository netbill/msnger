package eventbox

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/logium"
)

const (
	DefaultOutboxWorkerRoutines = 10

	DefaultOutboxWorkerSleep = 200 * time.Millisecond

	DefaultOutboxWorkerBatch = 100

	DefaultOutboxWorkerMinNextAttempt = time.Minute
	DefaultOutboxWorkerMaxNextAttempt = 10 * time.Minute
)

// OutboxWorkerConfig configures OutboxWorker behavior.
type OutboxWorkerConfig struct {
	// Routines is the maximum number of parallel send loops.
	// If 0, it defaults to DefaultOutboxWorkerRoutines.
	Routines int

	// Slots is the maximum number of in-flight processing events.
	// If 0, it defaults to Routines * 4.
	Slots int

	// Sleep is the duration to sleep when there are no events to process or when all processing slots are occupied.
	// If 0, it defaults to DefaultOutboxWorkerSleep.
	Sleep time.Duration

	// BatchSize is the number of events to reserve in one batch.
	// If 0, it defaults to DefaultOutboxWorkerBatch.
	BatchSize int

	// MinNextAttempt is the minimum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultOutboxWorkerMinNextAttempt.
	MinNextAttempt time.Duration
	// MaxNextAttempt is the maximum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultOutboxWorkerMaxNextAttempt.
	MaxNextAttempt time.Duration

	//MaxAttempts is the maximum number of attempts to process an event before it is marked as failed.
	// If 0, it defaults to 5.
	MaxAttempts int32
}

// OutboxWorker is responsible for processing Outbox events for a given process ID
// by reserving events from the outbox,
type OutboxWorker struct {
	id  string
	log *Logger

	box      Outbox
	producer *Producer
	config   OutboxWorkerConfig
}

// NewOutboxWorker creates a new OutboxWorker instance with the provided logger, outbox repository, producer, and configuration.
func NewOutboxWorker(
	id string,
	logger logium.Logger,
	repo Outbox,
	producer *Producer,
	config OutboxWorkerConfig,
) *OutboxWorker {
	if config.Routines <= 0 {
		config.Routines = DefaultOutboxWorkerRoutines
	}
	if config.Slots <= 0 {
		config.Slots = config.Routines * 4
	}
	if config.Sleep <= 0 {
		config.Sleep = DefaultOutboxWorkerSleep
	}
	if config.BatchSize <= 0 {
		config.BatchSize = DefaultOutboxWorkerBatch
	}
	if config.MinNextAttempt <= 0 {
		config.MinNextAttempt = DefaultOutboxWorkerMinNextAttempt
	}
	if config.MaxNextAttempt <= 0 {
		config.MaxNextAttempt = DefaultOutboxWorkerMaxNextAttempt
	}
	if config.MaxAttempts < 0 {
		config.MaxAttempts = 0
	}
	if config.BatchSize > config.Slots {
		config.BatchSize = config.Slots
	}

	return &OutboxWorker{
		id:       id,
		log:      NewLogger(logger).WithField("worker_id", id),
		box:      repo,
		producer: producer,
		config:   config,
	}
}

type producerJob struct {
	event OutboxEvent
}

func sendOutboxJob(ctx context.Context, jobs chan<- producerJob, job producerJob) bool {
	select {
	case <-ctx.Done():
		return false
	case jobs <- job:
		return true
	}
}

type producerRes struct {
	event OutboxEvent
	err   error

	processedAt time.Time
}

func sendOutboxResult(ctx context.Context, results chan<- producerRes, res producerRes) bool {
	select {
	case <-ctx.Done():
		return false
	case results <- res:
		return true
	}
}

func getOutboxResult(ctx context.Context, results <-chan producerRes) (producerRes, bool) {
	select {
	case <-ctx.Done():
		return producerRes{}, false
	case r, ok := <-results:
		return r, ok
	}
}

// Run starts the outbox worker, which continuously processes batches of Outbox events
// until the provided context is canceled.
func (p *OutboxWorker) Run(ctx context.Context) {
	p.log.Info("starting outbox worker")

	jobs := make(chan producerJob, p.config.Slots)
	results := make(chan producerRes, p.config.Slots)

	var wg sync.WaitGroup
	wg.Add(p.config.Routines)

	for i := 0; i < p.config.Routines; i++ {
		go p.sendLoop(ctx, &wg, jobs, results)
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

		processed := p.processBatch(ctx, p.config.BatchSize, jobs, results)

		if processed == 0 {
			p.sleep(ctx)
		}
	}
}

// sendLoop continuously receives producer jobs from the jobs channel,
// sends the corresponding events to Kafka using the producer,
func (p *OutboxWorker) sendLoop(
	ctx context.Context,
	wg *sync.WaitGroup,
	jobs <-chan producerJob,
	results chan<- producerRes,
) {
	defer wg.Done()

	for job := range jobs {
		event := job.event

		entry := p.log.WithOutboxEvent(event)

		err := p.producer.WriteToKafka(ctx, Event{
			ID:       event.EventID,
			Topic:    event.Topic,
			Key:      event.Key,
			Type:     event.Type,
			Version:  event.Version,
			Producer: event.Producer,
			Payload:  event.Payload,
		})
		if err != nil {
			entry.WithError(err).Error("failed to send outbox event")

			_ = sendOutboxResult(ctx, results, producerRes{
				event:       event,
				err:         err,
				processedAt: time.Now().UTC(),
			})
		} else {
			entry.Debug("outbox event sent successfully")

			_ = sendOutboxResult(ctx, results, producerRes{
				event:       event,
				err:         nil,
				processedAt: time.Now().UTC(),
			})
		}
	}
}

// processBatch reserves a batch of Outbox events, sends them to the producer jobs channel,
// and processes the results to determine which events were sent successfully,
// which should be delayed for future processing, and which should be marked as failed.
func (p *OutboxWorker) processBatch(
	ctx context.Context,
	batch int,
	jobs chan<- producerJob,
	results <-chan producerRes,
) int {
	events, err := p.box.ReserveOutboxEvents(ctx, p.id, batch)
	if err != nil {
		p.log.WithError(err).Error("failed to reserve outbox events")

		return 0
	}
	if len(events) == 0 {
		return 0
	}

	for _, ev := range events {
		if !sendOutboxJob(ctx, jobs, producerJob{event: ev}) {
			return 0
		}
	}

	commit := make(map[uuid.UUID]CommitOutboxEventParams, len(events))
	pending := make(map[uuid.UUID]DelayOutboxEventData, len(events))
	failed := make(map[uuid.UUID]FailedOutboxEventData, len(events))

	for i := 0; i < len(events); i++ {
		r, ok := getOutboxResult(ctx, results)
		if !ok {
			return 0
		}

		entry := p.log.WithOutboxEvent(r.event)

		if r.err != nil {
			if p.config.MaxAttempts != 0 && r.event.Attempts+1 >= p.config.MaxAttempts {
				failed[r.event.EventID] = FailedOutboxEventData{
					LastAttemptAt: r.processedAt,
					Reason:        r.err.Error(),
				}

				entry.Error("event marked as failed after reaching max attempts")
				continue
			} else {
				pending[r.event.EventID] = DelayOutboxEventData{
					LastAttemptAt: r.processedAt,
					NextAttemptAt: p.nextAttemptAt(r.event.Attempts + 1),
					Reason:        r.err.Error(),
				}

				entry.Warn("event will be delayed for future processing after failed attempt")
				continue
			}
		}

		commit[r.event.EventID] = CommitOutboxEventParams{SentAt: r.processedAt}
	}

	if len(commit) > 0 {
		if err = p.box.CommitOutboxEvents(ctx, p.id, commit); err != nil {
			p.log.WithError(err).Error("failed to mark events as sent")
		}

		p.log.WithField("count", len(commit)).Debug("events marked as sent")
	}

	if len(pending) > 0 {
		if err = p.box.DelayOutboxEvents(ctx, p.id, pending); err != nil {
			p.log.WithError(err).Error("failed to delay events")
		}

		p.log.WithField("count", len(pending)).Debug("events delayed for future processing")
	}

	if len(failed) > 0 {
		if err = p.box.FailedOutboxEvents(ctx, p.id, failed); err != nil {
			p.log.WithError(err).Error("failed to mark events as failed")
		}

		p.log.WithField("count", len(failed)).Debug("events marked as failed after reaching max attempts")
	}

	return len(events)
}

// nextAttemptAt calculates the next attempt time for a failed event based on the number of attempts
// and the configured minimum and maximum next attempt durations.
func (p *OutboxWorker) nextAttemptAt(attempts int32) time.Time {
	res := time.Second * time.Duration(30*attempts)
	if res < p.config.MinNextAttempt {
		return time.Now().UTC().Add(p.config.MinNextAttempt)
	}
	if res > p.config.MaxNextAttempt {
		return time.Now().UTC().Add(p.config.MaxNextAttempt)
	}

	return time.Now().UTC().Add(res)
}

// sleep pauses the worker for the configured sleep duration or until the context is canceled, whichever comes first.
func (p *OutboxWorker) sleep(ctx context.Context) {
	t := time.NewTimer(p.config.Sleep)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return
	case <-t.C:
		return
	}
}

// Stop gracefully stops the outbox worker by cleaning up any events that are currently reserved for processing by this worker.
// should be called which deffer after Run to ensure proper cleanup
func (p *OutboxWorker) Stop(ctx context.Context) {
	if err := p.box.CleanProcessingOutboxEvents(ctx, p.id); err != nil {
		p.log.WithError(err).Error("failed to clean processing events for worker")
	}

	p.log.Info("outbox worker stopped successfully")
}
