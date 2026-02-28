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
func (w *OutboxWorker) Run(ctx context.Context) {
	w.log.Info("starting outbox worker")

	jobs := make(chan producerJob, w.config.Slots)
	results := make(chan producerRes, w.config.Slots)

	var wg sync.WaitGroup
	wg.Add(w.config.Routines)

	for i := 0; i < w.config.Routines; i++ {
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

		processed := w.processBatch(ctx, w.config.BatchSize, jobs, results)

		if processed == 0 {
			w.sleep(ctx)
		}
	}
}

// sendLoop continuously receives producer jobs from the jobs channel,
// sends the corresponding events to Kafka using the producer,
func (w *OutboxWorker) sendLoop(
	ctx context.Context,
	wg *sync.WaitGroup,
	jobs <-chan producerJob,
	results chan<- producerRes,
) {
	defer wg.Done()

	for job := range jobs {
		event := job.event

		err := w.producer.WriteToKafka(ctx, Message{
			ID:       event.EventID,
			Topic:    event.Topic,
			Key:      event.Key,
			Type:     event.Type,
			Version:  event.Version,
			Producer: event.Producer,
			Payload:  event.Payload,
		})
		if err != nil {
			w.log.WithOutboxEvent(event).WithError(err).Error("failed to send outbox event")
			_ = sendOutboxResult(ctx, results, producerRes{
				event:       event,
				err:         err,
				processedAt: time.Now().UTC(),
			})

			continue
		}

		w.log.WithOutboxEvent(event).Debug("outbox event sent successfully")
		_ = sendOutboxResult(ctx, results, producerRes{
			event:       event,
			err:         nil,
			processedAt: time.Now().UTC(),
		})
	}
}

// processBatch reserves a batch of Outbox events, sends them to the producer jobs channel,
// and processes the results to determine which events were sent successfully,
// which should be delayed for future processing, and which should be marked as failed.
func (w *OutboxWorker) processBatch(
	ctx context.Context,
	batch int,
	jobs chan<- producerJob,
	results <-chan producerRes,
) int {
	events, err := w.box.ReserveOutboxEvents(ctx, w.id, batch)
	if err != nil {
		w.log.WithError(err).Error("failed to reserve outbox events")

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

		if r.err != nil {
			if w.config.MaxAttempts != 0 && r.event.Attempts+1 >= w.config.MaxAttempts {
				failed[r.event.EventID] = FailedOutboxEventData{
					LastAttemptAt: r.processedAt,
					Reason:        r.err.Error(),
				}

				w.log.WithOutboxEvent(r.event).
					Error("event marked as failed after reaching max attempts")
				continue
			}

			pending[r.event.EventID] = DelayOutboxEventData{
				LastAttemptAt: r.processedAt,
				NextAttemptAt: w.nextAttemptAt(r.event.Attempts + 1),
				Reason:        r.err.Error(),
			}

			w.log.WithOutboxEvent(r.event).
				Warn("event will be delayed for future processing after failed attempt")
			continue
		}

		commit[r.event.EventID] = CommitOutboxEventParams{SentAt: r.processedAt}
	}

	if len(commit) > 0 {
		if err = w.box.CommitOutboxEvents(ctx, w.id, commit); err != nil {
			w.log.WithError(err).Error("failed to mark events as sent")
		}

		w.log.WithField("count", len(commit)).Debug("events marked as sent")
	}

	if len(pending) > 0 {
		if err = w.box.DelayOutboxEvents(ctx, w.id, pending); err != nil {
			w.log.WithError(err).Error("failed to delay events")
		}

		w.log.WithField("count", len(pending)).Debug("events delayed for future processing")
	}

	if len(failed) > 0 {
		if err = w.box.FailedOutboxEvents(ctx, w.id, failed); err != nil {
			w.log.WithError(err).Error("failed to mark events as failed")
		}

		w.log.WithField("count", len(failed)).Debug("events marked as failed after reaching max attempts")
	}

	return len(events)
}

// nextAttemptAt calculates the next attempt time for a failed event based on the number of attempts
// and the configured minimum and maximum next attempt durations.
func (w *OutboxWorker) nextAttemptAt(attempts int32) time.Time {
	res := w.config.MinNextAttempt * time.Duration(attempts)
	if res < w.config.MinNextAttempt {
		return time.Now().UTC().Add(w.config.MinNextAttempt)
	}
	if res > w.config.MaxNextAttempt {
		return time.Now().UTC().Add(w.config.MaxNextAttempt)
	}

	return time.Now().UTC().Add(res)
}

// sleep pauses the worker for the configured sleep duration or until the context is canceled, whichever comes first.
func (w *OutboxWorker) sleep(ctx context.Context) {
	t := time.NewTimer(w.config.Sleep)
	defer t.Stop()

	select {
	case <-ctx.Done():
		return
	case <-t.C:
		return
	}
}

// Clean gracefully stops the outbox worker by cleaning up any events that are currently reserved for processing by this worker.
// should be called which deffer after Run to ensure proper cleanup
func (w *OutboxWorker) Clean() {
	if err := w.box.CleanProcessingOutboxEvents(context.Background(), w.id); err != nil {
		w.log.WithError(err).Error("failed to clean processing outbox events for worker")
		return
	}

	w.log.Info("outbox worker stopped successfully")
}
