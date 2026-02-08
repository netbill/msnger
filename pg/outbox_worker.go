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

// OutboxWorkerConfig defines configuration for OutboxWorker.
type OutboxWorkerConfig struct {
	// MaxRutin defines maximum number of parallel routines for sending events.
	// If 0, it will be set to 1.
	MaxRutin uint

	// MinSleep defines minimum sleep time between ticks. It will be used when there are no events to send.
	MinSleep time.Duration
	// MaxSleep defines maximum sleep time between ticks. It will be used when there are no events to send for a long time.
	MaxSleep time.Duration

	// MinButchSize defines minimum number of events to reserve for sending in one tick.
	MinButchSize uint
	// MaxButchSize defines maximum number of events to reserve for sending in one tick.
	MaxButchSize uint
}

type OutboxWorker struct {
	log    *logium.Logger
	config OutboxWorkerConfig

	//box realization of outbox
	box outbox

	writer *kafka.Writer
}

func NewOutboxWorker(
	log *logium.Logger,
	cfg OutboxWorkerConfig,
	box outbox,
	writer *kafka.Writer,
) *OutboxWorker {
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
	id     uuid.UUID
	now    time.Time
	err    error
	reason string
}

// Run starts the worker loop. It will run until the context is cancelled.
// also here transferring the ID, so the worker can clean up only its own events in case of shutdown or restart.
// Below is an explanation of how it works.
// So first we create a channel for jobs and results,
// after that for each routine (up to MaxRutin) we start a goroutine that listens channel eith jobs,
// and for each job tru to send event to kafka, and then send result to results channel.
// so after that send the result of success or failure to the results channel
func (w *OutboxWorker) Run(ctx context.Context, id string) {
	defer func() {
		if err := w.CleanOwnProcessingEvents(context.Background(), id); err != nil {
			w.log.WithError(err).Error("failed to clean processing events for worker")
		}
	}()

	butchSize := w.config.MinButchSize
	sleep := w.config.MinSleep

	maxParallel := int(w.config.MaxRutin)
	if maxParallel <= 0 {
		maxParallel = 1
	}

	jobs := make(chan outboxWorkerJob, maxParallel)
	results := make(chan outboxWorkerRes, maxParallel)

	var wg sync.WaitGroup
	wg.Add(maxParallel)

	for i := 0; i < maxParallel; i++ {
		go w.jober(ctx, &wg, jobs, results)
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

		numEvents := w.tick(ctx, id, butchSize, jobs, results)

		butchSize = w.calculateBatch(numEvents)
		sleep = w.calculateSleep(numEvents, sleep)

		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

func (w *OutboxWorker) jober(
	ctx context.Context,
	wg *sync.WaitGroup,
	jobs <-chan outboxWorkerJob,
	results chan<- outboxWorkerRes,
) {
	defer wg.Done()

	for job := range jobs {
		if ctx.Err() != nil {
			return
		}

		ev := job.ev
		msg := ev.ToKafkaMessage()
		sendErr := w.writer.WriteMessages(ctx, msg)
		now := time.Now().UTC()

		if sendErr != nil {
			w.log.WithError(sendErr).Errorf("failed to send event id=%s", ev.EventID)
			results <- outboxWorkerRes{
				id:     ev.EventID,
				now:    now,
				err:    sendErr,
				reason: sendErr.Error(),
			}
			continue
		}

		w.log.Debugf("sent event id=%s", ev.EventID)
		results <- outboxWorkerRes{id: ev.EventID, now: now}
	}
}

func (w *OutboxWorker) tick(
	ctx context.Context,
	id string,
	butchSize uint,
	jobs chan<- outboxWorkerJob,
	results <-chan outboxWorkerRes,
) uint {
	events, err := w.box.ReserveOutboxEvents(ctx, id, butchSize)
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
			return uint(len(events))
		case jobs <- outboxWorkerJob{ev: ev}:
		}
	}

	commit := make(map[uuid.UUID]CommitOutboxEventParams, len(events))
	pending := make(map[uuid.UUID]DelayOutboxEventData, len(events))

	for i := 0; i < len(events); i++ {
		select {
		case <-ctx.Done():
			return uint(len(events))
		case r := <-results:
			if r.err != nil {
				pending[r.id] = DelayOutboxEventData{
					NextAttemptAt: r.now.Add(5 * time.Minute),
					Reason:        r.reason,
				}
			} else {
				commit[r.id] = CommitOutboxEventParams{SentAt: r.now}
			}
		}
	}

	if len(commit) > 0 {
		if err := w.box.CommitOutboxEvents(ctx, id, commit); err != nil {
			w.log.WithError(err).Error("failed to mark events as sent")
		}
	}

	if len(pending) > 0 {
		if err := w.box.DelayOutboxEvents(ctx, id, pending); err != nil {
			w.log.WithError(err).Error("failed to delay events")
		}
	}

	return uint(len(events))
}

func (w *OutboxWorker) calculateBatch(
	numEvents uint,
) uint {
	minBatch := w.config.MinButchSize
	maxBatch := w.config.MaxButchSize
	if maxBatch == 0 {
		maxBatch = 100
	}

	var batch uint
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

func (w *OutboxWorker) calculateSleep(
	numEvents uint,
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

	case numEvents >= w.config.MaxButchSize:
		sleep = 0

	default:
		fill := float64(numEvents) / float64(w.config.MaxButchSize)

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

func (w *OutboxWorker) CleanOwnFailedEvents(
	ctx context.Context,
	id string,
) error {
	return w.box.CleanFailedOutboxEventForWorker(ctx, id)
}

func (w *OutboxWorker) CleanOwnProcessingEvents(
	ctx context.Context,
	id string,
) error {
	return w.box.CleanProcessingOutboxEventForWorker(ctx, id)
}

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
