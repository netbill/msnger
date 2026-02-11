package pg

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/netbill/logium"
	"github.com/netbill/msnger"
	"github.com/netbill/msnger/logfields"
	"github.com/netbill/pgdbx"
)

const (
	DefaultInboxProcessorRoutines = 10

	DefaultInboxProcessorMaxSleep = 5 * time.Second
	DefaultInboxProcessorMinSleep = 50 * time.Millisecond

	DefaultInboxProcessorMaxBatch = 100
	DefaultInboxProcessorMinBatch = 10

	DefaultInboxProcessorMinNextAttempt = time.Minute
	DefaultInboxProcessorMaxNextAttempt = 10 * time.Minute
)

// InboxProcessorConfig defines configuration for InboxProcessor.
type InboxProcessorConfig struct {
	// Routines is the maximum number of parallel handle loops.
	// If 0, it defaults to DefaultInboxProcessorRoutines.
	Routines int

	// MinSleep is the minimum delay between iterations when there are few/no events.
	// If 0, it defaults to DefaultInboxProcessorMinSleep.
	MinSleep time.Duration
	// MaxSleep is the maximum delay between iterations during long idle periods.
	// If 0, it defaults to DefaultInboxProcessorMaxSleep.
	MaxSleep time.Duration

	// MinBatch is the minimum number of events reserved per batch.
	// If 0, it defaults to DefaultInboxProcessorMinBatch.
	MinBatch int
	// MaxBatch is the maximum number of events reserved per batch.
	// If 0, it defaults to DefaultInboxProcessorMaxBatch.
	MaxBatch int

	// MinNextAttempt is the minimum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultInboxProcessorMinNextAttempt.
	MinNextAttempt time.Duration
	// MaxNextAttempt is the maximum delay before next attempt to process failed event.
	// If 0, it defaults to DefaultInboxProcessorMaxNextAttempt.
	MaxNextAttempt time.Duration

	// MaxAttempts is the maximum number of attempts to process event before marking it as failed.
	// If 0, the event will always receive the status InboxEventStatusPending in case of failure of processing.
	MaxAttempts int32
}

type InboxProcessor struct {
	log    *logium.Entry
	config InboxProcessorConfig

	box   inbox
	route map[string]msnger.InboxHandlerFunc
}

// NewInboxProcessor creates a new InboxProcessor.
func NewInboxProcessor(
	log *logium.Entry,
	db *pgdbx.DB,
	config InboxProcessorConfig,
) *InboxProcessor {
	if config.Routines <= 0 {
		config.Routines = DefaultInboxProcessorRoutines
	}
	if config.MinSleep <= 0 {
		config.MinSleep = DefaultInboxProcessorMinSleep
	}
	if config.MaxSleep <= 0 {
		config.MaxSleep = DefaultInboxProcessorMaxSleep
	}
	if config.MinBatch <= 0 {
		config.MinBatch = DefaultInboxProcessorMinBatch
	}
	if config.MaxBatch <= 0 {
		config.MaxBatch = DefaultInboxProcessorMaxBatch
	}
	if config.MinNextAttempt <= 0 {
		config.MinNextAttempt = DefaultInboxProcessorMinNextAttempt
	}
	if config.MaxNextAttempt <= 0 {
		config.MaxNextAttempt = DefaultInboxProcessorMaxNextAttempt
	}

	return &InboxProcessor{
		log:    log,
		box:    inbox{db: db},
		config: config,
		route:  make(map[string]msnger.InboxHandlerFunc),
	}
}

type inboxProcessorSlot struct{}

func takeSlot(ctx context.Context, slots <-chan inboxProcessorSlot) bool {
	select {
	case <-ctx.Done():
		return false
	case <-slots:
		return true
	}
}

func giveSlot(slots chan<- inboxProcessorSlot) {
	select {
	case slots <- inboxProcessorSlot{}:
	default:
	}
}

type inboxProcessorJob struct {
	event msnger.InboxEvent
}

func sendJob(ctx context.Context, jobs chan<- inboxProcessorJob, job inboxProcessorJob) bool {
	select {
	case <-ctx.Done():
		return false
	case jobs <- job:
		return true
	}
}

func (p *InboxProcessor) Route(eventType string, handler msnger.InboxHandlerFunc) {
	if _, ok := p.route[eventType]; ok {
		panic(fmt.Errorf("double handler for event type=%s", eventType))
	}

	p.route[eventType] = handler
}

// RunProcess starts processing inbox events for the given process ID.
func (p *InboxProcessor) RunProcess(ctx context.Context, processID string) {
	entry := p.log.WithField(logfields.ProcessID, processID)

	defer func() {
		if err := p.StopProcess(context.Background(), processID); err != nil {
			entry.WithError(err).Error("failed to stop inbox processor")
		}
	}()

	entry.Info("starting inbox processor")

	// Initialize the slots channel with the configured number of in-flight processing slots.
	slots := make(chan inboxProcessorSlot, p.config.Routines*4)
	for i := 0; i < p.config.Routines*4; i++ {
		slots <- inboxProcessorSlot{}
	}

	// Initialize the jobs channel for processing events.
	jobs := make(chan inboxProcessorJob, p.config.Routines*4)

	var wg sync.WaitGroup
	wg.Add(p.config.Routines + 1)

	go func() {
		defer wg.Done()
		p.feederLoop(ctx, processID, slots, jobs)
	}()

	for i := 0; i < p.config.Routines; i++ {
		go func() {
			defer wg.Done()
			p.handleLoop(ctx, processID, slots, jobs)
		}()
	}

	wg.Wait()
}

// feederLoop continuously reserves batches of inbox events and sends them to the jobs channel for processing.
func (p *InboxProcessor) feederLoop(
	ctx context.Context,
	processID string,
	slots chan inboxProcessorSlot,
	jobs chan<- inboxProcessorJob,
) {
	defer close(jobs)

	sleep := p.config.MinSleep
	maxSleep := p.config.MaxSleep

	for {
		if ctx.Err() != nil {
			return
		}

		free := len(slots)
		if free == 0 {
			p.sleep(ctx, 0, sleep)

			continue
		}

		limit := calculateBatch(free, p.config.MinBatch, p.config.MaxBatch)

		// Reserve a batch of inbox events for processing. If there are no events,
		// increase the sleep duration before the next attempt.
		events, err := p.box.ReserveInboxEvents(ctx, processID, limit)
		if err != nil {
			p.log.WithError(err).
				WithField(logfields.ProcessID, processID).
				Error("failed to reserve inbox events")

			p.sleep(ctx, 0, sleep)

			continue
		}

		if len(events) == 0 {
			next := sleep * 2
			if next > maxSleep {
				next = maxSleep
			}

			p.sleep(ctx, len(events), sleep)

			continue
		}

		sleep = p.config.MinSleep

		for _, ev := range events {
			if !takeSlot(ctx, slots) {
				return
			}
			if !sendJob(ctx, jobs, inboxProcessorJob{event: ev}) {
				giveSlot(slots)
				return
			}
		}
	}
}

// handleLoop continuously processes inbox events received from the jobs channel.
func (p *InboxProcessor) handleLoop(
	ctx context.Context,
	processID string,
	slots chan inboxProcessorSlot,
	jobs <-chan inboxProcessorJob,
) {
	for job := range jobs {
		ev := job.event

		err := p.box.Transaction(ctx, func(ctx context.Context) error {
			var err error

			hErr := p.handleEvent(ctx, ev)
			if hErr != nil {
				switch {
				case p.config.MaxAttempts != 0 && ev.Attempts >= p.config.MaxAttempts:
					ev, err = p.box.FailedInboxEvent(ctx, processID, ev.EventID, hErr.Error())
					if err != nil {
						return err
					}

					return nil
				default:
					ev, err = p.box.DelayInboxEvent(ctx, processID, ev.EventID, hErr.Error(), p.nextAttemptAt(ev.Attempts))
					if err != nil {
						return err
					}

					return nil
				}
			}

			ev, err = p.box.CommitInboxEvent(ctx, processID, ev.EventID)
			if err != nil {
				return err
			}

			return nil
		})
		if err != nil {
			p.log.WithError(err).
				WithField(logfields.ProcessID, processID).
				WithFields(logfields.FromInboxEvent(ev)).
				Error("failed to process inbox event")
		}

		giveSlot(slots)

		if ctx.Err() != nil {
			return
		}
	}
}

// handleEvent processes a single inbox event by looking up the appropriate handler based on the event type.
func (p *InboxProcessor) handleEvent(ctx context.Context, event msnger.InboxEvent) error {
	handler, ok := p.route[event.Type]
	if !ok {
		p.log.WithFields(logfields.FromInboxEvent(event)).
			Infof("no handler for event type=%s", event.Type)

		return nil
	}

	return handler(ctx, event.ToKafkaMessage())
}

// nextAttemptAt calculates the next attempt time for processing a failed event based on the number of attempts
// and the configured minimum and maximum next attempt durations.
func (p *InboxProcessor) nextAttemptAt(attempts int32) time.Time {
	res := time.Second * time.Duration(30*attempts)
	if res < p.config.MinNextAttempt {
		return time.Now().UTC().Add(p.config.MinNextAttempt)
	}
	if res > p.config.MaxNextAttempt {
		return time.Now().UTC().Add(p.config.MaxNextAttempt)
	}

	return time.Now().UTC().Add(res)
}

func (p *InboxProcessor) sleep(
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

// StopProcess stops processing inbox events for the given process ID by cleaning up any events that are currently marked as processing for that processor.
func (p *InboxProcessor) StopProcess(ctx context.Context, processID string) error {
	return p.box.CleanProcessingInboxEvents(ctx, processID)
}
