package pg

import (
	"context"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/netbill/logium"
	"github.com/segmentio/kafka-go"
)

type OutboxWorkerConfig struct {
	maxRutin uint

	minSleep time.Duration
	maxSleep time.Duration

	minButchSize uint
	maxButchSize uint
}

type OutboxWorker struct {
	id     string
	log    *logium.Logger
	config OutboxWorkerConfig
	box    outbox

	writer *kafka.Writer
}

func (w *OutboxWorker) Run(ctx context.Context) {
	butchSize := w.config.minButchSize
	sleep := w.config.minSleep

	for {
		if ctx.Err() != nil {
			return
		}

		numEvents := w.tick(ctx, butchSize)
		butchSize, sleep = w.calculateButchAndSleep(numEvents, sleep)

		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}

}

func (w *OutboxWorker) tick(ctx context.Context, butchSize uint) uint {
	events, err := w.box.ReserveOutboxEvents(ctx, w.id, butchSize)
	if err != nil {
		w.log.WithError(err).Error("failed to reserve events")
		return 0
	}
	if len(events) == 0 {
		return 0
	}

	commit := make(map[uuid.UUID]CommitOutboxEventParams, len(events))
	pending := make(map[uuid.UUID]DelayedOutboxEventData, len(events))

	maxParallel := int(w.config.maxRutin)
	if maxParallel <= 0 {
		maxParallel = 1
	}

	sem := make(chan struct{}, maxParallel)
	errCh := make(chan error, len(events))

	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, ev := range events {
		ev := ev

		sem <- struct{}{}
		wg.Add(1)

		go func() {
			defer func() {
				wg.Done()
				<-sem
			}()

			if ctx.Err() != nil {
				return
			}

			msg := ev.ToKafkaMessage()

			sendErr := w.writer.WriteMessages(ctx, msg)
			now := time.Now().UTC()

			if sendErr != nil {
				w.log.WithError(sendErr).Errorf("failed to send event id=%s", ev.EventID)

				mu.Lock()
				pending[ev.EventID] = DelayedOutboxEventData{
					NextAttemptAt: now.Add(5 * time.Minute),
					Reason:        sendErr.Error(),
				}
				mu.Unlock()

				errCh <- sendErr
				return
			}

			w.log.Debugf("sent event id=%s", ev.EventID)

			mu.Lock()
			commit[ev.EventID] = CommitOutboxEventParams{
				SentAt: now,
			}
			mu.Unlock()
		}()
	}

	wg.Wait()
	close(errCh)

	if len(commit) > 0 {
		if err := w.box.CommitOutboxEvents(ctx, w.id, commit); err != nil {
			w.log.WithError(err).Error("failed to mark events as sent")
		}
	}

	if len(pending) > 0 {
		if err := w.box.DelayOutboxEvents(ctx, w.id, pending); err != nil {
			w.log.WithError(err).Error("failed to delay events")
		}
	}

	return uint(len(events))
}

func (w *OutboxWorker) calculateButchAndSleep(numEvents uint, lastSleep time.Duration) (uint, time.Duration) {
	if w.config.maxButchSize <= 0 {
		w.config.maxButchSize = 100
	}

	if numEvents >= w.config.maxButchSize {
		return w.config.maxButchSize, 0
	}

	var sleep time.Duration
	if numEvents == 0 {
		if lastSleep <= 0 {
			sleep = w.config.minSleep
		} else {
			if lastSleep >= w.config.maxSleep {
				sleep = w.config.maxSleep
			} else if lastSleep > w.config.maxSleep/2 {
				sleep = w.config.maxSleep
			} else {
				sleep = lastSleep * 2
				if sleep < w.config.minSleep {
					sleep = w.config.minSleep
				}
				if sleep > w.config.maxSleep {
					sleep = w.config.maxSleep
				}
			}
		}
		if sleep < 0 {
			sleep = 0
		}
		return w.config.minButchSize, sleep
	}

	butchSize := numEvents * 2
	if butchSize < w.config.minButchSize {
		butchSize = w.config.minButchSize
	}
	if butchSize > w.config.maxButchSize {
		butchSize = w.config.maxButchSize
	}

	fill := float64(numEvents) / float64(w.config.maxButchSize)

	switch {
	case fill >= 0.75:
		sleep = 0
	case fill >= 0.50:
		sleep = w.config.minSleep
	case fill >= 0.25:
		sleep = w.config.minSleep * 2
	default:
		sleep = w.config.minSleep * 4
		if sleep < lastSleep {
			sleep = lastSleep
		}
	}

	if sleep > w.config.maxSleep {
		sleep = w.config.maxSleep
	}
	if sleep < 0 {
		sleep = 0
	}

	return butchSize, sleep
}

func (w *OutboxWorker) CleanOwnFailedEvents(ctx context.Context) error {
	return w.box.CleanFailedOutboxEventForWorker(ctx, w.id)
}

func (w *OutboxWorker) CleanOwnProcessingEvents(ctx context.Context) error {
	return w.box.CleanProcessingOutboxEventForWorker(ctx, w.id)
}

func (w *OutboxWorker) Shutdown(ctx context.Context) error {
	err := w.CleanOwnProcessingEvents(ctx)
	if err != nil {
		w.log.WithError(err).Error("failed to clean processing events for worker")
	}

	return err
}
