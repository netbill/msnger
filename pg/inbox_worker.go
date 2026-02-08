package pg

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"

	"github.com/netbill/logium"
	"github.com/netbill/msnger"
	"github.com/netbill/msnger/headers"
)

type InboxWorkerConfig struct {
	maxRutin uint
	minSleep time.Duration
	maxSleep time.Duration

	minButchSize uint
	maxButchSize uint
}

type InboxWorker struct {
	id     string
	log    *logium.Logger
	config InboxWorkerConfig

	box      inbox
	handlers map[string]msnger.InHandlerFunc
}

func (i *InboxWorker) Route(eventType string, handler msnger.InHandlerFunc) {
	_, ok := i.handlers[eventType]
	if ok {
		panic(fmt.Errorf("for one type event double define handler in inbox worker %s", i.id))
	}

	i.handlers[eventType] = handler
}

func (i *InboxWorker) onUnknown(ctx context.Context, m kafka.Message) error {
	i.log.Warnf(
		"onUnknown called for event on topic %s, partition %d, offset %d", m.Topic, m.Partition, m.Offset,
	)

	return nil
}

func (i *InboxWorker) invalidContent(ctx context.Context, m kafka.Message) error {
	i.log.Warnf(
		"invalid content in message on topic %s, partition %d, offset %d", m.Topic, m.Partition, m.Offset,
	)

	return nil
}

func (i *InboxWorker) Run(ctx context.Context) {
	butchSize := i.config.maxButchSize
	sleep := i.config.minSleep

	for {
		if ctx.Err() != nil {
			return
		}

		numEvents := i.tick(ctx, butchSize)

		butchSize, sleep = i.calculateButchAndSleep(numEvents, sleep)

		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

func (i *InboxWorker) tick(ctx context.Context, butchSize uint) uint {
	events, err := i.box.ReserveInboxEvents(ctx, i.id, butchSize)
	if err != nil {
		i.log.WithError(err).Error("failed to reserve inbox events")
		return 0
	}
	if len(events) == 0 {
		return 0
	}

	maxParallel := int(i.config.maxRutin)
	if maxParallel <= 0 {
		maxParallel = 1
	}

	sem := make(chan struct{}, maxParallel)
	errCh := make(chan error, len(events))

	var wg sync.WaitGroup

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

			txErr := i.box.Transaction(ctx, func(ctx context.Context) error {
				handler, ok := i.handlers[ev.Type]
				if !ok {
					return i.onUnknown(ctx, ev.ToKafkaMessage())
				}

				res := handler(ctx, ev.ToKafkaMessage())
				if res != nil {
					i.log.WithError(res).Errorf("handler for event %s type=%s failed", ev.EventID, ev.Type)
					return i.box.DelayInboxEvent(ctx, i.id, ev.EventID, i.nextAttemptAt(ev.Attempts), res.Error())
				}

				i.log.Debugf("event %s type=%s processed successfully", ev.EventID, ev.Type)
				return i.box.CommitInboxEvent(ctx, i.id, ev.EventID)
			})

			if txErr != nil {
				errCh <- txErr
			}
		}()
	}

	wg.Wait()

	close(errCh)

	for e := range errCh {
		i.log.WithError(e).Error("failed to process inbox event")
	}

	return uint(len(events))
}

func (i *InboxWorker) nextAttemptAt(attempts uint) time.Duration {
	res := time.Second * time.Duration(30*attempts)
	if res < time.Minute {
		return time.Minute
	}
	if res > time.Minute*10 {
		return time.Minute * 10
	}

	return res
}

func (i *InboxWorker) calculateButchAndSleep(
	numEvents uint,
	lastSleep time.Duration,
) (uint, time.Duration) {
	if numEvents >= i.config.maxButchSize {
		return i.config.maxButchSize, i.config.minSleep
	}

	var sleep time.Duration
	if numEvents == 0 {
		if lastSleep <= 0 {
			sleep = i.config.minSleep
		} else {
			sleep = lastSleep * 2
			if sleep < i.config.minSleep {
				sleep = i.config.minSleep
			}
		}
		if sleep > i.config.maxSleep {
			sleep = i.config.maxSleep
		}
		if sleep < 0 {
			sleep = 0
		}

		return i.config.minButchSize, sleep
	}

	butchSize := numEvents * 2
	if butchSize < i.config.minButchSize {
		butchSize = i.config.minButchSize
	}
	if butchSize > i.config.maxButchSize {
		butchSize = i.config.maxButchSize
	}

	fill := float64(numEvents) / float64(i.config.maxButchSize)

	switch {
	case fill >= 0.75:
		sleep = 0
	case fill >= 0.50:
		sleep = i.config.minSleep
	case fill >= 0.25:
		sleep = i.config.minSleep * 2
	default:
		sleep = i.config.minSleep * 4
		if sleep < lastSleep {
			sleep = lastSleep
		}
	}

	return butchSize, sleep
}

func (i *InboxWorker) CleanOwnFailedEvents(ctx context.Context) error {
	return i.box.CleanFailedInboxEventForWorker(ctx, i.id)
}

func (i *InboxWorker) CleanOwnProcessingEvents(ctx context.Context) error {
	return i.box.CleanProcessingInboxEventForWorker(ctx, i.id)
}

func (i *InboxWorker) Shutdown(ctx context.Context) error {
	err := i.CleanOwnProcessingEvents(ctx)
	if err != nil {
		i.log.WithError(err).Errorf("failed to clean processing inbox events for worker %s", i.id)
	}

	return err
}
