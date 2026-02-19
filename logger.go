package eventbox

import "github.com/segmentio/kafka-go"

type Logger interface {
	WithField(key string, value any) Logger
	WithFields(fields map[string]any) Logger
	WithError(err error) Logger

	// WithMessage adds the specific fields from the kafka message
	WithMessage(m kafka.Message) Logger
	// WithInboxEvent adds the specific fields from the outbox event
	WithInboxEvent(ev InboxEvent) Logger
	// WithOutboxEvent adds the specific fields from the inbox event
	WithOutboxEvent(ev OutboxEvent) Logger
	// WithTopic adds the topic field to the logger
	WithTopic(topic string) Logger

	Debug(args ...any)
	Debugf(format string, args ...any)
	Info(args ...any)
	Infof(format string, args ...any)
	Warn(args ...any)
	Warnf(format string, args ...any)
	Error(args ...any)
	Errorf(format string, args ...any)
}
