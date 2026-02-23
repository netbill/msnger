package eventbox

import (
	"context"

	"github.com/netbill/eventbox/headers"
	"github.com/netbill/logium"
	"github.com/segmentio/kafka-go"
)

const (
	EventIDField       = "event_id"
	EventTypeField     = "event_type"
	EventTopicField    = "event_topic"
	EventVersionField  = "event_version"
	EventProducerField = "event_producer"
	EventAttemptField  = "event_attempt"
)

type Logger struct {
	base logium.Logger
}

func NewLogger(l logium.Logger) *Logger {
	return &Logger{base: l}
}

func (l *Logger) WithFields(fields map[string]any) *Logger {
	return &Logger{base: l.base.WithFields(fields)}
}

func (l *Logger) WithField(key string, value any) *Logger {
	return &Logger{base: l.base.WithField(key, value)}
}

func (l *Logger) WithError(err error) *Logger {
	return &Logger{base: l.base.WithError(err)}
}

func (l *Logger) WithTopic(topic string) *Logger {
	return &Logger{base: l.base.WithField(EventTopicField, topic)}
}

func (l *Logger) WithMessage(msg kafka.Message) *Logger {
	fields := map[string]any{
		EventTopicField:    msg.Topic,
		EventIDField:       "unknown",
		EventTypeField:     "unknown",
		EventProducerField: "unknown",
		EventVersionField:  "unknown",
	}

	hs, err := headers.ParseMessageRequiredHeaders(msg.Headers)
	if err == nil {
		fields[EventIDField] = hs.EventID.String()
		fields[EventTypeField] = hs.EventType
		fields[EventVersionField] = int(hs.EventVersion)
		fields[EventProducerField] = hs.Producer
	}

	return &Logger{base: l.base.WithFields(fields)}
}

func (l *Logger) WithOutboxEvent(ev OutboxEvent) *Logger {
	return &Logger{base: l.base.WithFields(map[string]any{
		EventIDField:       ev.EventID.String(),
		EventTopicField:    ev.Topic,
		EventTypeField:     ev.Type,
		EventVersionField:  int(ev.Version),
		EventProducerField: ev.Producer,
		EventAttemptField:  int(ev.Attempts),
	})}
}

func (l *Logger) WithInboxEvent(ev InboxEvent) *Logger {
	return &Logger{base: l.base.WithFields(map[string]any{
		EventIDField:       ev.EventID.String(),
		EventTopicField:    ev.Topic,
		EventTypeField:     ev.Type,
		EventVersionField:  int(ev.Version),
		EventProducerField: ev.Producer,
		EventAttemptField:  int(ev.Attempts),
	})}
}

func (l *Logger) Debug(msg string, args ...any) {
	l.base.Debug(msg, args...)
}

func (l *Logger) Info(msg string, args ...any) {
	l.base.Info(msg, args...)
}

func (l *Logger) Warn(msg string, args ...any) {
	l.base.Warn(msg, args...)
}

func (l *Logger) Error(msg string, args ...any) {
	l.base.Error(msg, args...)
}

func (l *Logger) DebugContext(ctx context.Context, msg string, args ...any) {
	l.base.DebugContext(ctx, msg, args...)
}
func (l *Logger) InfoContext(ctx context.Context, msg string, args ...any) {
	l.base.InfoContext(ctx, msg, args...)
}
func (l *Logger) WarnContext(ctx context.Context, msg string, args ...any) {
	l.base.WarnContext(ctx, msg, args...)
}
func (l *Logger) ErrorContext(ctx context.Context, msg string, args ...any) {
	l.base.ErrorContext(ctx, msg, args...)
}
