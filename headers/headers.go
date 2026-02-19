package headers

import (
	"fmt"
	"strconv"

	"github.com/google/uuid"
	"github.com/segmentio/kafka-go"
)

const (
	EventID      = "event_id"
	EventType    = "event_type"
	EventVersion = "event_version"
	Producer     = "outbound"
	ContentType  = "content_type"
)

type MessageRequired struct {
	EventID      uuid.UUID
	EventType    string
	EventVersion int32
	Producer     string
	ContentType  string
}

func (m MessageRequired) ToKafka() []kafka.Header {
	return []kafka.Header{
		{Key: EventID, Value: []byte(m.EventID.String())},
		{Key: EventType, Value: []byte(m.EventType)},
		{Key: EventVersion, Value: []byte(strconv.FormatInt(int64(m.EventVersion), 10))},
		{Key: Producer, Value: []byte(m.Producer)},
		{Key: ContentType, Value: []byte(m.ContentType)},
	}
}

func RequiredHeaderValue(hs []kafka.Header, key string) ([]byte, error) {
	var (
		found bool
		value []byte
	)

	for i := range hs {
		if hs[i].Key != key {
			continue
		}

		if found {
			return nil, fmt.Errorf("duplicate %s header", key)
		}

		found = true
		value = hs[i].Value
	}

	if !found {
		return nil, fmt.Errorf("missing %s header", key)
	}

	return value, nil
}

func ParseMessageRequiredHeaders(hs []kafka.Header) (MessageRequired, error) {
	var out MessageRequired

	eventIDBytes, err := RequiredHeaderValue(hs, EventID)
	if err != nil {
		return out, err
	}
	out.EventID, err = uuid.ParseBytes(eventIDBytes)
	if err != nil {
		return out, fmt.Errorf("invalid %s header: %w", EventID, err)
	}

	eventTypeBytes, err := RequiredHeaderValue(hs, EventType)
	if err != nil {
		return out, err
	}
	out.EventType = string(eventTypeBytes)

	eventVersionBytes, err := RequiredHeaderValue(hs, EventVersion)
	if err != nil {
		return out, err
	}
	v64, err := strconv.ParseInt(string(eventVersionBytes), 10, 32)
	if err != nil {
		return MessageRequired{}, fmt.Errorf("invalid %s header: %w", EventVersion, err)
	}
	out.EventVersion = int32(v64)

	producerBytes, err := RequiredHeaderValue(hs, Producer)
	if err != nil {
		return out, err
	}
	out.Producer = string(producerBytes)

	contentTypeBytes, err := RequiredHeaderValue(hs, ContentType)
	if err != nil {
		return out, err
	}
	out.ContentType = string(contentTypeBytes)

	return out, nil
}
