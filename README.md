# eventbox

**eventbox** is a Go library implementing the [Transactional Inbox/Outbox pattern](https://microservices.io/patterns/data/transactional-outbox.html) on top of PostgreSQL and Kafka. It guarantees **at-least-once delivery** with **per-key ordering**.

---

## Table of Contents

- [Concepts](#concepts)
- [Architecture](#architecture)
- [Installation](#installation)
- [Database Setup](#database-setup)
- [The Four Entities](#the-four-entities)
    - [Producer](#producer)
    - [Outbox + OutboxWorker](#outbox--outboxworker)
    - [Consumer](#consumer)
    - [Inbox + InboxWorker](#inbox--inboxworker)
- [Putting It All Together](#putting-it-all-together)
- [API Reference](#api-reference)
- [Configuration Reference](#configuration-reference)
- [Event Statuses](#event-statuses)
- [Message Headers](#message-headers)

---

## Concepts

### Overview

- **Outbox**: Write events to `outbox_events` inside your business transaction. A background worker reads and publishes them to Kafka.
- **Inbox**: Write incoming Kafka messages to `inbox_events` before processing. A background worker dispatches them to your handlers with full retry logic.
- **Producer**: Manages Kafka writers for outbound topics. Used exclusively by `OutboxWorker`.
- **Consumer**: Manages Kafka readers for inbound topics. Writes messages to `inbox'
### Per-Key Ordering

Events with the same `topic + key` are always processed in sequence. The reservation query ensures that if a key is already `processing`, no other worker picks up a newer event for that key.

### Worker Concurrency Model

Both workers use a **feeder + handler goroutine pool** pattern. A single feeder reserves batches from the database and pushes jobs to a bounded channel; a pool of goroutines processes them concurrently. The number of in-flight events is capped by `Slots`.

---

## Architecture

```
Your Service

  Business Logic                outbox_events (pg)
       │                               │
       └──── WriteOutboxEvent ────────►│
             (same db transaction)     │
                                OutboxWorker
                                + Producer ──────────────────► Kafka topic


  Kafka topic ──► Consumer ──► inbox_events (pg)
                                       │
                                 InboxWorker
                                 + your handlers
```

---

## Installation

```bash
go get github.com/netbill/eventbox
```

Requires: `pgx/v5`, `kafka-go`, `pgdbx`, `logium`.

---

## Database Setup

### Outbox

```sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE outbox_event_status AS ENUM ('pending', 'sent', 'processing', 'failed');

CREATE TABLE outbox_events (
    event_id        UUID PRIMARY KEY NOT NULL,
    seq             BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL UNIQUE CHECK (seq >= 0),
    topic           VARCHAR NOT NULL,
    key             VARCHAR NOT NULL,
    type            VARCHAR NOT NULL,
    version         INT NOT NULL,
    producer        VARCHAR NOT NULL,
    payload         JSONB NOT NULL,
    reserved_by     VARCHAR,
    status          outbox_event_status NOT NULL DEFAULT 'pending',
    attempts        INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    last_attempt_at TIMESTAMPTZ,
    last_error      VARCHAR,
    sent_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE INDEX outbox_events_pending_ready_idx ON outbox_events (next_attempt_at, seq)
    WHERE status = 'pending';
CREATE INDEX outbox_events_key_idx  ON outbox_events (key);
CREATE INDEX outbox_events_type_idx ON outbox_events (type);
```

### Inbox

```sql
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TYPE inbox_event_status AS ENUM ('pending', 'processed', 'processing', 'failed');

CREATE TABLE inbox_events (
    event_id        UUID PRIMARY KEY NOT NULL,
    seq             BIGINT GENERATED ALWAYS AS IDENTITY NOT NULL UNIQUE CHECK (seq >= 0),
    topic           TEXT NOT NULL,
    key             TEXT NOT NULL,
    type            TEXT NOT NULL,
    version         INT NOT NULL,
    producer        TEXT NOT NULL,
    payload         JSONB NOT NULL,
    partition       INT NOT NULL CHECK (partition >= 0),
    kafka_offset    BIGINT NOT NULL CHECK (kafka_offset >= 0),
    reserved_by     TEXT,
    status          inbox_event_status NOT NULL DEFAULT 'pending',
    attempts        INT NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC'),
    last_attempt_at TIMESTAMPTZ,
    last_error      TEXT,
    processed_at    TIMESTAMPTZ,
    produced_at     TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')
);

CREATE UNIQUE INDEX inbox_events_kafka_pos_uidx ON inbox_events (topic, partition, kafka_offset);
CREATE INDEX inbox_events_pending_ready_idx     ON inbox_events (next_attempt_at, produced_at, partition, kafka_offset)
    WHERE status = 'pending' AND reserved_by IS NULL;
CREATE INDEX inbox_events_processing_key_idx    ON inbox_events (topic, key)
    WHERE status = 'processing' AND reserved_by IS NOT NULL;
```

---

## The Four Entities

### Producer

`Producer` manages per-topic Kafka writers. Each topic gets its own writer with independent configuration. You register writers upfront with `AddWriter` — one call per topic.

In practice, wrap `Producer` creation in a constructor that maps your service config to `WriterTopicConfig`:

```go
func NewProducer(log *log.Logger, cfg ProducerConfig) (*eventbox.Producer, error) {
    producer := eventbox.NewProducer(log, cfg.Brokers...)

    if err := producer.AddWriter(topics.OrganizationsV1, eventbox.WriterTopicConfig{
        RequiredAcks: cfg.OrganizationsV1.RequiredAcks,
        Compression:  cfg.OrganizationsV1.Compression,
        Balancer:     cfg.OrganizationsV1.Balancer,
        BatchSize:    cfg.OrganizationsV1.BatchSize,
        BatchTimeout: cfg.OrganizationsV1.BatchTimeout,
    }); err != nil {
        return nil, err
    }

    if err := producer.AddWriter(topics.OrgMembersV1, eventbox.WriterTopicConfig{
        RequiredAcks: cfg.OrgMembersV1.RequiredAcks,
        Compression:  cfg.OrgMembersV1.Compression,
        Balancer:     cfg.OrgMembersV1.Balancer,
        BatchSize:    cfg.OrgMembersV1.BatchSize,
        BatchTimeout: cfg.OrgMembersV1.BatchTimeout,
    }); err != nil {
        return nil, err
    }

    return producer, nil
}
```

> ⚠️ For outbox use, always set `Balancer: "hash"`, `RequiredAcks: "all"`, `Async: false`. Any other combination risks message loss or out-of-order delivery.

`Producer` is consumed exclusively by `OutboxWorker` — your application code never calls `WriteToKafka` directly.

---

### Outbox + OutboxWorker

`Outbox` is the write-side repository. You call `WriteOutboxEvent` inside your business transaction to durably enqueue an event. If the transaction rolls back, the event is never enqueued.

In practice, wrap outbox writes in a domain-specific publisher so business logic never constructs `eventbox.Message` directly:

```go
type Publisher struct {
    identity string
    outbox   eventbox.Outbox
}

func (p *Publisher) WriteOrganizationCreated(ctx context.Context, org models.Organization) error {
    payload, err := json.Marshal(OrganizationCreatedPayload{
        OrganizationID: org.ID,
        Name:           org.Name,
        CreatedAt:      org.CreatedAt,
    })
    if err != nil {
        return err
    }

    _, err = p.outbox.WriteOutboxEvent(ctx, eventbox.Message{
        ID:       uuid.New(),
        Type:     events.OrganizationCreated,
        Version:  1,
        Topic:    topics.OrganizationsV1,
        Key:      org.ID.String(),
        Payload:  payload,
        Producer: p.identity,
    })
    return err
}
```

`OutboxWorker` is a background process that polls `outbox_events`, sends events to Kafka via `Producer`, and updates statuses in bulk. Start it alongside your application:

```go
func NewOutboxWorker(
    log *log.Logger,
    outbox eventbox.Outbox,
    producer *eventbox.Producer,
    cfg eventbox.OutboxWorkerConfig,
) *eventbox.OutboxWorker {
    return eventbox.NewOutboxWorker(uuid.New().String(), log, outbox, producer, cfg)
}
```

```go
outboxWorker := NewOutboxWorker(log, outbox, producer, eventbox.OutboxWorkerConfig{
    Routines:       cfg.Routines,
    BatchSize:      cfg.BatchSize,
    MaxAttempts:    cfg.MaxAttempts,
    MinNextAttempt: cfg.MinNextAttempt,
    MaxNextAttempt: cfg.MaxNextAttempt,
})
defer outboxWorker.Clean() // releases reserved events on shutdown
go outboxWorker.Run(ctx)
```

`Clean()` must always be deferred after `Run` — it resets any events that were reserved by this worker but not committed before shutdown.

---

### Consumer

`Consumer` is the Kafka reader. It fetches messages from one or more topics and writes them to `inbox_events`. Kafka offset is committed only after a successful database write, so no message is ever lost.

Each topic gets one or more reader goroutines configured via `AddReader`. In practice, wrap the constructor to register all topics for your service:

```go
func NewConsumer(log *log.Logger, inbox eventbox.Inbox, cfg ConsumerConfig) *eventbox.Consumer {
    consumer := eventbox.NewConsumer(log, inbox, eventbox.ConsumerConfig{
        MinBackoff: cfg.MinBackoff,
        MaxBackoff: cfg.MaxBackoff,
    })

    consumer.AddReader(eventbox.ReaderConfig{
        Brokers:       cfg.Brokers,
        GroupID:       cfg.GroupID,
        Topic:         topics.ProfilesV1,
        Instances:     cfg.ProfilesV1.Instances,
        MaxWait:       cfg.ProfilesV1.MaxWait,
        MinBytes:      cfg.ProfilesV1.MinBytes,
        MaxBytes:      cfg.ProfilesV1.MaxBytes,
        QueueCapacity: cfg.ProfilesV1.QueueCapacity,
    })

    consumer.AddReader(eventbox.ReaderConfig{
        Brokers:       cfg.Brokers,
        GroupID:       cfg.GroupID,
        Topic:         topics.PlacesV1,
        Instances:     cfg.PlacesV1.Instances,
        MaxWait:       cfg.PlacesV1.MaxWait,
        MinBytes:      cfg.PlacesV1.MinBytes,
        MaxBytes:      cfg.PlacesV1.MaxBytes,
        QueueCapacity: cfg.PlacesV1.QueueCapacity,
    })

    return consumer
}
```

`Instances` controls how many parallel reader goroutines are spawned for that topic. Backoff applies per-reader on fetch or write errors.

---

### Inbox + InboxWorker

`Inbox` is the read-side repository. `InboxWorker` polls `inbox_events`, dispatches each event to a registered handler by `event.Type`, and updates the status based on the result.

Handlers are plain functions with the signature `func(ctx context.Context, event eventbox.InboxEvent) error`. Register them with `Route` — one handler per event type. Registering the same type twice panics.

In practice, group handler registration by domain controller:

```go
func NewInboxWorker(deps InboxWorkerDeps) *eventbox.InboxWorker {
    worker := eventbox.NewInboxWorker(uuid.New().String(), deps.Logger, deps.Inbox, deps.Config)

    worker.Route(events.ProfileCreated, deps.ProfileController.Created)
    worker.Route(events.ProfileUpdated, deps.ProfileController.Updated)
    worker.Route(events.ProfileDeleted, deps.ProfileController.Deleted)

    worker.Route(events.PlaceCreated, deps.PlaceController.Created)
    worker.Route(events.PlaceDeleted, deps.PlaceController.Deleted)

    return worker
}
```

Each controller method unmarshals the payload and calls domain logic. Returning an error triggers retry; returning `nil` commits the event as processed:

```go
func (c *ProfileController) Created(ctx context.Context, event eventbox.InboxEvent) error {
    var payload ProfileCreatedPayload
    if err := json.Unmarshal(event.Payload, &payload); err != nil {
        return err
    }

    _, err := c.core.Create(ctx, profile.CreateParams{
        AccountID: payload.AccountID,
        Username:  payload.Username,
        CreatedAt: payload.CreatedAt,
    })
    switch {
    case errors.Is(err, ErrAlreadyExists):
        return nil // idempotent — already created, skip
    case err != nil:
        return err // will be retried
    default:
        return nil
    }
}
```

**Retry behavior:**
- On error → delayed with exponential backoff: `MinNextAttempt × attempts`, capped at `MaxNextAttempt`.
- Once `attempts >= MaxAttempts` → event marked `failed`, no more retries.
- `MaxAttempts: 0` → retries forever, never marked `failed`.
- Events with no registered handler are silently acknowledged (committed as processed).

---

## Putting It All Together

Below is a condensed wiring example showing how all four entities are initialized and started:

```go
func (a *App) Run(ctx context.Context) error {
    db := pgdbx.NewDB(pool)

    // repositories
    outbox := eventpg.NewOutbox(db)
    inbox  := eventpg.NewInbox(db)

    // producer: one writer per outbound topic
    producer, err := NewProducer(a.log, a.config.Kafka.Producer)
    if err != nil {
        return err
    }
    defer producer.Close()

    // domain publisher wraps outbox writes
    pub := publisher.New(a.config.Kafka.Identity, outbox)

    // outbox worker: polls outbox_events, sends to Kafka
    outboxWorker := NewOutboxWorker(a.log, outbox, producer, a.config.Kafka.Outbox)
    defer outboxWorker.Clean()
    go outboxWorker.Run(ctx)

    // consumer: Kafka → inbox_events
    consumer := NewConsumer(a.log, inbox, a.config.Kafka.Consumer)
    defer consumer.Close()
    go consumer.Run(ctx)

    // inbox worker: polls inbox_events, dispatches to handlers
    inboxWorker := NewInboxWorker(InboxWorkerDeps{
        Logger:            a.log,
        Inbox:             inbox,
        ProfileController: evcontrollers.NewProfileController(profileCore),
        PlaceController:   evcontrollers.NewPlaceController(placeCore),
        Config:            a.config.Kafka.Inbox,
    })
    defer inboxWorker.Clean()
    go inboxWorker.Run(ctx)

    // ... rest of your application (HTTP server, etc.)
}
```

The `publisher` (`pub`) is injected into domain services that need to emit events as part of their transactions. The workers and consumer run as background goroutines for the lifetime of the context.

---

## API Reference

### Outbox Interface

```go
type Outbox interface {
    // Saves a new event. Returns ErrOutboxEventAlreadyExists on duplicate ID.
    WriteOutboxEvent(ctx context.Context, message Message) (OutboxEvent, error)

    // Returns ErrOutboxEventNotFound if not found.
    GetOutboxEventByID(ctx context.Context, id uuid.UUID) (OutboxEvent, error)

    // Atomically reserves a batch for a worker, skipping keys already in-flight.
    ReserveOutboxEvents(ctx context.Context, workerID string, limit int) ([]OutboxEvent, error)

    // Marks a batch as sent.
    CommitOutboxEvents(ctx context.Context, workerID string, events map[uuid.UUID]CommitOutboxEventParams) error

    // Re-queues a batch for retry at a future time.
    DelayOutboxEvents(ctx context.Context, workerID string, events map[uuid.UUID]DelayOutboxEventData) error

    // Permanently marks a batch as failed.
    FailedOutboxEvents(ctx context.Context, workerID string, events map[uuid.UUID]FailedOutboxEventData) error

    // Resets stuck "processing" events back to "pending".
    // Without workerIDs — cleans all; with workerIDs — targets specific workers only.
    CleanProcessingOutboxEvents(ctx context.Context, workerIDs ...string) error

    // Resets all "failed" events back to "pending".
    CleanFailedOutboxEvents(ctx context.Context) error
}
```

### Inbox Interface

```go
type Inbox interface {
    // Saves a Kafka message. Returns ErrInboxEventAlreadyExists on duplicate.
    WriteInboxEvent(ctx context.Context, message kafka.Message) (InboxEvent, error)

    // Returns ErrInboxEventNotFound if not found.
    GetInboxEventByID(ctx context.Context, id uuid.UUID) (InboxEvent, error)

    // Atomically reserves a batch, skipping keys already in-flight.
    ReserveInboxEvents(ctx context.Context, workerID string, limit int) ([]InboxEvent, error)

    // Marks a single event as processed.
    CommitInboxEvent(ctx context.Context, workerID string, eventID uuid.UUID) (InboxEvent, error)

    // Re-queues an event for retry at nextAttemptAt.
    DelayInboxEvent(ctx context.Context, workerID string, eventID uuid.UUID, reason string, nextAttemptAt time.Time) (InboxEvent, error)

    // Permanently marks an event as failed.
    FailedInboxEvent(ctx context.Context, workerID string, eventID uuid.UUID, reason string) (InboxEvent, error)

    // Resets stuck "processing" events back to "pending".
    CleanProcessingInboxEvents(ctx context.Context, workerIDs ...string) error

    // Resets all "failed" events back to "pending".
    CleanFailedInboxEvents(ctx context.Context) error
}
```

---

## Configuration Reference

### OutboxWorkerConfig / InboxWorkerConfig

| Field | Default        | Description |
|---|----------------|---|
| `Routines` | `10`           | Parallel send/handle goroutines |
| `Slots` | `Routines × 4` | Max in-flight events |
| `Sleep` | `200ms`        | Polling interval when queue is empty or all slots are busy |
| `BatchSize` | `100`          | Events reserved per poll cycle |
| `MinNextAttempt` | `1s`           | Minimum retry delay |
| `MaxNextAttempt` | `1m`           | Maximum retry delay cap |
| `MaxAttempts` | `0`            | Max attempts before `failed`; `0` = retry forever |

Retry delay: `min(MinNextAttempt × attempts, MaxNextAttempt)`.

### ConsumerConfig / ReaderConfig

| Field | Description |
|---|---|
| `MinBackoff` / `MaxBackoff` | Backoff bounds on fetch or write errors |
| `Brokers` | Kafka broker addresses |
| `GroupID` | Kafka consumer group ID |
| `Topic` | Topic to read from |
| `Instances` | Number of parallel reader goroutines for this topic |
| `MinBytes` / `MaxBytes` | Kafka fetch size bounds |
| `MaxWait` | Max time to wait for `MinBytes` before returning |
| `QueueCapacity` | Internal reader queue size |

### WriterTopicConfig

| Field | Values | Description |
|---|---|---|
| `Balancer` | `hash` / `leastbytes` / `roundrobin` | Partition strategy. Use `hash` for outbox. |
| `RequiredAcks` | `all` / `one` / `none` | Ack level. Use `all` for outbox. |
| `Async` | `bool` | Must be `false` for outbox. |
| `Compression` | `none` / `gzip` / `snappy` / `lz4` / `zstd` | Message compression. |
| `BatchSize` / `BatchBytes` / `BatchTimeout` | — | Kafka writer batching parameters. |
| `WriteBackoffMin` / `WriteBackoffMax` | — | Retry backoff on write errors. |
| `ReadTimeout` / `WriteTimeout` | — | Per-request timeouts. |

---

## Event Statuses

| Status | Outbox | Inbox |
|---|---|---|
| `pending` | Waiting to be sent | Waiting to be processed |
| `processing` | Reserved by a worker | Reserved by a worker |
| `sent` | ✓ Published to Kafka | — |
| `processed` | — | ✓ Handler returned nil |
| `failed` | Max attempts exhausted | Max attempts exhausted |

---

## Message Headers

Set automatically by `Producer`, parsed automatically by `Consumer`.

| Header | Value |
|---|---|
| `event_id` | UUID of the event |
| `event_type` | Domain event type (e.g. `organization.created`) |
| `event_version` | Schema version as integer string |
| `producer` | Name of the producing service |
| `content-type` | Always `application/json` |