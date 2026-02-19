package cli

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	eventpg "github.com/netbill/eventbox/pg"
	"github.com/netbill/logium"
	"github.com/netbill/pgdbx"
)

func CleanupOutboxFailed(ctx context.Context, log *logium.Entry, url string) error {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	db := pgdbx.NewDB(pool)

	OutboxCleaner := eventpg.NewOutbox(db)

	err = OutboxCleaner.CleanFailedOutboxEvents(ctx)
	if err != nil {
		log.WithError(err).Error("failed to clean Outbox failed")
		return err
	}

	log.Info("Outbox failed cleaned successfully")
	return nil
}

func CleanupOutboxProcessing(ctx context.Context, log *logium.Entry, url string, processIDs ...string) error {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	db := pgdbx.NewDB(pool)

	OutboxCleaner := eventpg.NewOutbox(db)

	err = OutboxCleaner.CleanProcessingOutboxEvents(ctx, processIDs...)
	if err != nil {
		log.WithError(err).Error("failed to clean Outbox processing")
		return err
	}

	log.Info("Outbox processing cleaned successfully")
	return nil
}
