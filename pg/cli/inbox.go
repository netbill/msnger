package cli

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/netbill/eventbox/pg"
	"github.com/netbill/logium"
	"github.com/netbill/pgdbx"
)

func CleanupInboxFailed(ctx context.Context, log *logium.Entry, url string) error {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	db := pgdbx.NewDB(pool)

	inboxCleaner := pg.NewInbox(db)

	err = inboxCleaner.CleanFailedInboxEvents(ctx)
	if err != nil {
		log.WithError(err).Error("failed to clean inbox failed")
		return err
	}

	log.Info("inbox failed cleaned successfully")
	return nil
}

func CleanupInboxProcessing(ctx context.Context, log *logium.Entry, url string, processIDs ...string) error {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer pool.Close()

	db := pgdbx.NewDB(pool)

	inboxCleaner := pg.NewInbox(db)

	err = inboxCleaner.CleanProcessingInboxEvents(ctx, processIDs...)
	if err != nil {
		log.WithError(err).Error("failed to clean inbox processing")
		return err
	}

	log.Info("inbox processing cleaned successfully")
	return nil
}
