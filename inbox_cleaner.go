package msnger

import "context"

type InboxCleaner interface {
	CleanInboxProcessing(ctx context.Context, processIDs ...string) error

	CleanInboxFailed(ctx context.Context) error
}
