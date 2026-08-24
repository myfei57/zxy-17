package shard

import (
	"fmt"

	"taskflow/internal/record"
)

// Complete advances the completion cursor after a shard result is durable.
// The cursor stores the number of completed shards and only moves forward.
//
// Re-completing an already-recorded shard is a no-op so that a retry after a
// crash between the cursor advance and the idempotency mark is idempotent:
// the result is durable and the cursor already passed this shard, so the
// retry must succeed rather than error out and strand the shard.
func Complete(cursorPath string, done int, result record.ShardResult) error {
	if result.Shard != done {
		return fmt.Errorf("shard: result %d does not match completion %d", result.Shard, done)
	}
	current, err := record.ReadCursor(cursorPath)
	if err != nil {
		return err
	}
	if uint64(done) < current {
		// Already recorded; the cursor is already past this shard.
		return nil
	}
	if uint64(done) > current {
		return fmt.Errorf("shard: out of order completion %d, expected %d", done, current)
	}
	return record.WriteCursor(cursorPath, uint64(done)+1)
}
