// backup.go
package main

import (
	"context"
)

// BackupChecker processes one BackupTask: HEAD the object, validate the
// input line's type against the HEAD ETag, verify multipart corruption
// (reusing the checker's chunk-signature probe), and copy the object into
// the configured backup bucket.
//
// Routing:
//   - HEAD fails                         → backup_failed (stage=head)
//   - line type != HEAD type             → mismatch (raw line), no copy
//   - regular line                       → copy directly (input list is a
//                                         prior corrupted_objects.txt —
//                                         already known corrupt, no re-probe)
//   - multipart line, any offset matches
//     the chunk signature                → copy
//   - multipart line, GET error          → backup_failed (stage=verify)
//   - multipart line, all offsets clean  → backup_skipped_clean, no copy
//   - copy fails                         → backup_failed (stage=copy)
type BackupChecker struct {
	worker S3API
	out    *Output
	stats  *Stats
	cfg    *Config
}

func NewBackupChecker(worker S3API, out *Output, stats *Stats, cfg *Config) *BackupChecker {
	return &BackupChecker{worker: worker, out: out, stats: stats, cfg: cfg}
}

func (c *BackupChecker) Handle(task BackupTask) {
	etag, err := c.worker.HeadObject(context.Background(), task.Key)
	if err != nil {
		c.fail(task.Key, "head", err)
		return
	}
	if headIsMultipart := !isNormalETag(etag); headIsMultipart != task.IsMultipart {
		c.out.WriteMismatch(task.RawLine)
		c.stats.IncrBackupMismatch()
		return
	}
	if task.IsMultipart && !c.verifyCorrupt(task) {
		return
	}
	c.backup(task)
}

// verifyCorrupt probes each offset with a 128-byte RangeGetAt; true means a
// probe matched the chunk signature (task is corrupt and should be backed
// up). A GET error routes to backup_failed and returns false; all-clean
// routes to backup_skipped_clean and returns false.
func (c *BackupChecker) verifyCorrupt(task BackupTask) bool {
	for _, off := range task.Offsets {
		body, err := c.worker.RangeGetAt(context.Background(), task.Key, off, 128)
		if err != nil {
			c.fail(task.Key, "verify", err)
			return false
		}
		if chunkSigRe.Match(body) {
			return true
		}
	}
	c.out.WriteBackupSkippedClean(task.Key)
	c.stats.IncrBackupSkippedClean()
	return false
}

func (c *BackupChecker) backup(task BackupTask) {
	if err := c.worker.CopyObject(context.Background(), task.Key, c.cfg.BackupBucket, task.Key); err != nil {
		c.fail(task.Key, "copy", err)
		return
	}
	c.out.WriteBackupOk(task.Key)
	c.stats.IncrBackupOk()
}

func (c *BackupChecker) fail(key, stage string, err error) {
	c.out.WriteBackupFailed(key)
	c.out.WriteBackupFailedLog(key, stage, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
	c.stats.IncrBackupFailed()
}
