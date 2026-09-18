// backup.go
package main

import (
	"context"
	"fmt"
	"io"
	"strings"
)

// BackupChecker processes one BackupTask: HEAD the object, validate the
// input line's type against the HEAD ETag, then relay the object into the
// configured backup bucket (download → re-upload) and verify the
// destination ETag against the source.
//
// No corruption probing happens here: the input list is the output of a
// check run (corrupted_objects.txt / corrupted_mp.txt, possibly unioned
// across runs) — whether an object is corrupt was decided there, and this
// mode's job is only to preserve the bytes.
//
// The three result .txt files (backup_ok / backup_failed / mismatch) all
// carry the task's raw input line verbatim (bkt|key or
// bkt|key|partcnt|offsets...) — the outputs stay shape-compatible with the
// input, so backup_failed.txt can be fed straight back into -backup-file
// for a retry (it carries the offsets), and mismatch.txt into -check-file
// (the object changed since the check run — re-check its current state).
// The .log files stay structured with the bare key.
//
// Routing:
//   - HEAD fails                         → backup_failed (stage=head)
//   - line type != HEAD type             → mismatch (raw line), no relay
//   - relay error                        → backup_failed (stage=upload)
//   - dst ETag != src ETag               → backup_failed (stage=etag);
//     the bad copy stays in the backup
//     bucket as evidence
type BackupChecker struct {
	// ctx bounds every S3 call the relay makes. It is the hard context of
	// the two-stage shutdown: cancelled only on a second signal, so a
	// graceful drain (first signal) finishes in-flight relays, while a hard
	// abort cancels them (multipart uploads are aborted, no partial object
	// lingers) and the error path records the task in backup_failed — the
	// file this mode's own retry consumes.
	ctx    context.Context
	worker S3API
	out    *Output
	stats  *Stats
	cfg    *Config
}

func NewBackupChecker(ctx context.Context, worker S3API, out *Output, stats *Stats, cfg *Config) *BackupChecker {
	return &BackupChecker{ctx: ctx, worker: worker, out: out, stats: stats, cfg: cfg}
}

func (c *BackupChecker) Handle(task BackupTask) {
	etag, size, err := c.worker.HeadObject(c.ctx, task.Key)
	if err != nil {
		c.fail(task, "head", err)
		return
	}
	if headIsMultipart := !isNormalETag(etag); headIsMultipart != task.IsMultipart {
		c.out.WriteMismatch(task.RawLine)
		c.out.WriteMismatchLog(task.Key, task.IsMultipart, etag, size)
		c.stats.IncrBackupMismatch()
		return
	}
	c.backup(task, etag, size)
}

// backup relays the object and verifies the destination ETag. The ETag
// comparison is meaningful because multipart relays re-upload parts split
// exactly at the input line's original offsets: a byte-faithful relay
// then reproduces the source's md5-of-part-md5s-N ETag (regular objects:
// plain MD5).
func (c *BackupChecker) backup(task BackupTask, srcETag string, size int64) {
	var dstETag string
	var err error
	if task.IsMultipart {
		dstETag, err = c.relayMultipart(task, size)
	} else {
		dstETag, err = c.relayRegular(task.Key, size)
	}
	if err != nil {
		c.fail(task, "upload", err)
		return
	}
	if dstETag != srcETag {
		c.fail(task, "etag", fmt.Errorf("backup bucket etag %q != source etag %q", dstETag, srcETag))
		return
	}
	c.out.WriteBackupOk(task.RawLine)
	c.stats.IncrBackupOk()
}

// relayRegular streams the whole object through one PUT. size==0 skips the
// download (empty body).
func (c *BackupChecker) relayRegular(key string, size int64) (string, error) {
	var r io.Reader = strings.NewReader("")
	if size > 0 {
		rc, err := c.worker.DownloadRange(c.ctx, key, 0, size)
		if err != nil {
			return "", err
		}
		defer rc.Close()
		r = rc
	}
	return c.worker.PutObjectStream(c.ctx, c.cfg.BackupBucket, key, r, size)
}

// relayMultipart re-uploads the object as multipart parts split at the
// input line's offsets: part i+1 covers [offset_i, offset_{i+1}), the last
// part runs to the object end. Parts are streamed (download reader piped
// straight into the part upload — no per-part buffering). Any failure
// aborts the in-progress upload so no partial object lingers.
func (c *BackupChecker) relayMultipart(task BackupTask, size int64) (string, error) {
	dst := c.cfg.BackupBucket
	uploadID, err := c.worker.CreateMultipart(c.ctx, dst, task.Key)
	if err != nil {
		return "", err
	}
	parts := make([]UploadedPart, 0, len(task.Offsets))
	for i, off := range task.Offsets {
		end := size
		if i+1 < len(task.Offsets) {
			end = task.Offsets[i+1]
		}
		length := end - off
		if length <= 0 {
			c.worker.AbortMultipart(c.ctx, dst, task.Key, uploadID)
			return "", fmt.Errorf("part %d at offset %d has non-positive length %d (object size %d)", i+1, off, length, size)
		}
		rc, err := c.worker.DownloadRange(c.ctx, task.Key, off, length)
		if err != nil {
			c.worker.AbortMultipart(c.ctx, dst, task.Key, uploadID)
			return "", err
		}
		partETag, err := c.worker.UploadPart(c.ctx, dst, task.Key, uploadID, i+1, rc, length)
		rc.Close()
		if err != nil {
			c.worker.AbortMultipart(c.ctx, dst, task.Key, uploadID)
			return "", err
		}
		parts = append(parts, UploadedPart{PartNumber: i + 1, ETag: partETag})
	}
	dstETag, err := c.worker.CompleteMultipart(c.ctx, dst, task.Key, uploadID, parts)
	if err != nil {
		c.worker.AbortMultipart(c.ctx, dst, task.Key, uploadID)
		return "", err
	}
	return dstETag, nil
}

func (c *BackupChecker) fail(task BackupTask, stage string, err error) {
	c.out.WriteBackupFailed(task.RawLine)
	c.out.WriteBackupFailedLog(task.Key, stage, extractHTTPStatusCode(err), extractS3Code(err), extractRequestID(err), err)
	c.stats.IncrBackupFailed()
}
