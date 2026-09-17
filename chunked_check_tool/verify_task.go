package main

// VerifyTask is the unit of verification work that flows through objCh.
// verify() interprets Offsets as follows:
//   - IsMultipart=false, Offsets=nil → normal object: probe offset 0 once
//     (no slice, no allocation — verify special-cases the single probe)
//   - IsMultipart=true, Offsets=nil → multipart with check off (or offset
//     mode with an unparseable ETag): do not probe, write to mp_all without
//     claiming ok_mp
//   - IsMultipart=true, Offsets=[...] → probe each offset (offset-etag,
//     fixed-segment, or list-file sources)
//
// ETag/Size may be empty/zero for list-file-sourced tasks; HeadFirst signals
// the checker to HEAD the object before probing to fill them in (mirroring
// backup mode's HEAD-in-Handle pattern so the two modes share HEAD→probe via
// runProbes; only post-probe routing differs).
type VerifyTask struct {
	Key         string
	OwnerID     string
	ETag        string
	Size        int64
	IsMultipart bool
	Offsets     []int64
	// HeadFirst signals the checker to HEAD the object before probing. Set
	// by the list-file source (its input format omits ETag/Size); left false
	// by S3 LIST (which already has both). Backup mode HEADs in its own
	// Handle, so it does not set HeadFirst.
	HeadFirst bool
}

// resolveOffsets builds a VerifyTask from an S3-listed object. Four cases:
//   - normal ETag → IsMultipart=false, Offsets=nil (verify probes offset 0)
//   - multipart ETag + mode=offset + parseable ETag → IsMultipart=true,
//     Offsets from the ETag's part boundaries (takes priority over segment
//     mode — real boundaries beat synthetic ones)
//   - multipart ETag + mode=segment + Size>0 → IsMultipart=true, Offsets =
//     [0, seg, 2*seg, ...] ceil(Size/seg) entries
//   - multipart ETag + mode=off (or mode=offset with an unparseable ETag,
//     or mode=segment with Size==0) → IsMultipart=true, Offsets=nil
//
// mode=offset with an unparseable ETag (server without the feature) keeps
// Offsets=nil so the checker routes the object into mp.txt unverified —
// identical to mode=off, a graceful degradation.
func resolveOffsets(obj ObjectInfo, cfg *Config) VerifyTask {
	if isNormalETag(obj.ETag) {
		return VerifyTask{
			Key:         obj.Key,
			OwnerID:     obj.OwnerID,
			ETag:        obj.ETag,
			Size:        obj.Size,
			IsMultipart: false,
			// Offsets stays nil — verify() probes offset 0 directly for
			// !IsMultipart, avoiding any per-object slice allocation.
		}
	}
	task := VerifyTask{
		Key:         obj.Key,
		OwnerID:     obj.OwnerID,
		ETag:        obj.ETag,
		Size:        obj.Size,
		IsMultipart: true,
	}
	if cfg.MultipartCheckMode == MultipartCheckModeOffset {
		if offs, ok := parseMultipartOffsetETag(obj.ETag); ok {
			task.Offsets = offs
		}
		// Unparseable ETag → Offsets stays nil → mp.txt fallback.
	} else if cfg.MultipartCheckMode == MultipartCheckModeSegment && cfg.MultipartSegmentSize > 0 && obj.Size > 0 {
		seg := cfg.MultipartSegmentSize
		numSegs := (obj.Size + seg - 1) / seg
		offs := make([]int64, numSegs)
		for i := int64(0); i < numSegs; i++ {
			offs[i] = i * seg
		}
		task.Offsets = offs
	}
	return task
}
