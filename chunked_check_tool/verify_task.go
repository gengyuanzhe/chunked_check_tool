package main

// VerifyTask is the unit of verification work that flows through objCh.
// verify() interprets Offsets as follows:
//   - IsMultipart=false, Offsets=nil → normal object: probe offset 0 once
//     (no slice, no allocation — verify special-cases the single probe)
//   - IsMultipart=true, Offsets=nil → multipart that cannot be probed (check
//     off, or offset mode with an unparseable ETag): do not probe, record
//     it unverified without claiming ok_mp
//   - IsMultipart=true, Offsets=[...] → probe each offset (offset-etag or
//     -check-file line sources; both are real part boundaries)
//
// ETag/Size may be empty/zero for file-input tasks (-check-file);
// HeadFirst signals the checker to HEAD the object before
// probing to fill them in and to re-validate the object type against the
// HEAD ETag (see Checker.Handle).
type VerifyTask struct {
	Key         string
	OwnerID     string
	ETag        string
	Size        int64
	IsMultipart bool
	Offsets     []int64
	// HeadFirst signals the checker to HEAD the object before probing. Set
	// by the file-input source (-check-file: its input format omits
	// ETag/Size); left false by S3 LIST (which already has
	// both). Backup mode HEADs in its own Handle, so it does not set
	// HeadFirst.
	HeadFirst bool
	// JournalPrefix/PageSeq carry the journal accounting identity (Mode 2
	// S3 listing only): the prefix the object was listed under and the page
	// within that prefix, reported back via JournalTracker.Terminal when the
	// checker finishes. Zero values in every other mode (journal off).
	JournalPrefix string
	PageSeq       int32
}

// resolveOffsets builds a VerifyTask from an S3-listed object. Three cases:
//   - normal ETag → IsMultipart=false, Offsets=nil (verify probes offset 0)
//   - multipart ETag + mode=offset + parseable ETag → IsMultipart=true,
//     Offsets from the ETag's part boundaries
//   - multipart ETag + mode=off (or mode=offset with an unparseable ETag)
//     → IsMultipart=true, Offsets=nil
//
// mode=offset with an unparseable ETag (server without the feature) keeps
// Offsets=nil; the checker then records the object in list_parse_failed.txt.
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
		if offs, ok := parseMultipartOffsetETag(obj.ETag, obj.Size); ok {
			task.Offsets = offs
		}
		// Unparseable ETag (format error or offset > Size) → Offsets stays
		// nil → list_parse_failed fallback.
	}
	return task
}
