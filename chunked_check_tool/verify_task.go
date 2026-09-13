package main

// VerifyTask is the unit of verification work that flows through objCh.
// verify() interprets Offsets as follows:
//   - IsMultipart=false, Offsets=nil → normal object: probe offset 0 once
//     (no slice, no allocation — verify special-cases the single probe)
//   - IsMultipart=true, Offsets=nil → multipart with check off (or offset
//     mode with an unparseable ETag): do not probe, write to mp_all without
//     claiming ok_mp
//   - IsMultipart=true, Offsets=[...] → probe each offset (fixed-segment or
//     list-file sources)
//
// ETag/Size may be empty/zero for list-file-sourced tasks.
type VerifyTask struct {
	Key         string
	OwnerID     string
	ETag        string
	Size        int64
	IsMultipart bool
	Offsets     []int64
}

// resolveOffsets builds a VerifyTask from an S3-listed object. Three cases:
//   - normal ETag → IsMultipart=false, Offsets=nil (verify probes offset 0)
//   - multipart ETag + mode=segment + Size>0 → IsMultipart=true, Offsets =
//     [0, seg, 2*seg, ...] ceil(Size/seg) entries
//   - multipart ETag + mode=off (or mode=segment with Size==0) →
//     IsMultipart=true, Offsets=nil
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
	if cfg.MultipartCheckMode == MultipartCheckModeSegment && cfg.MultipartSegmentSize > 0 && obj.Size > 0 {
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
