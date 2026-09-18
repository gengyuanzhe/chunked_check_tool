// check_file_source.go — parser for the -check-file input format.
package main

// parseCheckFileLine parses one line of the -check-file input: the mixed
// regular/multipart format shared with -backup-file (parseMixedLine). The
// intended inputs are the failure files a check run produced —
// check_failed.txt (bkt|key), mp_check_failed.txt / mismatch.txt
// (bkt|key|partcnt|off...) — or any manifest in that shape; a legacy
// -list-file (multipart-only) is a valid subset.
//
// On success returns a VerifyTask with HeadFirst=true: the checker HEADs the
// object to fill ETag/Size and treats the HEAD ETag as the authoritative
// object type (the line describes the object as it was during a previous
// run; it may have been overwritten since — see Checker.Handle).
func parseCheckFileLine(line string, expectedBucket string, lineNum int) (VerifyTask, error) {
	t, err := parseMixedLine(line, expectedBucket, lineNum)
	if err != nil {
		return VerifyTask{}, err
	}
	return VerifyTask{
		Key:         t.Key,
		IsMultipart: t.IsMultipart,
		Offsets:     t.Offsets,
		HeadFirst:   true,
	}, nil
}
