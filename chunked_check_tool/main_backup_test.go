// main_backup_test.go
package main

import (
	"bytes"
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// fakeBackupObject is a source object in fakeBackupS3Server's srcbucket.
type fakeBackupObject struct {
	ETag    string // quotes stripped
	Content []byte
}

// fakeBackupS3Server is an httptest S3 server covering the full backup-mode
// relay surface: the ?location= region probe, HEAD (ETag/size), ranged GET
// (verification probes and relay downloads), the list-file archive PUT,
// the regular-relay PUT, and the multipart upload lifecycle
// (POST ?uploads / PUT ?partNumber&uploadId / POST ?uploadId=... complete /
// DELETE ?uploadId). Multipart ETags are computed the way S3 does, so a
// byte-faithful relay reproduces the source ETag.
type fakeBackupS3Server struct {
	mu        sync.Mutex
	srv       *httptest.Server
	objects   map[string]fakeBackupObject // srcbucket key → object
	uploads   map[string]map[int][]byte   // uploadID → part content
	nextUp    int
	listPuts  []string          // .backup_lists/ archive keys
	relayed   map[string][]byte // completed dst objects (regular + multipart)
	relayETag map[string]string
	aborted   []string
}

func newFakeBackupS3Server(t *testing.T) *fakeBackupS3Server {
	t.Helper()
	f := &fakeBackupS3Server{
		objects:   map[string]fakeBackupObject{},
		uploads:   map[string]map[int][]byte{},
		relayed:   map[string][]byte{},
		relayETag: map[string]string{},
	}
	f.srv = httptest.NewServer(http.HandlerFunc(f.handle))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeBackupS3Server) handle(w http.ResponseWriter, r *http.Request) {
	if r.URL.RawQuery == "location=" {
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(`<LocationConstraint>us-east-1</LocationConstraint>`))
		return
	}
	// Drain request bodies BEFORE taking the lock: a relay pipes a GET
	// response straight into an upload body, so the client can only finish
	// writing the PUT/POST body while the GET handler is free to run.
	// Reading under the lock deadlocks that pipeline.
	var reqBody []byte
	if r.Method == "PUT" || r.Method == "POST" {
		reqBody, _ = io.ReadAll(r.Body)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	q := r.URL.Query()
	switch {
	case r.Method == "HEAD":
		obj, ok := f.objects[strings.TrimPrefix(path, "/srcbucket/")]
		if !ok {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("ETag", `"`+obj.ETag+`"`)
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.Header().Set("Content-Length", strconv.Itoa(len(obj.Content)))
		w.WriteHeader(200)

	case r.Method == "GET" && strings.HasPrefix(path, "/srcbucket/"):
		obj, ok := f.objects[strings.TrimPrefix(path, "/srcbucket/")]
		if !ok {
			w.WriteHeader(404)
			return
		}
		body := obj.Content
		if rng := r.Header.Get("Range"); rng != "" {
			start, end, err := parseRange(rng, len(body))
			if err != nil {
				w.WriteHeader(416)
				return
			}
			body = body[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.Content)))
		}
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.Header().Set("Content-Length", strconv.Itoa(len(body)))
		w.WriteHeader(206)
		w.Write(body)

	case r.Method == "PUT" && q.Has("uploadId"):
		parts, ok := f.uploads[q.Get("uploadId")]
		if !ok {
			w.WriteHeader(404)
			return
		}
		content := decodeAwsChunkedBytes(reqBody)
		partNum, _ := strconv.Atoi(q.Get("partNumber"))
		parts[partNum] = content
		w.Header().Set("ETag", `"`+md5Hex(content)+`"`)
		w.WriteHeader(200)

	case r.Method == "POST" && q.Has("uploads"):
		f.nextUp++
		id := fmt.Sprintf("up-%d", f.nextUp)
		f.uploads[id] = map[int][]byte{}
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><InitiateMultipartUploadResult><Bucket>dstbucket</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`,
			strings.TrimPrefix(path, "/dstbucket/"), id)

	case r.Method == "POST" && q.Has("uploadId"):
		parts, ok := f.uploads[q.Get("uploadId")]
		if !ok {
			w.WriteHeader(404)
			return
		}
		var req struct {
			Parts []struct {
				PartNumber int    `xml:"PartNumber"`
				ETag       string `xml:"ETag"`
			} `xml:"Part"`
		}
		if err := xml.Unmarshal(reqBody, &req); err != nil {
			w.WriteHeader(400)
			return
		}
		content := map[int][]byte{}
		var assembled []byte
		for _, p := range req.Parts {
			c, ok := parts[p.PartNumber]
			if !ok {
				w.WriteHeader(400)
				return
			}
			content[p.PartNumber] = c
			assembled = append(assembled, c...)
		}
		etag := multipartETag(content)
		key := strings.TrimPrefix(path, "/dstbucket/")
		delete(f.uploads, q.Get("uploadId"))
		f.relayed[key] = assembled
		f.relayETag[key] = etag
		w.Header().Set("Content-Type", "application/xml")
		fmt.Fprintf(w, `<?xml version="1.0" encoding="UTF-8"?><CompleteMultipartUploadResult><Bucket>dstbucket</Bucket><Key>%s</Key><ETag>"%s"</ETag></CompleteMultipartUploadResult>`, key, etag)

	case r.Method == "DELETE" && q.Has("uploadId"):
		delete(f.uploads, q.Get("uploadId"))
		f.aborted = append(f.aborted, q.Get("uploadId"))
		w.WriteHeader(204)

	case r.Method == "PUT":
		// Regular PUT: the list-file archive or a regular-object relay.
		content := decodeAwsChunkedBytes(reqBody)
		key := strings.TrimPrefix(path, "/dstbucket/")
		f.relayed[key] = content
		f.relayETag[key] = md5Hex(content)
		if strings.HasPrefix(key, ".backup_lists/") {
			f.listPuts = append(f.listPuts, key)
		}
		w.Header().Set("ETag", `"`+md5Hex(content)+`"`)
		w.WriteHeader(200)

	default:
		w.WriteHeader(400)
	}
}

// parseRange parses "bytes=start-end" (inclusive end), clamping end to the
// last byte like S3 does (a Range whose end exceeds the object size returns
// the available bytes, not 416). start beyond the object is 416.
func parseRange(rng string, size int) (start, end int, err error) {
	if !strings.HasPrefix(rng, "bytes=") {
		return 0, 0, fmt.Errorf("bad range %q", rng)
	}
	parts := strings.SplitN(strings.TrimPrefix(rng, "bytes="), "-", 2)
	if len(parts) != 2 {
		return 0, 0, fmt.Errorf("bad range %q", rng)
	}
	start, err = strconv.Atoi(parts[0])
	if err != nil {
		return 0, 0, err
	}
	end, err = strconv.Atoi(parts[1])
	if err != nil {
		return 0, 0, err
	}
	if start < 0 || start >= size || start > end {
		return 0, 0, fmt.Errorf("range %q out of bounds for size %d", rng, size)
	}
	if end >= size {
		end = size - 1
	}
	return start, end, nil
}

// decodeAwsChunkedBytes is decodeAwsChunked for []byte.
func decodeAwsChunkedBytes(b []byte) []byte {
	return []byte(decodeAwsChunked(b))
}

var _ = bytes.MinRead // keep bytes import if unused elsewhere

// TestRunBackupFileEndToEnd drives the full -backup-file relay pipeline
// against a fake S3 server: list-file upload, HEAD typing, multipart
// verification, per-offset part re-upload, ETag verification, and the
// local result files + summary line.
func TestRunBackupFileEndToEnd(t *testing.T) {
	f := newFakeBackupS3Server(t)

	regContent := []byte("plain regular object body")
	mpContent := append([]byte(corruptBody), []byte("BBBBB")...) // part1 corrupt, part2 clean
	mpCleanContent := []byte("all clean multipart content")
	f.objects["reg1"] = fakeBackupObject{ETag: md5Hex(regContent), Content: regContent}
	f.objects["mp1"] = fakeBackupObject{
		ETag:    multipartETag(map[int][]byte{1: mpContent[:len(corruptBody)], 2: mpContent[len(corruptBody):]}),
		Content: mpContent,
	}
	f.objects["mpclean"] = fakeBackupObject{
		ETag:    multipartETag(map[int][]byte{1: mpCleanContent}),
		Content: mpCleanContent,
	}
	f.objects["mm1"] = fakeBackupObject{ETag: "0123456789abcdef0123456789abcdef-2", Content: mpContent} // line says regular, HEAD says multipart

	host := strings.TrimPrefix(f.srv.URL, "http://")
	dir := t.TempDir()

	cfgPath := filepath.Join(dir, "cfg.yaml")
	cfgContent := "endpoints:\n  - " + host + "\n" +
		"scheme: http\nak: test\nsk: test\n" +
		"list_api_version: 2\nlist_concurrency: 2\ncheck_concurrency: 2\n" +
		"output_dir: " + dir + "\nbackup_output_dir: " + dir + "\nprogress_interval: 1000\n" +
		"backup_bucket: dstbucket\n"
	if err := os.WriteFile(cfgPath, []byte(cfgContent), 0644); err != nil {
		t.Fatal(err)
	}
	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatal(err)
	}

	backupPath := filepath.Join(dir, "list.txt")
	content := "srcbucket|reg1\n" +
		fmt.Sprintf("srcbucket|mp1|2|0|%d\n", len(corruptBody)) +
		"srcbucket|mpclean|1|0\n" +
		"srcbucket|mm1\n" +
		"srcbucket|bad|1|100\n"
	if err := os.WriteFile(backupPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := run(context.Background(), cfg, "srcbucket", "", "", "", backupPath, &buf); err != nil {
		t.Fatalf("run returned err: %v", err)
	}

	out := buf.String()
	if !strings.Contains(out, "backup_ok: 2 backup_failed: 0 backup_mismatch: 1 backup_skipped_clean: 1") {
		t.Errorf("stdout missing backup summary line\nfull:\n%s", out)
	}
	// Input consumption replaces the bucket modes' list_all line: 5 lines in
	// the backup list, all read (the malformed one included).
	if !strings.Contains(out, "read: 5") {
		t.Errorf("stdout missing read: 5 summary line\nfull:\n%s", out)
	}
	if strings.Contains(out, "list_all:") {
		t.Errorf("stdout should not contain list_all (all-zero noise in backup mode)\nfull:\n%s", out)
	}

	// Local result files: the backup .txt outputs carry the raw input lines
	// (shape-compatible with -backup-file input). cfg.OutputDir carries the
	// timestamp suffix (default-on output_dir_timestamp), which runBackup
	// created.
	assertFileContent(t, cfg.OutputDir, "backup_ok.txt",
		"srcbucket|reg1\n"+fmt.Sprintf("srcbucket|mp1|2|0|%d\n", len(corruptBody)))
	assertFileContent(t, cfg.OutputDir, "backup_skipped_clean.txt", "srcbucket|mpclean|1|0\n")
	assertFileContent(t, cfg.OutputDir, "mismatch.txt", "srcbucket|mm1\n")
	assertFileContent(t, cfg.OutputDir, "list_failed.txt", "srcbucket|bad|1|100\n")

	// Remote side: the list archive landed under .backup_lists/ with a
	// timestamp suffix.
	if len(f.listPuts) != 1 {
		t.Fatalf("list puts = %v, want exactly 1", f.listPuts)
	}
	if !strings.HasPrefix(f.listPuts[0], ".backup_lists/list_") || !strings.HasSuffix(f.listPuts[0], ".txt") {
		t.Errorf("list put key = %q, want .backup_lists/list_<stamp>.txt", f.listPuts[0])
	}
	// The relays landed byte-identical (ETag equality is what gated
	// backup_ok, so reaching here already proves it).
	if got := f.relayed["reg1"]; !bytes.Equal(got, regContent) {
		t.Errorf("relayed reg1 = %q, want %q", got, regContent)
	}
	if got := f.relayed["mp1"]; !bytes.Equal(got, mpContent) {
		t.Errorf("relayed mp1 = %q, want %q", got, mpContent)
	}
	if _, ok := f.relayed["mpclean"]; ok {
		t.Errorf("mpclean must not be relayed (clean multipart)")
	}
	if len(f.aborted) != 0 {
		t.Errorf("aborted = %v, want none on the happy path", f.aborted)
	}
}

// TestRunBackupFileETagMismatchEndToEnd — a source ETag that disagrees
// with what the relay reproduces flags backup_failed (stage=etag) at the
// wire level.
func TestRunBackupFileETagMismatchEndToEnd(t *testing.T) {
	f := newFakeBackupS3Server(t)
	mpContent := append([]byte(corruptBody), []byte("BBBBB")...)
	// Source ETag is bogus (not what the parts reproduce).
	f.objects["mpbad"] = fakeBackupObject{ETag: "ffffffffffffffffffffffffffffffff-2", Content: mpContent}

	host := strings.TrimPrefix(f.srv.URL, "http://")
	dir := t.TempDir()
	cfg := &Config{
		Endpoints:        []string{host},
		Scheme:           "http",
		AK:               "test",
		SK:               "test",
		OutputDir:        dir,
		BackupOutputDir:  dir,
		ProgressInterval: 1000,
		BackupBucket:     "dstbucket",
		CheckConcurrency: 2,
		ListConcurrency:  2,
		ListAPIVersion:   2,
	}
	backupPath := filepath.Join(dir, "list.txt")
	content := fmt.Sprintf("srcbucket|mpbad|2|0|%d\n", len(corruptBody))
	if err := os.WriteFile(backupPath, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if err := run(context.Background(), cfg, "srcbucket", "", "", "", backupPath, &buf); err != nil {
		t.Fatalf("run returned err: %v", err)
	}

	assertFileContent(t, dir, "backup_failed.txt", fmt.Sprintf("srcbucket|mpbad|2|0|%d\n", len(corruptBody)))
	logData, err := os.ReadFile(filepath.Join(dir, "backup_failed.log"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(logData), "stage=etag") {
		t.Errorf("backup_failed.log missing stage=etag: %q", logData)
	}
	if !strings.Contains(buf.String(), "backup_failed: 1") {
		t.Errorf("summary missing backup_failed: 1\nfull:\n%s", buf.String())
	}
	// The bad copy is still in the destination bucket (evidence).
	if _, ok := f.relayed["mpbad"]; !ok {
		t.Errorf("relayed mpbad missing — bad copy should be kept")
	}
}

// TestRunBackupListUploadFailure — when the startup list-file upload fails
// (unreachable endpoint), the run aborts with a descriptive error before
// any object is processed.
func TestRunBackupListUploadFailure(t *testing.T) {
	dir := t.TempDir()
	cfg := &Config{
		Endpoints:        []string{"127.0.0.1:1"},
		Scheme:           "http",
		AK:               "test",
		SK:               "test",
		OutputDir:        dir,
		BackupOutputDir:  dir,
		ProgressInterval: 1000,
		BackupBucket:     "dstbucket",
	}
	backupPath := filepath.Join(dir, "list.txt")
	if err := os.WriteFile(backupPath, []byte("srcbucket|k1\n"), 0644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	err := run(context.Background(), cfg, "srcbucket", "", "", "", backupPath, &buf)
	if err == nil || !strings.Contains(err.Error(), "upload backup list") {
		t.Fatalf("run err = %v, want upload backup list failure", err)
	}
}

// TestRunBackupProgressLine — the backup progress line must fire through
// the full runBackup wiring (mode-aware printer + read/total counters +
// backup queue snapshot): interval=1 fires one line per task.
func TestRunBackupProgressLine(t *testing.T) {
	f := newFakeBackupS3Server(t)
	regContent := []byte("plain regular object body")
	f.objects["reg1"] = fakeBackupObject{ETag: md5Hex(regContent), Content: regContent}
	// reg2 is absent on purpose: HEAD 404 → backup_failed (stage=head).
	host := strings.TrimPrefix(f.srv.URL, "http://")
	dir := t.TempDir()
	cfg := &Config{
		Endpoints:        []string{host},
		Scheme:           "http",
		AK:               "test",
		SK:               "test",
		OutputDir:        dir,
		BackupOutputDir:  dir,
		ProgressInterval: 1,
		BackupBucket:     "dstbucket",
		CheckConcurrency: 2,
		ListConcurrency:  2,
		ListAPIVersion:   2,
	}
	backupPath := filepath.Join(dir, "list.txt")
	if err := os.WriteFile(backupPath, []byte("srcbucket|reg1\nsrcbucket|reg2\n"), 0644); err != nil {
		t.Fatal(err)
	}
	var buf bytes.Buffer
	if err := run(context.Background(), cfg, "srcbucket", "", "", "", backupPath, &buf); err != nil {
		t.Fatalf("run returned err: %v", err)
	}
	out := buf.String()
	if !strings.Contains(out, "[progress] read=") {
		t.Errorf("no backup progress line in output:\n%s", out)
	}
	// A task is only handed to a worker after its line was read, so the
	// progress line following the second task shows read=2.
	if !strings.Contains(out, "read=2 ") {
		t.Errorf("progress output missing read=2:\n%s", out)
	}
	for _, want := range []string{"backup_ok=", "backup_failed=", "(backed="} {
		if !strings.Contains(out, want) {
			t.Errorf("backup progress line missing %q\nfull:\n%s", want, out)
		}
	}
	if strings.Contains(out, "list_all=") || strings.Contains(out, "corrupt_obj=") {
		t.Errorf("backup progress should not contain check-mode noise:\n%s", out)
	}
}
