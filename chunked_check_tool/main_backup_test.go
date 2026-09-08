// main_backup_test.go
package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

// fakeBackupS3Server is an httptest S3 server covering the full backup-mode
// surface: the ?location= region probe, HEAD (ETag per key), ranged GET
// (chunk-signature body for multipart keys), server-side copy (PUT with
// x-amz-copy-source), and the list-file PUT into the backup bucket.
type fakeBackupS3Server struct {
	mu          sync.Mutex
	srv         *httptest.Server
	etags       map[string]string // srcbucket key → ETag
	corruptKeys map[string]bool   // keys whose GET body matches the chunk signature
	listPuts    []string          // keys PUT into dstbucket without copy-source
	copies      []string          // x-amz-copy-source values
}

func newFakeBackupS3Server(t *testing.T) *fakeBackupS3Server {
	t.Helper()
	f := &fakeBackupS3Server{
		etags:       map[string]string{},
		corruptKeys: map[string]bool{},
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
	f.mu.Lock()
	defer f.mu.Unlock()
	path := r.URL.Path
	switch {
	case r.Method == "HEAD":
		etag, ok := f.etags[strings.TrimPrefix(path, "/srcbucket/")]
		if !ok {
			w.WriteHeader(404)
			return
		}
		w.Header().Set("ETag", `"`+etag+`"`)
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		w.WriteHeader(200)
	case r.Method == "GET" && strings.HasPrefix(path, "/srcbucket/"):
		// Ranged GET for chunk verification: multipart keys return a body
		// matching the chunk signature; regular keys return plain bytes.
		// Last-Modified is required — minio-go parses it off the object
		// response headers.
		w.Header().Set("Last-Modified", "Mon, 02 Jan 2006 15:04:05 GMT")
		key := strings.TrimPrefix(path, "/srcbucket/")
		if f.corruptKeys[key] {
			w.Write([]byte(corruptBody))
		} else {
			w.Write([]byte("plain object body"))
		}
	case r.Method == "PUT":
		if cs := r.Header.Get("x-amz-copy-source"); cs != "" {
			f.copies = append(f.copies, cs)
			w.Header().Set("Content-Type", "application/xml")
			fmt.Fprint(w, `<?xml version="1.0" encoding="UTF-8"?><CopyObjectResult><LastModified>2026-09-08T00:00:00Z</LastModified><ETag>"abc"</ETag></CopyObjectResult>`)
			return
		}
		// Regular PUT: the list-file archive into the backup bucket.
		io.Copy(io.Discard, r.Body)
		f.listPuts = append(f.listPuts, path)
		w.WriteHeader(200)
	default:
		w.WriteHeader(400)
	}
}

// TestRunBackupFileEndToEnd drives the full -backup-file pipeline against a
// fake S3 server: list-file upload, HEAD typing, multipart verify, copy,
// and the local result files + summary line.
func TestRunBackupFileEndToEnd(t *testing.T) {
	f := newFakeBackupS3Server(t)
	f.etags["reg1"] = normalETag
	f.etags["mp1"] = mpETag
	f.etags["mpclean"] = mpETag
	f.etags["mm1"] = mpETag // line says regular, HEAD says multipart
	f.corruptKeys["mp1"] = true

	host := strings.TrimPrefix(f.srv.URL, "http://")
	dir := t.TempDir()

	cfgPath := filepath.Join(dir, "cfg.yaml")
	cfgContent := "endpoints:\n  - " + host + "\n" +
		"scheme: http\nak: test\nsk: test\n" +
		"list_api_version: 2\nlist_concurrency: 2\ncheck_concurrency: 2\n" +
		"output_dir: " + dir + "\nprogress_interval: 1000\n" +
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
		"srcbucket|mp1|1|0\n" +
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

	// Result files.
	assertFileContent(t, dir, "backup_ok.txt", "reg1\nmp1\n")
	assertFileContent(t, dir, "backup_skipped_clean.txt", "mpclean\n")
	assertFileContent(t, dir, "mismatch.txt", "srcbucket|mm1\n")
	assertFileContent(t, dir, "list_failed.txt", "srcbucket|bad|1|100\n")

	// Remote side: the list archive landed under .backup_lists/ with a
	// timestamp suffix, and both corrupt objects were copied into dstbucket.
	if len(f.listPuts) != 1 {
		t.Fatalf("list puts = %v, want exactly 1", f.listPuts)
	}
	if !strings.HasPrefix(f.listPuts[0], "/dstbucket/.backup_lists/list_") || !strings.HasSuffix(f.listPuts[0], ".txt") {
		t.Errorf("list put key = %q, want .backup_lists/list_<stamp>.txt", f.listPuts[0])
	}
	if len(f.copies) != 2 {
		t.Fatalf("copies = %v, want 2 (reg1 + mp1)", f.copies)
	}
	for _, cs := range f.copies {
		if !strings.HasPrefix(cs, "srcbucket/") {
			t.Errorf("copy source = %q, want srcbucket/... prefix", cs)
		}
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
