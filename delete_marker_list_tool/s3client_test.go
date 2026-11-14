package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
)

func newFakeVersionedS3(t *testing.T, versionsXML string) (*httptest.Server, string) {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		// Only the actual LIST versions request carries versions=.
		// Other requests (location lookup, HEAD bucket) get generic responses.
		// Use Has() not Get() — versions= has an empty value.
		if !q.Has("versions") {
			w.Header().Set("Content-Type", "application/xml")
			if r.Method == http.MethodHead {
				w.WriteHeader(http.StatusOK)
				return
			}
			w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>`))
			return
		}
		if versionsXML == "" {
			http.Error(w, "InternalError", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		w.Write([]byte(versionsXML))
	}))
	return srv, srv.Listener.Addr().String()
}

const versionsXMLOneMarker = `<?xml version="1.0" encoding="UTF-8"?>
<ListVersionsResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
<Name>testbucket</Name><Prefix></Prefix><KeyMarker></KeyMarker><VersionIdMarker></VersionIdMarker>
<IsTruncated>false</IsTruncated>
<Version><Key>a.txt</Key><VersionId>regular-v1</VersionId><IsLatest>false</IsLatest><LastModified>2024-01-01T00:00:00.000Z</LastModified><ETag>"abc"</ETag><Size>5</Size></Version>
<DeleteMarker><Key>a.txt</Key><VersionId>dm-v2</VersionId><IsLatest>true</IsLatest><LastModified>2024-01-02T00:00:00.000Z</LastModified></DeleteMarker>
</ListVersionsResult>`

func TestListVersions_OneMarkerOneVersion(t *testing.T) {
	srv, url := newFakeVersionedS3(t, versionsXMLOneMarker)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{url}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, url)
	if err != nil {
		t.Fatal(err)
	}
	page, err := c.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if len(page.Versions) != 2 {
		t.Fatalf("got %d entries, want 2", len(page.Versions))
	}
	var markerCount, regularCount int
	for _, o := range page.Versions {
		if o.IsDeleteMarker {
			markerCount++
		} else {
			regularCount++
		}
	}
	if markerCount != 1 || regularCount != 1 {
		t.Errorf("markerCount=%d regularCount=%d want 1/1", markerCount, regularCount)
	}
}

func TestListVersions_ServerError(t *testing.T) {
	srv, _ := newFakeVersionedS3(t, "") // empty XML triggers 500 in handler
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.ListVersionsPage(context.Background(), "testbucket", "", true, "", "")
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestBucketExists(t *testing.T) {
	srv, _ := newFakeVersionedS3(t, versionsXMLOneMarker)
	defer srv.Close()
	cfg := &Config{Endpoints: []string{srv.Listener.Addr().String()}, Scheme: "http", AK: "a", SK: "s"}
	c, err := NewS3Client(cfg, srv.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	exists, err := c.BucketExists(context.Background(), "testbucket")
	if err != nil {
		t.Fatalf("BucketExists err: %v", err)
	}
	if !exists {
		t.Errorf("exists=false want true")
	}
}
