package maptiler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// minimal shapes used by the handler to assert request bodies
type ingestReqBody struct {
	ID       string `json:"id,omitempty"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
}

type testResp struct {
	Status int
	Body   []byte
}

func newTestClient(t *testing.T, base, token string) (*Client, error) {
	t.Helper()

	c, err := New(base, token)
	if err != nil {
		return nil, err
	}

	return c, nil
}

func newClientWithPool(t *testing.T, proc processor[uploadTask], conc int) *Client {
	t.Helper()
	if conc <= 0 {
		conc = 2
	}
	wp := newPool(proc, withPoolConcurrency(conc))
	return &Client{
		wp: wp,
	}
}

func TestClientIngest(t *testing.T) { //nolint:cyclop
	t.Parallel()

	type wantReq struct {
		Path     string
		Filename string
		Size     int64
	}

	type tc struct {
		name   string
		req    ingestRequest
		want   wantReq
		resp   testResp
		assert func(t *testing.T, got IngestResponse, gotErr error)
		token  string
	}

	const (
		okJSON = `{
		  "id":"ing-123",
		  "state":"upload",
		  "upload":{
		    "part_size": 5242880,
		    "parts":[{"part_id":1,"url":"https://example/1"},{"part_id":2,"url":"https://example/2"}],
		    "type":"s3_multipart"
		  }
		}`
	)

	makeServer := func(t *testing.T, w wantReq, resp testResp) *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(wr http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if r.URL.Path != w.Path {
				http.Error(wr, "bad path: "+r.URL.Path, http.StatusNotFound)
				return
			}
			// auth check
			auth := r.Header.Get("Authorization")
			if auth == "Token wrong-token" {
				http.Error(wr, "wrong token", http.StatusUnauthorized)
				return
			}

			// body check (filename & size)
			var got ingestReqBody
			dec := json.NewDecoder(r.Body)
			if err := dec.Decode(&got); err != nil {
				http.Error(wr, "bad json", http.StatusBadRequest)
				return
			}
			if got.Filename != w.Filename {
				http.Error(wr, "bad filename", http.StatusBadRequest)
				return
			}
			if got.Size != w.Size {
				http.Error(wr, "bad size", http.StatusBadRequest)
				return
			}

			// respond
			if resp.Status != 0 {
				wr.WriteHeader(resp.Status)
			}
			if len(resp.Body) > 0 {
				_, _ = wr.Write(resp.Body)
			}
		}))
	}

	tests := []tc{
		{
			name: "create success 200",
			req:  newIngestRequest("", "v.pmtiles", 1234),
			want: wantReq{
				Path:     "/v1/datasets/ingest",
				Filename: "v.pmtiles",
				Size:     1234,
			},
			resp:  testResp{Status: http.StatusOK, Body: []byte(okJSON)},
			token: "test-token",
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()

				if gotErr != nil {
					t.Fatalf("unexpected error: %v", gotErr)
				}
				if got.ID == "" {
					t.Fatalf("expected non-empty id")
				}
				if got.State != "upload" {
					t.Fatalf("state=%q want upload", got.State)
				}
				if got.Upload.PartSize == 0 || len(got.Upload.Parts) == 0 {
					t.Fatalf("expected upload.part_size and upload.parts to be set")
				}
			},
		},
		{
			name: "update success 200 with id param",
			req:  newIngestRequest("abc123", "w.pmtiles", 999),
			want: wantReq{
				Path:     "/v1/datasets/abc123/ingest",
				Filename: "w.pmtiles",
				Size:     999,
			},
			resp:  testResp{Status: http.StatusOK, Body: []byte(okJSON)},
			token: "test-token",
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()

				if gotErr != nil {
					t.Fatalf("unexpected error: %v", gotErr)
				}
				if got.State != "upload" {
					t.Fatalf("state=%q want upload", got.State)
				}
			},
		},
		{
			name: "missing auth -> 401",
			req:  newIngestRequest("", "x.pmtiles", 1),
			want: wantReq{
				Path:     "/v1/datasets/ingest",
				Filename: "x.pmtiles",
				Size:     1,
			},
			resp:  testResp{Status: http.StatusUnauthorized, Body: nil},
			token: "wrong-token", // wrong token
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()

				if gotErr == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(gotErr.Error(), "request failed with 401") {
					t.Fatalf("got error %v, want 401 mapping", gotErr)
				}
			},
		},
		{
			name: "non-2xx -> error",
			req:  newIngestRequest("", "y.pmtiles", 2),
			want: wantReq{
				Path:     "/v1/datasets/ingest",
				Filename: "y.pmtiles",
				Size:     2,
			},
			resp:  testResp{Status: http.StatusInternalServerError, Body: []byte(`{"error":"boom"}`)},
			token: "test-token",
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()

				if gotErr == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(gotErr.Error(), "request failed with 500") {
					t.Fatalf("got error %v, want 500 mapping", gotErr)
				}
			},
		},
		{
			name: "2xx but invalid JSON -> unmarshal error",
			req:  newIngestRequest("", "z.pmtiles", 3),
			want: wantReq{
				Path:     "/v1/datasets/ingest",
				Filename: "z.pmtiles",
				Size:     3,
			},
			resp:  testResp{Status: http.StatusOK, Body: []byte(`not-json`)},
			token: "test-token",
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()

				if gotErr == nil {
					t.Fatalf("expected unmarshal error, got nil")
				}
				var syntaxErr *json.SyntaxError
				if !errors.As(gotErr, &syntaxErr) {
					t.Fatalf("expected *json.SyntaxError, got %T (%v)", gotErr, gotErr)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := makeServer(t, tt.want, tt.resp)
			defer srv.Close()

			base := srv.URL + "/v1"
			cl, err := newTestClient(t, base, tt.token)
			if err != nil {
				t.Fatalf("creating test client should not fail")
			}

			got, err := cl.ingest(t.Context(), tt.req)
			tt.assert(t, got, err)
		})
	}
}

func TestClientUploadSortsAndCollects(t *testing.T) {
	t.Parallel()

	parts := uploadParts{
		{PartID: 3, URL: "u3"},
		{PartID: 1, URL: "u1"},
		{PartID: 4, URL: "u4"},
		{PartID: 2, URL: "u2"},
	}
	ir := IngestResponse{
		Size: 40,
		Upload: upload{
			PartSize: 10,
			Parts:    parts,
			Type:     ingestUploadTypeS3MultiPart,
		},
	}

	proc := &fakeProcessor{}
	cl := newClientWithPool(t, proc, 3)

	got, err := cl.upload(t.Context(), ir, "ignored/path")
	if err != nil {
		t.Fatalf("upload returned error: %v", err)
	}

	// Expect Parts sorted by PartID: 1,2,3,4
	if len(got.Parts) != len(parts) {
		t.Fatalf("got %d parts, want %d", len(got.Parts), len(parts))
	}
	wantOrder := []int64{1, 2, 3, 4}
	for i, wantID := range wantOrder {
		if got.Parts[i].PartID != wantID {
			t.Fatalf("at %d: PartID=%d, want %d", i, got.Parts[i].PartID, wantID)
		}
		if got.Parts[i].ETag != fmt.Sprintf("etag-%d", wantID) {
			t.Fatalf("at %d: ETag=%q, want %q", i, got.Parts[i].ETag, fmt.Sprintf("etag-%d", wantID))
		}
	}
}

func TestClientUploadComputesRanges(t *testing.T) {
	t.Parallel()
	// file size 35, part size 10 => 4 chunks; lengths: 10,10,10,5
	const (
		fileSize = int64(35)
		partSize = int64(10)
	)
	parts := uploadParts{
		{PartID: 1, URL: "u1"},
		{PartID: 2, URL: "u2"},
		{PartID: 3, URL: "u3"},
		{PartID: 4, URL: "u4"},
	}
	ir := IngestResponse{
		Size: fileSize,
		Upload: upload{
			PartSize: partSize,
			Parts:    parts,
			Type:     ingestUploadTypeS3MultiPart,
		},
	}

	type rng struct{ off, len int64 }
	expected := []rng{
		{0, 10},
		{10, 10},
		{20, 10},
		{30, 5},
	}

	var (
		seen = make(map[[2]int64]bool)
		mu   sync.Mutex
	)
	// verify each enqueued task's Offset/Length matches one of the expected ranges.
	checkRanges := func(t *testing.T) func(uploadTask) error {
		t.Helper()
		expected := []struct{ off, len int64 }{
			{0, 10},
			{10, 10},
			{20, 10},
			{30, 5},
		}

		return func(task uploadTask) error {
			key := [2]int64{task.Offset, task.Length}
			for _, e := range expected {
				if key == ([2]int64{e.off, e.len}) {
					mu.Lock()
					seen[key] = true
					mu.Unlock()
					return nil
				}
			}
			t.Errorf("unexpected range: offset=%d length=%d", task.Offset, task.Length)
			return fmt.Errorf("bad range: %v", key)
		}
	}

	proc := &fakeProcessor{check: checkRanges(t)}
	cl := newClientWithPool(t, proc, 2)

	got, err := cl.upload(t.Context(), ir, "ignored/path")
	if err != nil {
		t.Fatalf("upload returned error: %v", err)
	}

	// ensure we saw all expected ranges
	for _, e := range expected {
		key := [2]int64{e.off, e.len}
		if !seen[key] {
			t.Fatalf("missing expected range offset=%d length=%d", e.off, e.len)
		}
	}

	// parts should be sorted by 1-based PartID: 1..4
	if len(got.Parts) != len(parts) {
		t.Fatalf("got %d parts, want %d", len(got.Parts), len(parts))
	}
	for i, wantID := range []int64{1, 2, 3, 4} {
		if got.Parts[i].PartID != wantID {
			t.Fatalf("at %d: PartID=%d, want %d", i, got.Parts[i].PartID, wantID)
		}
	}
}

func TestClientFinalize(t *testing.T) { //nolint:cyclop
	t.Parallel()

	type wantReq struct {
		Path string
		Auth bool
	}

	type tc struct {
		name   string
		ur     UploadResult
		want   wantReq
		resp   testResp
		token  string
		assert func(t *testing.T, got IngestResponse, gotErr error)
	}

	okJSON := `{
	  "id":"ing-123",
	  "state":"processing",
	  "upload":{
	    "part_size": 5242880,
	    "parts":[{"part_id":1,"url":"https://example/1"}],
	    "type":"s3_multipart"
	  }
	}`

	makeServer := func(t *testing.T, w wantReq, resp testResp) *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(wr http.ResponseWriter, r *http.Request) {
			t.Logf("got path: %s", r.URL.Path)
			t.Logf("want path: %s", w.Path)
			if r.Method != http.MethodPost {
				http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if r.URL.Path != w.Path {
				http.Error(wr, "bad path: "+r.URL.Path, http.StatusNotFound)
				return
			}
			if w.Auth {
				if r.Header.Get("Authorization") == "" {
					http.Error(wr, "missing auth", http.StatusUnauthorized)
					return
				}
			}

			if resp.Status != 0 {
				wr.WriteHeader(resp.Status)
			}
			if len(resp.Body) > 0 {
				_, _ = wr.Write(resp.Body)
			}
		}))
	}

	tests := []tc{
		{
			name: "success 200",
			ur:   newUploadResult("ing-123", []uploadTaskResponse{{PartID: 1, ETag: "etag-1"}}),
			want: wantReq{
				Path: "/v1/datasets/ingest/ing-123/process", // expected final resolved path
				Auth: true,
			},
			resp:  testResp{Status: http.StatusOK, Body: []byte(okJSON)},
			token: "test-token",
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()
				if gotErr != nil {
					t.Fatalf("unexpected error: %v", gotErr)
				}
				if got.ID != "ing-123" {
					t.Fatalf("id=%q want ing-123", got.ID)
				}
				if got.State != "processing" {
					t.Fatalf("state=%q want processing", got.State)
				}
			},
		},
		{
			name: "non-2xx -> error",
			ur:   newUploadResult("ing-500", []uploadTaskResponse{}),
			want: wantReq{
				Path: "/v1/datasets/ingest/ing-500/process",
				Auth: true,
			},
			resp:  testResp{Status: http.StatusInternalServerError, Body: []byte(`{"error":"boom"}`)},
			token: "test-token",
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()
				if gotErr == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(gotErr.Error(), "request failed with 500") {
					t.Fatalf("got error %v, want 500 mapping", gotErr)
				}
			},
		},
		{
			name: "2xx but invalid JSON -> unmarshal error",
			ur:   newUploadResult("ing-json", []uploadTaskResponse{}),
			want: wantReq{
				Path: "/v1/datasets/ingest/ing-json/process",
				Auth: true,
			},
			resp:  testResp{Status: http.StatusOK, Body: []byte(`not-json`)},
			token: "test-token",
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()
				if gotErr == nil {
					t.Fatalf("expected unmarshal error, got nil")
				}
				var syntaxErr *json.SyntaxError
				if !errors.As(gotErr, &syntaxErr) {
					t.Fatalf("expected *json.SyntaxError, got %T (%v)", gotErr, gotErr)
				}
			},
		},
		{
			name: "state failed -> UploadFailedError",
			ur:   UploadResult{ID: "ing-failed", Type: "s3_multipart"},
			want: wantReq{
				Path: "/v1/datasets/ingest/ing-failed/process",
				Auth: true,
			},
			resp:  testResp{Status: http.StatusOK, Body: []byte(`{"id":"ing-failed","state":"failed"}`)},
			token: "test-token",
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()
				if gotErr == nil {
					t.Fatalf("expected UploadFailedError, got nil")
				}
				var ufe UploadFailedError
				if !errors.As(gotErr, &ufe) {
					t.Fatalf("expected UploadFailedError, got %T (%v)", gotErr, gotErr)
				}
				if ufe.ID != "ing-failed" {
					t.Fatalf("UploadFailedError.ID=%q want ing-failed", ufe.ID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := makeServer(t, tt.want, tt.resp)
			defer srv.Close()

			base := srv.URL + "/v1"
			cl, err := newTestClient(t, base, tt.token)
			if err != nil {
				t.Fatalf("creating test client should not fail")
			}

			got, err := cl.finalize(t.Context(), tt.ur)
			tt.assert(t, got, err)
		})
	}
}

func TestClient_Finalize(t *testing.T) { //nolint:cyclop
	t.Parallel()

	type wantReq struct {
		Path string
		Auth bool
	}

	type tc struct {
		name   string
		ur     UploadResult
		want   wantReq
		resp   testResp
		token  string
		assert func(t *testing.T, got IngestResponse, gotErr error)
	}

	okJSON := `{
	  "id":"ing-123",
	  "state":"processing",
	  "upload":{
	    "part_size": 5242880,
	    "parts":[{"part_id":1,"url":"https://example/1"}],
	    "type":"s3_multipart"
	  }
	}`

	makeServer := func(t *testing.T, w wantReq, resp testResp) *httptest.Server {
		t.Helper()
		return httptest.NewServer(http.HandlerFunc(func(wr http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPost {
				http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			if r.URL.Path != w.Path {
				http.Error(wr, "bad path: "+r.URL.Path, http.StatusNotFound)
				return
			}
			if w.Auth {
				if r.Header.Get("Authorization") == "" {
					http.Error(wr, "missing auth", http.StatusUnauthorized)
					return
				}
			}
			if resp.Status != 0 {
				wr.WriteHeader(resp.Status)
			}
			if len(resp.Body) > 0 {
				_, _ = wr.Write(resp.Body)
			}
		}))
	}

	tests := []tc{
		{
			name: "success 200",
			ur:   UploadResult{ID: "ing-123", Type: "s3_multipart", Parts: []uploadTaskResponse{{PartID: 1, ETag: "etag-1"}}},
			want: wantReq{
				Path: "/v1/datasets/ingest/ing-123/process", // expected final resolved path
				Auth: true,
			},
			resp:  testResp{Status: http.StatusOK, Body: []byte(okJSON)},
			token: "test-token",
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()
				if gotErr != nil {
					t.Fatalf("unexpected error: %v", gotErr)
				}
				if got.ID != "ing-123" {
					t.Fatalf("id=%q want ing-123", got.ID)
				}
				if got.State != "processing" {
					t.Fatalf("state=%q want processing", got.State)
				}
			},
		},
		{
			name: "non-2xx -> error",
			ur:   UploadResult{ID: "ing-500", Type: "s3_multipart"},
			want: wantReq{
				Path: "/v1/datasets/ingest/ing-500/process",
				Auth: true,
			},
			resp:  testResp{Status: http.StatusInternalServerError, Body: []byte(`{"error":"boom"}`)},
			token: "test-token",
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()
				if gotErr == nil {
					t.Fatalf("expected error, got nil")
				}
				if !strings.Contains(gotErr.Error(), "request failed with 500") {
					t.Fatalf("got error %v, want 500 mapping", gotErr)
				}
			},
		},
		{
			name: "2xx but invalid JSON -> unmarshal error",
			ur:   UploadResult{ID: "ing-json", Type: "s3_multipart"},
			want: wantReq{
				Path: "/v1/datasets/ingest/ing-json/process",
				Auth: true,
			},
			resp:  testResp{Status: http.StatusOK, Body: []byte(`not-json`)},
			token: "test-token",
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()
				if gotErr == nil {
					t.Fatalf("expected unmarshal error, got nil")
				}
				var syntaxErr *json.SyntaxError
				if !errors.As(gotErr, &syntaxErr) {
					t.Fatalf("expected *json.SyntaxError, got %T (%v)", gotErr, gotErr)
				}
			},
		},
		{
			name: "state failed -> UploadFailedError",
			ur:   UploadResult{ID: "ing-failed", Type: "s3_multipart"},
			want: wantReq{
				Path: "/v1/datasets/ingest/ing-failed/process",
				Auth: true,
			},
			resp:  testResp{Status: http.StatusOK, Body: []byte(`{"id":"ing-failed","state":"failed"}`)},
			token: "test-token",
			assert: func(t *testing.T, got IngestResponse, gotErr error) {
				t.Helper()
				if gotErr == nil {
					t.Fatalf("expected UploadFailedError, got nil")
				}
				var ufe UploadFailedError
				if !errors.As(gotErr, &ufe) {
					t.Fatalf("expected UploadFailedError, got %T (%v)", gotErr, gotErr)
				}
				if ufe.ID != "ing-failed" {
					t.Fatalf("UploadFailedError.ID=%q want ing-failed", ufe.ID)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			srv := makeServer(t, tt.want, tt.resp)
			defer srv.Close()

			base := srv.URL + "/v1"
			cl, err := newTestClient(t, base, tt.token)
			if err != nil {
				t.Fatalf("creating test client should not fail")
			}

			got, err := cl.finalize(t.Context(), tt.ur)
			tt.assert(t, got, err)
		})
	}
}

func TestClientCreateCancelsOnUploadError(t *testing.T) { //nolint:cyclop
	t.Parallel()

	data := []byte("abcdefghijklmnopqrstuvwxyz")
	f, err := os.CreateTemp(t.TempDir(), "upload-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	fp := f.Name()

	const (
		token    = "test-token"
		partSize = int64(10) // expect 3 parts: 10,10,6
	)

	var cancelHits int32

	mux := http.NewServeMux()

	// ingest create (POST /v1/datasets/ingest)
	mux.HandleFunc("/v1/datasets/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if got := r.Header.Get("Authorization"); got != "Token "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}

		base := "http://" + r.Host
		resp := IngestResponse{
			ID:    "ing-xyz",
			Size:  int64(len(data)),
			State: "upload",
			Upload: upload{
				PartSize: partSize,
				Type:     ingestUploadTypeS3MultiPart,
				Parts: uploadParts{
					{PartID: 1, URL: base + "/upload/part1"},
					{PartID: 2, URL: base + "/upload/part2"},
					{PartID: 3, URL: base + "/upload/part3"},
				},
			},
		}
		b, _ := json.Marshal(resp)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	})

	// part uploads — make the last one fail with 500 to trigger cancel
	mux.HandleFunc("/upload/part1", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("ETag", `"etag-1"`)
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/upload/part2", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("ETag", `"etag-2"`)
		w.WriteHeader(http.StatusOK)
	})

	mux.HandleFunc("/upload/part3", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		http.Error(w, "boom", http.StatusInternalServerError)
	})

	// cancel (POST /v1/datasets/ingest/:id/cancel)
	mux.HandleFunc("/v1/datasets/ingest/ing-xyz/cancel", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		// must include auth
		if got := r.Header.Get("Authorization"); got != "Token "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		atomic.AddInt32(&cancelHits, 1)
		resp := IngestResponse{ID: "ing-xyz", State: "canceled"}
		b, _ := json.Marshal(resp)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	})

	mux.HandleFunc("/v1/datasets/ingest/ing-xyz/process", func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("finalize should never be called on cancelled uploads")
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	cl, err := New(srv.URL+"/v1", token)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	_, cerr := cl.Create(t.Context(), fp)
	if cerr == nil {
		t.Fatalf("Create() expected error due to part3 500, got nil")
	}

	// must be an UploadFailedError (returned by process() on upload failure)
	var ufe UploadFailedError
	if !errors.As(cerr, &ufe) {
		t.Fatalf("expected UploadFailedError, got %T (%v)", cerr, cerr)
	}
	if ufe.ID != "ing-xyz" {
		t.Fatalf("UploadFailedError.ID=%q want ing-xyz", ufe.ID)
	}

	if atomic.LoadInt32(&cancelHits) != 1 {
		t.Fatalf("cancel endpoint should be called once, got %d", cancelHits)
	}
}

func TestClientCreateNoCancelOnSuccess(t *testing.T) { //nolint:cyclop
	t.Parallel()

	// tmp file: 26 bytes
	data := []byte("abcdefghijklmnopqrstuvwxyz")
	f, err := os.CreateTemp(t.TempDir(), "upload-*")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	fp := f.Name()

	const (
		token    = "test-token"
		partSize = int64(10)
	)

	var cancelHits int32

	mux := http.NewServeMux()

	// ingest (success)
	mux.HandleFunc("/v1/datasets/ingest", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Authorization") != "Token "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		base := "http://" + r.Host
		resp := IngestResponse{
			ID:    "ing-ok",
			Size:  int64(len(data)),
			State: "upload",
			Upload: upload{
				PartSize: partSize,
				Type:     ingestUploadTypeS3MultiPart,
				Parts: uploadParts{
					{PartID: 1, URL: base + "/upload/part1"},
					{PartID: 2, URL: base + "/upload/part2"},
					{PartID: 3, URL: base + "/upload/part3"},
				},
			},
		}
		b, _ := json.Marshal(resp)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	})

	// upload parts (all success)
	for n := 1; n <= 3; n++ {
		n := n
		mux.HandleFunc(fmt.Sprintf("/upload/part%d", n), func(w http.ResponseWriter, r *http.Request) {
			if r.Method != http.MethodPut {
				http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
				return
			}
			// return ETag for each part
			w.Header().Set("ETag", fmt.Sprintf(`"etag-%d"`, n))
			w.WriteHeader(http.StatusOK)
		})
	}

	// finalize (success)
	mux.HandleFunc("/v1/datasets/ingest/ing-ok/process", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		if r.Header.Get("Authorization") != "Token "+token {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		// Read raw body for assertions
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, "read body error", http.StatusBadRequest)
			return
		}
		_ = r.Body.Close()

		if !bytes.Contains(raw, []byte(`"upload_result"`)) {
			http.Error(w, "missing upload_result wrapper", http.StatusBadRequest)
			return
		}

		resp := IngestResponse{ID: "ing-ok", State: "completed"}
		b, _ := json.Marshal(resp)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(b)
	})

	// cancel endpoint (should NOT be called)
	mux.HandleFunc("/v1/datasets/ing-ok/ingest/cancel", func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&cancelHits, 1)
		w.WriteHeader(http.StatusOK)
	})

	srv := httptest.NewServer(mux)
	defer srv.Close()

	cl, err := New(srv.URL+"/v1", token)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	got, cerr := cl.Create(t.Context(), fp)
	if cerr != nil {
		t.Fatalf("Create() unexpected error: %v", cerr)
	}
	if got.ID != "ing-ok" || got.State == "failed" {
		t.Fatalf("unexpected final response: %+v", got)
	}
	if atomic.LoadInt32(&cancelHits) != 0 {
		t.Fatalf("cancel endpoint should not be called, got %d", cancelHits)
	}
}

type fakeProcessor struct {
	check func(uploadTask) error
}

func (p *fakeProcessor) Process(ctx context.Context, tsk task[uploadTask]) error {
	if p.check != nil {
		if err := p.check(tsk.Body); err != nil {
			return err
		}
	}
	// always respond with an ETag for the part
	tsk.Body.RespCh <- uploadTaskResponse{
		PartID: tsk.Body.PartID,
		ETag:   fmt.Sprintf("etag-%d", tsk.Body.PartID),
	}
	return nil
}
func (p *fakeProcessor) Close() {}
