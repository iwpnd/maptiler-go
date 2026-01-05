// Package maptiler provides a client for interacting with the MapTiler service API.
// It handles dataset ingestion, file uploads, and processing operations.
package maptiler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/http/cookiejar"
	"os"
	"slices"
	"time"

	"github.com/iwpnd/rip"
	"golang.org/x/sync/errgroup"
)

const (
	serviceHost          = "https://service.maptiler.com/v1"
	serviceIngestUpdate  = "/datasets/:id/ingest"
	serviceIngestCreate  = "/datasets/ingest"
	serviceIngestGet     = "/datasets/ingest/:id"
	serviceIngestCancel  = "/datasets/ingest/:id/cancel"
	serviceIngestProcess = "/datasets/ingest/:id/process"
)

// processorFn defines a function type for processing dataset operations.
// It takes a context, dataset ID, and file path, returning an IngestResponse.
type processorFn func(context.Context, string, string) (IngestResponse, error)

// Client provides methods for interacting with the MapTiler service API.
// It manages HTTP requests and concurrent file uploads.
type Client struct {
	h  *rip.Client
	wp *pool[uploadTask]
}

// New creates a new MapTiler client with the specified host and authentication token.
// If host is empty, it defaults to the MapTiler service host.
// If token is empty, it attempts to read from the MAPTILER_TOKEN environment variable.
func New(host, token string) (*Client, error) {
	tok := token
	if tok == "" {
		tok = os.Getenv("MAPTILER_TOKEN")
	}
	if tok == "" {
		return nil, fmt.Errorf("initializing maptiler client, empty token")
	}

	var addr string
	if host == "" {
		addr = serviceHost
	} else {
		addr = host
	}

	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, fmt.Errorf("initializing maptiler client: %w", err)
	}
	h, err := rip.NewClient(
		addr,
		rip.WithCookieJar(jar),
		rip.WithDefaultHeaders(map[string]string{
			"Authorization": "Token " + tok,
		}),
	)
	if err != nil {
		return nil, err
	}

	tr := &http.Transport{
		IdleConnTimeout: 30 * time.Second,
		MaxIdleConns:    10,
	}

	// initialize with empty host, as part uris are provided later.
	wc, err := rip.NewClient("", rip.WithTransport(tr))
	if err != nil {
		return nil, fmt.Errorf("initializing worker http client: %w", err)
	}

	wp := newPool(newUploadProcessor(wc), withPoolConcurrency(10))
	return &Client{wp: wp, h: h}, nil
}

// Create initiates a new dataset ingestion process with the specified file.
// It uploads the file and processes it, returning the ingestion response.
func (c *Client) Create(ctx context.Context, fp string) (IngestResponse, error) {
	return c.withCancel(
		ctx,
		c.process,
		"", fp,
	)
}

// Update updates an existing dataset with the specified ID using the provided file.
// It uploads the file and processes it, returning the ingestion response.
func (c *Client) Update(ctx context.Context, id, fp string) (IngestResponse, error) {
	return c.withCancel(
		ctx,
		c.process,
		id, fp,
	)
}

// Cancel sends a cancellation request to the MapTiler service for the specified ingest/dataset ID.
func (c *Client) Cancel(ctx context.Context, id string) (IngestResponse, error) {
	return c.cancel(ctx, id)
}

// Get returns an active upload by ID.
func (c *Client) Get(ctx context.Context, id string) (IngestGetResponse, error) {
	req := c.h.NR().SetParams(rip.Params{"id": id})
	resp, err := req.Execute(ctx, "GET", serviceIngestGet)
	if err != nil {
		return IngestGetResponse{}, fmt.Errorf("getting upload: %w", err)
	}
	defer resp.Close() //nolint:errcheck

	if resp.IsError() {
		return IngestGetResponse{}, fmt.Errorf("getting upload: %w", err)
	}

	var ir IngestGetResponse
	uerr := json.Unmarshal(resp.Body(), &ir)
	if uerr != nil {
		return ir, fmt.Errorf("getting upload: %w", uerr)
	}

	return ir, err
}

// process handles the complete ingestion workflow: file validation, ingestion request,
// upload, and finalization. It returns an IngestResponse or an error.
func (c *Client) process(ctx context.Context, id, fp string) (IngestResponse, error) {
	info, err := fileInfo(fp)
	if err != nil {
		return IngestResponse{}, err
	}

	req := newIngestRequest(id, info.Name(), info.Size())
	resp, err := c.ingest(ctx, req)
	if err != nil {
		return resp, err
	}

	uresp, err := c.upload(ctx, resp, fp)
	if err != nil {
		return IngestResponse{}, UploadFailedError{
			ID:  resp.ID,
			Err: err,
		}
	}

	presp, err := c.finalize(ctx, uresp)
	if err != nil {
		return IngestResponse{}, UploadFailedError{
			ID:  resp.ID,
			Err: err,
		}
	}

	return presp, nil
}

// withCancel wraps the processFn and automatically cancels the upload with the MapTiler
// service API if an UploadFailedError occurs during processing.
func (c *Client) withCancel(ctx context.Context, run processorFn, id, fp string) (IngestResponse, error) {
	ir, err := run(ctx, id, fp)
	if err == nil {
		return ir, nil
	}

	var uerr UploadFailedError
	if errors.As(err, &uerr) {
		if ir, cerr := c.cancel(ctx, uerr.ID); cerr != nil {
			return ir, fmt.Errorf("upload failed: %w; cancel failed: %w", err, cerr)
		}
	}

	return ir, fmt.Errorf("upload failed with: %w", err)
}

// cancel sends a cancellation request to the MapTiler service for the specified dataset ID.
// It returns the final ingestion response after cancellation.
func (c *Client) cancel(ctx context.Context, id string) (IngestResponse, error) {
	req := c.h.NR().SetParams(rip.Params{"id": id})
	resp, err := req.Execute(ctx, "POST", serviceIngestCancel)
	if err != nil {
		return IngestResponse{}, fmt.Errorf("canceling upload: %w", err)
	}
	defer resp.Close() //nolint:errcheck

	if resp.IsError() {
		return IngestResponse{}, fmt.Errorf("canceling upload: %w", err)
	}

	var ir IngestResponse
	uerr := json.Unmarshal(resp.Body(), &ir)
	if uerr != nil {
		return ir, fmt.Errorf("canceling upload: %w", uerr)
	}

	return ir, err
}

// upload handles concurrent multipart file upload using the upload URLs provided
// in the IngestResponse. It returns an UploadResult containing all part responses.
func (c *Client) upload(ctx context.Context, ir IngestResponse, fp string) (UploadResult, error) {
	parts := ir.Upload.Parts
	partSize := ir.Upload.PartSize
	fileSize := ir.Size

	respCh := make(chan uploadTaskResponse, len(parts))
	results := make(map[string]uploadTaskResponse)

	eg, gctx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		defer close(respCh)
		if wErr := c.wp.Start(gctx); wErr != nil {
			return fmt.Errorf("processing worker pool: %w", wErr)
		}
		return nil
	})

	eg.Go(func() error {
		for i, p := range parts {
			offset, length := getRange(int64(i), partSize, fileSize)
			if length <= 0 {
				break
			}
			c.wp.Enqueue(newTask(uploadTask{
				uploadPart: uploadPart{
					PartID: p.PartID,
					URL:    p.URL,
				},
				FilePath: fp,
				RespCh:   respCh,
				Offset:   offset,
				Length:   length,
			}))
		}
		c.wp.Stop()
		return nil
	})

	eg.Go(func() error {
		for r := range respCh {
			results[fmt.Sprint(r.PartID)] = r
		}
		return nil
	})

	if gErr := eg.Wait(); gErr != nil {
		return UploadResult{}, fmt.Errorf("waiting for error group to finish: %w", gErr)
	}

	seq := maps.Values(results)
	responses := slices.Collect(seq)

	slices.SortFunc(responses, func(a, b uploadTaskResponse) int {
		if a.PartID < b.PartID {
			return -1
		}
		if a.PartID > b.PartID {
			return 1
		}
		return 0
	})

	return newUploadResult(ir.ID, responses), nil
}

// getRange calculates the byte offset and length for a specific part in a multipart upload.
// It returns zero length when the offset exceeds the file size, signaling completion.
func getRange(idx, partSize, fileSize int64) (off, length int64) {
	if partSize <= 0 {
		return 0, 0
	}
	off = idx * partSize
	if off >= fileSize {
		// Past EOF; signal caller to stop/skip.
		return off, 0
	}
	remaining := fileSize - off
	if remaining < partSize {
		return off, remaining
	}
	return off, partSize
}

// ingest sends an ingestion request to the MapTiler service, either creating a new
// dataset or updating an existing one based on the request ID.
func (c *Client) ingest(ctx context.Context, request ingestRequest) (IngestResponse, error) {
	req := c.h.NR()
	var url string
	if request.ID != "" {
		url = serviceIngestUpdate
		req.SetParams(rip.Params{"id": request.ID})
	} else {
		url = serviceIngestCreate
	}

	resp, err := req.SetBody(request).Execute(ctx, "POST", url)
	if err != nil {
		return IngestResponse{}, err
	}
	defer resp.Close() //nolint:errcheck

	if resp.IsError() {
		return IngestResponse{}, fmt.Errorf("request failed with %d", resp.StatusCode())
	}

	var ir IngestResponse
	uerr := json.Unmarshal(resp.Body(), &ir)
	if uerr != nil {
		return IngestResponse{}, uerr
	}

	// we something goes wrong at this point we force a cancel
	if ir.State == "failed" {
		return IngestResponse{}, UploadFailedError{ID: ir.ID}
	}

	return ir, nil
}

// finalize completes the ingestion process by sending the upload results to the
// MapTiler service for final processing.
func (c *Client) finalize(ctx context.Context, ur UploadResult) (IngestResponse, error) {
	req := c.h.NR().SetBody(uploadResultRequest{UploadResult: ur}).SetParams(rip.Params{"id": ur.ID})
	resp, err := req.Execute(ctx, "POST", serviceIngestProcess)
	if err != nil {
		return IngestResponse{}, UploadFailedError{ID: ur.ID}
	}
	defer resp.Close() //nolint:errcheck

	if resp.IsError() {
		return IngestResponse{}, fmt.Errorf("request failed with %d", resp.StatusCode())
	}

	var ir IngestResponse
	uerr := json.Unmarshal(resp.Body(), &ir)
	if uerr != nil {
		return IngestResponse{}, uerr
	}

	// we something goes wrong at this point we force a cancel
	if ir.State == "failed" {
		return IngestResponse{}, UploadFailedError{ID: ur.ID}
	}

	return ir, nil
}

// fileInfo validates that the specified file path exists and is not a directory.
// It returns the file information or an error if validation fails.
func fileInfo(fp string) (os.FileInfo, error) {
	info, err := os.Stat(fp)
	if err == nil {
		if info.IsDir() {
			return nil, fmt.Errorf("expected file %q to exist, but it is a directory", fp)
		}
	}
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("expected file %q to exist, but it does not", fp)
	}
	return info, nil
}
