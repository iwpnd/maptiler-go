package maptiler

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/iwpnd/rip"
)

// processor defines the interface for processing a task.
type processor[T any] interface {
	Process(ctx context.Context, t task[T]) error
	Close()
}

func newUploadProcessor(h *rip.Client) processor[uploadTask] {
	return &uploadProcessor{
		h: h,
	}
}

type uploadProcessor struct {
	h *rip.Client
}

func (u *uploadProcessor) Process(ctx context.Context, t task[uploadTask]) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("processing upload: %w", err)
	}

	info, err := os.Stat(t.Body.FilePath)
	if err == nil {
		if info.IsDir() {
			return fmt.Errorf("expected file %q to exist, but it is a directory", t.Body.FilePath)
		}
	}
	if os.IsNotExist(err) {
		return fmt.Errorf("expected file %q to exist, but it does not", t.Body.FilePath)
	}

	file, err := os.Open(t.Body.FilePath)
	if err != nil {
		return fmt.Errorf("failed to open file at path '%s': %w", t.Body.FilePath, err)
	}
	defer file.Close() //nolint:errcheck

	part := io.NewSectionReader(file, t.Body.Offset, t.Body.Length)
	resp, err := u.h.NR().SetBody(part).SetContentLength(t.Body.Length).Execute(ctx, "PUT", t.Body.URL)
	if err != nil {
		return fmt.Errorf("sending part %d: %w", t.Body.PartID, err)
	}
	defer resp.Close() //nolint:errcheck

	if resp.IsError() {
		return fmt.Errorf("sending part %d: %w", t.Body.PartID, err)
	}

	etag := resp.Header().Clone().Get("ETag")
	if etag == "" {
		return fmt.Errorf("empty etag in response header")
	}

	t.Body.RespCh <- uploadTaskResponse{
		PartID: t.Body.PartID,
		ETag:   etag,
	}

	return err
}

func (*uploadProcessor) Close() {}
