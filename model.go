package maptiler

import (
	"encoding/json"
	"fmt"
)

const ingestUploadTypeS3MultiPart = "s3_multipart"

type MapTilerError struct {
	Message string `json:"message"`
}

type IngestResponse struct {
	ID         string        `json:"id"`
	DocumentID string        `json:"document_id"`
	State      string        `json:"state"`
	Filename   string        `json:"Filename"`
	Size       int64         `json:"size"`
	Progress   float64       `json:"progress"`
	Error      MapTilerError `json:"error"`
	Upload     upload        `json:"upload"`
	UploadURL  string        `json:"upload_url"`
}

type UploadResult struct {
	ID    string               `json:"-"`
	Type  string               `json:"type"`
	Parts []uploadTaskResponse `json:"parts"`
}

func (m MapTilerError) String() string  { return toJSONString(m) }
func (r IngestResponse) String() string { return toJSONString(r) }
func (r UploadResult) String() string   { return toJSONString(r) }

type uploadPart struct {
	PartID int64  `json:"part_id"`
	URL    string `json:"url"`
}

type uploadParts []uploadPart

type uploadTaskResponse struct {
	PartID int64  `json:"part_id"`
	ETag   string `json:"etag"`
}

type uploadTask struct {
	uploadPart
	FilePath string
	Offset   int64
	Length   int64
	RespCh   chan uploadTaskResponse
}

type upload struct {
	PartSize int64       `json:"part_size"`
	Parts    uploadParts `json:"parts"`
	Type     string      `json:"type"`
}

type ingestRequest struct {
	ID                   string   `json:"id"`
	Filename             string   `json:"filename"`
	Size                 int64    `json:"size"`
	SupportedUploadTypes []string `json:"supported_upload_types"`
}

func newUploadResult(id string, parts []uploadTaskResponse) UploadResult {
	return UploadResult{
		ID:    id,
		Type:  ingestUploadTypeS3MultiPart,
		Parts: parts,
	}
}

type uploadResultRequest struct {
	UploadResult UploadResult `json:"upload_result"`
}

func newIngestRequest(id, fn string, size int64) ingestRequest {
	return ingestRequest{
		ID:                   id,
		Filename:             fn,
		Size:                 size,
		SupportedUploadTypes: []string{ingestUploadTypeS3MultiPart},
	}
}

func toJSONString(v any) string {
	b, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return fmt.Sprintf("<error marshaling: %v>", err)
	}
	return string(b)
}

func (p uploadPart) String() string          { return toJSONString(p) }
func (p uploadParts) String() string         { return toJSONString(p) }
func (u uploadTaskResponse) String() string  { return toJSONString(u) }
func (u uploadTask) String() string          { return toJSONString(u) }
func (u upload) String() string              { return toJSONString(u) }
func (r ingestRequest) String() string       { return toJSONString(r) }
func (r uploadResultRequest) String() string { return toJSONString(r) }
