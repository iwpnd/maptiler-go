package maptiler

import "fmt"

type UploadFailedError struct {
	ID  string
	Err error
}

func (e UploadFailedError) Error() string {
	return fmt.Sprintf("upload %s failed, err: %s", e.ID, e.Err)
}
