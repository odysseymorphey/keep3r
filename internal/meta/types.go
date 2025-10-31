package meta

import "time"

type ObjectMeta struct {
	Bucket      string    `json:"bucket"`
	Key         string    `json:"key"`
	Size        int64     `json:"size"`
	ContentType string    `json:"content_type"`
	ETag        string    `json:"etag"`
	BlobPath    string    `json:"blob_path"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type ListOptions struct {
	Prefix string
	Limit  int
	Cursor string
}

type Store interface {
	Put(m ObjectMeta) error
	Get(bucket, key string) (ObjectMeta, error)
	Delete(bucket, key string) error
	List(bucket string, opt ListOptions) (items []ObjectMeta, nextCursor string, err error)
	Close() error
}