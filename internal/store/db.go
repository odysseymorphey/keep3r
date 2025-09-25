package store

import (
	"errors"
	"os"
	"path/filepath"
	"time"

	bolt "go.etcd.io/bbolt"
)

const (
	bktBuckets = "meta/buckets"
	bktObjects = "meta/objects"
)

var (
	ErrNotFound = errors.New("not found")
)

type DB struct {
	bolt *bolt.DB
}

func OpenDB(metaDir string) (*DB, error) {
	if err := os.MkdirAll(metaDir, 0755); err != nil {
		return nil, err
	}

	dbpath := filepath.Join(metaDir, "meta.db")
	d, err := bolt.Open(dbpath, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	if err := d.Update(func(tx *bolt.Tx) error {
		for _, name := range []string{bktBuckets, bktObjects} {
			if _, err := tx.CreateBucketIfNotExists([]byte(name)); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		_ = d.Close()
		return nil, err
	}
	return &DB{d}, nil
}

func (db *DB) Close() error {
	return db.bolt.Close()
}
