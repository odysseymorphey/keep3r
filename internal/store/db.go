package store

import (
	"errors"
	"time"

	bolt "go.etcd.io/bbolt"
)

var (
	rootBucket  = []byte("objects")
	ErrNotFound = errors.New("not found")
)

type Store struct {
	bolt *bolt.DB
}

type Options struct {
	Path string
}

func Open(opt Options) (*Store, error) {
	db, err := bolt.Open(opt.Path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(rootBucket)
		return e
	}); err != nil {
		_ = db.Close()
		return nil, err
	}

	return &Store{bolt: db}, nil
}

func (db *Store) Close() error {
	return db.bolt.Close()
}
