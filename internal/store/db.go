package store

import (
	"errors"
	"time"

	"go.etcd.io/bbolt"
)

var (
	rootBucket  = []byte("objects")
	ErrNotFound = errors.New("not found")
)

type Store struct {
	bolt *bbolt.DB
}

type Options struct {
	Path string
}

func Open(opt Options) (*Store, error) {
	db, err := bbolt.Open(opt.Path, 0600, &bbolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bbolt.Tx) error {
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
