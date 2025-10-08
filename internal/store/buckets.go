package store

import (
	"encoding/json"
	"time"

	bolt "go.etcd.io/bbolt"
)

type BucketMeta struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

func (db *DB) CreateBucket(name string) error {
	now := time.Now().UTC()
	meta := BucketMeta{Name: name, CreatedAt: now}

	return db.bolt.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bktBuckets))
		if v := b.Get([]byte(name)); v != nil {
			return nil
		}

		buf, _ := json.Marshal(meta)
		return b.Put([]byte(name), buf)
	})
}

func (db *DB) BucketExists(name string) (bool, error) {
	var ok bool

	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bktBuckets))
		ok = b.Get([]byte(name)) != nil
		return nil
	})

	return ok, err
}

func (db *DB) ListBuckets() ([]BucketMeta, error) {
	var buckets []BucketMeta

	err := db.bolt.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(bktBuckets))
		if b == nil {
			return nil
		}

		return b.ForEach(func(_, v []byte) error {
			var meta BucketMeta
			if err := json.Unmarshal(v, &meta); err != nil {
				return err
			}
			buckets = append(buckets, meta)
			return nil
		})
	})

	if err != nil {
		return nil, err
	}

	return buckets, nil
}
