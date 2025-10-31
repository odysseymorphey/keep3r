package store

import (
	"bytes"
	"encoding/json"
	"errors"
	"keep3r/internal/meta"

	"go.etcd.io/bbolt"
)

func (s *Store) Put(m meta.ObjectMeta) error {
	payload, err := json.Marshal(m)
	if err != nil {
		return err
	}

	return s.bolt.Update(func(tx *bbolt.Tx) error {
		bkt, err := ensureBucket(tx, m.Bucket)
		if err != nil {
			return err
		}

		return bkt.Put([]byte(m.Key), payload)
	})
}

func (s *Store) Get(bucket, key string) (meta.ObjectMeta, error) {
	var out meta.ObjectMeta
	err := s.bolt.View(func(tx *bbolt.Tx) error {
		bkt := getBucket(tx, bucket)
		if bkt == nil {
			return errors.New("bucket not found")
		}

		v := bkt.Get([]byte(key))
		if v == nil {
			return errors.New("value not found")
		}

		return json.Unmarshal(v, &out)
	})

	return out, err
}

func (s *Store) Delete(bucket, key string) error {
	return s.bolt.Update(func(tx *bbolt.Tx) error {
		bkt := getBucket(tx, bucket)
		if bkt == nil {
			return nil
		}

		return bkt.Delete([]byte(key))
	})
}

func (s *Store) List(bucket string, opt meta.ListOptions) ([]meta.ObjectMeta, meta.NextCursor, error) {
	var items []meta.ObjectMeta
	var next meta.NextCursor

	err := s.bolt.View(func(tx *bbolt.Tx) error {
		bkt := getBucket(tx, bucket)
		if bkt == nil {
			return nil
		}

		c := bkt.Cursor()

		start := []byte(opt.Prefix)

		if opt.Cursor != "" {
			start = []byte(opt.Cursor)

			k, _ := c.Seek(start)
			if k != nil && bytes.Equal(k, start) {
				k, _ = c.Next()
			}

			if k != nil && !bytes.HasPrefix(k, []byte(opt.Prefix)) {
				k, _ = c.Seek([]byte(opt.Prefix))
			}

			if k == nil {
				return nil
			}
			return collect(c, k, opt, &items, &next)
		}

		k, _ := c.Seek(start)
		if k == nil {
			return nil
		}
		return collect(c, k, opt, &items, &next)
	})

	return items, next, err
}

func bucketPath(b string) []byte {
	return []byte(b)
}

func getBucket(tx *bbolt.Tx, bucket string) *bbolt.Bucket {
	root := tx.Bucket(rootBucket)
	if root == nil {
		return nil
	}

	return root.Bucket(bucketPath(bucket))
}

func ensureBucket(tx *bbolt.Tx, bucket string) (*bbolt.Bucket, error) {
	root := tx.Bucket(rootBucket)
	if root == nil {
		return nil, errors.New("root bucket missed")
	}

	b, err := root.CreateBucketIfNotExists(bucketPath(bucket))
	if err != nil {
		return nil, err
	}

	return b, nil
}

func collect(c *bbolt.Cursor, k []byte, opt meta.ListOptions, items *[]meta.ObjectMeta, next *meta.NextCursor) error {
	limit := opt.Limit

	if limit <= 0 || limit > 1000 {
		limit = 100
	}

	var v []byte
	for ; k != nil && bytes.HasPrefix(k, []byte(opt.Prefix)) && len(*items) < limit; k, v = c.Next() {
		var m meta.ObjectMeta
		if err := json.Unmarshal(v, &m); err != nil {
			return err
		}

		*items = append(*items, m)
	}

	if k != nil && bytes.HasPrefix(k, []byte(opt.Prefix)) {
		*next = meta.NextCursor(k)
	}

	return nil
}
