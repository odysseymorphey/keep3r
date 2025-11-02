package httpapi

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"keep3r/internal/meta"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const MAX_UPLOAD_SIZE = 10 << 30

func sUpload(store meta.Store, dataRoot string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		bucket := fisrtNonEmpty(r.URL.Query().Get("bucket"), r.Header.Get("X-Bucket"))
		key := fisrtNonEmpty(r.URL.Query().Get("key"), r.Header.Get("X-Key"))

		if bucket == "" || key == "" {
			http.Error(w, "bucket and key required (query: ?bucket=&key& or headers X-Bucket/X-Key)", http.StatusBadRequest)
			return
		}

		if err := validateName(bucket); err != nil {
			http.Error(w, "bucket: "+err.Error(), http.StatusBadRequest)
		}

		if err := validateKey(key); err != nil {
			http.Error(w, "key: "+err.Error(), http.StatusBadRequest)
		}

		r.Body = http.MaxBytesReader(w, r.Body, MAX_UPLOAD_SIZE)
		defer r.Body.Close()

		finalDir := filepath.Join(dataRoot, bucket, filepath.Dir(key))
		finalPath := filepath.Join(dataRoot, bucket, key)

		if !strings.HasPrefix(filepath.Clean(finalPath)+string(os.PathSeparator), filepath.Clean(filepath.Join(dataRoot, bucket))+string(os.PathSeparator)) {
			http.Error(w, "invalid path traversal", http.StatusBadRequest)
			// todo: add log
			return
		}

		if err := os.MkdirAll(finalDir, 0755); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			// todo: add log
			return
		}

		tmpFile, err := os.CreateTemp(finalDir, ".upload-*")
		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			// todo: add log
			return
		}
		tmpPath := tmpFile.Name()

		defer func() {
			_ = os.Remove(tmpPath)
		}()

		hasher := sha256.New()

		head := make([]byte, 512)
		nHead, _ := io.ReadFull(r.Body, head)
		head = head[:nHead]

		ctype := r.Header.Get("Content-Type")
		if ctype == "" {
			ctype = http.DetectContentType(head)
		}

		size := int64(0)
		if len(head) > 0 {
			if _, err := tmpFile.Write(head); err != nil {
				http.Error(w, "internal server error", http.StatusInternalServerError)
				// todo: add log; failed to write head in temp file
				return
			}
			if _, err := hasher.Write(head); err != nil {
				http.Error(w, "internal server error", http.StatusInternalServerError)
				// todo: add log; failed to write head in hasher
				return
			}
			size += int64(len(head))
		}

		n, err := io.Copy(io.MultiWriter(tmpFile, hasher), r.Body)
		if err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			// todo: add log; stream copy error
			return
		}
		size += n

		if err := tmpFile.Sync(); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			// todo: add log; tmp file sync failed
			return
		}
		if err := tmpFile.Close(); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			// todo: add log; temp file close failed
			return
		}

		_ = os.Remove(finalPath)
		if err := os.Rename(tmpPath, finalPath); err != nil {
			http.Error(w, "internal server error", http.StatusInternalServerError)
			// todo: add log; rename failed
			return
		}

		etag := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
		now := time.Now().UTC()

		om := meta.ObjectMeta{
			Bucket:      bucket,
			Key:         key,
			Size:        size,
			ContentType: ctype,
			ETag:        etag,
			BlobPath:    finalDir,
			CreatedAt:   now,
			UpdatedAt:   now,
		}

		if err := store.Put(om); err != nil {
			_ = os.Remove(finalPath)
			http.Error(w, "internal server error", http.StatusInternalServerError)
			// todo: add log; failed to save object meta
			return
		}

		w.Header().Set("ETag", etag)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]any{
			"bucket": bucket,
			"key":    key,
			"size":   size,
			"type":   ctype,
			"path":   finalPath,
		})
	}
}

func fisrtNonEmpty(a, b string) string {
	if a != "" {
		return a
	}

	return b
}

func validateName(s string) error {
	if s == "" {
		return errors.New("empty")
	}
	if strings.ContainsAny(s, `/\`) {
		return errors.New(`must not contain "/" or "\" `)
	}
	if strings.HasPrefix(s, ".") {
		return errors.New("must not start with dot")
	}

	return nil
}

func validateKey(s string) error {
	if s == "" {
		return errors.New("empty key")
	}
	if strings.Contains(s, "..") {
		return errors.New("path traversal '..' is not allowed")
	}
	cleaned := filepath.Clean("/" + s)
	if cleaned == "/" {
		return errors.New("key is invalid")
	}

	return nil
}
