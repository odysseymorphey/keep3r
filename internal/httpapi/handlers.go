package httpapi

import (
	"errors"
	"io"
	"keep3r/internal/meta"
	"net/http"
	"os"
	"path/filepath"
)

func BaseHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello"))
	// todo: Delete this
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	const MAX_UPLOAD_SIZE = 10 << 30
	r.Body = http.MaxBytesReader(w, r.Body, MAX_UPLOAD_SIZE)

	reader, err := r.MultipartReader()
	if err != nil {
		http.Error(w, "waiting for multipart/form-data", http.StatusBadRequest)
		return
	}

	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			http.Error(w, "failed to read body", http.StatusInternalServerError)
			return
		}

		if part.FileName() == "" {
			continue
		}

		dstPath := filepath.Join("data", part.FileName())
		if err := os.MkdirAll(filepath.Dir(dstPath), 0755); err != nil {
			http.Error(w, "failed to prepare path", http.StatusInternalServerError)
			return
		}
		
		dst, err := os.Create(dstPath)
		if err != nil {
			http.Error(w, "failed to create file", http.StatusInternalServerError)
		}

		if _, err := io.Copy(dst, part); err != nil {
			dst.Close()
			http.Error(w, "failed to write file", http.StatusInternalServerError)
			return
		}
		dst.Close()
	}
	w.WriteHeader(http.StatusCreated)
}

func sUpload(store meta.Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("upload endponit is wired; meta store ready"))
	}
} 