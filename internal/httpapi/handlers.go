package httpapi

import "net/http"

func BaseHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("hello"))
	// todo: Delete this
}

func uploadHandler(w http.ResponseWriter, r *http.Request) {
	w.Write([]byte("uploading"))

	err := r.ParseMultipartForm(32 << 20)
	if err != nil {
		return
	}

	err = r.FormFile()

	// todo: Implement this
}