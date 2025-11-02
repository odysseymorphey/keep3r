package main

import (
	"keep3r/internal/httpapi"
	"log"
)

func main() {
	s, err := httpapi.New(httpapi.Options{
		HTTPAddr: ":8088",
		MetaDBPath: "./meta/meta.db",
		DataRoot: "./data/blobs",
	})
	if err != nil {
		log.Fatal(err)
	}

	// todo: Add normal errors
	// todo: Add logs
	// todo: Add tests
	// todo: Add readme

	s.Run()
}