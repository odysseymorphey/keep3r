package main

import (
	"keep3r/internal/httpapi"
)

func main() {
	s := httpapi.New()

	s.Run()
}