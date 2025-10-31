package httpapi

import (
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
)

type Server struct {
	http *http.Server
}

func New() *Server {
	r := chi.NewRouter()

	r.Use(middleware.RequestID)
	r.Use(middleware.RealIP)
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)

	r.Get("/", BaseHandler)
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("Status OK"))
	})

	r.Route("/api", func(r chi.Router) {
		r.Put("/upload", uploadHandler)
	})

	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8088"
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           r,
	}

	return &Server{
		http: srv,
	}
}

func (s *Server) Run() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := s.http.ListenAndServe(); err != nil {
			log.Fatal(err)
		}

	}()

	<-sig
	log.Println("Shutting down server...")

	s.stop()

	log.Println("Server stoped gracefully")
}

func (s *Server) stop() {

}
