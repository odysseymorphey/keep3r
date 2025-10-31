package httpapi

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/chi/v5"
)

type Server struct {
	http  *http.Server
	stopC chan os.Signal
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
		Addr:    addr,
		Handler: r,
	}

	return &Server{
		http:  srv,
		stopC: make(chan os.Signal, 1),
	}
}

func (s *Server) Run() {
	signal.Notify(s.stopC, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		if err := s.http.ListenAndServe(); err != nil {
			log.Fatal(err)
		}

	}()

	<-s.stopC
	log.Println("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), 10 * time.Second)
	defer cancel()

	s.stop(ctx)

	log.Println("Server stoped gracefully")
}

func (s *Server) stop(ctx context.Context) {
	s.http.Shutdown(ctx)
}
