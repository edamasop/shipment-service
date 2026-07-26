package server

import (
	"context"
	"fmt"
	"net/http"
	"shipment-service/internal/config"
	"time"
)

type Server struct {
	server *http.Server
}

func NewServer(cfg *config.Config, handler http.Handler) (*Server, error) {
	return &Server{
		server: &http.Server{
			Addr:           fmt.Sprintf(":%s", cfg.Port),
			Handler:        handler,
			ReadTimeout:    30 * time.Second,
			WriteTimeout:   60 * time.Second,
			MaxHeaderBytes: 2 << 20, // 2MB
		},
	}, nil
}

func (s *Server) Run() error {
	return s.server.ListenAndServe()
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
