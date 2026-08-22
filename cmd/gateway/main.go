package main

import (
	"log/slog"
	"net/http"
	"dualroute-gateway/internal/gateway"
	"dualroute-gateway/internal/version"
	"os"
	"time"
)

func main() {
	cfg, err := gateway.LoadConfig()
	if err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}
	g, err := gateway.New(cfg, slog.Default())
	if err != nil {
		slog.Error("init failed", "error", err)
		os.Exit(1)
	}
	srv := &http.Server{Addr: cfg.ListenAddr, Handler: g.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
	slog.Info("gateway listening", "version", version.Number(), "addr", cfg.ListenAddr, "upstream", cfg.UpstreamURL, "concurrency", cfg.MaxConcurrency)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
