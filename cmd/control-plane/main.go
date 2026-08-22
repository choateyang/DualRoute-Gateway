package main

import (
	"dualroute-gateway/internal/controlplane"
	"dualroute-gateway/internal/version"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"time"
)

func main() {
	cfg, err := controlplane.LoadConfig()
	if err != nil {
		slog.Error("invalid config", "error", err)
		os.Exit(1)
	}
	server := controlplane.New(cfg)
	var reconcileErr error
	for attempt := 0; attempt < 20; attempt++ {
		reconcileErr = server.ReconcileInstances()
		if reconcileErr == nil {
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if reconcileErr != nil {
		slog.Error("initial instance discovery failed", "error", reconcileErr)
		os.Exit(1)
	}
	go func() {
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := server.ReconcileInstances(); err != nil {
				slog.Warn("instance route refresh failed", "error", err)
			}
			if err := server.ReconcileEgresses(); err != nil {
				slog.Warn("instance egress reconciliation failed", "error", err)
			}
		}
	}()
	controlServer := &http.Server{Addr: cfg.ListenAddr, Handler: server.Handler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
	apiServer := &http.Server{Addr: cfg.APIListenAddr, Handler: server.APIHandler(), ReadHeaderTimeout: 10 * time.Second, IdleTimeout: 2 * time.Minute}
	errs := make(chan error, 2)
	go func() { errs <- controlServer.ListenAndServe() }()
	go func() { errs <- apiServer.ListenAndServe() }()
	slog.Info("control plane listening", "version", version.Number(), "addr", cfg.ListenAddr, "api_addr", cfg.APIListenAddr, "instances", len(cfg.Instances))
	if err := <-errs; err != nil && !errors.Is(err, http.ErrServerClosed) {
		slog.Error("control plane stopped", "error", err)
		os.Exit(1)
	}
}
