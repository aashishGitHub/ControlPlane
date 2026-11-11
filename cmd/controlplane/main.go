// Command controlplane boots the HTTP API + message consumer + outbox relay as one
// process (the brief's "one deployable unit"). As of M2 it boots the DB pool,
// service, and HTTP router; the messaging/outbox pieces arrive in M3.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aashishGithub/controlplane/internal/httpapi"
	"github.com/aashishGithub/controlplane/internal/service"
	"github.com/aashishGithub/controlplane/internal/store"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := store.Open(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		logger.Error("store open failed", "error", err)
		os.Exit(1)
	}
	defer pool.Close()

	svc := service.New(pool)
	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(svc, pool),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Pattern: graceful shutdown via signal.NotifyContext. Why: docker compose
	// sends SIGTERM on `down`/`stop`; without catching it, the process is killed
	// after Docker's default 10s grace period rather than exiting promptly, and
	// in-flight requests/transactions would be cut off mid-way instead of
	// finishing.
	go func() {
		logger.Info("controlplane listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	logger.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown error", "error", err)
	}
}
