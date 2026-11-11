// Command controlplane boots the HTTP API + message consumer + outbox relay as one
// process (the brief's "one deployable unit"). Right now (M1) it only proves the
// process starts cleanly inside Docker and shuts down gracefully - the store,
// service, httpapi, messaging, and outbox packages are wired in here as they're
// built (M2-M3), not before, so this file never carries dead imports.
package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	addr := os.Getenv("HTTP_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	// No routes yet - every request 404s. That's fine for M1: the only thing being
	// proven here is that the binary starts, binds a port, and shuts down cleanly
	// under SIGTERM (which `docker compose down` sends). Real routes arrive in M2.
	srv := &http.Server{
		Addr:              addr,
		Handler:           http.NewServeMux(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Pattern: graceful shutdown via signal.NotifyContext. Why: docker compose
	// sends SIGTERM on `down`/`stop`; without catching it, the process is killed
	// after Docker's default 10s grace period rather than exiting promptly, and
	// in-flight requests/transactions (once they exist, M2+) would be cut off
	// mid-way instead of finishing.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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
