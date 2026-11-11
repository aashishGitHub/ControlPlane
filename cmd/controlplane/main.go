// Command controlplane boots the HTTP API + message consumer + outbox relay as one
// process (the brief's "one deployable unit").
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
	"github.com/aashishGithub/controlplane/internal/messaging"
	"github.com/aashishGithub/controlplane/internal/outbox"
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

	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}
	mc, err := messaging.NewConnManager(messaging.Config{
		URL:               amqpURL,
		ReconnectInterval: 500 * time.Millisecond,
		Log:               logger,
	})
	if err != nil {
		logger.Error("messaging init failed", "error", err)
		os.Exit(1)
	}
	defer mc.Close()

	relay := outbox.NewRelay(pool, mc, logger)
	svc := service.New(pool, service.WithNotifier(relay.Notify))

	go relay.Run(ctx)
	go messaging.ConsumeUpdates(ctx, mc, logger, func(ctx context.Context, update messaging.TaskUpdate) error {
		_, err := svc.ApplyTaskUpdate(ctx, service.TaskUpdate{
			MessageID: update.MessageID,
			TaskID:    update.TaskID,
			Status:    update.Status,
			Reason:    update.Reason,
		})
		return err
	})

	srv := &http.Server{
		Addr:              addr,
		Handler:           httpapi.New(svc, pool, mc),
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
