// Command worker runs the worker simulator: it consumes task messages and
// publishes progress updates. Every behavior flag has an env-var default so the
// same image works in compose and under `make worker-restart FAIL_RATE=1`.
package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aashishGithub/controlplane/internal/messaging"
	"github.com/aashishGithub/controlplane/internal/worker"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	failRate := flag.Float64("fail-rate", envFloat("FAIL_RATE", 0), "fraction of tasks that end failed (0..1)")
	minDelayMs := flag.Int("min-delay-ms", envInt("MIN_DELAY_MS", 100), "minimum simulated processing delay in ms")
	maxDelayMs := flag.Int("max-delay-ms", envInt("MAX_DELAY_MS", 800), "maximum simulated processing delay in ms")
	duplicateRate := flag.Float64("duplicate-rate", envFloat("DUPLICATE_RATE", 0), "fraction of terminal updates published twice (0..1)")
	reorder := flag.Bool("reorder", envBool("REORDER", false), "publish terminal update before in_progress")
	flag.Parse()

	opts := worker.Options{
		FailRate:      *failRate,
		MinDelay:      time.Duration(*minDelayMs) * time.Millisecond,
		MaxDelay:      time.Duration(*maxDelayMs) * time.Millisecond,
		DuplicateRate: *duplicateRate,
		Reorder:       *reorder,
	}

	amqpURL := os.Getenv("AMQP_URL")
	if amqpURL == "" {
		amqpURL = "amqp://guest:guest@localhost:5672/"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	cm, err := messaging.NewConnManager(messaging.Config{
		URL:               amqpURL,
		ReconnectInterval: 500 * time.Millisecond,
		Log:               logger,
	})
	if err != nil {
		logger.Error("messaging init failed", "error", err)
		os.Exit(1)
	}
	defer cm.Close()

	w, err := worker.New(cm, logger, opts)
	if err != nil {
		logger.Error("invalid worker configuration", "error", err)
		os.Exit(1)
	}

	logger.Info("worker starting",
		"fail_rate", opts.FailRate,
		"min_delay_ms", opts.MinDelay.Milliseconds(),
		"max_delay_ms", opts.MaxDelay.Milliseconds(),
		"duplicate_rate", opts.DuplicateRate,
		"reorder", opts.Reorder,
	)
	w.Run(ctx)
	logger.Info("worker stopped")
}

func envFloat(name string, fallback float64) float64 {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return fallback
	}
	return value
}

func envInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil {
		return fallback
	}
	return value
}

func envBool(name string, fallback bool) bool {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.ParseBool(raw)
	if err != nil {
		return fallback
	}
	return value
}
