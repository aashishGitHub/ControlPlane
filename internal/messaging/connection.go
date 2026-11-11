package messaging

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultReconnectInterval = 500 * time.Millisecond
	maxReconnectInterval     = 30 * time.Second
)

// ErrNotConnected is returned by Channel while the manager is between
// connections. Callers treat it as transient; the reconnect loop is already
// running.
var ErrNotConnected = errors.New("messaging: not connected")

// Config holds the AMQP URL and reconnect policy.
type Config struct {
	URL               string
	ReconnectInterval time.Duration
	Log               *slog.Logger
}

// ConnManager owns one AMQP connection and re-establishes it after a drop.
//
// Pattern: connection supervisor / reconnect-with-backoff (DESIGN.md §6
// Resilience). Why: AMQP connections are expensive and channels die with their
// connection. A single manager watches NotifyClose, dials with exponential
// backoff + jitter, re-declares the idempotent topology, and hands out fresh
// channels. Crucially, a closed NotifyClose channel returns nil forever; the
// watcher therefore reacts to channel closure itself, not to a nil error value.
type ConnManager struct {
	cfg Config
	log *slog.Logger

	mu   sync.RWMutex
	conn *amqp.Connection

	done      chan struct{}
	closeOnce sync.Once
	wg        sync.WaitGroup
}

func NewConnManager(cfg Config) (*ConnManager, error) {
	if cfg.URL == "" {
		return nil, errors.New("messaging: Config.URL is required")
	}
	if cfg.ReconnectInterval <= 0 {
		cfg.ReconnectInterval = defaultReconnectInterval
	}
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	cm := &ConnManager{
		cfg:  cfg,
		log:  cfg.Log,
		done: make(chan struct{}),
	}
	if err := cm.connect(); err != nil {
		return nil, err
	}
	cm.wg.Add(1)
	go cm.run()
	return cm, nil
}

// connect dials, declares topology, and publishes the new connection to Channel
// callers. Topology declaration happens before the connection is visible, so no
// publisher can race ahead of the bindings.
func (cm *ConnManager) connect() error {
	conn, err := amqp.Dial(cm.cfg.URL)
	if err != nil {
		return fmt.Errorf("messaging: dial: %w", err)
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("messaging: open topology channel: %w", err)
	}
	if err := DeclareTopology(ch); err != nil {
		_ = ch.Close()
		_ = conn.Close()
		return fmt.Errorf("messaging: declare topology: %w", err)
	}
	_ = ch.Close()

	cm.mu.Lock()
	cm.conn = conn
	cm.mu.Unlock()
	return nil
}

// run is the single lifecycle goroutine: wait for the current connection to
// close, then reconnect with capped exponential backoff until success or Close.
func (cm *ConnManager) run() {
	defer cm.wg.Done()

	for {
		cm.mu.RLock()
		conn := cm.conn
		cm.mu.RUnlock()

		if conn == nil {
			delay := cm.cfg.ReconnectInterval
			for {
				select {
				case <-cm.done:
					return
				case <-time.After(withJitter(delay)):
				}
				if err := cm.connect(); err != nil {
					cm.log.Warn("messaging: reconnect failed", "error", err)
					delay = minDuration(delay*2, maxReconnectInterval)
					continue
				}
				cm.log.Info("messaging: reconnected")
				break
			}
			continue
		}

		closed := conn.NotifyClose(make(chan *amqp.Error, 1))
		select {
		case <-cm.done:
			return
		case <-closed:
			cm.mu.Lock()
			if cm.conn == conn {
				cm.conn = nil
			}
			cm.mu.Unlock()
		}
	}
}

// Channel returns a fresh channel on the current connection. Channels are not
// cached because they are invalidated by a connection drop.
func (cm *ConnManager) Channel() (*amqp.Channel, error) {
	cm.mu.RLock()
	conn := cm.conn
	cm.mu.RUnlock()
	if conn == nil || conn.IsClosed() {
		return nil, ErrNotConnected
	}
	return conn.Channel()
}

// Ping reports broker reachability for /healthz.
func (cm *ConnManager) Ping(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ch, err := cm.Channel()
	if err != nil {
		return err
	}
	return ch.Close()
}

func (cm *ConnManager) Close() error {
	cm.closeOnce.Do(func() { close(cm.done) })

	cm.mu.Lock()
	conn := cm.conn
	cm.conn = nil
	cm.mu.Unlock()

	var err error
	if conn != nil {
		err = conn.Close()
	}
	cm.wg.Wait()
	return err
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
