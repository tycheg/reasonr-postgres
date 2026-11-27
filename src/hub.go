package postgres

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/tycheg/reasonr-engine/port"
)

// Hub is the link between server instances on LISTEN/NOTIFY
// (port.HubLink): each instance listens to a database channel and gets
// every message sent to it. A message larger than the NOTIFY limit is
// written to a temporary row, encrypted with a random key, and the channel
// gets the reference and the key: the table has no plain text of personal
// arguments (RFC §20). Each database schema has its own channel.
type Hub struct {
	pool *pgxpool.Pool
	log  *slog.Logger

	mu      sync.Mutex
	started bool
	deliver func([]byte)
	channel string
	cancel  context.CancelFunc
	done    chan struct{}
}

var _ port.HubLink = (*Hub)(nil)

// notifyLimit is the largest NOTIFY payload with a margin (8000 bytes).
const notifyLimit = 7900

// NewHub returns a link over a connection pool.
func NewHub(pool *pgxpool.Pool, log *slog.Logger) *Hub {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Hub{pool: pool, log: log}
}

// channelName is the channel of the pool's schema.
func (h *Hub) channelName(ctx context.Context) error {
	if h.channel != "" {
		return nil
	}
	var schema string
	if err := h.pool.QueryRow(ctx, "select current_schema()").Scan(&schema); err != nil {
		return err
	}
	h.channel = "reasonr_hub_" + schema
	if len(h.channel) > 63 {
		h.channel = h.channel[:63]
	}
	return nil
}

// Listen starts the channel listener and waits until LISTEN runs
// (port.HubLink).
func (h *Hub) Listen(ctx context.Context, deliver func(msg []byte)) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.started {
		return nil
	}
	if err := h.channelName(ctx); err != nil {
		return err
	}
	h.deliver = deliver
	lctx, cancel := context.WithCancel(context.Background())
	ready := make(chan error, 1)
	h.cancel, h.done = cancel, make(chan struct{})
	go h.listen(lctx, ready)
	if err := <-ready; err != nil {
		cancel()
		return err
	}
	h.started = true
	return nil
}

// listen listens to the channel; it reconnects when the connection breaks.
func (h *Hub) listen(ctx context.Context, ready chan<- error) {
	defer close(h.done)
	first := true
	for ctx.Err() == nil {
		err := h.session(ctx, func() {
			if first {
				first = false
				ready <- nil
			}
		})
		if first {
			ready <- err
			return
		}
		if ctx.Err() == nil {
			h.log.Warn("notification hub: the connection is lost, reconnecting", "error", err)
			select {
			case <-ctx.Done():
			case <-time.After(time.Second):
			}
		}
	}
}

func (h *Hub) session(ctx context.Context, listening func()) error {
	conn, err := h.pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	if _, err := conn.Exec(ctx, `listen "`+h.channel+`"`); err != nil {
		return err
	}
	listening()
	for {
		n, err := conn.Conn().WaitForNotification(ctx)
		if err != nil {
			return err
		}
		data, err := h.read(ctx, n.Payload)
		if err != nil {
			h.log.Error("notification hub: a message cannot be read", "error", err)
			continue
		}
		h.deliver(data)
	}
}

// read gives the message of a channel payload.
func (h *Hub) read(ctx context.Context, payload string) ([]byte, error) {
	switch {
	case strings.HasPrefix(payload, "r:"):
		ref, key, ok := strings.Cut(payload[2:], ":")
		id, err := strconv.ParseInt(ref, 10, 64)
		if err != nil || !ok {
			return nil, fmt.Errorf("bad reference %.20q", payload)
		}
		var sealed []byte
		if err := h.pool.QueryRow(ctx, "select payload from rt_hub_payloads where id = $1", id).Scan(&sealed); err != nil {
			return nil, err
		}
		return openPayload(key, sealed)
	case strings.HasPrefix(payload, "v:"):
		return base64.StdEncoding.DecodeString(payload[2:])
	}
	return nil, fmt.Errorf("unknown message %.20q", payload)
}

// Send sends a message to the channel (port.HubLink).
func (h *Hub) Send(ctx context.Context, data []byte) error {
	h.mu.Lock()
	err := h.channelName(ctx)
	h.mu.Unlock()
	if err != nil {
		return err
	}
	payload := "v:" + base64.StdEncoding.EncodeToString(data)
	if len(payload) > notifyLimit {
		key, sealed, err := sealPayload(data)
		if err != nil {
			return err
		}
		var id int64
		if err := h.pool.QueryRow(ctx, "insert into rt_hub_payloads (payload) values ($1) returning id", sealed).Scan(&id); err != nil {
			return err
		}
		payload = "r:" + strconv.FormatInt(id, 10) + ":" + key
		_, _ = h.pool.Exec(ctx, "delete from rt_hub_payloads where created_at < now() - interval '5 minutes'")
	}
	_, err = h.pool.Exec(ctx, "select pg_notify($1, $2)", h.channel, payload)
	return err
}

// sealPayload encrypts a notification for rt_hub_payloads with a random
// AES-256-GCM key; the key (base64) goes only to the NOTIFY channel.
func sealPayload(data []byte) (key string, sealed []byte, err error) {
	k := make([]byte, 32)
	if _, err := rand.Read(k); err != nil {
		return "", nil, err
	}
	aead, err := newGCM(k)
	if err != nil {
		return "", nil, err
	}
	nonce := make([]byte, aead.NonceSize(), aead.NonceSize()+len(data)+aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		return "", nil, err
	}
	return base64.RawStdEncoding.EncodeToString(k), aead.Seal(nonce, nonce, data, nil), nil
}

// openPayload decrypts a notification from rt_hub_payloads.
func openPayload(key string, sealed []byte) ([]byte, error) {
	k, err := base64.RawStdEncoding.DecodeString(key)
	if err != nil {
		return nil, err
	}
	aead, err := newGCM(k)
	if err != nil {
		return nil, err
	}
	if len(sealed) < aead.NonceSize() {
		return nil, errPayload
	}
	return aead.Open(nil, sealed[:aead.NonceSize()], sealed[aead.NonceSize():], nil)
}

var errPayload = errors.New("postgres: a corrupt hub payload")

func newGCM(key []byte) (cipher.AEAD, error) {
	b, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(b)
}

// Close stops the listener.
func (h *Hub) Close() error {
	h.mu.Lock()
	started := h.started
	h.started = false
	h.mu.Unlock()
	if started {
		h.cancel()
		<-h.done
	}
	return nil
}
