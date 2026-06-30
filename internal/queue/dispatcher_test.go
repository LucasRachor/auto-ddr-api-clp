package queue

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"go_auto_ddr_clp/internal/plc"
)

// fakePLC simula o handshake do CLP: ao ver gCmd_Req=TRUE, responde com o ack
// correlacionado pelo id, conforme o modo configurado.
type fakePLC struct {
	mu   sync.Mutex
	tags map[string]plc.TagValue
	mode string // "ok" | "error" | "timeout"
}

func newFakePLC(mode string) *fakePLC {
	return &fakePLC{tags: make(map[string]plc.TagValue), mode: mode}
}

func (f *fakePLC) Connect(context.Context) error { return nil }
func (f *fakePLC) Close() error                  { return nil }
func (f *fakePLC) Start(context.Context) error   { return nil }
func (f *fakePLC) Stop(context.Context) error    { return nil }
func (f *fakePLC) Reset(context.Context) error   { return nil }
func (f *fakePLC) Status(context.Context) (plc.Status, error) {
	return plc.Status{}, nil
}

func (f *fakePLC) ReadTag(_ context.Context, tag string) (plc.TagValue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.tags[tag], nil
}

func (f *fakePLC) WriteTag(_ context.Context, tag string, v plc.TagValue) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tags[tag] = v
	if tag == plc.TagCmdReq && v.Bool {
		if f.mode == "timeout" {
			return nil // nunca confirma
		}
		status := int32(ackStatusOK)
		detail := ""
		if f.mode == "error" {
			status = ackStatusError
			detail = "falha simulada"
		}
		f.tags[plc.TagCmdAckId] = f.tags[plc.TagCmdId]
		f.tags[plc.TagCmdAckStatus] = plc.TagValue{Kind: plc.KindInt, Int: status}
		f.tags[plc.TagCmdAckDetail] = plc.TagValue{Kind: plc.KindString, String: detail}
		f.tags[plc.TagCmdAck] = plc.TagValue{Kind: plc.KindBool, Bool: true}
	}
	if tag == plc.TagCmdReq && !v.Bool {
		f.tags[plc.TagCmdAck] = plc.TagValue{Kind: plc.KindBool, Bool: false}
	}
	return nil
}

func newTestQueue(t *testing.T, client plc.Client, cfg Config) *Queue {
	t.Helper()
	path := filepath.Join(t.TempDir(), "queue.db")
	store, err := OpenStore(path)
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	return New(store, client, log, cfg)
}

func waitState(t *testing.T, q *Queue, id int64, want State) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec, ok, err := q.store.Get(id)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if ok && rec.State == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	rec, _, _ := q.store.Get(id)
	t.Fatalf("comando %d: estado %q, esperava %q", id, rec.State, want)
}

func TestDispatch_AckOK(t *testing.T) {
	q := newTestQueue(t, newFakePLC("ok"), Config{AckTimeout: time.Second, PollEvery: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.Run(ctx)

	if _, err := q.Enqueue(101, "insert", "MB09", 1, 1); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitState(t, q, 101, StateAckedOK)
}

func TestDispatch_AckError(t *testing.T) {
	q := newTestQueue(t, newFakePLC("error"), Config{AckTimeout: time.Second, PollEvery: 5 * time.Millisecond})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.Run(ctx)

	if _, err := q.Enqueue(102, "insert", "MB09", 2, 5); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitState(t, q, 102, StateAckedError)
	rec, _, _ := q.store.Get(102)
	if rec.Detail != "falha simulada" {
		t.Fatalf("detail = %q, esperava 'falha simulada'", rec.Detail)
	}
}

func TestDispatch_TimeoutThenFailed(t *testing.T) {
	q := newTestQueue(t, newFakePLC("timeout"), Config{AckTimeout: 60 * time.Millisecond, PollEvery: 5 * time.Millisecond, MaxRetries: 1})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go q.Run(ctx)

	if _, err := q.Enqueue(103, "insert", "MB09", 1, 1); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitState(t, q, 103, StateFailed)
}

func TestEnqueue_Validation(t *testing.T) {
	q := newTestQueue(t, newFakePLC("ok"), Config{})
	cases := []struct {
		name          string
		id            int64
		action, mb    string
		robot, finger int32
	}{
		{"id zero", 0, "insert", "MB09", 1, 1},
		{"acao invalida", 1, "remove", "MB09", 1, 1},
		{"mb vazio", 1, "insert", "", 1, 1},
		{"robot fora", 1, "insert", "MB09", 3, 1},
		{"finger fora", 1, "insert", "MB09", 1, 6},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := q.Enqueue(c.id, c.action, c.mb, c.robot, c.finger); err == nil {
				t.Fatalf("esperava erro de validação")
			}
		})
	}
}

func TestEnqueue_Idempotent(t *testing.T) {
	q := newTestQueue(t, newFakePLC("ok"), Config{AckTimeout: time.Second, PollEvery: 5 * time.Millisecond})
	if _, err := q.Enqueue(200, "insert", "MB09", 1, 1); err != nil {
		t.Fatalf("Enqueue 1: %v", err)
	}
	// Reenfileirar o mesmo id não cria registro novo.
	rec, err := q.Enqueue(200, "insert", "MB09", 1, 1)
	if err != nil {
		t.Fatalf("Enqueue 2: %v", err)
	}
	if rec.ID != 200 {
		t.Fatalf("id = %d", rec.ID)
	}
}

func TestRecover_FinalizesFromAck(t *testing.T) {
	fake := newFakePLC("ok")
	q := newTestQueue(t, fake, Config{AckTimeout: time.Second, PollEvery: 5 * time.Millisecond})

	// Simula crash: registro em DISPATCHED e ack presente no CLP.
	if _, _, err := q.store.Enqueue(Record{ID: 301, Action: "insert", Motherboard: "MB09", Robot: 1, Finger: 1}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rec, _, _ := q.store.Get(301)
	rec.State = StateDispatched
	if err := q.store.Update(rec); err != nil {
		t.Fatalf("update: %v", err)
	}
	fake.tags[plc.TagCmdAckId] = plc.TagValue{Kind: plc.KindDInt, DInt: 301}
	fake.tags[plc.TagCmdAckStatus] = plc.TagValue{Kind: plc.KindInt, Int: ackStatusOK}
	fake.tags[plc.TagCmdAck] = plc.TagValue{Kind: plc.KindBool, Bool: true}

	if err := q.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec, _, _ := q.store.Get(301); rec.State != StateAckedOK {
		t.Fatalf("estado = %q, esperava ACKED_OK", rec.State)
	}
}

func TestRecover_RequeuesWithoutAck(t *testing.T) {
	fake := newFakePLC("ok") // sem ack pré-setado
	q := newTestQueue(t, fake, Config{AckTimeout: time.Second, PollEvery: 5 * time.Millisecond})

	if _, _, err := q.store.Enqueue(Record{ID: 302, Action: "insert", Motherboard: "MB09", Robot: 1, Finger: 1}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rec, _, _ := q.store.Get(302)
	rec.State = StateDispatched
	q.store.Update(rec)

	if err := q.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec, _, _ := q.store.Get(302); rec.State != StateQueued {
		t.Fatalf("estado = %q, esperava QUEUED", rec.State)
	}
}
