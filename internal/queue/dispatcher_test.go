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

// fakePLC simula o handshake de lote do CLP: ao ver gLote_Req_RoboN=TRUE, sobe o
// Start, grava o status de cada item conforme o modo e sobe o Ack.
type fakePLC struct {
	mu      sync.Mutex
	tags    map[string]plc.TagValue
	structs map[string][]byte
	mode    string // "ok" | "error" | "timeout" (nem Start nem Ack) | "no-ack" (só Start)
}

func newFakePLC(mode string) *fakePLC {
	return &fakePLC{
		tags:    make(map[string]plc.TagValue),
		structs: make(map[string][]byte),
		mode:    mode,
	}
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

func (f *fakePLC) WriteStructRaw(_ context.Context, name string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.structs[name] = append([]byte(nil), data...)
	return nil
}

func (f *fakePLC) WriteTag(_ context.Context, tag string, v plc.TagValue) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tags[tag] = v
	for _, robot := range plc.Robots {
		if tag != plc.TagLoteReq(robot) {
			continue
		}
		if !v.Bool {
			f.tags[plc.TagLoteStart(robot)] = plc.TagValue{Kind: plc.KindBool, Bool: false}
			f.tags[plc.TagLoteAck(robot)] = plc.TagValue{Kind: plc.KindBool, Bool: false}
			return nil
		}
		if f.mode == "timeout" {
			return nil // nunca sobe Start nem Ack
		}
		f.tags[plc.TagLoteStart(robot)] = plc.TagValue{Kind: plc.KindBool, Bool: true}
		if f.mode == "no-ack" {
			return nil // travou o lote mas nunca conclui: exercita o timeout do Ack
		}
		status := int32(plc.ItemStatusOK)
		if f.mode == "error" {
			status = plc.ItemStatusError
		}
		qtd := int(f.tags[plc.TagLoteQtd(robot)].Int)
		for i := 1; i <= qtd; i++ {
			f.tags[plc.TagLoteItemStatus(robot, i)] = plc.TagValue{Kind: plc.KindInt, Int: status}
		}
		f.tags[plc.TagLoteAck(robot)] = plc.TagValue{Kind: plc.KindBool, Bool: true}
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

// batch monta um lote válido de ação 1 (bandeja) com qtd itens.
func batch(batchID int64, robot int32, qtd int32) Record {
	items := make([]Item, 0, qtd)
	for i := int32(1); i <= qtd; i++ {
		items = append(items, Item{
			Position: i, Dedo: i, Estande: 1, Bandeja: 1, Fileira: 1, Coluna: i,
		})
	}
	return Record{BatchID: batchID, Robot: robot, Acao: 1, Qtd: qtd, Items: items}
}

func waitState(t *testing.T, q *Queue, robot int32, batchID int64, want State) Record {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		rec, ok, err := q.store.Get(robot, batchID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if ok && rec.State == want {
			return rec
		}
		time.Sleep(5 * time.Millisecond)
	}
	rec, _, _ := q.store.Get(robot, batchID)
	t.Fatalf("lote %d (robô %d): estado %q, esperava %q", batchID, robot, rec.State, want)
	return Record{}
}

func runAll(t *testing.T, q *Queue) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for _, robot := range plc.Robots {
		go q.Run(ctx, robot)
	}
}

func fastCfg() Config {
	return Config{AckTimeout: time.Second, PollEvery: 5 * time.Millisecond}
}

func TestDispatch_AckOK(t *testing.T) {
	q := newTestQueue(t, newFakePLC("ok"), fastCfg())
	runAll(t, q)

	if _, _, err := q.Enqueue(batch(101, 1, 3)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	rec := waitState(t, q, 1, 101, StateAckedOK)
	if len(rec.Items) != 3 {
		t.Fatalf("itens = %d, esperava 3", len(rec.Items))
	}
	for _, it := range rec.Items {
		if it.Status != plc.ItemStatusOK {
			t.Fatalf("position %d: status = %d, esperava OK", it.Position, it.Status)
		}
	}
}

// O lote inteiro tem de chegar ao CLP num frame só, com os 5 elementos (os itens
// além de Qtd zerados) e a ação replicada em cada item válido.
func TestDispatch_WritesFullBatchFrame(t *testing.T) {
	fake := newFakePLC("ok")
	q := newTestQueue(t, fake, fastCfg())
	runAll(t, q)

	if _, _, err := q.Enqueue(batch(110, 1, 2)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitState(t, q, 1, 110, StateAckedOK)

	fake.mu.Lock()
	data := fake.structs[plc.TagLote(1)]
	fake.mu.Unlock()

	want, err := plc.EncodeBatch([]plc.BatchItem{
		{Acao: 1, Dedo: 1, Estande: 1, Bandeja: 1, Fileira: 1, Coluna: 1},
		{Acao: 1, Dedo: 2, Estande: 1, Bandeja: 1, Fileira: 1, Coluna: 2},
	})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	if string(data) != string(want) {
		t.Fatalf("payload de %s:\n got %x\nwant %x", plc.TagLote(1), data, want)
	}
}

func TestDispatch_AckError(t *testing.T) {
	q := newTestQueue(t, newFakePLC("error"), fastCfg())
	runAll(t, q)

	if _, _, err := q.Enqueue(batch(102, 2, 2)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	rec := waitState(t, q, 2, 102, StateAckedError)
	if rec.Detail == "" {
		t.Fatal("esperava detail apontando os itens com falha")
	}
}

func TestDispatch_StartTimeoutThenFailed(t *testing.T) {
	q := newTestQueue(t, newFakePLC("timeout"), Config{
		AckTimeout: 60 * time.Millisecond, StartTimeout: 60 * time.Millisecond,
		PollEvery: 5 * time.Millisecond, MaxRetries: 1,
	})
	runAll(t, q)

	if _, _, err := q.Enqueue(batch(103, 1, 1)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitState(t, q, 1, 103, StateFailed)
}

// O CLP trava o lote (Start) mas nunca conclui: o timeout do Ack tem de levar a FAILED.
func TestDispatch_AckTimeoutAfterStart(t *testing.T) {
	q := newTestQueue(t, newFakePLC("no-ack"), Config{
		AckTimeout: 60 * time.Millisecond, StartTimeout: 60 * time.Millisecond,
		PollEvery: 5 * time.Millisecond, MaxRetries: 0,
	})
	runAll(t, q)

	if _, _, err := q.Enqueue(batch(104, 1, 1)); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	waitState(t, q, 1, 104, StateFailed)
}

// Os dois robôs têm slots independentes: um lote em cada roda em paralelo.
func TestDispatch_BothRobotsInParallel(t *testing.T) {
	q := newTestQueue(t, newFakePLC("ok"), fastCfg())
	runAll(t, q)

	if _, _, err := q.Enqueue(batch(201, 1, 2)); err != nil {
		t.Fatalf("Enqueue robô 1: %v", err)
	}
	if _, _, err := q.Enqueue(batch(201, 2, 3)); err != nil { // mesmo id, robô diferente
		t.Fatalf("Enqueue robô 2: %v", err)
	}
	waitState(t, q, 1, 201, StateAckedOK)
	waitState(t, q, 2, 201, StateAckedOK)
}

func TestEnqueue_Validation(t *testing.T) {
	q := newTestQueue(t, newFakePLC("ok"), Config{})
	base := batch(1, 1, 1)

	withItem := func(mut func(*Item)) Record {
		rec := batch(1, 1, 1)
		mut(&rec.Items[0])
		return rec
	}

	cases := []struct {
		name string
		rec  Record
	}{
		{"batch_id zero", Record{BatchID: 0, Robot: 1, Acao: 1, Qtd: 1, Items: base.Items}},
		{"robot fora", Record{BatchID: 1, Robot: 3, Acao: 1, Qtd: 1, Items: base.Items}},
		{"acao fora", Record{BatchID: 1, Robot: 1, Acao: 6, Qtd: 1, Items: base.Items}},
		{"qtd fora", Record{BatchID: 1, Robot: 1, Acao: 1, Qtd: 6, Items: base.Items}},
		{"qtd != len(items)", Record{BatchID: 1, Robot: 1, Acao: 1, Qtd: 2, Items: base.Items}},
		{"position duplicada", Record{BatchID: 1, Robot: 1, Acao: 1, Qtd: 2, Items: []Item{
			{Position: 1, Dedo: 1, Estande: 1, Bandeja: 1, Fileira: 1, Coluna: 1},
			{Position: 1, Dedo: 2, Estande: 1, Bandeja: 1, Fileira: 1, Coluna: 2},
		}}},
		{"dedo fora", withItem(func(it *Item) { it.Dedo = 6 })},
		{"coluna fora", withItem(func(it *Item) { it.Coluna = 26 })},
		{"campo de placa na acao de bandeja", withItem(func(it *Item) { it.Rack = 1 })},
		{"dedo_2 fora da acao 5", withItem(func(it *Item) { it.Dedo2 = 2 })},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, _, err := q.Enqueue(c.rec); err == nil {
				t.Fatal("esperava erro de validação")
			}
		})
	}
}

// A ação 5 usa os campos de placa e exige dedo_2; os de bandeja têm de vir zerados.
func TestEnqueue_AcaoPlaca(t *testing.T) {
	q := newTestQueue(t, newFakePLC("ok"), Config{})
	rec := Record{BatchID: 300, Robot: 1, Acao: 5, Qtd: 1, Items: []Item{
		{Position: 1, Dedo: 1, Dedo2: 2, Rack: 3, Placa: 4, Slot: 1},
	}}
	if _, _, err := q.Enqueue(rec); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}

	rec.BatchID = 301
	rec.Items[0].Dedo2 = 0 // dedo_2 é obrigatório na ação 5
	if _, _, err := q.Enqueue(rec); err == nil {
		t.Fatal("esperava erro por dedo_2 ausente na acao 5")
	}
}

func TestEnqueue_IdempotentPorRoboEId(t *testing.T) {
	q := newTestQueue(t, newFakePLC("ok"), fastCfg())
	_, isNew, err := q.Enqueue(batch(200, 1, 1))
	if err != nil {
		t.Fatalf("Enqueue 1: %v", err)
	}
	if !isNew {
		t.Fatal("isNew = false no primeiro enqueue, esperava true")
	}
	// Reenfileirar o mesmo (robot, batch_id) não cria registro novo.
	rec, isNew, err := q.Enqueue(batch(200, 1, 1))
	if err != nil {
		t.Fatalf("Enqueue 2: %v", err)
	}
	if rec.BatchID != 200 || rec.Seq != 1 {
		t.Fatalf("rec = %+v, esperava o registro original (seq 1)", rec)
	}
	// isNew=false é o único sinal de colisão de batch_id: sem ele o chamador
	// acha que despachou e o lote fica parado para sempre.
	if isNew {
		t.Fatal("isNew = true ao reenfileirar (robot, batch_id) existente, esperava false")
	}
}

func TestRecover_FinalizesFromAck(t *testing.T) {
	fake := newFakePLC("ok")
	q := newTestQueue(t, fake, fastCfg())

	// Simula crash: lote em voo e ack já presente no CLP.
	seed := batch(301, 1, 2)
	if _, _, err := q.store.Enqueue(seed); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rec, _, _ := q.store.Get(1, 301)
	rec.State = StateStarted
	if err := q.store.Update(rec); err != nil {
		t.Fatalf("update: %v", err)
	}
	fake.tags[plc.TagLoteId(1)] = plc.TagValue{Kind: plc.KindDInt, DInt: 301}
	fake.tags[plc.TagLoteAck(1)] = plc.TagValue{Kind: plc.KindBool, Bool: true}
	for i := 1; i <= 2; i++ {
		fake.tags[plc.TagLoteItemStatus(1, i)] = plc.TagValue{Kind: plc.KindInt, Int: plc.ItemStatusOK}
	}

	if err := q.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec, _, _ := q.store.Get(1, 301); rec.State != StateAckedOK {
		t.Fatalf("estado = %q, esperava ACKED_OK", rec.State)
	}
}

// Ack de OUTRO lote (id não bate) não pode finalizar este: re-enfileira.
func TestRecover_RequeuesOnIdMismatch(t *testing.T) {
	fake := newFakePLC("ok")
	q := newTestQueue(t, fake, fastCfg())

	if _, _, err := q.store.Enqueue(batch(302, 1, 1)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rec, _, _ := q.store.Get(1, 302)
	rec.State = StateDispatched
	q.store.Update(rec)
	fake.tags[plc.TagLoteId(1)] = plc.TagValue{Kind: plc.KindDInt, DInt: 999}
	fake.tags[plc.TagLoteAck(1)] = plc.TagValue{Kind: plc.KindBool, Bool: true}

	if err := q.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec, _, _ := q.store.Get(1, 302); rec.State != StateQueued {
		t.Fatalf("estado = %q, esperava QUEUED", rec.State)
	}
}

func TestRecover_RequeuesWithoutAck(t *testing.T) {
	q := newTestQueue(t, newFakePLC("ok"), fastCfg()) // sem ack pré-setado

	if _, _, err := q.store.Enqueue(batch(303, 2, 1)); err != nil {
		t.Fatalf("seed: %v", err)
	}
	rec, _, _ := q.store.Get(2, 303)
	rec.State = StateDispatched
	q.store.Update(rec)

	if err := q.Recover(context.Background()); err != nil {
		t.Fatalf("Recover: %v", err)
	}
	if rec, _, _ := q.store.Get(2, 303); rec.State != StateQueued {
		t.Fatalf("estado = %q, esperava QUEUED", rec.State)
	}
}
