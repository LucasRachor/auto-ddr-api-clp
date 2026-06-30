package queue

import (
	"log/slog"
	"sync"
	"time"

	"go_auto_ddr_clp/internal/plc"
)

// Config controla o comportamento do dispatcher (vindo de internal/config).
type Config struct {
	AckTimeout time.Duration // tempo máximo aguardando o ack do CLP por tentativa
	MaxRetries int           // tentativas extras antes de marcar FAILED
	PollEvery  time.Duration // intervalo de poll das tags de ack
}

// Queue serializa comandos do Nest até o CLP, com persistência durável (bbolt)
// e confirmação por handshake de tags. Um único dispatcher consome a fila.
type Queue struct {
	store  *Store
	plc    plc.Client
	log    *slog.Logger
	cfg    Config
	broker *statusBroker
	wake   chan struct{}
}

func New(store *Store, client plc.Client, log *slog.Logger, cfg Config) *Queue {
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 5 * time.Second
	}
	if cfg.PollEvery <= 0 {
		cfg.PollEvery = 50 * time.Millisecond
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	return &Queue{
		store:  store,
		plc:    client,
		log:    log,
		cfg:    cfg,
		broker: newStatusBroker(),
		wake:   make(chan struct{}, 1),
	}
}

// Enqueue valida e persiste um novo comando como QUEUED, então acorda o
// dispatcher. Só retorna após o commit no bbolt (garante não-perda).
func (q *Queue) Enqueue(id int64, action, motherboard string, robot, finger int32) (Record, error) {
	if err := Validate(id, action, motherboard, robot, finger); err != nil {
		return Record{}, err
	}
	rec, isNew, err := q.store.Enqueue(Record{
		ID:          id,
		Action:      action,
		Motherboard: motherboard,
		Robot:       robot,
		Finger:      finger,
	})
	if err != nil {
		return Record{}, err
	}
	if isNew {
		q.broker.publish(rec)
		q.signal()
		q.log.Info("command enqueued", "id", rec.ID, "action", rec.Action,
			"motherboard", rec.Motherboard, "robot", rec.Robot, "finger", rec.Finger)
	}
	return rec, nil
}

// Subscribe registra um consumidor do fluxo de status. Retorna o canal e uma
// função de cancelamento. O replay inicial é responsabilidade do chamador
// (ver ListUndelivered).
func (q *Queue) Subscribe(buf int) (<-chan Record, func()) {
	return q.broker.subscribe(buf)
}

func (q *Queue) ListUndelivered() ([]Record, error) { return q.store.ListUndelivered() }

func (q *Queue) ConfirmDelivery(id int64) error { return q.store.SetDelivered(id) }

func (q *Queue) signal() {
	select {
	case q.wake <- struct{}{}:
	default:
	}
}

// statusBroker faz fan-out de transições de estado para os streams gRPC.
type statusBroker struct {
	mu   sync.RWMutex
	subs map[chan Record]struct{}
}

func newStatusBroker() *statusBroker {
	return &statusBroker{subs: make(map[chan Record]struct{})}
}

func (b *statusBroker) subscribe(buf int) (<-chan Record, func()) {
	ch := make(chan Record, buf)
	b.mu.Lock()
	b.subs[ch] = struct{}{}
	b.mu.Unlock()
	var once sync.Once
	cancel := func() {
		once.Do(func() {
			b.mu.Lock()
			delete(b.subs, ch)
			b.mu.Unlock()
			close(ch)
		})
	}
	return ch, cancel
}

func (b *statusBroker) publish(rec Record) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	for ch := range b.subs {
		select {
		case ch <- rec:
		default: // subscriber lento: o replay no reconnect garante a não-perda
		}
	}
}
