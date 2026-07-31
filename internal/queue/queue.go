package queue

import (
	"log/slog"
	"sync"
	"time"

	"go_auto_ddr_clp/internal/plc"
)

// Config controla o comportamento dos dispatchers (vindo de internal/config).
type Config struct {
	AckTimeout   time.Duration // tempo máximo aguardando o Ack do CLP por tentativa
	StartTimeout time.Duration // tempo máximo aguardando o Start (default: AckTimeout)
	MaxRetries   int           // tentativas extras antes de marcar FAILED
	PollEvery    time.Duration // intervalo de poll das tags do handshake
}

// Queue leva os lotes do Nest até o CLP, com persistência durável (bbolt) e
// confirmação por handshake de tags. Há um slot serial POR ROBÔ: um dispatcher
// por robô (ver Run), cada um consumindo só a sua fatia da fila.
type Queue struct {
	store  *Store
	plc    plc.Client
	log    *slog.Logger
	cfg    Config
	broker *statusBroker
	wake   map[int32]chan struct{}
}

func New(store *Store, client plc.Client, log *slog.Logger, cfg Config) *Queue {
	if cfg.AckTimeout <= 0 {
		cfg.AckTimeout = 5 * time.Second
	}
	if cfg.StartTimeout <= 0 {
		cfg.StartTimeout = cfg.AckTimeout
	}
	if cfg.PollEvery <= 0 {
		cfg.PollEvery = 50 * time.Millisecond
	}
	if cfg.MaxRetries < 0 {
		cfg.MaxRetries = 0
	}
	wake := make(map[int32]chan struct{}, len(plc.Robots))
	for _, robot := range plc.Robots {
		wake[robot] = make(chan struct{}, 1)
	}
	return &Queue{
		store:  store,
		plc:    client,
		log:    log,
		cfg:    cfg,
		broker: newStatusBroker(),
		wake:   wake,
	}
}

// Enqueue valida e persiste um novo lote como QUEUED, então acorda o dispatcher
// do robô. Só retorna após o commit no bbolt (garante não-perda).
//
// Idempotente por (robot, batch_id): reenviar o mesmo lote devolve o registro já
// existente com isNew=false, sem despachar de novo. Se o chamador considerava o
// lote novo, isNew=false é uma COLISÃO de batch_id — daí ele ser devolvido em vez
// de engolido aqui: sem esse sinal o lote fica parado sem erro.
func (q *Queue) Enqueue(req Record) (Record, bool, error) {
	if err := Validate(req); err != nil {
		return Record{}, false, err
	}
	rec, isNew, err := q.store.Enqueue(req)
	if err != nil {
		return Record{}, false, err
	}
	if isNew {
		q.broker.publish(rec)
		q.signal(rec.Robot)
		q.log.Info("batch enqueued", "batch_id", rec.BatchID, "robot", rec.Robot,
			"acao", rec.Acao, "qtd", rec.Qtd)
	} else {
		q.log.Warn("batch duplicado: não despachado (idempotência por robot+batch_id)",
			"batch_id", rec.BatchID, "robot", rec.Robot, "state", rec.State)
	}
	return rec, isNew, nil
}

// Subscribe registra um consumidor do fluxo de status. Retorna o canal e uma
// função de cancelamento. O replay inicial é responsabilidade do chamador
// (ver ListUndelivered).
func (q *Queue) Subscribe(buf int) (<-chan Record, func()) {
	return q.broker.subscribe(buf)
}

func (q *Queue) ListUndelivered() ([]Record, error) { return q.store.ListUndelivered() }

func (q *Queue) ConfirmDelivery(robot int32, batchID int64) error {
	return q.store.SetDelivered(robot, batchID)
}

func (q *Queue) signal(robot int32) {
	ch, ok := q.wake[robot]
	if !ok {
		return
	}
	select {
	case ch <- struct{}{}:
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
