package queue

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

// State é o estado de um lote no ciclo de vida da fila. Os valores espelham o
// enum BatchState do proto (mapeados em internal/server).
type State string

const (
	StateQueued     State = "QUEUED"
	StateDispatched State = "DISPATCHED"
	StateStarted    State = "STARTED"
	StateAckedOK    State = "ACKED_OK"
	StateAckedError State = "ACKED_ERROR"
	StateFailed     State = "FAILED"
)

// Item é um TComando do lote. Status é preenchido a partir de
// gLote_RoboN[Position].status quando o CLP conclui (0=pendente, 1=ok, 2=erro).
type Item struct {
	Position int32 `json:"position"` // 1..Qtd — índice base-1 em gLote_RoboN
	Dedo     int32 `json:"dedo"`
	Dedo2    int32 `json:"dedo_2"`
	Estande  int32 `json:"estande"`
	Bandeja  int32 `json:"bandeja"`
	Fileira  int32 `json:"fileira"`
	Coluna   int32 `json:"coluna"`
	Rack     int32 `json:"rack"`
	Placa    int32 `json:"placa"`
	Slot     int32 `json:"slot"`
	Status   int32 `json:"status"`
}

// Record é o registro durável de um lote. Persistido como JSON no bbolt,
// chaveado por (Robot, BatchID) — cada robô tem um slot independente no CLP.
type Record struct {
	BatchID    int64  `json:"batch_id"` // gLote_Id, fornecido pelo Nest (cabe em DINT)
	Robot      int32  `json:"robot"`    // 1..2 -> _Robo1/_Robo2
	Acao       int32  `json:"acao"`     // 1..5 — lote homogêneo
	Qtd        int32  `json:"qtd"`      // itens válidos (1..5)
	Items      []Item `json:"items"`
	State      State  `json:"state"`
	Detail     string `json:"detail"`
	Attempts   int    `json:"attempts"`
	Seq        uint64 `json:"seq"` // ordem FIFO de chegada
	EnqueuedAt int64  `json:"enqueued_at"`
	UpdatedAt  int64  `json:"updated_at"`
	Delivered  bool   `json:"delivered"` // status confirmado pelo Nest (ConfirmBatchStatus)
}

var (
	bucketBatches = []byte("batches")
	bucketMeta    = []byte("meta")
	keySeq        = []byte("seq")
)

// Store encapsula o bbolt. As transações do bbolt já garantem 1 escritor e N
// leitores concorrentes; Enqueue só retorna após o commit (fsync).
type Store struct {
	db *bolt.DB
}

func OpenStore(path string) (*Store, error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("open bbolt %s: %w", path, err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, e := tx.CreateBucketIfNotExists(bucketBatches); e != nil {
			return e
		}
		_, e := tx.CreateBucketIfNotExists(bucketMeta)
		return e
	})
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("init buckets: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// recKey chaveia o lote por (robot, batch_id): o id só é único dentro do robô.
func recKey(robot int32, batchID int64) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint32(b[:4], uint32(robot))
	binary.BigEndian.PutUint64(b[4:], uint64(batchID))
	return b
}

// Enqueue persiste um novo lote como QUEUED, atribuindo Seq e timestamps.
// Idempotente por (robot, batch_id): se já existir, devolve o registro existente
// com isNew=false.
func (s *Store) Enqueue(rec Record) (Record, bool, error) {
	isNew := true
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketBatches)
		if existing := b.Get(recKey(rec.Robot, rec.BatchID)); existing != nil {
			isNew = false
			return json.Unmarshal(existing, &rec)
		}
		seq, err := nextSeq(tx)
		if err != nil {
			return err
		}
		now := time.Now().UnixMilli()
		rec.Seq = seq
		rec.State = StateQueued
		rec.Attempts = 0
		rec.Delivered = false
		rec.EnqueuedAt = now
		rec.UpdatedAt = now
		return putRec(b, rec)
	})
	return rec, isNew, err
}

// Update persiste o estado atual de um registro (atualiza UpdatedAt).
func (s *Store) Update(rec Record) error {
	rec.UpdatedAt = time.Now().UnixMilli()
	return s.db.Update(func(tx *bolt.Tx) error {
		return putRec(tx.Bucket(bucketBatches), rec)
	})
}

func (s *Store) Get(robot int32, batchID int64) (Record, bool, error) {
	var rec Record
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketBatches).Get(recKey(robot, batchID))
		if raw == nil {
			return nil
		}
		found = true
		return json.Unmarshal(raw, &rec)
	})
	return rec, found, err
}

// NextQueued retorna o lote QUEUED de menor Seq (FIFO) do robô — cada dispatcher
// só consome a sua própria fila.
func (s *Store) NextQueued(robot int32) (Record, bool, error) {
	var best Record
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketBatches).ForEach(func(_, v []byte) error {
			var rec Record
			if e := json.Unmarshal(v, &rec); e != nil {
				return e
			}
			if rec.State != StateQueued || rec.Robot != robot {
				return nil
			}
			if !found || rec.Seq < best.Seq {
				best, found = rec, true
			}
			return nil
		})
	})
	return best, found, err
}

// ListInFlight retorna os lotes do robô que ficaram em voo (DISPATCHED/STARTED)
// por um shutdown/crash, ordenados por Seq — entrada do Recover.
func (s *Store) ListInFlight(robot int32) ([]Record, error) {
	return s.filter(func(r Record) bool {
		return r.Robot == robot && (r.State == StateDispatched || r.State == StateStarted)
	})
}

// ListByState retorna todos os registros em um estado, ordenados por Seq.
func (s *Store) ListByState(state State) ([]Record, error) {
	return s.filter(func(r Record) bool { return r.State == state })
}

// ListUndelivered retorna os registros cujo status ainda não foi confirmado
// pelo Nest (Delivered=false), ordenados por Seq — usado no replay do stream.
func (s *Store) ListUndelivered() ([]Record, error) {
	return s.filter(func(r Record) bool { return !r.Delivered })
}

func (s *Store) filter(keep func(Record) bool) ([]Record, error) {
	var out []Record
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketBatches).ForEach(func(_, v []byte) error {
			var rec Record
			if e := json.Unmarshal(v, &rec); e != nil {
				return e
			}
			if keep(rec) {
				out = append(out, rec)
			}
			return nil
		})
	})
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out, err
}

// SetDelivered marca o status de um lote como entregue ao Nest.
func (s *Store) SetDelivered(robot int32, batchID int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketBatches)
		raw := b.Get(recKey(robot, batchID))
		if raw == nil {
			return nil
		}
		var rec Record
		if e := json.Unmarshal(raw, &rec); e != nil {
			return e
		}
		rec.Delivered = true
		rec.UpdatedAt = time.Now().UnixMilli()
		return putRec(b, rec)
	})
}

func putRec(b *bolt.Bucket, rec Record) error {
	raw, err := json.Marshal(rec)
	if err != nil {
		return err
	}
	return b.Put(recKey(rec.Robot, rec.BatchID), raw)
}

func nextSeq(tx *bolt.Tx) (uint64, error) {
	m := tx.Bucket(bucketMeta)
	var cur uint64
	if raw := m.Get(keySeq); raw != nil {
		cur = binary.BigEndian.Uint64(raw)
	}
	cur++
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, cur)
	return cur, m.Put(keySeq, b)
}
