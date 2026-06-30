package queue

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	bolt "go.etcd.io/bbolt"
)

// State é o estado de um comando no ciclo de vida da fila. Os valores espelham
// o enum CommandState do proto (mapeados em internal/server).
type State string

const (
	StateQueued     State = "QUEUED"
	StateDispatched State = "DISPATCHED"
	StateAckedOK    State = "ACKED_OK"
	StateAckedError State = "ACKED_ERROR"
	StateFailed     State = "FAILED"
)

// Record é o registro durável de um comando. Persistido como JSON no bbolt,
// chaveado pelo id (DINT) enviado pelo Nest.
type Record struct {
	ID          int64  `json:"id"`
	Action      string `json:"action"`
	Motherboard string `json:"motherboard"`
	Robot       int32  `json:"robot"`
	Finger      int32  `json:"finger"`
	State       State  `json:"state"`
	Detail      string `json:"detail"`
	Attempts    int    `json:"attempts"`
	Seq         uint64 `json:"seq"` // ordem FIFO de chegada
	EnqueuedAt  int64  `json:"enqueued_at"`
	UpdatedAt   int64  `json:"updated_at"`
	Delivered   bool   `json:"delivered"` // status confirmado pelo Nest (ConfirmStatus)
}

var (
	bucketCommands = []byte("commands")
	bucketMeta     = []byte("meta")
	keySeq         = []byte("seq")
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
		if _, e := tx.CreateBucketIfNotExists(bucketCommands); e != nil {
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

func itob(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// Enqueue persiste um novo comando como QUEUED, atribuindo Seq e timestamps.
// Idempotente por id: se já existir, retorna o registro existente com isNew=false.
func (s *Store) Enqueue(rec Record) (Record, bool, error) {
	isNew := true
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketCommands)
		if existing := b.Get(itob(rec.ID)); existing != nil {
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
		return putRec(tx.Bucket(bucketCommands), rec)
	})
}

func (s *Store) Get(id int64) (Record, bool, error) {
	var rec Record
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		raw := tx.Bucket(bucketCommands).Get(itob(id))
		if raw == nil {
			return nil
		}
		found = true
		return json.Unmarshal(raw, &rec)
	})
	return rec, found, err
}

// NextQueued retorna o comando QUEUED de menor Seq (FIFO).
func (s *Store) NextQueued() (Record, bool, error) {
	var best Record
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketCommands).ForEach(func(_, v []byte) error {
			var rec Record
			if e := json.Unmarshal(v, &rec); e != nil {
				return e
			}
			if rec.State != StateQueued {
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
		return tx.Bucket(bucketCommands).ForEach(func(_, v []byte) error {
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

// SetDelivered marca o status de um comando como entregue ao Nest.
func (s *Store) SetDelivered(id int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketCommands)
		raw := b.Get(itob(id))
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
	return b.Put(itob(rec.ID), raw)
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
