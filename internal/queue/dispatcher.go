package queue

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go_auto_ddr_clp/internal/plc"
)

// Códigos de gCmd_AckStatus retornados pelo CLP.
const (
	ackStatusOK    = 1
	ackStatusError = 2
)

var errAckTimeout = errors.New("timeout aguardando ack do CLP")

// Run é o loop do dispatcher (goroutine única). Consome a fila em ordem FIFO,
// executando o handshake com o CLP para um comando de cada vez. Bloqueia até
// ctx ser cancelado.
func (q *Queue) Run(ctx context.Context) {
	q.log.Info("dispatcher started", "ack_timeout", q.cfg.AckTimeout,
		"max_retries", q.cfg.MaxRetries, "poll", q.cfg.PollEvery)
	for {
		rec, ok, err := q.store.NextQueued()
		if err != nil {
			q.log.Error("dispatcher: NextQueued", "err", err)
			if !q.sleep(ctx, time.Second) {
				return
			}
			continue
		}
		if !ok {
			// Fila vazia: aguarda sinal de Enqueue ou cancelamento.
			select {
			case <-ctx.Done():
				return
			case <-q.wake:
			}
			continue
		}
		q.process(ctx, rec)
		if ctx.Err() != nil {
			return
		}
	}
}

// process executa um comando com retries até obter ack ou esgotar tentativas.
func (q *Queue) process(ctx context.Context, rec Record) {
	q.transition(rec.ID, StateDispatched, "")

	for {
		status, detail, err := q.dispatch(ctx, rec)
		if err == nil {
			if status == ackStatusOK {
				q.transition(rec.ID, StateAckedOK, detail)
			} else {
				if detail == "" {
					detail = fmt.Sprintf("ack status %d", status)
				}
				q.transition(rec.ID, StateAckedError, detail)
			}
			return
		}
		if ctx.Err() != nil {
			return // shutdown: comando permanece DISPATCHED e será reconciliado no boot
		}

		rec.Attempts++
		q.log.Warn("dispatch failed", "id", rec.ID, "attempt", rec.Attempts, "err", err)
		if rec.Attempts > q.cfg.MaxRetries {
			q.transition(rec.ID, StateFailed, fmt.Sprintf("%v (após %d tentativas)", err, rec.Attempts))
			return
		}
		q.persistAttempts(rec)
		if !q.sleep(ctx, q.backoff(rec.Attempts)) {
			return
		}
	}
}

// dispatch executa um ciclo de handshake de 4 vias com o CLP.
func (q *Queue) dispatch(ctx context.Context, rec Record) (status int64, detail string, err error) {
	code, ok := actionCode(rec.Action)
	if !ok {
		return 0, "", fmt.Errorf("action %q sem código", rec.Action)
	}

	// 1. Garante slot ocioso (req=false e ack=false).
	if err := q.ensureIdle(ctx); err != nil {
		return 0, "", err
	}

	// 2. Escreve os campos do comando.
	writes := []struct {
		tag string
		v   plc.TagValue
	}{
		{plc.TagCmdId, plc.TagValue{Kind: plc.KindDInt, DInt: rec.ID}},
		{plc.TagCmdAction, plc.TagValue{Kind: plc.KindInt, Int: code}},
		{plc.TagCmdMotherboard, plc.TagValue{Kind: plc.KindString, String: rec.Motherboard}},
		{plc.TagCmdRobot, plc.TagValue{Kind: plc.KindInt, Int: rec.Robot}},
		{plc.TagCmdFinger, plc.TagValue{Kind: plc.KindInt, Int: rec.Finger}},
	}
	for _, w := range writes {
		if err := q.plc.WriteTag(ctx, w.tag, w.v); err != nil {
			return 0, "", fmt.Errorf("write %s: %w", w.tag, err)
		}
	}

	// 3. Sinaliza comando presente.
	if err := q.writeBool(ctx, plc.TagCmdReq, true); err != nil {
		return 0, "", err
	}

	// 4. Aguarda ack correlacionado pelo id, com timeout.
	status, detail, err = q.waitAck(ctx, rec.ID)
	if err != nil {
		return 0, "", err
	}

	// 5. Confirma recebimento (req=false) e 6. aguarda CLP limpar o ack.
	if err := q.writeBool(ctx, plc.TagCmdReq, false); err != nil {
		return 0, "", err
	}
	q.waitAckClear(ctx) // best-effort; falha aqui não invalida o ack já lido
	return status, detail, nil
}

// ensureIdle zera gCmd_Req e espera gCmd_Ack=false (CLP pronto para novo comando).
func (q *Queue) ensureIdle(ctx context.Context) error {
	if err := q.writeBool(ctx, plc.TagCmdReq, false); err != nil {
		return err
	}
	if !q.waitAckClear(ctx) {
		return fmt.Errorf("CLP não liberou o slot (ack preso em TRUE): %w", errAckTimeout)
	}
	return nil
}

// waitAck faz poll de gCmd_Ack até TRUE com gCmd_AckId == id, ou timeout.
func (q *Queue) waitAck(ctx context.Context, id int64) (int64, string, error) {
	deadline := time.Now().Add(q.cfg.AckTimeout)
	for {
		ack, err := q.readBool(ctx, plc.TagCmdAck)
		if err != nil {
			return 0, "", err
		}
		if ack {
			ackID, err := q.readInt(ctx, plc.TagCmdAckId)
			if err != nil {
				return 0, "", err
			}
			if ackID == id {
				status, err := q.readInt(ctx, plc.TagCmdAckStatus)
				if err != nil {
					return 0, "", err
				}
				detail, err := q.readString(ctx, plc.TagCmdAckDetail)
				if err != nil {
					return 0, "", err
				}
				return status, detail, nil
			}
		}
		if time.Now().After(deadline) {
			return 0, "", errAckTimeout
		}
		if !q.sleep(ctx, q.cfg.PollEvery) {
			return 0, "", ctx.Err()
		}
	}
}

// waitAckClear espera gCmd_Ack voltar a FALSE. Retorna false em timeout/cancelamento.
func (q *Queue) waitAckClear(ctx context.Context) bool {
	deadline := time.Now().Add(q.cfg.AckTimeout)
	for {
		ack, err := q.readBool(ctx, plc.TagCmdAck)
		if err == nil && !ack {
			return true
		}
		if time.Now().After(deadline) || ctx.Err() != nil {
			return false
		}
		if !q.sleep(ctx, q.cfg.PollEvery) {
			return false
		}
	}
}

// Recover reconcilia comandos deixados em DISPATCHED por um shutdown/crash.
// Se o CLP ainda exibe o ack correspondente, finaliza a partir dele; caso
// contrário re-enfileira (depende da idempotência do ladder por id).
func (q *Queue) Recover(ctx context.Context) error {
	pending, err := q.store.ListByState(StateDispatched)
	if err != nil {
		return err
	}
	for _, rec := range pending {
		ack, err := q.readBool(ctx, plc.TagCmdAck)
		if err == nil && ack {
			if ackID, e := q.readInt(ctx, plc.TagCmdAckId); e == nil && ackID == rec.ID {
				status, _ := q.readInt(ctx, plc.TagCmdAckStatus)
				detail, _ := q.readString(ctx, plc.TagCmdAckDetail)
				if status == ackStatusOK {
					q.transition(rec.ID, StateAckedOK, detail)
				} else {
					q.transition(rec.ID, StateAckedError, detail)
				}
				_ = q.writeBool(ctx, plc.TagCmdReq, false)
				continue
			}
		}
		q.log.Warn("recover: re-enfileirando comando em voo", "id", rec.ID)
		q.transition(rec.ID, StateQueued, "re-enfileirado após reinício")
	}
	q.signal()
	return nil
}

// --- helpers ---

func (q *Queue) transition(id int64, state State, detail string) {
	rec, ok, err := q.store.Get(id)
	if err != nil || !ok {
		q.log.Error("transition: registro não encontrado", "id", id, "err", err)
		return
	}
	rec.State = state
	rec.Detail = detail
	if err := q.store.Update(rec); err != nil {
		q.log.Error("transition: persist", "id", id, "state", state, "err", err)
		return
	}
	rec, _, _ = q.store.Get(id)
	q.broker.publish(rec)
	q.log.Info("command state", "id", id, "state", state, "detail", detail)
}

func (q *Queue) persistAttempts(rec Record) {
	cur, ok, err := q.store.Get(rec.ID)
	if err != nil || !ok {
		return
	}
	cur.Attempts = rec.Attempts
	_ = q.store.Update(cur)
}

func (q *Queue) backoff(attempt int) time.Duration {
	d := time.Duration(attempt) * 500 * time.Millisecond
	if d > 5*time.Second {
		d = 5 * time.Second
	}
	return d
}

// sleep dorme d respeitando o cancelamento. Retorna false se ctx foi cancelado.
func (q *Queue) sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

func (q *Queue) writeBool(ctx context.Context, tag string, val bool) error {
	if err := q.plc.WriteTag(ctx, tag, plc.TagValue{Kind: plc.KindBool, Bool: val}); err != nil {
		return fmt.Errorf("write %s: %w", tag, err)
	}
	return nil
}

func (q *Queue) readBool(ctx context.Context, tag string) (bool, error) {
	v, err := q.plc.ReadTag(ctx, tag)
	if err != nil {
		return false, fmt.Errorf("read %s: %w", tag, err)
	}
	return asInt64(v) != 0, nil
}

func (q *Queue) readInt(ctx context.Context, tag string) (int64, error) {
	v, err := q.plc.ReadTag(ctx, tag)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", tag, err)
	}
	return asInt64(v), nil
}

func (q *Queue) readString(ctx context.Context, tag string) (string, error) {
	v, err := q.plc.ReadTag(ctx, tag)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", tag, err)
	}
	return v.String, nil
}

// asInt64 extrai um inteiro de um TagValue independente do Kind (BOOL/INT/DINT).
func asInt64(v plc.TagValue) int64 {
	switch v.Kind {
	case plc.KindDInt:
		return v.DInt
	case plc.KindInt:
		return int64(v.Int)
	case plc.KindBool:
		if v.Bool {
			return 1
		}
	}
	return 0
}
