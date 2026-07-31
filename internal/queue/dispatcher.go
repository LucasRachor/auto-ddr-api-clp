package queue

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go_auto_ddr_clp/internal/plc"
)

var errTimeout = errors.New("timeout aguardando o CLP")

// Run é o loop do dispatcher de UM robô. Os slots gLote_*_RoboN são independentes,
// então roda uma goroutine por robô (ver plc.Robots) e cada uma consome apenas os
// lotes do seu robô, em ordem FIFO, um por vez. Bloqueia até ctx ser cancelado.
func (q *Queue) Run(ctx context.Context, robot int32) {
	q.log.Info("dispatcher started", "robot", robot,
		"ack_timeout", q.cfg.AckTimeout, "start_timeout", q.cfg.StartTimeout,
		"max_retries", q.cfg.MaxRetries, "poll", q.cfg.PollEvery)
	for {
		rec, ok, err := q.store.NextQueued(robot)
		if err != nil {
			q.log.Error("dispatcher: NextQueued", "robot", robot, "err", err)
			if !q.sleep(ctx, time.Second) {
				return
			}
			continue
		}
		if !ok {
			// Fila do robô vazia: aguarda sinal de Enqueue ou cancelamento.
			select {
			case <-ctx.Done():
				return
			case <-q.wake[robot]:
			}
			continue
		}
		q.process(ctx, rec)
		if ctx.Err() != nil {
			return
		}
	}
}

// process executa um lote com retries até obter o ack ou esgotar as tentativas.
func (q *Queue) process(ctx context.Context, rec Record) {
	for {
		q.transition(rec, StateDispatched, nil, "")

		items, err := q.dispatch(ctx, rec)
		if err == nil {
			state, detail := classify(items)
			q.transition(rec, state, items, detail)
			return
		}
		if ctx.Err() != nil {
			return // shutdown: o lote fica em voo e será reconciliado no boot (Recover)
		}

		rec.Attempts++
		q.log.Warn("dispatch failed", "batch_id", rec.BatchID, "robot", rec.Robot,
			"attempt", rec.Attempts, "err", err)
		if rec.Attempts > q.cfg.MaxRetries {
			q.transition(rec, StateFailed, nil, fmt.Sprintf("%v (após %d tentativas)", err, rec.Attempts))
			return
		}
		q.persistAttempts(rec)
		if !q.sleep(ctx, q.backoff(rec.Attempts)) {
			return
		}
	}
}

// dispatch executa um ciclo do handshake de lote (docs/mudancas-batch.md §4) e
// devolve os itens com o status que o CLP gravou em cada um. Diferente do modelo
// de comando único, há o sinal Start: o CLP avisa que travou o lote antes de
// executá-lo, e só depois vem o Ack.
func (q *Queue) dispatch(ctx context.Context, rec Record) ([]Item, error) {
	robot := rec.Robot

	// 1. Garante o slot do robô ocioso (Req=false, Start/Ack baixados).
	if err := q.ensureIdle(ctx, robot); err != nil {
		return nil, err
	}

	// 2. Escreve o lote e seus metadados — tudo ANTES do Req, que é o gatilho.

	// Antes do encode: o lote em forma legível (o que a API decidiu enviar).
	q.log.Info("batch → CLP (legível)",
		"robot", robot,
		"tag", plc.TagLote(robot),
		"batch_id", rec.BatchID,
		"acao", rec.Acao,
		"qtd", rec.Qtd,
		"items", describeItems(rec))

	data, err := plc.EncodeBatch(toBatchItems(rec))
	if err != nil {
		return nil, err
	}

	// Depois do encode: o frame do array em hex, exatamente como o WriteStructRaw
	// grava. É aqui que se confere o packing do UDT contra o Sysmac.
	q.log.Info("batch → CLP (frame)",
		"robot", robot,
		"tag", plc.TagLote(robot),
		"frame_len", len(data),
		"frame_hex", fmt.Sprintf("%X", data))
	if err := q.plc.WriteStructRaw(ctx, plc.TagLote(robot), data); err != nil {
		return nil, fmt.Errorf("write %s: %w", plc.TagLote(robot), err)
	}
	if err := q.writeTag(ctx, plc.TagLoteQtd(robot), plc.TagValue{Kind: plc.KindInt, Int: rec.Qtd}); err != nil {
		return nil, err
	}
	if err := q.writeTag(ctx, plc.TagLoteId(robot), plc.TagValue{Kind: plc.KindDInt, DInt: rec.BatchID}); err != nil {
		return nil, err
	}

	// 3. Sinaliza lote pronto.
	if err := q.writeBool(ctx, plc.TagLoteReq(robot), true); err != nil {
		return nil, err
	}

	// 4. O CLP trava o lote e começa a executar.
	if err := q.waitBool(ctx, plc.TagLoteStart(robot), true, q.cfg.StartTimeout); err != nil {
		return nil, err
	}
	q.transition(rec, StateStarted, nil, "")

	// 5. O CLP concluiu o lote (todos os .status gravados).
	if err := q.waitBool(ctx, plc.TagLoteAck(robot), true, q.cfg.AckTimeout); err != nil {
		return nil, err
	}

	// 6. Confere o eco do id e colhe o status de cada item.
	items, err := q.readAck(ctx, rec)
	if err != nil {
		return nil, err
	}

	// 7. Confirma o recebimento e 8. espera o CLP liberar o slot. Uma falha aqui
	// não invalida o ack já lido — o ensureIdle do próximo lote recobra o slot.
	if err := q.writeBool(ctx, plc.TagLoteReq(robot), false); err != nil {
		return nil, err
	}
	_ = q.waitIdle(ctx, robot)
	return items, nil
}

// readAck confere o eco de gLote_Id_RoboN (correlação do ack) e lê
// gLote_RoboN[i].status de cada item do lote.
func (q *Queue) readAck(ctx context.Context, rec Record) ([]Item, error) {
	ackID, err := q.readInt(ctx, plc.TagLoteId(rec.Robot))
	if err != nil {
		return nil, err
	}
	if ackID != rec.BatchID {
		return nil, fmt.Errorf("eco de %s = %d, esperava %d",
			plc.TagLoteId(rec.Robot), ackID, rec.BatchID)
	}
	items := append([]Item(nil), rec.Items...)
	for i := range items {
		st, err := q.readInt(ctx, plc.TagLoteItemStatus(rec.Robot, int(items[i].Position)))
		if err != nil {
			return nil, err
		}
		items[i].Status = int32(st)
	}
	return items, nil
}

// classify decide o estado final do lote pelo status que o CLP gravou nos itens:
// ACKED_OK só se todos vieram OK.
func classify(items []Item) (State, string) {
	var bad []string
	for _, it := range items {
		if it.Status != plc.ItemStatusOK {
			bad = append(bad, fmt.Sprintf("position %d (status=%d)", it.Position, it.Status))
		}
	}
	if len(bad) == 0 {
		return StateAckedOK, ""
	}
	return StateAckedError, "itens com falha: " + strings.Join(bad, ", ")
}

// toBatchItems posiciona os itens no array do CLP (índice 0 = gLote_RoboN[1]). As
// posições não usadas ficam zeradas e todos os itens levam a ação do lote, que é
// homogêneo. Validate já garantiu Position em 1..Qtd.
func toBatchItems(rec Record) []plc.BatchItem {
	out := make([]plc.BatchItem, plc.BatchSize)
	for _, it := range rec.Items {
		out[it.Position-1] = plc.BatchItem{
			Acao:    rec.Acao,
			Dedo:    it.Dedo,
			Dedo2:   it.Dedo2,
			Estande: it.Estande,
			Bandeja: it.Bandeja,
			Fileira: it.Fileira,
			Coluna:  it.Coluna,
			Rack:    it.Rack,
			Placa:   it.Placa,
			Slot:    it.Slot,
		}
	}
	return out
}

// describeItems monta uma descrição legível dos itens do lote para o log,
// mostrando só os campos que se aplicam à ação (bandeja x placa).
func describeItems(rec Record) string {
	var b strings.Builder
	for i, it := range rec.Items {
		if i > 0 {
			b.WriteString(" | ")
		}
		fmt.Fprintf(&b, "pos%d dedo=%d", it.Position, it.Dedo)
		if rec.Acao == 5 {
			fmt.Fprintf(&b, " dedo2=%d", it.Dedo2)
		}
		switch rec.Acao {
		case 1, 4: // ações de bandeja
			fmt.Fprintf(&b, " estande=%d bandeja=%d fileira=%d coluna=%d",
				it.Estande, it.Bandeja, it.Fileira, it.Coluna)
		default: // ações de placa
			fmt.Fprintf(&b, " rack=%d placa=%d slot=%d", it.Rack, it.Placa, it.Slot)
		}
	}
	return b.String()
}

// ensureIdle zera gLote_Req_RoboN e espera o CLP baixar Start e Ack — o slot do
// robô só aceita um lote novo depois disso.
func (q *Queue) ensureIdle(ctx context.Context, robot int32) error {
	if err := q.writeBool(ctx, plc.TagLoteReq(robot), false); err != nil {
		return err
	}
	if err := q.waitIdle(ctx, robot); err != nil {
		return fmt.Errorf("CLP não liberou o slot do robô %d: %w", robot, err)
	}
	return nil
}

// waitIdle espera Start e Ack voltarem a FALSE (slot do robô livre).
func (q *Queue) waitIdle(ctx context.Context, robot int32) error {
	if err := q.waitBool(ctx, plc.TagLoteStart(robot), false, q.cfg.AckTimeout); err != nil {
		return err
	}
	return q.waitBool(ctx, plc.TagLoteAck(robot), false, q.cfg.AckTimeout)
}

// waitBool faz poll de uma tag BOOL até ela valer want, ou estourar o timeout.
func (q *Queue) waitBool(ctx context.Context, tag string, want bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		v, err := q.readBool(ctx, tag)
		if err != nil {
			return err
		}
		if v == want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: esperava %v: %w", tag, want, errTimeout)
		}
		if !q.sleep(ctx, q.cfg.PollEvery) {
			return ctx.Err()
		}
	}
}

// Recover reconcilia lotes deixados em voo (DISPATCHED/STARTED) por um
// shutdown/crash. Se o CLP ainda exibe o ack correspondente, finaliza a partir
// dele; caso contrário re-enfileira (depende da idempotência do ladder por id).
func (q *Queue) Recover(ctx context.Context) error {
	for _, robot := range plc.Robots {
		inflight, err := q.store.ListInFlight(robot)
		if err != nil {
			return err
		}
		for _, rec := range inflight {
			if q.recoverFromAck(ctx, rec) {
				continue
			}
			q.log.Warn("recover: re-enfileirando lote em voo", "batch_id", rec.BatchID, "robot", robot)
			q.transition(rec, StateQueued, nil, "re-enfileirado após reinício")
		}
		q.signal(robot)
	}
	return nil
}

// recoverFromAck finaliza o lote a partir do ack ainda presente no CLP, evitando
// reexecutar o que já rodou. Retorna false se não há ack correspondente.
func (q *Queue) recoverFromAck(ctx context.Context, rec Record) bool {
	ack, err := q.readBool(ctx, plc.TagLoteAck(rec.Robot))
	if err != nil || !ack {
		return false
	}
	items, err := q.readAck(ctx, rec) // confere o eco do id antes de aceitar o ack
	if err != nil {
		return false
	}
	state, detail := classify(items)
	q.transition(rec, state, items, detail)
	_ = q.writeBool(ctx, plc.TagLoteReq(rec.Robot), false)
	return true
}

// --- helpers ---

// transition persiste o novo estado do lote e publica a transição nos streams.
// items != nil sobrescreve o status dos itens (o que o CLP gravou).
func (q *Queue) transition(rec Record, state State, items []Item, detail string) {
	cur, ok, err := q.store.Get(rec.Robot, rec.BatchID)
	if err != nil || !ok {
		q.log.Error("transition: registro não encontrado",
			"batch_id", rec.BatchID, "robot", rec.Robot, "err", err)
		return
	}
	cur.State = state
	cur.Detail = detail
	if items != nil {
		cur.Items = items
	}
	if err := q.store.Update(cur); err != nil {
		q.log.Error("transition: persist", "batch_id", rec.BatchID, "robot", rec.Robot,
			"state", state, "err", err)
		return
	}
	cur, _, _ = q.store.Get(rec.Robot, rec.BatchID)
	q.broker.publish(cur)
	q.log.Info("batch state", "batch_id", rec.BatchID, "robot", rec.Robot,
		"state", state, "detail", detail)
}

func (q *Queue) persistAttempts(rec Record) {
	cur, ok, err := q.store.Get(rec.Robot, rec.BatchID)
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

func (q *Queue) writeTag(ctx context.Context, tag string, v plc.TagValue) error {
	if err := q.plc.WriteTag(ctx, tag, v); err != nil {
		return fmt.Errorf("write %s: %w", tag, err)
	}
	return nil
}

func (q *Queue) writeBool(ctx context.Context, tag string, val bool) error {
	return q.writeTag(ctx, tag, plc.TagValue{Kind: plc.KindBool, Bool: val})
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
