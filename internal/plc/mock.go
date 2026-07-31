package plc

import (
	"context"
	"math/rand"
	"os"
	"strconv"
	"sync"
	"time"
)

// Mock é uma impl in-memory para testes do servidor gRPC sem CLP físico.
type Mock struct {
	mu      sync.RWMutex
	tags    map[string]TagValue
	structs map[string][]byte // último payload escrito por WriteStructRaw
	status  Status

	// Faixa (ms) que o mock leva entre Start e Ack, simulando o tempo de execução
	// do robô. 0 (default) = instantâneo — os testes de unidade não penam. Ligado no
	// harness via CLP_MOCK_ACTION_MIN_MS / CLP_MOCK_ACTION_MAX_MS para ver o fluxo no
	// ritmo real. Exige CMD_ACK_TIMEOUT_MS > actionMaxMS, senão o dispatch estoura.
	actionMinMS int
	actionMaxMS int
}

func NewMock() *Mock {
	min := envInt("CLP_MOCK_ACTION_MIN_MS", 0)
	max := envInt("CLP_MOCK_ACTION_MAX_MS", 0)
	if max < min {
		max = min
	}
	return &Mock{
		tags:        make(map[string]TagValue),
		structs:     make(map[string][]byte),
		status:      Status{Mode: "PROGRAM"},
		actionMinMS: min,
		actionMaxMS: max,
	}
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func (m *Mock) Connect(context.Context) error { return nil }
func (m *Mock) Close() error                  { return nil }

func (m *Mock) Start(context.Context) error {
	m.mu.Lock()
	m.status.Mode = "RUN"
	m.mu.Unlock()
	return nil
}

func (m *Mock) Stop(context.Context) error {
	m.mu.Lock()
	m.status.Mode = "PROGRAM"
	m.mu.Unlock()
	return nil
}

func (m *Mock) Reset(context.Context) error {
	m.mu.Lock()
	m.status.Faulted = false
	m.status.Detail = ""
	m.mu.Unlock()
	return nil
}

func (m *Mock) ReadTag(_ context.Context, tag string) (TagValue, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if v, ok := m.tags[tag]; ok {
		return v, nil
	}
	return TagValue{}, nil
}

func (m *Mock) WriteTag(_ context.Context, tag string, v TagValue) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.tags[tag] = v
	m.simulateBatch(tag, v)
	return nil
}

// WriteStructRaw guarda o payload do lote para inspeção nos testes; o CLP simulado
// só reage ao handshake (gLote_Req_RoboN), não ao conteúdo.
func (m *Mock) WriteStructRaw(_ context.Context, name string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.structs[name] = append([]byte(nil), data...)
	return nil
}

// StructRaw devolve o último payload escrito em name (para asserções em teste).
func (m *Mock) StructRaw(name string) []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.structs[name]
}

// simulateBatch imita o ladder de lote do CLP: ao ver gLote_Req_RoboN=TRUE, trava o
// lote (Start), grava status=OK nos Qtd itens válidos e conclui (Ack); ao ver
// Req=FALSE, baixa Start/Ack, liberando o slot do robô. Permite exercitar a fila
// fim-a-fim sem CLP físico (QUEUED -> DISPATCHED -> STARTED -> ACKED_OK).
// Chamado com m.mu já travado.
func (m *Mock) simulateBatch(tag string, v TagValue) {
	for _, robot := range Robots {
		if tag != TagLoteReq(robot) {
			continue
		}
		if !v.Bool {
			m.tags[TagLoteStart(robot)] = TagValue{Kind: KindBool, Bool: false}
			m.tags[TagLoteAck(robot)] = TagValue{Kind: KindBool, Bool: false}
			return
		}
		qtd := int(m.tags[TagLoteQtd(robot)].Int)
		for i := 1; i <= qtd; i++ {
			m.tags[TagLoteItemStatus(robot, i)] = TagValue{Kind: KindInt, Int: ItemStatusOK}
		}
		// Start = robô travou o lote (imediato). Ack = robô terminou de executar.
		m.tags[TagLoteStart(robot)] = TagValue{Kind: KindBool, Bool: true}

		if m.actionMaxMS <= 0 {
			m.tags[TagLoteAck(robot)] = TagValue{Kind: KindBool, Bool: true}
			return
		}

		// Simula o tempo de execução do robô: o Ack só sobe depois do atraso, numa
		// goroutine (não dá para dormir aqui — simulateBatch roda com m.mu travado).
		// O dispatcher fica em waitBool(Ack) e o vê no poll seguinte.
		delay := m.actionMinMS
		if m.actionMaxMS > m.actionMinMS {
			delay += rand.Intn(m.actionMaxMS - m.actionMinMS + 1)
		}
		go func(robot int32, d time.Duration) {
			time.Sleep(d)
			m.mu.Lock()
			m.tags[TagLoteAck(robot)] = TagValue{Kind: KindBool, Bool: true}
			m.mu.Unlock()
		}(robot, time.Duration(delay)*time.Millisecond)
		return
	}
}

func (m *Mock) Status(context.Context) (Status, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status, nil
}
