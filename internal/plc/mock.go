package plc

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"strings"
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

	// Emulação do leitor de código (CLP_MOCK_SERIAL=1): o mock mantém um gêmeo
	// mínimo de onde cada "memória" (identificada pelo serial) está — dedo da garra
	// ou slot de placa — e, a cada coleta, varre os dedos ocupados escrevendo
	// gSerial_RoboN[dedo], como a centralização real faz sobre gDedoOcupado. Dedos
	// vazios ficam intocados (resíduo), também como no CLP. Default desligado: o
	// harness antigo segue ligando o serial só no início do teste.
	serialSim    bool
	fingerSerial map[fingerKey]string
	slotSerial   map[slotKey]string
}

type fingerKey struct{ robot, dedo int32 }
type slotKey struct{ robot, rack, placa, slot int32 }

func NewMock() *Mock {
	min := envInt("CLP_MOCK_ACTION_MIN_MS", 0)
	max := envInt("CLP_MOCK_ACTION_MAX_MS", 0)
	if max < min {
		max = min
	}
	m := &Mock{
		tags:         make(map[string]TagValue),
		structs:      make(map[string][]byte),
		status:       Status{Mode: "PROGRAM"},
		actionMinMS:  min,
		actionMaxMS:  max,
		serialSim:    os.Getenv("CLP_MOCK_SERIAL") == "1",
		fingerSerial: make(map[fingerKey]string),
		slotSerial:   make(map[slotKey]string),
	}
	m.seedFeederFromEnv(os.Getenv("CLP_MOCK_FEEDER_PRESENT"))
	return m
}

// seedFeederFromEnv semeia bandejas presentes a partir de CLP_MOCK_FEEDER_PRESENT
// (formato "robô:estande:bandeja" separado por vírgula, ex. "1:2:1,1:2:2"). Sem o
// seed toda posição nasce ausente (ReadTag de tag desconhecida devolve zero), o que
// obrigaria o harness a escrever as tags via WriteTag antes de o backend assinar —
// o env evita essa corrida de ordem de subida. Entradas malformadas são ignoradas.
func (m *Mock) seedFeederFromEnv(spec string) {
	for _, entry := range strings.Split(spec, ",") {
		parts := strings.Split(strings.TrimSpace(entry), ":")
		if len(parts) != 3 {
			continue
		}
		robot, err1 := strconv.Atoi(parts[0])
		estande, err2 := strconv.Atoi(parts[1])
		bandeja, err3 := strconv.Atoi(parts[2])
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		tag := TagBandejaPresenteMember(int32(robot), int32(estande), int32(bandeja))
		m.tags[tag] = TagValue{Kind: KindBool, Bool: true}
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
		// Os serials são gravados antes do Ack, como no CLP: quando o Ack sobe,
		// gSerial já está válido (clp-software.md §gSerial).
		if m.serialSim {
			m.simulateSerials(robot, qtd)
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

// simulateSerials move as "memórias" do gêmeo do mock conforme os itens do lote
// e, se o lote é de coleta, varre os dedos ocupados gravando gSerial_RoboN[dedo].
// Chamado com m.mu já travado. A ação vem do frame (lote homogêneo: a do item 1);
// a coleta vazia (qtd=0) é o único lote sem itens, então qtd=0 é coleta.
func (m *Mock) simulateSerials(robot int32, qtd int) {
	items, err := DecodeBatch(m.structs[TagLote(robot)])
	if err != nil {
		return
	}
	acao := int32(1)
	if qtd > 0 {
		acao = m.tags[TagLoteAcao(robot)].Int
	}
	for i := 0; i < qtd && i < len(items); i++ {
		it := items[i]
		finger := fingerKey{robot, it.Dedo}
		slot := slotKey{robot, it.Rack, it.Placa, it.Slot}
		switch acao {
		case 1: // coleta: a memória da célula de origem entra no dedo
			m.fingerSerial[finger] = mockSerial(robot, it)
		case 2: // inserção: do dedo para o slot
			m.slotSerial[slot] = m.fingerSerial[finger]
			delete(m.fingerSerial, finger)
		case 3: // retirada: do slot para o dedo
			m.fingerSerial[finger] = m.slotSerial[slot]
			delete(m.slotSerial, slot)
		case 4: // guarda: o dedo esvazia
			delete(m.fingerSerial, finger)
		case 5: // substituição: dedo retira do slot, dedo_2 insere nele
			finger2 := fingerKey{robot, it.Dedo2}
			m.fingerSerial[finger] = m.slotSerial[slot]
			m.slotSerial[slot] = m.fingerSerial[finger2]
			delete(m.fingerSerial, finger2)
		}
	}
	if acao != 1 {
		return
	}
	for dedo := int32(1); dedo <= BatchSize; dedo++ {
		if s, ok := m.fingerSerial[fingerKey{robot, dedo}]; ok && s != "" {
			m.tags[TagSerial(robot, int(dedo))] = TagValue{Kind: KindString, String: s}
		}
	}
}

// mockSerial é determinístico por (robô, célula de origem) e tem 18 caracteres,
// o comprimento do serial real do cliente — assim o harness consegue prever o
// serial de cada memória e escrever o mesmo valor no In.txt.
func mockSerial(robot int32, it BatchItem) string {
	return fmt.Sprintf("MK%d%d%d%d%02d%010d", robot, it.Estande, it.Bandeja, it.Fileira, it.Coluna, 0)
}

func (m *Mock) Status(context.Context) (Status, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.status, nil
}
