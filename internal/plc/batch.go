package plc

import (
	"encoding/binary"
	"fmt"
)

// Tags do handshake de LOTE (gLote), duplicadas por robô — cada robô tem o seu
// conjunto completo e portanto um slot independente. A sequência do handshake
// está em internal/queue/dispatcher.go; o layout do UDT, em docs/mudancas-batch.md.
//
//	API -> CLP: gLote_RoboN (ARRAY[1..5] OF TComando), gLote_Id_RoboN (DINT),
//	            gLote_Qtd_RoboN (INT), gLote_Req_RoboN (BOOL)
//	CLP -> API: gLote_Start_RoboN (BOOL), gLote_Ack_RoboN (BOOL) e o membro
//	            .status de cada elemento de gLote_RoboN
const (
	// BatchSize é o nº de elementos de gLote_RoboN (ARRAY[1..5] OF TComando).
	// O lote sempre vai completo no frame: os itens além de Qtd são zeros.
	BatchSize = 5

	// tcomandoBytes é o stride de um TComando: 11 membros INT × 2 bytes.
	tcomandoBytes = 22
)

// Status de um item do lote, gravado pelo CLP em gLote_RoboN[i].status.
const (
	ItemStatusPending = 0
	ItemStatusOK      = 1
	ItemStatusError   = 2
)

// Robots são os robôs atendidos (sufixo _RoboN das tags). Cada um tem seu próprio
// slot de lote e é despachado por uma goroutine independente.
var Robots = []int32{1, 2}

func TagLote(robot int32) string      { return fmt.Sprintf("gLote_Robo%d", robot) }
func TagLoteId(robot int32) string    { return fmt.Sprintf("gLote_Id_Robo%d", robot) }
func TagLoteQtd(robot int32) string   { return fmt.Sprintf("gLote_Qtd_Robo%d", robot) }
func TagLoteReq(robot int32) string   { return fmt.Sprintf("gLote_Req_Robo%d", robot) }
func TagLoteStart(robot int32) string { return fmt.Sprintf("gLote_Start_Robo%d", robot) }
func TagLoteAck(robot int32) string   { return fmt.Sprintf("gLote_Ack_Robo%d", robot) }

// TagLoteItemStatus endereça o membro .status do i-ésimo elemento do lote. O
// índice é base-1, como o ARRAY[1..5] declarado no Sysmac (ver escrita-struct-clp.md §3).
func TagLoteItemStatus(robot int32, i int) string {
	return fmt.Sprintf("gLote_Robo%d[%d].status", robot, i)
}

// BatchItem é um elemento do UDT TComando. Os campos não aplicáveis à ação do
// lote vão zerados (a validação em internal/queue/action.go garante isso).
// O membro .status não está aqui: quem o grava é o CLP, e a API sempre envia 0.
type BatchItem struct {
	Acao    int32
	Dedo    int32
	Dedo2   int32
	Estande int32
	Bandeja int32
	Fileira int32
	Coluna  int32
	Rack    int32
	Placa   int32
	Slot    int32
}

// EncodeBatch serializa os itens no packing do UDT TComando para WriteStructRaw:
// BatchSize elementos de tcomandoBytes bytes, todos os membros INT em
// little-endian, na ordem exata da declaração
// (acao, dedo, dedo_2, estande, bandeja, fileira, coluna, rack, placa, slot, status).
//
// items é indexado por posição (items[0] = gLote_RoboN[1]); posições além das
// preenchidas vão zeradas, assim como o .status de todos os elementos.
func EncodeBatch(items []BatchItem) ([]byte, error) {
	if len(items) > BatchSize {
		return nil, fmt.Errorf("lote com %d itens: máximo %d", len(items), BatchSize)
	}
	data := make([]byte, BatchSize*tcomandoBytes)
	for i, it := range items {
		members := [...]int32{
			it.Acao, it.Dedo, it.Dedo2, it.Estande, it.Bandeja,
			it.Fileira, it.Coluna, it.Rack, it.Placa, it.Slot,
			ItemStatusPending,
		}
		off := i * tcomandoBytes
		for j, m := range members {
			binary.LittleEndian.PutUint16(data[off+j*2:], uint16(int16(m)))
		}
	}
	return data, nil
}
