package plc

import (
	"encoding/binary"
	"fmt"
	"strings"
)

// Tags do handshake de LOTE (gLote), duplicadas por robô — cada robô tem o seu
// conjunto completo e portanto um slot independente. A sequência do handshake
// está em internal/queue/dispatcher.go; o layout do UDT, em docs/mudancas-batch.md.
//
//	API -> CLP: gLote_RoboN (ARRAY[1..5] OF TComando), gLote_Id_RoboN (DINT),
//	            gLote_Acao_RoboN (INT), gLote_Qtd_RoboN (INT), gLote_Req_RoboN (BOOL)
//	CLP -> API: gLote_Start_RoboN (BOOL), gLote_Ack_RoboN (BOOL) e o membro
//	            .status de cada elemento de gLote_RoboN
const (
	// BatchSize é o nº de elementos de gLote_RoboN (ARRAY[1..5] OF TComando).
	// O lote sempre vai completo no frame: os itens além de Qtd são zeros.
	BatchSize = 5

	// tcomandoBytes é o stride de um TComando: 10 membros INT × 2 bytes. A ação
	// NÃO é membro do item: é a variável de lote gLote_Acao_RoboN
	// (clp-software.md §TComando). Mandar 22 bytes por item é recusado pelo NX1P2
	// com GeneralStatus=0x1F addStatus=0102 (tamanho dos dados ≠ tamanho da tag).
	tcomandoBytes = 20
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
func TagLoteAcao(robot int32) string  { return fmt.Sprintf("gLote_Acao_Robo%d", robot) }
func TagLoteQtd(robot int32) string   { return fmt.Sprintf("gLote_Qtd_Robo%d", robot) }
func TagLoteReq(robot int32) string   { return fmt.Sprintf("gLote_Req_Robo%d", robot) }
func TagLoteStart(robot int32) string { return fmt.Sprintf("gLote_Start_Robo%d", robot) }
func TagLoteAck(robot int32) string   { return fmt.Sprintf("gLote_Ack_Robo%d", robot) }

// TagLoteItemStatus endereça o membro .status do i-ésimo elemento do lote. O
// índice é base-1, como o ARRAY[1..5] declarado no Sysmac (ver escrita-struct-clp.md §3).
func TagLoteItemStatus(robot int32, i int) string {
	return fmt.Sprintf("gLote_Robo%d[%d].status", robot, i)
}

// TagSerial endereça gSerial_RoboN[dedo] (ARRAY[1..5] OF STRING[32]): o serial que
// o leitor de código gravou para a memória carregada por aquele dedo, durante a
// centralização. Índice base-1, como o array declarado no Sysmac. CLP -> API, sem
// handshake: válido quando sobe o Ack de um lote de coleta (clp-software.md §gSerial).
func TagSerial(robot int32, dedo int) string {
	return fmt.Sprintf("gSerial_Robo%d[%d]", robot, dedo)
}

// NormalizeSerial limpa o STRING lido do CLP antes de entregá-lo ao backend: tira
// NUL (byte de preenchimento do leitor) e espaços das pontas. Se ainda sobrar
// caractere de controle no meio, devolve "" — melhor a memória seguir anônima (o
// serial chega no início do teste) do que ligar lixo a ela, que viraria divergência
// no cruzamento com o arquivo do cliente.
func NormalizeSerial(s string) string {
	s = strings.Trim(s, "\x00 \t\r\n")
	for _, r := range s {
		if r < 0x20 || r == 0x7f {
			return ""
		}
	}
	return s
}

// BatchItem é um elemento do UDT TComando. Os campos não aplicáveis à ação do
// lote vão zerados (a validação em internal/queue/action.go garante isso).
// O membro .status não está aqui: quem o grava é o CLP, e a API sempre envia 0.
// A ação não está aqui porque não é membro do item: vai em gLote_Acao_RoboN.
type BatchItem struct {
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
// (dedo, dedo_2, estande, bandeja, fileira, coluna, rack, placa, slot, status).
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
			it.Dedo, it.Dedo2, it.Estande, it.Bandeja, it.Fileira,
			it.Coluna, it.Rack, it.Placa, it.Slot, ItemStatusPending,
		}
		off := i * tcomandoBytes
		for j, m := range members {
			binary.LittleEndian.PutUint16(data[off+j*2:], uint16(int16(m)))
		}
	}
	return data, nil
}

// DecodeBatch é o inverso de EncodeBatch: devolve os BatchSize elementos do frame
// (o .status não entra no BatchItem). Usado pelo CLP simulado para saber o que o
// lote pede sem depender de tags avulsas.
func DecodeBatch(data []byte) ([]BatchItem, error) {
	if len(data) != BatchSize*tcomandoBytes {
		return nil, fmt.Errorf("frame com %d bytes: esperava %d", len(data), BatchSize*tcomandoBytes)
	}
	items := make([]BatchItem, BatchSize)
	for i := range items {
		off := i * tcomandoBytes
		m := func(j int) int32 {
			return int32(int16(binary.LittleEndian.Uint16(data[off+j*2:])))
		}
		items[i] = BatchItem{
			Dedo: m(0), Dedo2: m(1), Estande: m(2), Bandeja: m(3),
			Fileira: m(4), Coluna: m(5), Rack: m(6), Placa: m(7), Slot: m(8),
		}
	}
	return items, nil
}
