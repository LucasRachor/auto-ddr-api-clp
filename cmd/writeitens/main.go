// Comando de teste: escreve um ARRAY OF STRUCT (UDT) no CLP via rede (UCMM-direto).
//
// Alvo: a tag "Itens" (ARRAY[0..2] OF ST_Item), publicada como Publish Only no
// Sysmac Studio. O UDT ST_Item (offset NJ) tem o layout, confirmado por read-back:
//
//	Numero INT   (2 bytes, little-endian)
//	Estado INT   (2 bytes, little-endian)
//	Ligado BOOL  (2 bytes no NX1P2: 0x0001 = TRUE, 0x0000 = FALSE)
//	                                            => 6 bytes por elemento
//
// O WriteStructRaw lê sozinho o "structure handle" (CRC do template) do controlador
// antes de escrever, então aqui só montamos os bytes dos membros.
//
// Uso (PowerShell):
//
//	$env:CLP_HOST="192.168.1.14"; go run ./cmd/writeitens
//
// Depois confira com: go run ./cmd/readtag Itens
//
// ATENÇÃO: escrever aciona lógica da máquina — use apenas com a tag de teste.
package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"time"

	"go_auto_ddr_clp/internal/config"
	"go_auto_ddr_clp/internal/plc"
)

// Item espelha o UDT ST_Item do CLP (mesma ordem de membros).
type Item struct {
	Numero int16
	Estado int16
	Ligado bool
}

// Valores de teste para os 3 elementos de Itens.
var itens = []Item{
	{Numero: 111, Estado: 1, Ligado: true},
	{Numero: 222, Estado: 2, Ligado: false},
	{Numero: 333, Estado: 0, Ligado: true},
}

func main() {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	clp := plc.NewOmronNX1P2(cfg.CLPHost, cfg.CLPPort)
	if err := clp.Connect(ctx); err != nil {
		fmt.Printf("connect %s:%d falhou: %v\n", cfg.CLPHost, cfg.CLPPort, err)
		os.Exit(1)
	}
	defer clp.Close()
	fmt.Printf("conectado em %s:%d\n", cfg.CLPHost, cfg.CLPPort)

	// Serializa os membros em little-endian, 6 bytes por elemento.
	data := make([]byte, 0, len(itens)*6)
	for _, it := range itens {
		var b [6]byte
		binary.LittleEndian.PutUint16(b[0:2], uint16(it.Numero))
		binary.LittleEndian.PutUint16(b[2:4], uint16(it.Estado))
		if it.Ligado {
			binary.LittleEndian.PutUint16(b[4:6], 1)
		}
		data = append(data, b[:]...)
	}

	if err := clp.WriteStructRaw(ctx, "Itens", uint16(len(itens)), data); err != nil {
		fmt.Printf("write falhou: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("OK: Itens escrito (%d elementos, %d bytes)\n", len(itens), len(data))
	for i, it := range itens {
		fmt.Printf("  Itens[%d] = {Numero:%d Estado:%d Ligado:%v}\n", i, it.Numero, it.Estado, it.Ligado)
	}
}
