// Comando de diagnóstico: descobre qual formato de escrita a tag estruturada "Itens"
// (ARRAY[..] OF ST_Item) aceita no NX1P2. Numa única execução tenta vários cabeçalhos
// de Write Tag para o array inteiro e também a escrita por MEMBRO (Itens[i].Numero),
// imprimindo o GeneralStatus/AdditionalStatus de cada tentativa. GeneralStatus=0x00 = OK.
//
// Uso (PowerShell):
//
//	$env:CLP_HOST="192.168.1.14"; go run ./cmd/structprobe
//
// Depois confira o efeito com: go run ./cmd/readtag Itens
//
// ATENÇÃO: escreve de verdade na tag de teste — não usar em tag de produção.
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

// 3 elementos de ST_Item (Numero INT, Estado INT, Ligado BOOL de 2 bytes = 6 bytes).
func memberData() []byte {
	type item struct {
		numero, estado int16
		ligado         bool
	}
	itens := []item{{111, 1, true}, {222, 2, false}, {333, 0, true}}
	data := make([]byte, 0, len(itens)*6)
	for _, it := range itens {
		var b [6]byte
		binary.LittleEndian.PutUint16(b[0:2], uint16(it.numero))
		binary.LittleEndian.PutUint16(b[2:4], uint16(it.estado))
		if it.ligado {
			binary.LittleEndian.PutUint16(b[4:6], 1)
		}
		data = append(data, b[:]...)
	}
	return data
}

func u16(v uint16) []byte {
	b := make([]byte, 2)
	binary.LittleEndian.PutUint16(b, v)
	return b
}

func main() {
	cfg := config.Load()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	clp := plc.NewOmronNX1P2(cfg.CLPHost, cfg.CLPPort)
	if err := clp.Connect(ctx); err != nil {
		fmt.Printf("connect %s:%d falhou: %v\n", cfg.CLPHost, cfg.CLPPort, err)
		os.Exit(1)
	}
	defer clp.Close()
	fmt.Printf("conectado em %s:%d\n\n", cfg.CLPHost, cfg.CLPPort)

	// Lê o handle do struct do próprio controlador.
	code, val, err := clp.ReadTagRaw(ctx, "Itens")
	if err != nil {
		fmt.Printf("read Itens falhou: %v\n", err)
		os.Exit(1)
	}
	if code != 0x02A0 || len(val) < 2 {
		fmt.Printf("Itens não é struct (tipo=%#04x, %d bytes)\n", code, len(val))
		os.Exit(1)
	}
	handle := val[:2] // 2 bytes do structure handle, na ordem do wire
	data := memberData()
	fmt.Printf("handle=%#04x  data=%d bytes\n\n", binary.LittleEndian.Uint16(handle), len(data))

	typ := u16(0x02A0)
	cnt3 := u16(3)
	cnt1 := u16(1)

	// Variações do cabeçalho de escrita do ARRAY inteiro.
	variants := []struct {
		nome    string
		payload []byte
	}{
		{"A [tipo][handle][count=3][data]", concat(typ, handle, cnt3, data)},
		{"B [tipo][handle][data] (sem count)", concat(typ, handle, data)},
		{"C [tipo][count=3][data] (sem handle)", concat(typ, cnt3, data)},
		{"D [tipo][handle][count=1][data]", concat(typ, handle, cnt1, data)},
	}
	fmt.Println("== escrita do ARRAY inteiro (tag \"Itens\") ==")
	for _, v := range variants {
		gs, add, err := clp.WriteRawDebug(ctx, "Itens", v.payload)
		report(v.nome, gs, add, err)
	}

	// Escrita por MEMBRO (atômica). Testa base 0 e base 1 do índice.
	fmt.Println("\n== escrita por MEMBRO (atômica) ==")
	for _, idx := range []int{0, 1} {
		tag := fmt.Sprintf("Itens[%d].Numero", idx)
		err := clp.WriteValue(ctx, tag, int16(700+idx))
		reportErr(fmt.Sprintf("%s = %d", tag, 700+idx), err)
	}
	for _, idx := range []int{0, 1} {
		tag := fmt.Sprintf("Itens[%d].Ligado", idx)
		err := clp.WriteValue(ctx, tag, true)
		reportErr(fmt.Sprintf("%s = true", tag), err)
	}

	fmt.Println("\nConfira agora: go run ./cmd/readtag Itens")
}

func concat(parts ...[]byte) []byte {
	var out []byte
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

func report(nome string, gs uint8, add []byte, err error) {
	if err != nil {
		fmt.Printf("  %-42s ERRO transporte: %v\n", nome, err)
		return
	}
	status := "OK"
	if gs != 0 {
		status = fmt.Sprintf("GeneralStatus=%#x addStatus=%x", gs, add)
	}
	fmt.Printf("  %-42s %s\n", nome, status)
}

func reportErr(nome string, err error) {
	if err != nil {
		fmt.Printf("  %-42s %v\n", nome, err)
		return
	}
	fmt.Printf("  %-42s OK\n", nome)
}
