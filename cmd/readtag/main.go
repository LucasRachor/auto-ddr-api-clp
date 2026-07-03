// Comando de diagnóstico (read-only): lê uma tag publicada do CLP e mostra o
// tipo CIP e o valor cru. Útil para descobrir o tipo real antes de escrever.
//
// Uso (PowerShell):
//
//	$env:CLP_HOST="192.168.1.14"; go run ./cmd/readtag <NomeDaTag>
//
// Exemplo:
//
//	go run ./cmd/readtag Pedido
package main

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"go_auto_ddr_clp/internal/config"
	"go_auto_ddr_clp/internal/plc"
)

var typeNames = map[uint16]string{
	0xC1: "BOOL", 0xC2: "SINT", 0xC3: "INT", 0xC4: "DINT", 0xC5: "LINT",
	0xC6: "USINT", 0xC7: "UINT", 0xC8: "UDINT", 0xCA: "REAL", 0xCB: "LREAL",
	0xD0: "STRING", 0xFCE: "STRING",
}

func main() {
	if len(os.Args) < 2 {
		fmt.Println("uso: go run ./cmd/readtag <NomeDaTag>")
		os.Exit(2)
	}
	name := os.Args[1]

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

	code, val, err := clp.ReadTagRaw(ctx, name)
	if err != nil {
		fmt.Printf("read falhou: %v\n", err)
		os.Exit(1)
	}

	name16 := typeNames[code]
	if name16 == "" {
		name16 = "desconhecido"
	}
	fmt.Printf("tag=%q  tipo=%#04x (%s)  bytes=%d\n", name, code, name16, len(val))
	fmt.Print(hex.Dump(val))

	// Decodifica os tipos numéricos mais comuns para facilitar a leitura.
	switch code {
	case 0xC1, 0xC2, 0xC6: // BOOL/SINT/USINT (1 byte)
		if len(val) >= 1 {
			fmt.Printf("valor = %d\n", val[0])
		}
	case 0xC3, 0xC7: // INT/UINT (2 bytes)
		if len(val) >= 2 {
			fmt.Printf("valor = %d\n", int16(binary.LittleEndian.Uint16(val)))
		}
	case 0xC4, 0xC8: // DINT/UDINT (4 bytes)
		if len(val) >= 4 {
			fmt.Printf("valor = %d\n", int32(binary.LittleEndian.Uint32(val)))
		}
	case 0xD0: // STRING: [tamanho UInt (2 bytes)][chars]
		if len(val) >= 2 {
			n := int(binary.LittleEndian.Uint16(val))
			if 2+n <= len(val) {
				fmt.Printf("valor = %q\n", string(val[2:2+n]))
			}
		}
	}
}
