// Comando de teste rápido: escreve um BOOL em uma tag publicada do CLP via rede.
//
// Uso (PowerShell):
//
//	$env:CLP_HOST="192.168.1.14"; go run ./cmd/writebool <NomeDaTag> <0|1>
//
// Exemplo:
//
//	go run ./cmd/writebool gCmd_Req 1
//	go run ./cmd/writebool gCmd_Req 0
//
// A tag precisa estar com Network Publish no Sysmac. ATENÇÃO: escrever um BOOL
// pode acionar lógica da máquina — use uma tag de teste segura.
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"go_auto_ddr_clp/internal/config"
	"go_auto_ddr_clp/internal/plc"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Println("uso: go run ./cmd/writebool <NomeDaTag> <0|1>")
		os.Exit(2)
	}
	tag := os.Args[1]
	val := os.Args[2] == "1" || os.Args[2] == "true"

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

	if err := clp.WriteBoolDebug(ctx, tag, val); err != nil {
		fmt.Printf("write falhou: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("OK: %s = %v\n", tag, val)
}
