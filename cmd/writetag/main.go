// Comando de teste genérico: escreve QUALQUER tipo em uma tag publicada do CLP,
// via rede (UCMM-direto). Cobre BOOL, inteiros, REAL/LREAL, STRING e arrays.
//
// Uso (PowerShell):
//
//	$env:CLP_HOST="192.168.1.14"; go run ./cmd/writetag <tag> <tipo> <valor[,valor,...]>
//
// Tipos aceitos:
//
//	bool  sint  int  dint  lint  usint  uint  udint  ulint  real  lreal  string
//
// Exemplos:
//
//	go run ./cmd/writetag gCmd_Req      bool   1
//	go run ./cmd/writetag gCmd_Id       dint   101
//	go run ./cmd/writetag gCmd_Robot    int    2
//	go run ./cmd/writetag gCmd_Motherboard string MB09
//	go run ./cmd/writetag MinhaTagArray dint   10,20,30      # array de DINT
//
// ATENÇÃO: escrever pode acionar lógica da máquina — use uma tag de teste segura.
package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"go_auto_ddr_clp/internal/config"
	"go_auto_ddr_clp/internal/plc"
)

func main() {
	if len(os.Args) < 4 {
		fmt.Println("uso: go run ./cmd/writetag <tag> <tipo> <valor[,valor,...]>")
		fmt.Println("tipos: bool sint int dint lint usint uint udint ulint real lreal string")
		os.Exit(2)
	}
	tag := os.Args[1]
	typ := strings.ToLower(os.Args[2])
	raw := os.Args[3]

	value, err := parseValue(typ, raw)
	if err != nil {
		fmt.Printf("valor inválido para %s: %v\n", typ, err)
		os.Exit(2)
	}

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

	if err := clp.WriteValue(ctx, tag, value); err != nil {
		fmt.Printf("write falhou: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("OK: %s (%s) = %v\n", tag, typ, value)
}

// parseValue converte a string da linha de comando no Go type que o WriteValue
// espera. Se "raw" tiver vírgulas, produz um slice (array) daquele tipo — exceto
// para string, que é escrita inteira como um único valor.
func parseValue(typ, raw string) (any, error) {
	if typ == "string" {
		return raw, nil
	}

	parts := strings.Split(raw, ",")
	isArray := len(parts) > 1

	switch typ {
	case "bool":
		return build(parts, isArray, parseBool)
	case "sint":
		return build(parts, isArray, func(s string) (int8, error) { return parseInt[int8](s, 8) })
	case "usint":
		return build(parts, isArray, func(s string) (uint8, error) { return parseUint[uint8](s, 8) })
	case "int":
		return build(parts, isArray, func(s string) (int16, error) { return parseInt[int16](s, 16) })
	case "uint":
		return build(parts, isArray, func(s string) (uint16, error) { return parseUint[uint16](s, 16) })
	case "dint":
		return build(parts, isArray, func(s string) (int32, error) { return parseInt[int32](s, 32) })
	case "udint":
		return build(parts, isArray, func(s string) (uint32, error) { return parseUint[uint32](s, 32) })
	case "lint":
		return build(parts, isArray, func(s string) (int64, error) { return parseInt[int64](s, 64) })
	case "ulint":
		return build(parts, isArray, func(s string) (uint64, error) { return parseUint[uint64](s, 64) })
	case "real":
		return build(parts, isArray, func(s string) (float32, error) { return parseFloat[float32](s, 32) })
	case "lreal":
		return build(parts, isArray, func(s string) (float64, error) { return parseFloat[float64](s, 64) })
	default:
		return nil, fmt.Errorf("tipo desconhecido %q", typ)
	}
}

// build aplica o parser a cada parte; devolve um escalar (T) ou um slice ([]T)
// conforme isArray, casando com o type switch de WriteValue.
func build[T any](parts []string, isArray bool, parse func(string) (T, error)) (any, error) {
	out := make([]T, len(parts))
	for i, p := range parts {
		v, err := parse(strings.TrimSpace(p))
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	if isArray {
		return out, nil
	}
	return out[0], nil
}

func parseBool(s string) (bool, error) {
	switch strings.ToLower(s) {
	case "1", "true", "t", "on":
		return true, nil
	case "0", "false", "f", "off":
		return false, nil
	default:
		return false, fmt.Errorf("bool inválido %q (use 0/1)", s)
	}
}

func parseInt[T int8 | int16 | int32 | int64](s string, bits int) (T, error) {
	n, err := strconv.ParseInt(s, 10, bits)
	return T(n), err
}

func parseUint[T uint8 | uint16 | uint32 | uint64](s string, bits int) (T, error) {
	n, err := strconv.ParseUint(s, 10, bits)
	return T(n), err
}

func parseFloat[T float32 | float64](s string, bits int) (T, error) {
	n, err := strconv.ParseFloat(s, bits)
	return T(n), err
}
