// Comando de teste: envia um LOTE (gLote/TComando) pelo caminho de produção —
// gRPC EnqueueBatch do servidor -> fila bbolt -> dispatcher -> handshake com o CLP —
// e acompanha as transições de estado até ACKED_OK / ACKED_ERROR / FAILED.
//
// Diferente do writeitens (que escreve a tag direto), aqui o servidor precisa estar
// rodando (go run ./cmd/server, SEM CLP_MOCK para falar com o CLP real). É o mesmo
// caminho que o Nest usa, então serve para validar driver, tags e timing do ladder
// antes de plugar o backend.
//
// Uso (PowerShell), com o servidor rodando em :50051:
//
//	# smoke test: coleta vazia (Qtd=0) — o CLP só centraliza a garra
//	go run ./cmd/sendlote -acao 1
//
//	# coleta de 2 memórias da bandeja (estande 1, bandeja 1)
//	go run ./cmd/sendlote -acao 1 `
//	  -item "dedo=1,estande=1,bandeja=1,fileira=1,coluna=1" `
//	  -item "dedo=2,estande=1,bandeja=1,fileira=1,coluna=2"
//
//	# inserção na placa (rack 1, placa 1, slot 1)
//	go run ./cmd/sendlote -acao 2 -item "dedo=1,rack=1,placa=1,slot=1"
//
// Campos por item: dedo, dedo2 (só acao 5), estande, bandeja, fileira, coluna
// (ações de bandeja 1/4) ou rack, placa, slot (ações de placa). Position é
// atribuída na ordem dos -item. A validação de domínio é a do servidor
// (internal/queue/action.go); erro volta na resposta do Enqueue.
//
// ATENÇÃO: contra o CLP real isto MOVE O ROBÔ. Use com a célula liberada.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "go_auto_ddr_clp/gen/plc/v1"
)

// itemsFlag acumula os -item repetidos.
type itemsFlag []string

func (f *itemsFlag) String() string     { return strings.Join(*f, "; ") }
func (f *itemsFlag) Set(v string) error { *f = append(*f, v); return nil }

func main() {
	var items itemsFlag
	addr := flag.String("addr", "localhost:50051", "endereço gRPC do servidor")
	robot := flag.Int("robot", 1, "robô alvo (1..2)")
	acao := flag.Int("acao", 1, "ação do lote (1..5, homogênea)")
	batchID := flag.Int("id", 0, "gLote_Id; 0 = deriva do relógio (único entre corridas)")
	timeout := flag.Duration("timeout", 90*time.Second, "espera máxima pelo desfecho do lote")
	flag.Var(&items, "item", `item do lote, "campo=valor,..." (repetível; position segue a ordem)`)
	flag.Parse()

	id := int32(*batchID)
	if id == 0 {
		// Unix segundos cabe em DINT e não colide com os ids do Nest (Batch.id
		// reinicia em 1); corridas seguidas ganham ids diferentes sem reset da fila.
		id = int32(time.Now().Unix())
	}

	msgs, err := parseItems(items)
	if err != nil {
		fmt.Println("item inválido:", err)
		os.Exit(2)
	}

	req := &pb.EnqueueBatchRequest{
		BatchId: id,
		Robot:   int32(*robot),
		Acao:    int32(*acao),
		Qtd:     int32(len(msgs)),
		Items:   msgs,
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Println("gRPC:", err)
		os.Exit(1)
	}
	defer conn.Close()
	client := pb.NewPLCServiceClient(conn)

	// Assina ANTES do enqueue para não perder transições rápidas.
	stream, err := client.SubscribeBatchStatus(ctx, &pb.SubscribeBatchRequest{})
	if err != nil {
		fmt.Println("subscribe:", err)
		os.Exit(1)
	}

	fmt.Printf("enfileirando lote id=%d robot=%d acao=%d qtd=%d em %s\n",
		req.BatchId, req.Robot, req.Acao, req.Qtd, *addr)
	for _, it := range msgs {
		fmt.Printf("  pos%d dedo=%d dedo2=%d estande=%d bandeja=%d fileira=%d coluna=%d rack=%d placa=%d slot=%d\n",
			it.Position, it.Dedo, it.Dedo_2, it.Estande, it.Bandeja, it.Fileira, it.Coluna, it.Rack, it.Placa, it.Slot)
	}

	resp, err := client.EnqueueBatch(ctx, req)
	if err != nil {
		fmt.Println("enqueue:", err)
		os.Exit(1)
	}
	if !resp.GetAccepted() {
		fmt.Println("lote recusado:", resp.GetMessage())
		os.Exit(1)
	}
	if resp.GetDuplicate() {
		fmt.Println("aviso: lote duplicado (mesmo robot+id) — acompanhando o registro existente")
	}

	// Acompanha só o nosso lote; o replay pode trazer lotes antigos de outros clientes.
	for {
		up, err := stream.Recv()
		if err != nil {
			fmt.Println("stream encerrado antes do desfecho:", err)
			os.Exit(1)
		}
		if up.GetBatchId() != req.BatchId || up.GetRobot() != req.Robot {
			continue
		}
		fmt.Printf("[%s] %s %s\n",
			time.UnixMilli(up.GetTsUnixMs()).Format("15:04:05.000"),
			up.GetState(), up.GetDetail())

		switch up.GetState() {
		case pb.BatchState_BQ_ACKED_OK, pb.BatchState_BQ_ACKED_ERROR, pb.BatchState_BQ_FAILED:
			for _, st := range up.GetItems() {
				fmt.Printf("  gLote[%d].status = %d\n", st.GetPosition(), st.GetStatus())
			}
			// Tira o lote do replay de reconexões — teste de bancada não deve
			// deixar pendência para o assinante do Nest.
			_, _ = client.ConfirmBatchStatus(ctx, &pb.ConfirmBatchRequest{
				BatchId: req.BatchId, Robot: req.Robot,
			})
			if up.GetState() == pb.BatchState_BQ_ACKED_OK {
				fmt.Println("OK: lote concluído pelo CLP")
				return
			}
			os.Exit(1)
		}
	}
}

// parseItems converte cada -item "campo=valor,..." num TComandoMsg. Campos
// omitidos ficam 0 (é o que o CLP espera para o grupo não-aplicável à ação).
func parseItems(raw []string) ([]*pb.TComandoMsg, error) {
	out := make([]*pb.TComandoMsg, 0, len(raw))
	for i, spec := range raw {
		it := &pb.TComandoMsg{Position: int32(i + 1)}
		for _, pair := range strings.FieldsFunc(spec, func(r rune) bool { return r == ',' || r == ' ' }) {
			k, v, ok := strings.Cut(pair, "=")
			if !ok {
				return nil, fmt.Errorf("%q: esperava campo=valor", pair)
			}
			n, err := strconv.Atoi(v)
			if err != nil {
				return nil, fmt.Errorf("%q: valor não numérico", pair)
			}
			val := int32(n)
			switch strings.ToLower(k) {
			case "position":
				it.Position = val
			case "dedo":
				it.Dedo = val
			case "dedo2", "dedo_2":
				it.Dedo_2 = val
			case "estande":
				it.Estande = val
			case "bandeja":
				it.Bandeja = val
			case "fileira":
				it.Fileira = val
			case "coluna":
				it.Coluna = val
			case "rack":
				it.Rack = val
			case "placa":
				it.Placa = val
			case "slot":
				it.Slot = val
			default:
				return nil, fmt.Errorf("campo %q desconhecido", k)
			}
		}
		out = append(out, it)
	}
	return out, nil
}
