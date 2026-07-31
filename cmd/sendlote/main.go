// Comando de teste: envia um LOTE (gLote/TComando) ao CLP e acompanha até o
// desfecho. Dois modos:
//
//   - padrão (gRPC): usa o caminho de produção — EnqueueBatch do servidor rodando
//     (go run ./cmd/server, SEM CLP_MOCK) -> fila bbolt -> dispatcher -> handshake.
//     É o mesmo caminho que o Nest usa; valida o serviço inteiro.
//
//   - -direct: fala DIRETO com o CLP, sem servidor nenhum (um processo só, como o
//     writeitens). Executa o handshake gLote inline (espelho de
//     internal/queue/dispatcher.go): escreve lote/Qtd/Id, sobe Req, espera Start,
//     espera Ack, confere o eco do Id e lê o .status de cada item. Ideal para a
//     bancada onde só tem o Go instalado.
//
// Uso (PowerShell):
//
//	# smoke test direto no CLP: coleta vazia (Qtd=0) — o CLP só centraliza a garra
//	$env:CLP_HOST="192.168.1.14"; go run ./cmd/sendlote -direct -acao 1
//
//	# coleta de 2 memórias da bandeja (estande 1, bandeja 1), direto no CLP
//	go run ./cmd/sendlote -direct -acao 1 `
//	  -item "dedo=1,estande=1,bandeja=1,fileira=1,coluna=1" `
//	  -item "dedo=2,estande=1,bandeja=1,fileira=1,coluna=2"
//
//	# inserção na placa (rack 1, placa 1, slot 1), pelo servidor (gRPC)
//	go run ./cmd/sendlote -acao 2 -item "dedo=1,rack=1,placa=1,slot=1"
//
// Campos por item: dedo, dedo2 (só acao 5), estande, bandeja, fileira, coluna
// (ações de bandeja 1/4) ou rack, placa, slot (ações de placa). Position é
// atribuída na ordem dos -item. A validação é a mesma do servidor
// (internal/queue/action.go), aplicada antes de tocar no CLP.
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
	"go_auto_ddr_clp/internal/config"
	"go_auto_ddr_clp/internal/plc"
	"go_auto_ddr_clp/internal/queue"
)

// itemsFlag acumula os -item repetidos.
type itemsFlag []string

func (f *itemsFlag) String() string     { return strings.Join(*f, "; ") }
func (f *itemsFlag) Set(v string) error { *f = append(*f, v); return nil }

func main() {
	var items itemsFlag
	direct := flag.Bool("direct", false, "fala direto com o CLP, sem o servidor gRPC")
	host := flag.String("host", "", "IP do CLP no modo -direct (padrão: CLP_HOST do ambiente)")
	addr := flag.String("addr", "localhost:50051", "endereço gRPC do servidor (modo padrão)")
	robot := flag.Int("robot", 1, "robô alvo (1..2)")
	acao := flag.Int("acao", 1, "ação do lote (1..5, homogênea)")
	batchID := flag.Int("id", 0, "gLote_Id; 0 = deriva do relógio (único entre corridas)")
	startTimeout := flag.Duration("start-timeout", 15*time.Second, "espera pelo gLote_Start")
	ackTimeout := flag.Duration("ack-timeout", 5*time.Minute, "espera pelo gLote_Ack (robô executando)")
	flag.Var(&items, "item", `item do lote, "campo=valor,..." (repetível; position segue a ordem)`)
	flag.Parse()

	id := int64(*batchID)
	if id == 0 {
		// Unix segundos cabe em DINT e não colide com os ids do Nest (Batch.id
		// reinicia em 1); corridas seguidas ganham ids diferentes sem reset da fila.
		id = time.Now().Unix()
	}

	parsed, err := parseItems(items)
	if err != nil {
		fmt.Println("item inválido:", err)
		os.Exit(2)
	}

	rec := queue.Record{
		BatchID: id,
		Robot:   int32(*robot),
		Acao:    int32(*acao),
		Qtd:     int32(len(parsed)),
		Items:   parsed,
	}
	// Mesmas regras de domínio do servidor, antes de tocar em qualquer coisa.
	if err := queue.Validate(rec); err != nil {
		fmt.Println("lote inválido:", err)
		os.Exit(2)
	}

	fmt.Printf("lote id=%d robot=%d acao=%d qtd=%d\n", rec.BatchID, rec.Robot, rec.Acao, rec.Qtd)
	for _, it := range rec.Items {
		fmt.Printf("  pos%d dedo=%d dedo2=%d estande=%d bandeja=%d fileira=%d coluna=%d rack=%d placa=%d slot=%d\n",
			it.Position, it.Dedo, it.Dedo2, it.Estande, it.Bandeja, it.Fileira, it.Coluna, it.Rack, it.Placa, it.Slot)
	}

	ok := false
	if *direct {
		ok = runDirect(rec, *host, *startTimeout, *ackTimeout)
	} else {
		ok = runGRPC(rec, *addr, *startTimeout+*ackTimeout)
	}
	if !ok {
		os.Exit(1)
	}
	fmt.Println("OK: lote concluído pelo CLP")
}

// --- modo -direct: handshake inline contra o CLP (espelho do dispatcher) ---

func runDirect(rec queue.Record, host string, startTimeout, ackTimeout time.Duration) bool {
	cfg := config.Load()
	if host == "" {
		host = cfg.CLPHost
	}

	ctx, cancel := context.WithTimeout(context.Background(), startTimeout+ackTimeout+30*time.Second)
	defer cancel()

	clp := plc.NewOmronNX1P2(host, cfg.CLPPort)
	if err := clp.Connect(ctx); err != nil {
		fmt.Printf("connect %s:%d falhou: %v\n", host, cfg.CLPPort, err)
		return false
	}
	defer clp.Close()
	fmt.Printf("conectado direto no CLP em %s:%d\n", host, cfg.CLPPort)

	robot := rec.Robot
	h := handshake{clp: clp, poll: 100 * time.Millisecond}

	// 1. Garante o slot do robô ocioso (Req=false, Start/Ack baixados).
	if err := h.writeBool(ctx, plc.TagLoteReq(robot), false); err != nil {
		return fail(err)
	}
	if err := h.waitBool(ctx, plc.TagLoteStart(robot), false, ackTimeout); err != nil {
		return fail(fmt.Errorf("CLP não liberou o slot do robô %d: %w", robot, err))
	}
	if err := h.waitBool(ctx, plc.TagLoteAck(robot), false, ackTimeout); err != nil {
		return fail(fmt.Errorf("CLP não liberou o slot do robô %d: %w", robot, err))
	}

	// 2. Escreve o lote e seus metadados — tudo ANTES do Req, que é o gatilho.
	data, err := plc.EncodeBatch(toBatchItems(rec))
	if err != nil {
		return fail(err)
	}
	fmt.Printf("frame %s (%d bytes): %X\n", plc.TagLote(robot), len(data), data)
	if err := clp.WriteStructRaw(ctx, plc.TagLote(robot), data); err != nil {
		return fail(fmt.Errorf("write %s: %w", plc.TagLote(robot), err))
	}
	if err := clp.WriteTag(ctx, plc.TagLoteQtd(robot), plc.TagValue{Kind: plc.KindInt, Int: rec.Qtd}); err != nil {
		return fail(fmt.Errorf("write %s: %w", plc.TagLoteQtd(robot), err))
	}
	if err := clp.WriteTag(ctx, plc.TagLoteId(robot), plc.TagValue{Kind: plc.KindDInt, DInt: rec.BatchID}); err != nil {
		return fail(fmt.Errorf("write %s: %w", plc.TagLoteId(robot), err))
	}

	// 3. Sinaliza lote pronto.
	if err := h.writeBool(ctx, plc.TagLoteReq(robot), true); err != nil {
		return fail(err)
	}
	fmt.Println("Req=TRUE — aguardando o CLP travar o lote (Start)...")

	// 4. O CLP trava o lote e começa a executar.
	if err := h.waitBool(ctx, plc.TagLoteStart(robot), true, startTimeout); err != nil {
		return fail(err)
	}
	fmt.Println("Start=TRUE — robô executando; aguardando Ack...")

	// 5. O CLP concluiu o lote.
	if err := h.waitBool(ctx, plc.TagLoteAck(robot), true, ackTimeout); err != nil {
		return fail(err)
	}
	fmt.Println("Ack=TRUE — conferindo eco do id e status dos itens")

	// 6. Confere o eco do id e colhe o status de cada item.
	echo, err := h.readInt(ctx, plc.TagLoteId(robot))
	if err != nil {
		return fail(err)
	}
	if echo != rec.BatchID {
		return fail(fmt.Errorf("eco de %s = %d, esperava %d", plc.TagLoteId(robot), echo, rec.BatchID))
	}
	allOK := true
	for _, it := range rec.Items {
		st, err := h.readInt(ctx, plc.TagLoteItemStatus(robot, int(it.Position)))
		if err != nil {
			return fail(err)
		}
		fmt.Printf("  gLote[%d].status = %d\n", it.Position, st)
		if st != plc.ItemStatusOK {
			allOK = false
		}
	}

	// 7. Confirma o recebimento e espera o CLP liberar o slot (best-effort).
	if err := h.writeBool(ctx, plc.TagLoteReq(robot), false); err != nil {
		return fail(err)
	}
	_ = h.waitBool(ctx, plc.TagLoteStart(robot), false, ackTimeout)
	_ = h.waitBool(ctx, plc.TagLoteAck(robot), false, ackTimeout)

	if !allOK {
		fmt.Println("lote concluído com itens em erro (status != 1)")
	}
	return allOK
}

func fail(err error) bool {
	fmt.Println("falha:", err)
	return false
}

// handshake agrupa os helpers de leitura/escrita usados na sequência do lote.
type handshake struct {
	clp  plc.Client
	poll time.Duration
}

func (h handshake) writeBool(ctx context.Context, tag string, v bool) error {
	if err := h.clp.WriteTag(ctx, tag, plc.TagValue{Kind: plc.KindBool, Bool: v}); err != nil {
		return fmt.Errorf("write %s: %w", tag, err)
	}
	return nil
}

func (h handshake) readInt(ctx context.Context, tag string) (int64, error) {
	v, err := h.clp.ReadTag(ctx, tag)
	if err != nil {
		return 0, fmt.Errorf("read %s: %w", tag, err)
	}
	switch v.Kind {
	case plc.KindDInt:
		return v.DInt, nil
	case plc.KindInt:
		return int64(v.Int), nil
	case plc.KindBool:
		if v.Bool {
			return 1, nil
		}
	}
	return 0, nil
}

func (h handshake) waitBool(ctx context.Context, tag string, want bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		v, err := h.readInt(ctx, tag)
		if err != nil {
			return err
		}
		if (v != 0) == want {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("%s: esperava %v: timeout após %s", tag, want, timeout)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(h.poll):
		}
	}
}

// toBatchItems posiciona os itens no array do CLP (índice 0 = gLote_RoboN[1]),
// igual ao dispatcher: posições não usadas zeradas, ação homogênea do lote.
func toBatchItems(rec queue.Record) []plc.BatchItem {
	out := make([]plc.BatchItem, plc.BatchSize)
	for _, it := range rec.Items {
		out[it.Position-1] = plc.BatchItem{
			Acao:    rec.Acao,
			Dedo:    it.Dedo,
			Dedo2:   it.Dedo2,
			Estande: it.Estande,
			Bandeja: it.Bandeja,
			Fileira: it.Fileira,
			Coluna:  it.Coluna,
			Rack:    it.Rack,
			Placa:   it.Placa,
			Slot:    it.Slot,
		}
	}
	return out
}

// --- modo padrão: pelo servidor gRPC (caminho de produção) ---

func runGRPC(rec queue.Record, addr string, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(context.Background(), timeout+30*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fail(err)
	}
	defer conn.Close()
	client := pb.NewPLCServiceClient(conn)

	// Assina ANTES do enqueue para não perder transições rápidas.
	stream, err := client.SubscribeBatchStatus(ctx, &pb.SubscribeBatchRequest{})
	if err != nil {
		return fail(fmt.Errorf("subscribe: %w", err))
	}

	req := &pb.EnqueueBatchRequest{
		BatchId: int32(rec.BatchID),
		Robot:   rec.Robot,
		Acao:    rec.Acao,
		Qtd:     rec.Qtd,
		Items:   toProtoItems(rec.Items),
	}
	fmt.Printf("enfileirando no servidor %s\n", addr)
	resp, err := client.EnqueueBatch(ctx, req)
	if err != nil {
		return fail(fmt.Errorf("enqueue: %w", err))
	}
	if !resp.GetAccepted() {
		return fail(fmt.Errorf("lote recusado: %s", resp.GetMessage()))
	}
	if resp.GetDuplicate() {
		fmt.Println("aviso: lote duplicado (mesmo robot+id) — acompanhando o registro existente")
	}

	// Acompanha só o nosso lote; o replay pode trazer lotes antigos de outros clientes.
	for {
		up, err := stream.Recv()
		if err != nil {
			return fail(fmt.Errorf("stream encerrado antes do desfecho: %w", err))
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
			return up.GetState() == pb.BatchState_BQ_ACKED_OK
		}
	}
}

func toProtoItems(items []queue.Item) []*pb.TComandoMsg {
	out := make([]*pb.TComandoMsg, 0, len(items))
	for _, it := range items {
		out = append(out, &pb.TComandoMsg{
			Position: it.Position,
			Dedo:     it.Dedo,
			Dedo_2:   it.Dedo2,
			Estande:  it.Estande,
			Bandeja:  it.Bandeja,
			Fileira:  it.Fileira,
			Coluna:   it.Coluna,
			Rack:     it.Rack,
			Placa:    it.Placa,
			Slot:     it.Slot,
		})
	}
	return out
}

// parseItems converte cada -item "campo=valor,..." num queue.Item. Campos
// omitidos ficam 0 (é o que o CLP espera para o grupo não-aplicável à ação).
func parseItems(raw []string) ([]queue.Item, error) {
	out := make([]queue.Item, 0, len(raw))
	for i, spec := range raw {
		it := queue.Item{Position: int32(i + 1)}
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
				it.Dedo2 = val
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
