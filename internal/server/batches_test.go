package server

import (
	"context"
	"io"
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	pb "go_auto_ddr_clp/gen/plc/v1"
	"go_auto_ddr_clp/internal/plc"
	"go_auto_ddr_clp/internal/queue"
)

// startServer sobe o PLCServer real (Mock + fila durável + um dispatcher por robô)
// sobre bufconn e devolve um client gRPC conectado.
func startServer(t *testing.T) pb.PLCServiceClient {
	t.Helper()
	store, err := queue.OpenStore(filepath.Join(t.TempDir(), "queue.db"))
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	t.Cleanup(func() { store.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	clp := plc.NewMock()
	q := queue.New(store, clp, log, queue.Config{AckTimeout: time.Second, PollEvery: 5 * time.Millisecond})

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for _, robot := range plc.Robots {
		go q.Run(ctx, robot)
	}

	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	pb.RegisterPLCServiceServer(gs, New(clp, q, log))
	go gs.Serve(lis)
	t.Cleanup(gs.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return pb.NewPLCServiceClient(conn)
}

func TestEndToEnd_EnqueueBatchAndStatusStream(t *testing.T) {
	client := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Assina o stream de status antes de enfileirar.
	stream, err := client.SubscribeBatchStatus(ctx, &pb.SubscribeBatchRequest{})
	if err != nil {
		t.Fatalf("SubscribeBatchStatus: %v", err)
	}

	resp, err := client.EnqueueBatch(ctx, &pb.EnqueueBatchRequest{
		BatchId: 101, Robot: 1, Acao: 1, Qtd: 3,
		Items: []*pb.TComandoMsg{
			{Position: 1, Dedo: 1, Estande: 1, Bandeja: 1, Fileira: 1, Coluna: 1},
			{Position: 2, Dedo: 2, Estande: 1, Bandeja: 1, Fileira: 1, Coluna: 2},
			{Position: 3, Dedo: 3, Estande: 1, Bandeja: 1, Fileira: 1, Coluna: 3},
		},
	})
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("lote recusado: %s", resp.GetMessage())
	}

	// Espera a sequência terminar em ACKED_OK (o Mock confirma o handshake).
	var last pb.BatchState
	for {
		upd, err := stream.Recv()
		if err != nil {
			t.Fatalf("stream.Recv: %v (último estado: %v)", err, last)
		}
		if upd.GetBatchId() != 101 {
			continue
		}
		last = upd.GetState()
		switch last {
		case pb.BatchState_BQ_ACKED_OK:
			if n := len(upd.GetItems()); n != 3 {
				t.Fatalf("items = %d, esperava 3", n)
			}
			for _, it := range upd.GetItems() {
				if it.GetStatus() != plc.ItemStatusOK {
					t.Fatalf("position %d: status = %d, esperava OK", it.GetPosition(), it.GetStatus())
				}
			}
			if _, err := client.ConfirmBatchStatus(ctx, &pb.ConfirmBatchRequest{BatchId: 101, Robot: 1}); err != nil {
				t.Fatalf("ConfirmBatchStatus: %v", err)
			}
			return
		case pb.BatchState_BQ_ACKED_ERROR, pb.BatchState_BQ_FAILED:
			t.Fatalf("estado terminal inesperado: %v (%s)", last, upd.GetDetail())
		}
	}
}

// Um batch_id reusado é aceito (idempotência) mas NÃO é despachado; o flag
// duplicate é o que deixa o Nest enxergar a colisão em vez de esperar para sempre
// por um status que nunca virá.
func TestEndToEnd_DuplicateBatchIdIsFlagged(t *testing.T) {
	client := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := &pb.EnqueueBatchRequest{
		BatchId: 501, Robot: 1, Acao: 1, Qtd: 1,
		Items: []*pb.TComandoMsg{{Position: 1, Dedo: 1, Estande: 1, Bandeja: 1, Fileira: 1, Coluna: 1}},
	}
	first, err := client.EnqueueBatch(ctx, req)
	if err != nil {
		t.Fatalf("EnqueueBatch 1: %v", err)
	}
	if !first.GetAccepted() || first.GetDuplicate() {
		t.Fatalf("primeiro enqueue: accepted=%v duplicate=%v, esperava true/false",
			first.GetAccepted(), first.GetDuplicate())
	}

	// Mesmo (robot, batch_id) com conteúdo diferente: colisão, não retry.
	req.Items[0].Coluna = 9
	second, err := client.EnqueueBatch(ctx, req)
	if err != nil {
		t.Fatalf("EnqueueBatch 2: %v", err)
	}
	if !second.GetAccepted() {
		t.Fatalf("segundo enqueue recusado: %s", second.GetMessage())
	}
	if !second.GetDuplicate() {
		t.Fatal("duplicate = false ao reusar (robot, batch_id), esperava true")
	}
}

func TestEndToEnd_RejectsInvalid(t *testing.T) {
	client := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.EnqueueBatch(ctx, &pb.EnqueueBatchRequest{
		BatchId: 1, Robot: 9, Acao: 1, Qtd: 1,
		Items: []*pb.TComandoMsg{{Position: 1, Dedo: 1, Estande: 1, Bandeja: 1, Fileira: 1, Coluna: 1}},
	})
	if err != nil {
		t.Fatalf("EnqueueBatch: %v", err)
	}
	if resp.GetAccepted() {
		t.Fatal("esperava recusa por robot inválido")
	}
}
