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

// startServer sobe o PLCServer real (Mock + fila durável) sobre bufconn e
// devolve um client gRPC conectado.
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
	go q.Run(ctx)

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

func TestEndToEnd_EnqueueAndStatusStream(t *testing.T) {
	client := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Assina o stream de status antes de enfileirar.
	stream, err := client.SubscribeCommandStatus(ctx, &pb.SubscribeCommandStatusRequest{})
	if err != nil {
		t.Fatalf("SubscribeCommandStatus: %v", err)
	}

	resp, err := client.EnqueueCommand(ctx, &pb.EnqueueCommandRequest{
		CommandId: 101, Action: "insert", Motherboard: "MB09", Robot: 1, Finger: 1,
	})
	if err != nil {
		t.Fatalf("EnqueueCommand: %v", err)
	}
	if !resp.GetAccepted() {
		t.Fatalf("comando recusado: %s", resp.GetMessage())
	}

	// Espera a sequência terminar em ACKED_OK (Mock confirma o handshake).
	var last pb.CommandState
	for {
		upd, err := stream.Recv()
		if err != nil {
			t.Fatalf("stream.Recv: %v (último estado: %v)", err, last)
		}
		if upd.GetCommandId() != 101 {
			continue
		}
		last = upd.GetState()
		if last == pb.CommandState_ACKED_OK {
			return
		}
		if last == pb.CommandState_ACKED_ERROR || last == pb.CommandState_FAILED {
			t.Fatalf("estado terminal inesperado: %v (%s)", last, upd.GetDetail())
		}
	}
}

func TestEndToEnd_RejectsInvalid(t *testing.T) {
	client := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.EnqueueCommand(ctx, &pb.EnqueueCommandRequest{
		CommandId: 1, Action: "insert", Motherboard: "MB09", Robot: 9, Finger: 1,
	})
	if err != nil {
		t.Fatalf("EnqueueCommand: %v", err)
	}
	if resp.GetAccepted() {
		t.Fatalf("esperava recusa por robot inválido")
	}
}
