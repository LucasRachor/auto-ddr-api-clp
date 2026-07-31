package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"

	//"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "go_auto_ddr_clp/gen/plc/v1"
	"go_auto_ddr_clp/internal/config"
	"go_auto_ddr_clp/internal/plc"
	"go_auto_ddr_clp/internal/queue"
	"go_auto_ddr_clp/internal/server"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	cfg := config.Load()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var clp plc.Client
	if cfg.Mock {
		clp = plc.NewMock()
		log.Warn("CLP_MOCK=1: CLP simulado em memória (sem hardware)")
	} else {
		clp = plc.NewOmronNX1P2(cfg.CLPHost, cfg.CLPPort)
	}
	if err := clp.Connect(ctx); err != nil {
		log.Error("plc connect", "host", cfg.CLPHost, "port", cfg.CLPPort, "err", err)
		os.Exit(1)
	}
	if !cfg.Mock {
		log.Info("plc connected", "host", cfg.CLPHost, "port", cfg.CLPPort)
	}
	defer clp.Close()

	// Mantém a sessão EtherNet-IP aquecida: o NX1P2 derruba conexões CIP ociosas,
	// então uma leitura leve periódica evita o reset e reconecta proativamente.
	// go clp.KeepAlive(ctx, 15*time.Second, log)

	// srv := server.New(clp, log)
	// _ = srv

	//   $env:DEBUG_TAG="gSys_Mode,IHM_Input00,IHM_Input[0]"; go run .\cmd\server\main.go
	// if tags := "RoboOUT"; tags != "" {
	// 	for _, tag := range strings.Split(tags, ",") {
	// 		tag = strings.TrimSpace(tag)
	// 		if tag == "" {
	// 			continue
	// 		}
	// 		if err := clp.ReadTagDebug(ctx, tag); err != nil {
	// 			log.Warn("readTagDebug", "tag", tag, "err", err)
	// 		}
	// 	}
	// }

	// Fila durável de lotes (bbolt) + um dispatcher por robô, cada um serializando
	// o handshake do seu slot gLote_*_RoboN.
	if dir := filepath.Dir(cfg.QueueDBPath); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Error("queue db dir", "path", dir, "err", err)
			os.Exit(1)
		}
	}
	store, err := queue.OpenStore(cfg.QueueDBPath)
	if err != nil {
		log.Error("open queue store", "path", cfg.QueueDBPath, "err", err)
		os.Exit(1)
	}
	defer store.Close()

	q := queue.New(store, clp, log, queue.Config{
		AckTimeout:   time.Duration(cfg.AckTimeoutMS) * time.Millisecond,
		StartTimeout: time.Duration(cfg.StartTimeoutMS) * time.Millisecond,
		MaxRetries:   cfg.MaxRetries,
		PollEvery:    time.Duration(cfg.CmdPollMS) * time.Millisecond,
	})
	if err := q.Recover(ctx); err != nil {
		log.Error("queue recover", "err", err)
	}
	for _, robot := range plc.Robots {
		go q.Run(ctx, robot)
	}

	srv := server.New(clp, q, log)

	gs := grpc.NewServer(
		grpc.KeepaliveParams(keepalive.ServerParameters{
			Time:    30 * time.Second,
			Timeout: 10 * time.Second,
		}),
	)

	pb.RegisterPLCServiceServer(gs, srv) // após gerar stubs

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		log.Error("listen", "addr", cfg.GRPCAddr, "err", err)
		os.Exit(1)
	}

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		gs.GracefulStop()
	}()

	log.Info("plc-api listening", "addr", cfg.GRPCAddr, "clp", cfg.CLPHost)
	if err := gs.Serve(lis); err != nil {
		log.Error("serve", "err", err)
		os.Exit(1)
	}
}
