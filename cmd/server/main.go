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

	clp := plc.NewOmronNX1P2(cfg.CLPHost, cfg.CLPPort)
	if err := clp.Connect(ctx); err != nil {
		log.Error("plc connect", "host", cfg.CLPHost, "port", cfg.CLPPort, "err", err)
		os.Exit(1)
	}
	log.Info("plc connected", "host", cfg.CLPHost, "port", cfg.CLPPort)
	defer clp.Close()

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

	// Fila durável de comandos (bbolt) + dispatcher serial com handshake/ack.
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
		AckTimeout: time.Duration(cfg.AckTimeoutMS) * time.Millisecond,
		MaxRetries: cfg.MaxRetries,
		PollEvery:  time.Duration(cfg.CmdPollMS) * time.Millisecond,
	})
	if err := q.Recover(ctx); err != nil {
		log.Error("queue recover", "err", err)
	}
	go q.Run(ctx)

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
