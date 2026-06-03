package main

import (
	"context"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/keepalive"

	pb "go_auto_ddr_clp/gen/plc/v1"
	"go_auto_ddr_clp/internal/config"
	"go_auto_ddr_clp/internal/plc"
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

	srv := server.New(clp, log)
	_ = srv

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
