package server

import (
	"log/slog"

	pb "go_auto_ddr_clp/gen/plc/v1" // disponível após `protoc`
	"go_auto_ddr_clp/internal/plc"
)

// PLCServer implementa pb.PLCServiceServer.
//
// Após gerar os stubs protobuf, descomente o embed abaixo:
type PLCServer struct {
	pb.UnimplementedPLCServiceServer

	PLC plc.Client
	Log *slog.Logger
}

func New(c plc.Client, log *slog.Logger) *PLCServer {
	return &PLCServer{PLC: c, Log: log}
}
