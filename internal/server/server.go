package server

import (
	"log/slog"

	"go_auto_ddr_clp/internal/plc"
	// pb "go_auto_ddr_clp/gen/plc/v1" // disponível após `protoc`
)

// PLCServer implementa pb.PLCServiceServer.
//
// Após gerar os stubs protobuf, descomente o embed abaixo:
//   pb.UnimplementedPLCServiceServer
type PLCServer struct {
	// pb.UnimplementedPLCServiceServer

	PLC plc.Client
	Log *slog.Logger
}

func New(c plc.Client, log *slog.Logger) *PLCServer {
	return &PLCServer{PLC: c, Log: log}
}
