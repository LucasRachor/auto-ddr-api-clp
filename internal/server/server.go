package server

import (
	"log/slog"

	pb "go_auto_ddr_clp/gen/plc/v1" // disponível após `protoc`
	"go_auto_ddr_clp/internal/plc"
	"go_auto_ddr_clp/internal/queue"
)

// PLCServer implementa pb.PLCServiceServer.
//
// Após gerar os stubs protobuf, descomente o embed abaixo:
type PLCServer struct {
	pb.PLCServiceServer

	PLC   plc.Client
	Queue *queue.Queue
	Log   *slog.Logger
}

// type PLCServer struct {
// 	pb.UnimplementedPLCServiceServer

// 	PLC plc.Client
// 	Log *slog.Logger
// }

// func New(c plc.Client, log *slog.Logger) *PLCServer {
// 	return &PLCServer{PLC: c, Log: log}
// }

// func New(c plc.Client, q *queue.Queue, log *slog.Logger) *PLCServer {
// 	return &PLCServer{PLC: c, Queue: q, Log: log}
// }

// type PLCServer struct {
// 	pb.PLCServiceServer

// 	PLC   plc.Client
// 	Queue *queue.Queue
// 	Log   *slog.Logger
// }

func New(c plc.Client, q *queue.Queue, log *slog.Logger) *PLCServer {
	return &PLCServer{PLC: c, Queue: q, Log: log}
}
