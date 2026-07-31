package server

import (
	"context"
	pb "go_auto_ddr_clp/gen/plc/v1"
)

// Após `protoc`, troque as assinaturas por:
//   func (s *PLCServer) Start(ctx context.Context, _ *pb.StartRequest) (*pb.CommandResponse, error)
// e por aí vai. As implementações abaixo já usam s.PLC, basta empacotar a resposta.

func (s *PLCServer) doStart(ctx context.Context, _ *pb.StartRequest) error {
	s.Log.Info("rpc Start")
	return s.PLC.Start(ctx)
}

func (s *PLCServer) doStop(ctx context.Context) error {
	s.Log.Info("rpc Stop")
	return s.PLC.Stop(ctx)
}

func (s *PLCServer) doReset(ctx context.Context) error {
	s.Log.Info("rpc Reset")
	return s.PLC.Reset(ctx)
}
