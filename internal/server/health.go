package server

import (
	"context"

	"go_auto_ddr_clp/internal/plc"
)

func (s *PLCServer) doStatus(ctx context.Context) (plc.Status, error) {
	return s.PLC.Status(ctx)
}

func (s *PLCServer) doHealthcheck(_ context.Context) bool {
	return s.PLC != nil
}
