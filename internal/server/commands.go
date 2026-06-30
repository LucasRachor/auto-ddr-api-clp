package server

import (
	"context"

	pb "go_auto_ddr_clp/gen/plc/v1"
	"go_auto_ddr_clp/internal/queue"
)

// EnqueueCommand valida e enfileira um comando vindo do Nest. O id é fornecido
// pelo Nest e correlaciona o registro com o ack do CLP.
func (s *PLCServer) EnqueueCommand(_ context.Context, req *pb.EnqueueCommandRequest) (*pb.EnqueueCommandResponse, error) {
	rec, err := s.Queue.Enqueue(req.GetCommandId(), req.GetAction(), req.GetMotherboard(), req.GetRobot(), req.GetFinger())
	if err != nil {
		return &pb.EnqueueCommandResponse{Accepted: false, CommandId: req.GetCommandId(), Message: err.Error()}, nil
	}
	return &pb.EnqueueCommandResponse{Accepted: true, CommandId: rec.ID}, nil
}

// SubscribeCommandStatus transmite as transições de estado dos comandos. Ao
// conectar, faz replay de tudo que ainda não foi confirmado pelo Nest
// (ConfirmStatus), garantindo que nenhum status se perca em reconexões.
func (s *PLCServer) SubscribeCommandStatus(_ *pb.SubscribeCommandStatusRequest, stream pb.PLCService_SubscribeCommandStatusServer) error {
	ctx := stream.Context()

	// Assina antes do replay para não perder transições que ocorram no meio.
	ch, cancel := s.Queue.Subscribe(64)
	defer cancel()

	pending, err := s.Queue.ListUndelivered()
	if err != nil {
		return err
	}
	seen := make(map[int64]bool, len(pending))
	for _, rec := range pending {
		seen[rec.ID] = true
		if err := stream.Send(toStatusUpdate(rec)); err != nil {
			return err
		}
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case rec, ok := <-ch:
			if !ok {
				return nil
			}
			if err := stream.Send(toStatusUpdate(rec)); err != nil {
				return err
			}
		}
	}
}

// ConfirmStatus marca o status de um comando como entregue ao Nest, removendo-o
// do replay de reconexões.
func (s *PLCServer) ConfirmStatus(_ context.Context, req *pb.ConfirmStatusRequest) (*pb.CommandResponse, error) {
	if err := s.Queue.ConfirmDelivery(req.GetCommandId()); err != nil {
		return &pb.CommandResponse{Ok: false, Message: err.Error()}, nil
	}
	return &pb.CommandResponse{Ok: true}, nil
}

func toStatusUpdate(rec queue.Record) *pb.CommandStatusUpdate {
	return &pb.CommandStatusUpdate{
		CommandId: rec.ID,
		State:     toProtoState(rec.State),
		Detail:    rec.Detail,
		TsUnixMs:  rec.UpdatedAt,
	}
}

func toProtoState(s queue.State) pb.CommandState {
	switch s {
	case queue.StateQueued:
		return pb.CommandState_QUEUED
	case queue.StateDispatched:
		return pb.CommandState_DISPATCHED
	case queue.StateAckedOK:
		return pb.CommandState_ACKED_OK
	case queue.StateAckedError:
		return pb.CommandState_ACKED_ERROR
	case queue.StateFailed:
		return pb.CommandState_FAILED
	default:
		return pb.CommandState_COMMAND_STATE_UNSPECIFIED
	}
}
