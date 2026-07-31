package server

import (
	"context"

	pb "go_auto_ddr_clp/gen/plc/v1"
	"go_auto_ddr_clp/internal/queue"
)

// EnqueueBatch valida e enfileira um lote vindo do Nest. O batch_id é fornecido
// pelo Nest e, junto do robot, correlaciona o registro com o ack do CLP.
func (s *PLCServer) EnqueueBatch(_ context.Context, req *pb.EnqueueBatchRequest) (*pb.EnqueueBatchResponse, error) {
	rec, isNew, err := s.Queue.Enqueue(fromBatchRequest(req))
	if err != nil {
		return &pb.EnqueueBatchResponse{Accepted: false, BatchId: req.GetBatchId(), Message: err.Error()}, nil
	}
	// Duplicado continua Accepted (reenviar o mesmo lote é sucesso idempotente),
	// mas o flag deixa o chamador separar isso de um despacho novo.
	if !isNew {
		return &pb.EnqueueBatchResponse{
			Accepted:  true,
			BatchId:   int32(rec.BatchID),
			Duplicate: true,
			Message:   "lote já existente para este robot+batch_id; não foi despachado de novo",
		}, nil
	}
	return &pb.EnqueueBatchResponse{Accepted: true, BatchId: int32(rec.BatchID)}, nil
}

// SubscribeBatchStatus transmite as transições de estado dos lotes. Ao conectar,
// faz replay de tudo que ainda não foi confirmado pelo Nest (ConfirmBatchStatus),
// garantindo que nenhum status se perca em reconexões.
func (s *PLCServer) SubscribeBatchStatus(_ *pb.SubscribeBatchRequest, stream pb.PLCService_SubscribeBatchStatusServer) error {
	ctx := stream.Context()

	// Assina antes do replay para não perder transições que ocorram no meio.
	ch, cancel := s.Queue.Subscribe(64)
	defer cancel()

	pending, err := s.Queue.ListUndelivered()
	if err != nil {
		return err
	}
	for _, rec := range pending {
		if err := stream.Send(toBatchUpdate(rec)); err != nil {
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
			if err := stream.Send(toBatchUpdate(rec)); err != nil {
				return err
			}
		}
	}
}

// ConfirmBatchStatus marca o status de um lote como entregue ao Nest, removendo-o
// do replay de reconexões.
func (s *PLCServer) ConfirmBatchStatus(_ context.Context, req *pb.ConfirmBatchRequest) (*pb.CommandResponse, error) {
	if err := s.Queue.ConfirmDelivery(req.GetRobot(), int64(req.GetBatchId())); err != nil {
		return &pb.CommandResponse{Ok: false, Message: err.Error()}, nil
	}
	return &pb.CommandResponse{Ok: true}, nil
}

// fromBatchRequest converte a mensagem protobuf no Record do domínio. Não valida:
// quem valida é queue.Enqueue.
func fromBatchRequest(req *pb.EnqueueBatchRequest) queue.Record {
	items := make([]queue.Item, 0, len(req.GetItems()))
	for _, it := range req.GetItems() {
		items = append(items, queue.Item{
			Position: it.GetPosition(),
			Dedo:     it.GetDedo(),
			Dedo2:    it.GetDedo_2(),
			Estande:  it.GetEstande(),
			Bandeja:  it.GetBandeja(),
			Fileira:  it.GetFileira(),
			Coluna:   it.GetColuna(),
			Rack:     it.GetRack(),
			Placa:    it.GetPlaca(),
			Slot:     it.GetSlot(),
		})
	}
	return queue.Record{
		BatchID: int64(req.GetBatchId()),
		Robot:   req.GetRobot(),
		Acao:    req.GetAcao(),
		Qtd:     req.GetQtd(),
		Items:   items,
	}
}

func toBatchUpdate(rec queue.Record) *pb.BatchStatusUpdate {
	items := make([]*pb.ItemStatusMsg, 0, len(rec.Items))
	for _, it := range rec.Items {
		items = append(items, &pb.ItemStatusMsg{Position: it.Position, Status: it.Status})
	}
	return &pb.BatchStatusUpdate{
		BatchId:  int32(rec.BatchID),
		Robot:    rec.Robot,
		State:    toProtoState(rec.State),
		Items:    items,
		Detail:   rec.Detail,
		TsUnixMs: rec.UpdatedAt,
	}
}

func toProtoState(s queue.State) pb.BatchState {
	switch s {
	case queue.StateQueued:
		return pb.BatchState_BQ_QUEUED
	case queue.StateDispatched:
		return pb.BatchState_BQ_DISPATCHED
	case queue.StateStarted:
		return pb.BatchState_BQ_STARTED
	case queue.StateAckedOK:
		return pb.BatchState_BQ_ACKED_OK
	case queue.StateAckedError:
		return pb.BatchState_BQ_ACKED_ERROR
	case queue.StateFailed:
		return pb.BatchState_BQ_FAILED
	default:
		return pb.BatchState_BQ_QUEUED
	}
}
