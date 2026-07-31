package server

import (
	"context"
	"time"

	pb "go_auto_ddr_clp/gen/plc/v1"
	"go_auto_ddr_clp/internal/plc"
)

// Este arquivo implementa a interface pb.PLCServiceServer de fato. Sem estes
// métodos com a assinatura exata gerada pelo protoc, o embed de
// pb.UnimplementedPLCServiceServer assume e todo RPC retorna
// "12 UNIMPLEMENTED: method X not implemented". Cada handler só empacota a
// resposta sobre os helpers do* já existentes.

func (s *PLCServer) Start(ctx context.Context, req *pb.StartRequest) (*pb.CommandResponse, error) {
	if err := s.doStart(ctx, req); err != nil {
		return &pb.CommandResponse{Ok: false, Message: err.Error()}, nil
	}
	return &pb.CommandResponse{Ok: true}, nil
}

func (s *PLCServer) Stop(ctx context.Context, _ *pb.StopRequest) (*pb.CommandResponse, error) {
	if err := s.doStop(ctx); err != nil {
		return &pb.CommandResponse{Ok: false, Message: err.Error()}, nil
	}
	return &pb.CommandResponse{Ok: true}, nil
}

func (s *PLCServer) Reset(ctx context.Context, _ *pb.ResetRequest) (*pb.CommandResponse, error) {
	if err := s.doReset(ctx); err != nil {
		return &pb.CommandResponse{Ok: false, Message: err.Error()}, nil
	}
	return &pb.CommandResponse{Ok: true}, nil
}

func (s *PLCServer) ReadTag(ctx context.Context, req *pb.ReadTagRequest) (*pb.ReadTagResponse, error) {
	v, err := s.doReadTag(ctx, req.GetTag())
	if err != nil {
		return nil, err
	}
	return &pb.ReadTagResponse{Tag: req.GetTag(), Value: toProto(v)}, nil
}

func (s *PLCServer) WriteTag(ctx context.Context, req *pb.WriteTagRequest) (*pb.CommandResponse, error) {
	if err := s.doWriteTag(ctx, req.GetTag(), fromProto(req.GetValue())); err != nil {
		return &pb.CommandResponse{Ok: false, Message: err.Error()}, nil
	}
	return &pb.CommandResponse{Ok: true}, nil
}

func (s *PLCServer) GetStatus(ctx context.Context, _ *pb.GetStatusRequest) (*pb.StatusResponse, error) {
	st, err := s.doStatus(ctx)
	if err != nil {
		return nil, err
	}
	return &pb.StatusResponse{Mode: st.Mode, Faulted: st.Faulted, Detail: st.Detail}, nil
}

func (s *PLCServer) Healthcheck(ctx context.Context, _ *pb.HealthcheckRequest) (*pb.HealthcheckResponse, error) {
	return &pb.HealthcheckResponse{Serving: s.doHealthcheck(ctx)}, nil
}

func (s *PLCServer) SubscribeTags(req *pb.SubscribeTagsRequest, stream pb.PLCService_SubscribeTagsServer) error {
	return s.SubscribeLoop(stream.Context(), req.GetTags(),
		time.Duration(req.GetPollMs())*time.Millisecond,
		func(tag string, v plc.TagValue, ts time.Time) error {
			return stream.Send(&pb.TagUpdate{
				Tag:      tag,
				Value:    toProto(v),
				TsUnixMs: ts.UnixMilli(),
			})
		})
}

// toProto converte o TagValue do domínio para a mensagem protobuf (oneof).
func toProto(v plc.TagValue) *pb.TagValue {
	switch v.Kind {
	case plc.KindBool:
		return &pb.TagValue{Kind: &pb.TagValue_BoolVal{BoolVal: v.Bool}}
	case plc.KindInt:
		return &pb.TagValue{Kind: &pb.TagValue_IntVal{IntVal: v.Int}}
	case plc.KindDInt:
		return &pb.TagValue{Kind: &pb.TagValue_DintVal{DintVal: v.DInt}}
	case plc.KindReal:
		return &pb.TagValue{Kind: &pb.TagValue_RealVal{RealVal: v.Real}}
	case plc.KindString:
		return &pb.TagValue{Kind: &pb.TagValue_StringVal{StringVal: v.String}}
	default:
		return &pb.TagValue{}
	}
}

// fromProto converte a mensagem protobuf (oneof) para o TagValue do domínio.
func fromProto(v *pb.TagValue) plc.TagValue {
	switch v.GetKind().(type) {
	case *pb.TagValue_BoolVal:
		return plc.TagValue{Kind: plc.KindBool, Bool: v.GetBoolVal()}
	case *pb.TagValue_IntVal:
		return plc.TagValue{Kind: plc.KindInt, Int: v.GetIntVal()}
	case *pb.TagValue_DintVal:
		return plc.TagValue{Kind: plc.KindDInt, DInt: v.GetDintVal()}
	case *pb.TagValue_RealVal:
		return plc.TagValue{Kind: plc.KindReal, Real: v.GetRealVal()}
	case *pb.TagValue_StringVal:
		return plc.TagValue{Kind: plc.KindString, String: v.GetStringVal()}
	default:
		return plc.TagValue{Kind: plc.KindUnknown}
	}
}
