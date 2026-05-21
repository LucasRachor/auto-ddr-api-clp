package server

import (
	"context"
	"time"

	"go_auto_ddr_clp/internal/plc"
)

func (s *PLCServer) doReadTag(ctx context.Context, tag string) (plc.TagValue, error) {
	return s.PLC.ReadTag(ctx, tag)
}

func (s *PLCServer) doWriteTag(ctx context.Context, tag string, v plc.TagValue) error {
	return s.PLC.WriteTag(ctx, tag, v)
}

// SubscribeLoop é o motor do RPC SubscribeTags (server-streaming).
//
// Uso esperado dentro do handler gerado:
//
//   func (s *PLCServer) SubscribeTags(req *pb.SubscribeTagsRequest, stream pb.PLCService_SubscribeTagsServer) error {
//       return s.SubscribeLoop(stream.Context(), req.Tags, time.Duration(req.PollMs)*time.Millisecond,
//           func(tag string, v plc.TagValue, ts time.Time) error {
//               return stream.Send(&pb.TagUpdate{ Tag: tag, Value: toProto(v), TsUnixMs: ts.UnixMilli() })
//           })
//   }
func (s *PLCServer) SubscribeLoop(
	ctx context.Context,
	tags []string,
	poll time.Duration,
	send func(tag string, v plc.TagValue, ts time.Time) error,
) error {
	if poll <= 0 {
		poll = 100 * time.Millisecond
	}
	t := time.NewTicker(poll)
	defer t.Stop()

	last := make(map[string]plc.TagValue, len(tags))
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case now := <-t.C:
			for _, tag := range tags {
				v, err := s.PLC.ReadTag(ctx, tag)
				if err != nil {
					s.Log.Warn("read failed", "tag", tag, "err", err)
					continue
				}
				if prev, ok := last[tag]; ok && prev == v {
					continue
				}
				last[tag] = v
				if err := send(tag, v, now); err != nil {
					return err
				}
			}
		}
	}
}
