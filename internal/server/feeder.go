package server

import (
	"context"
	"time"

	pb "go_auto_ddr_clp/gen/plc/v1"
	"go_auto_ddr_clp/internal/plc"
)

// Presença de bandeja (gBandejaPresente_RoboN / TStatusAlimentador).
//
// Não há handshake: o CLP mantém os 8 BOOLs sempre atualizados pelos sensores e
// este servidor polla membro a membro (decodeTagValue recusa o UDT inteiro;
// leitura por nome de membro é o caminho comprovado no NX1P2). Cada frame
// emitido carrega o snapshot COMPLETO do robô — nunca delta — para que o
// consumidor seja idempotente e se auto-corrija em qualquer reconexão.
const (
	feederDefaultPoll = 500 * time.Millisecond
	feederMinPoll     = 100 * time.Millisecond
)

// feederPositions lista as 8 posições na ordem canônica (estande, bandeja).
type feederPosition struct{ estande, bandeja int32 }

func feederPositions() []feederPosition {
	out := make([]feederPosition, 0, plc.FeederEstandes*plc.FeederBandejasPorEstande)
	for e := int32(1); e <= plc.FeederEstandes; e++ {
		for b := int32(1); b <= plc.FeederBandejasPorEstande; b++ {
			out = append(out, feederPosition{e, b})
		}
	}
	return out
}

// readFeeder lê as 8 posições de um robô, membro a membro.
func (s *PLCServer) readFeeder(ctx context.Context, robot int32) ([]*pb.TrayPresence, error) {
	positions := feederPositions()
	trays := make([]*pb.TrayPresence, 0, len(positions))
	for _, p := range positions {
		v, err := s.PLC.ReadTag(ctx, plc.TagBandejaPresenteMember(robot, p.estande, p.bandeja))
		if err != nil {
			return nil, err
		}
		trays = append(trays, &pb.TrayPresence{
			Estande: p.estande,
			Bandeja: p.bandeja,
			Present: v.Kind == plc.KindBool && v.Bool,
		})
	}
	return trays, nil
}

func (s *PLCServer) GetFeederStatus(ctx context.Context, _ *pb.GetFeederStatusRequest) (*pb.FeederStatusSnapshot, error) {
	now := time.Now().UnixMilli()
	snap := &pb.FeederStatusSnapshot{}
	for _, robot := range plc.Robots {
		trays, err := s.readFeeder(ctx, robot)
		if err != nil {
			return nil, err
		}
		snap.Robots = append(snap.Robots, &pb.FeederStatusUpdate{
			Robot: robot, Trays: trays, TsUnixMs: now,
		})
	}
	return snap, nil
}

func (s *PLCServer) SubscribeFeederStatus(req *pb.SubscribeFeederStatusRequest, stream pb.PLCService_SubscribeFeederStatusServer) error {
	ctx := stream.Context()
	poll := time.Duration(req.GetPollMs()) * time.Millisecond
	if poll <= 0 {
		poll = feederDefaultPoll
	}
	if poll < feederMinPoll {
		poll = feederMinPoll
	}

	// last guarda o retrato enviado por robô, como string canônica "01010101".
	// Frame inicial: last vazio difere de qualquer leitura, então o primeiro tick
	// sempre emite — mas sem esperar o primeiro tick inteiro, lemos já na entrada.
	last := make(map[int32]string, len(plc.Robots))

	emit := func() error {
		now := time.Now().UnixMilli()
		for _, robot := range plc.Robots {
			trays, err := s.readFeeder(ctx, robot)
			if err != nil {
				// Mantém o último estado e segue: uma falha transitória de leitura
				// não derruba o stream (mesmo espírito do SubscribeLoop).
				s.Log.Warn("feeder read failed", "robot", robot, "err", err)
				continue
			}
			key := feederKey(trays)
			if prev, ok := last[robot]; ok && prev == key {
				continue
			}
			last[robot] = key
			if err := stream.Send(&pb.FeederStatusUpdate{
				Robot: robot, Trays: trays, TsUnixMs: now,
			}); err != nil {
				return err
			}
		}
		return nil
	}

	if err := emit(); err != nil {
		return err
	}

	t := time.NewTicker(poll)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := emit(); err != nil {
				return err
			}
		}
	}
}

func feederKey(trays []*pb.TrayPresence) string {
	key := make([]byte, len(trays))
	for i, tr := range trays {
		if tr.GetPresent() {
			key[i] = '1'
		} else {
			key[i] = '0'
		}
	}
	return string(key)
}
