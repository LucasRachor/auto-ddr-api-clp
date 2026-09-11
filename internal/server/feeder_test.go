package server

import (
	"context"
	"testing"
	"time"

	pb "go_auto_ddr_clp/gen/plc/v1"
	"go_auto_ddr_clp/internal/plc"
)

func writeMember(t *testing.T, client pb.PLCServiceClient, ctx context.Context, robot, estande, bandeja int32, present bool) {
	t.Helper()
	resp, err := client.WriteTag(ctx, &pb.WriteTagRequest{
		Tag:   plc.TagBandejaPresenteMember(robot, estande, bandeja),
		Value: &pb.TagValue{Kind: &pb.TagValue_BoolVal{BoolVal: present}},
	})
	if err != nil {
		t.Fatalf("WriteTag: %v", err)
	}
	if !resp.GetOk() {
		t.Fatalf("WriteTag recusado: %s", resp.GetMessage())
	}
}

func presenceOf(t *testing.T, upd *pb.FeederStatusUpdate, estande, bandeja int32) bool {
	t.Helper()
	for _, tr := range upd.GetTrays() {
		if tr.GetEstande() == estande && tr.GetBandeja() == bandeja {
			return tr.GetPresent()
		}
	}
	t.Fatalf("posição (%d,%d) ausente do update do robô %d", estande, bandeja, upd.GetRobot())
	return false
}

func TestGetFeederStatus_DefaultsToAbsent(t *testing.T) {
	client := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	snap, err := client.GetFeederStatus(ctx, &pb.GetFeederStatusRequest{})
	if err != nil {
		t.Fatalf("GetFeederStatus: %v", err)
	}
	if n := len(snap.GetRobots()); n != len(plc.Robots) {
		t.Fatalf("robots = %d, esperava %d", n, len(plc.Robots))
	}
	for _, upd := range snap.GetRobots() {
		if n := len(upd.GetTrays()); n != 8 {
			t.Fatalf("robô %d: trays = %d, esperava 8", upd.GetRobot(), n)
		}
		for _, tr := range upd.GetTrays() {
			if tr.GetPresent() {
				t.Fatalf("robô %d (%d,%d): present com mock zerado",
					upd.GetRobot(), tr.GetEstande(), tr.GetBandeja())
			}
		}
	}
}

func TestGetFeederStatus_ReflectsWrittenMember(t *testing.T) {
	client := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	writeMember(t, client, ctx, 1, 2, 1, true)

	snap, err := client.GetFeederStatus(ctx, &pb.GetFeederStatusRequest{})
	if err != nil {
		t.Fatalf("GetFeederStatus: %v", err)
	}
	for _, upd := range snap.GetRobots() {
		want := upd.GetRobot() == 1
		if got := presenceOf(t, upd, 2, 1); got != want {
			t.Fatalf("robô %d (2,1): present = %v, esperava %v", upd.GetRobot(), got, want)
		}
	}
}

func TestSubscribeFeederStatus_InitialFrameAndOnChange(t *testing.T) {
	client := startServer(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	stream, err := client.SubscribeFeederStatus(ctx, &pb.SubscribeFeederStatusRequest{PollMs: 100})
	if err != nil {
		t.Fatalf("SubscribeFeederStatus: %v", err)
	}

	// Frame inicial: um snapshot por robô, tudo ausente.
	seen := map[int32]bool{}
	for range plc.Robots {
		upd, err := stream.Recv()
		if err != nil {
			t.Fatalf("Recv inicial: %v", err)
		}
		seen[upd.GetRobot()] = true
		if n := len(upd.GetTrays()); n != 8 {
			t.Fatalf("frame inicial do robô %d com %d posições", upd.GetRobot(), n)
		}
	}
	for _, robot := range plc.Robots {
		if !seen[robot] {
			t.Fatalf("frame inicial do robô %d não chegou", robot)
		}
	}

	// Mudança: só o robô alterado emite, com o snapshot completo refletindo-a.
	writeMember(t, client, ctx, 2, 1, 3, true)
	upd, err := stream.Recv()
	if err != nil {
		t.Fatalf("Recv on-change: %v", err)
	}
	if upd.GetRobot() != 2 {
		t.Fatalf("update do robô %d, esperava 2", upd.GetRobot())
	}
	if !presenceOf(t, upd, 1, 3) {
		t.Fatal("(1,3) deveria estar presente após WriteTag")
	}
}
