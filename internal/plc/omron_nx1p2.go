package plc

import (
	"context"
	"fmt"
	"sync"
	"time"

	eip "github.com/loki-os/go-ethernet-ip"
)

// Tags de comando convencionadas no programa do CLP (Sysmac Studio).
// Defina BOOLs globais com Network Publish e dispare o pulso aqui.
const (
	TagCmdStart = "gSysCmd_Start"
	TagCmdStop  = "gSysCmd_Stop"
	TagCmdReset = "gSysCmd_Reset"

	TagSysMode    = "gSys_Mode"    // STRING ou INT mapeando RUN/PROGRAM
	TagSysFaulted = "gSys_Faulted" // BOOL
	TagSysFault   = "gSys_FaultDetail"
)

// OmronNX1P2 implementa Client via EtherNet/IP CIP usando go-ethernet-ip.
//
// O package github.com/loki-os/go-ethernet-ip expõe um Client CIP. As chamadas
// reais (RegisterSession, ReadTag, WriteTag) ficam marcadas com TODO porque a
// API exata varia entre versões da lib — preencher conforme a release fixada
// no go.mod. A camada acima (server/*) só fala com esta interface.
type OmronNX1P2 struct {
	host string
	port uint16

	mu        sync.Mutex
	connected bool
	//raw       *eip.EIPTCP
	tags map[string]*eip.Tag
}

func NewOmronNX1P2(host string, port uint16) *OmronNX1P2 {
	return &OmronNX1P2{host: host, port: port}
}

func (c *OmronNX1P2) Connect(ctx context.Context) error {
	// c.mu.Lock()
	// defer c.mu.Unlock()
	// if c.connected {
	// 	return nil
	// }
	// _ = ctx
	// cfg := eip.DefaultConfig()
	// if c.port != 0 {
	// 	cfg.TCPPort = c.port
	// }
	// cli, err := eip.NewTCP(c.host, cfg)
	// if err != nil {
	// 	return fmt.Errorf("plc resolve %s:%d: %w", c.host, c.port, err)
	// }
	// // Connect() já chama RegisterSession() internamente.
	// if err := cli.Connect(); err != nil {
	// 	return fmt.Errorf("plc connect/register %s:%d: %w", c.host, c.port, err)
	// }
	// c.raw = cli
	c.connected = true
	c.tags = make(map[string]*eip.Tag)

	// Descobre as tags publicadas pelo CLP e cacheia o map name -> *eip.Tag.
	// Tags adicionadas/renomeadas no Sysmac exigem reconectar.
	// all, err := cli.AllTags()
	// if err != nil {
	// 	return fmt.Errorf("plc list tags %s:%d: %w", c.host, c.port, err)
	// }
	// c.tags = all
	return nil
}

// getTag retorna o *eip.Tag do cache; se não existir, inicializa sob demanda.
func (c *OmronNX1P2) getTag(name string) *eip.Tag {
	if c.tags == nil {
		c.tags = make(map[string]*eip.Tag)
	}
	if t, ok := c.tags[name]; ok {
		return t
	}
	t := &eip.Tag{}
	// c.raw.InitializeTag(name, t)
	c.tags[name] = t
	return t
}

func (c *OmronNX1P2) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.connected {
		return nil
	}
	var unregErr error
	//if c.raw != nil {
	//	unregErr = c.raw.UnRegisterSession()
	//}
	//c.raw = nil
	c.tags = nil
	c.connected = false
	if unregErr != nil {
		return fmt.Errorf("plc unregister session: %w", unregErr)
	}
	return nil
}

func (c *OmronNX1P2) Start(ctx context.Context) error { return c.pulse(ctx, TagCmdStart) }
func (c *OmronNX1P2) Stop(ctx context.Context) error  { return c.pulse(ctx, TagCmdStop) }
func (c *OmronNX1P2) Reset(ctx context.Context) error { return c.pulse(ctx, TagCmdReset) }

// pulse escreve TRUE -> aguarda -> FALSE em uma tag de comando.
func (c *OmronNX1P2) pulse(ctx context.Context, tag string) error {
	if err := c.WriteTag(ctx, tag, TagValue{Kind: KindBool, Bool: true}); err != nil {
		return fmt.Errorf("pulse %s on: %w", tag, err)
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(50 * time.Millisecond):
	}
	if err := c.WriteTag(ctx, tag, TagValue{Kind: KindBool, Bool: false}); err != nil {
		return fmt.Errorf("pulse %s off: %w", tag, err)
	}
	return nil
}

func (c *OmronNX1P2) ReadTag(ctx context.Context, tag string) (TagValue, error) {
	if !c.connected {
		return TagValue{}, ErrNotConnected
	}
	_ = ctx
	t := c.getTag(tag)
	// if err := t.Read(); err != nil {
	// 	return TagValue{}, fmt.Errorf("read %s: %w", tag, err)
	// }
	switch v := t.GetValue().(type) {
	case bool:
		return TagValue{Kind: KindBool, Bool: v}, nil
	case int8:
		return TagValue{Kind: KindInt, Int: int32(v)}, nil
	case int16:
		return TagValue{Kind: KindInt, Int: int32(v)}, nil
	case uint8:
		return TagValue{Kind: KindInt, Int: int32(v)}, nil
	case uint16:
		return TagValue{Kind: KindInt, Int: int32(v)}, nil
	case int32:
		return TagValue{Kind: KindDInt, DInt: int64(v)}, nil
	case uint32:
		return TagValue{Kind: KindDInt, DInt: int64(v)}, nil
	case int64:
		return TagValue{Kind: KindDInt, DInt: v}, nil
	case uint64:
		return TagValue{Kind: KindDInt, DInt: int64(v)}, nil
	case float32:
		return TagValue{Kind: KindReal, Real: v}, nil
	case float64:
		return TagValue{Kind: KindReal, Real: float32(v)}, nil
	case string:
		return TagValue{Kind: KindString, String: v}, nil
	default:
		return TagValue{Kind: KindString, String: t.String()}, nil
	}
}

func (c *OmronNX1P2) WriteTag(ctx context.Context, tag string, v TagValue) error {
	if !c.connected {
		return ErrNotConnected
	}
	_ = ctx
	t := c.getTag(tag)
	switch v.Kind {
	case KindBool:
		// A lib só expõe SetInt32/SetString; para BOOL usamos 0/1.
		if v.Bool {
			t.SetInt32(1)
		} else {
			t.SetInt32(0)
		}
	case KindInt:
		t.SetInt32(v.Int)
	case KindDInt:
		t.SetInt32(int32(v.DInt))
	case KindString:
		t.SetString(v.String)
	case KindReal:
		return ErrUnsupportedKind
	default:
		return ErrUnsupportedKind
	}
	if err := t.Write(); err != nil {
		return fmt.Errorf("write %s: %w", tag, err)
	}
	return nil
}

func (c *OmronNX1P2) Status(ctx context.Context) (Status, error) {
	if !c.connected {
		return Status{}, ErrNotConnected
	}
	// Implementação típica: ler TagSysMode + TagSysFaulted + TagSysFault.
	mode, err := c.ReadTag(ctx, TagSysMode)
	if err != nil {
		return Status{Mode: "UNKNOWN", Detail: err.Error()}, nil
	}
	faulted, _ := c.ReadTag(ctx, TagSysFaulted)
	detail, _ := c.ReadTag(ctx, TagSysFault)
	return Status{
		Mode:    mode.String,
		Faulted: faulted.Bool,
		Detail:  detail.String,
	}, nil
}
