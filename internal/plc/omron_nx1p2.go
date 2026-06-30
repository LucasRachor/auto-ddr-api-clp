package plc

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"strings"
	"sync"
	"time"

	eip "github.com/loki-os/go-ethernet-ip"
	"github.com/loki-os/go-ethernet-ip/bufferx"
	"github.com/loki-os/go-ethernet-ip/messages/packet"
	"github.com/loki-os/go-ethernet-ip/path"
	"github.com/loki-os/go-ethernet-ip/types"
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

// Tags do handshake de comando (fila com ack). Slot único: um comando em
// execução por vez. Veja internal/queue/dispatcher.go para a sequência.
//
// Requisição (API -> CLP):
const (
	TagCmdId          = "gCmd_Id"          // DINT  — id do comando (vindo do Nest)
	TagCmdAction      = "gCmd_Action"      // INT   — código da ação (insert=1)
	TagCmdMotherboard = "gCmd_Motherboard" // STRING
	TagCmdRobot       = "gCmd_Robot"       // INT   — 1..2
	TagCmdFinger      = "gCmd_Finger"      // INT   — 1..5
	TagCmdReq         = "gCmd_Req"         // BOOL  — TRUE = comando válido presente

	// Resposta (CLP -> API):
	TagCmdAckId     = "gCmd_AckId"     // DINT  — eco do id processado
	TagCmdAckStatus = "gCmd_AckStatus" // INT   — 0=none, 1=OK, 2=erro
	TagCmdAckDetail = "gCmd_AckDetail" // STRING — detalhe/erro (opcional)
	TagCmdAck       = "gCmd_Ack"       // BOOL  — TRUE = ack pronto
)

// OmronNX1P2 implementa Client via EtherNet/IP CIP usando go-ethernet-ip.
type OmronNX1P2 struct {
	host string
	port uint16

	mu        sync.Mutex
	connected bool
	raw       *eip.EIPTCP
	tags      map[string]*eip.Tag
}

func NewOmronNX1P2(host string, port uint16) *OmronNX1P2 {
	return &OmronNX1P2{host: host, port: port}
}

func parseDINTArray(data []byte) ([]int32, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("payload inválido")
	}

	payload := data[2:] // remove o type code

	if len(payload)%4 != 0 {
		return nil, fmt.Errorf("payload não múltiplo de 4")
	}

	result := make([]int32, len(payload)/4)

	for i := 0; i < len(result); i++ {
		offset := i * 4
		result[i] = int32(binary.LittleEndian.Uint32(payload[offset : offset+4]))
	}

	return result, nil
}

func (c *OmronNX1P2) Connect(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.connected {
		return nil
	}
	_ = ctx
	cfg := eip.DefaultConfig()
	if c.port != 0 {
		cfg.TCPPort = c.port
	}
	cli, err := eip.NewTCP(c.host, cfg)
	if err != nil {
		return fmt.Errorf("plc resolve %s:%d: %w", c.host, c.port, err)
	}
	// Connect() já chama RegisterSession() internamente.
	if err := cli.Connect(); err != nil {
		return fmt.Errorf("plc connect/register %s:%d: %w", c.host, c.port, err)
	}
	c.raw = cli
	c.connected = true
	c.tags = make(map[string]*eip.Tag)
	return nil
}

// getTag retorna o *eip.Tag do cache; se não existir, inicializa sob demanda.
// InitializeTag preenche Lock/TCP (obrigatórios para Read/Write não dar panic).
func (c *OmronNX1P2) getTag(name string) *eip.Tag {
	if c.tags == nil {
		c.tags = make(map[string]*eip.Tag)
	}
	if t, ok := c.tags[name]; ok {
		return t
	}
	t := &eip.Tag{}
	if c.raw != nil {
		c.raw.InitializeTag(name, t)
	}
	c.tags[name] = t
	return t
}

// TagMeta descreve uma tag publicada (Network Publish) lida do CLP.
type TagMeta struct {
	InstanceID uint32
	Name       string
	Type       uint16
	TypeName   string
	Atomic     bool
	Dims       [3]uint32
}

// ReadTagDebug lê UMA tag publicada pelo nome, via symbolic addressing + UCMM
// direto (sem o Unconnected_Send que quebra no NX1P2). Loga o status e o hex cru
// da resposta. Serve para validar que o acesso por nome funciona, já que o NX1P2
// não suporta enumerar a lista de tags (GetInstanceAttributeList = status 0x08).
func (c *OmronNX1P2) ReadTagDebug(ctx context.Context, name string) error {
	if !c.connected || c.raw == nil {
		return ErrNotConnected
	}
	_ = ctx

	// Monta o Request Path com ANSI Extended Symbolic Segments (um por nível "a.b.c").
	var paths []byte
	for _, seg := range strings.Split(name, ".") {
		paths = packet.Paths(paths, path.DataBuild(path.DataTypeANSI, []byte(seg), true))
	}

	io := bufferx.New(nil)
	io.WL(types.UInt(1)) // número de elementos a ler

	mr := packet.NewMessageRouter(packet.ServiceReadTag, paths, io.Bytes())

	res, err := c.raw.SendRRData(packet.NewUCMM(mr), 10)
	if err != nil {
		return fmt.Errorf("readTagDebug send %q: %w", name, err)
	}

	mrres := new(packet.MessageRouterResponse)
	mrres.Decode(res.Packet.Items[1].Data)

	fmt.Printf("[readTagDebug] tag=%q reply=%#x status=%#x addStatus=%x dataLen=%d\n",
		name, mrres.ReplyService, mrres.GeneralStatus, mrres.AdditionalStatus, len(mrres.ResponseData))

	if len(mrres.ResponseData) > 0 {
		fmt.Print(hex.Dump(mrres.ResponseData))
	}

	// if mrres.GeneralStatus != 0x00 {
	// 	return fmt.Errorf("readTagDebug %q: GeneralStatus=%#x", name, mrres.GeneralStatus)
	// }

	// Os 2 primeiros bytes do ResponseData são o type code CIP; o resto é o valor.

	// if len(mrres.ResponseData) >= 2 {
	// 	io2 := bufferx.New(mrres.ResponseData)
	// 	var ttype types.UInt
	// 	io2.RL(&ttype)
	// 	fmt.Printf("[readTagDebug] type=%#04x(%s)\n", ttype, eip.TypeMap[0xFFF&ttype])
	// }

	values, err := parseDINTArray(mrres.ResponseData)
	if err != nil {
		return err
	}

	fmt.Printf("RoboOUT = %+v\n", values)

	return nil
}

// WriteBoolDebug escreve UM BOOL por nome, via symbolic addressing + UCMM direto
// (mesmo caminho comprovado do ReadTagDebug, já que o write de alto nível da lib
// exige InstanceID/browse, que o NX1P2 não suporta). Loga o status CIP da resposta.
//
// Para um teste rápido na rede:
//
//	clp := plc.NewOmronNX1P2(host, port)
//	clp.Connect(ctx)
//	clp.WriteBoolDebug(ctx, "MinhaTagBool", true)
func (c *OmronNX1P2) WriteBoolDebug(ctx context.Context, name string, val bool) error {
	if !c.connected || c.raw == nil {
		return ErrNotConnected
	}
	_ = ctx

	// Request Path: ANSI Extended Symbolic Segment por nível ("a.b.c").
	var paths []byte
	for _, seg := range strings.Split(name, ".") {
		paths = packet.Paths(paths, path.DataBuild(path.DataTypeANSI, []byte(seg), true))
	}

	// Payload do Write Tag Service: {tipo CIP, nº de elementos, valor}.
	io := bufferx.New(nil)
	io.WL(types.UInt(0x00C1)) // type code BOOL
	io.WL(types.UInt(1))      // 1 elemento
	var b types.USInt         // Omron: 0x01=TRUE, 0x00=FALSE (1 byte)
	if val {
		b = 1
	}
	io.WL(b)

	mr := packet.NewMessageRouter(packet.ServiceWriteTag, paths, io.Bytes())
	res, err := c.raw.SendRRData(packet.NewUCMM(mr), 10)
	if err != nil {
		return fmt.Errorf("writeBoolDebug send %q: %w", name, err)
	}

	mrres := new(packet.MessageRouterResponse)
	mrres.Decode(res.Packet.Items[1].Data)

	fmt.Printf("[writeBoolDebug] tag=%q val=%v reply=%#x status=%#x addStatus=%x\n",
		name, val, mrres.ReplyService, mrres.GeneralStatus, mrres.AdditionalStatus)

	if mrres.GeneralStatus != 0x00 {
		return fmt.Errorf("writeBoolDebug %q: GeneralStatus=%#x addStatus=%x", name, mrres.GeneralStatus, mrres.AdditionalStatus)
	}
	return nil
}

func (c *OmronNX1P2) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.connected {
		return nil
	}
	var unregErr error
	if c.raw != nil {
		unregErr = c.raw.UnRegisterSession()
	}
	c.raw = nil
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
	if err := t.Read(); err != nil {
		return TagValue{}, fmt.Errorf("read %s: %w", tag, err)
	}
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
