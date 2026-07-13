package plc

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"syscall"
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

// isConnErr reporta se err indica que a sessão TCP/EtherNet-IP morreu (reset pelo
// host, EOF, broken pipe). O NX1P2 derruba sessões CIP ociosas, então esses erros
// são esperados após períodos sem tráfego — a resposta certa é reconectar e repetir,
// não falhar o comando. Cobre tanto os códigos de erro do net/syscall quanto o texto
// da mensagem do Winsock em pt-BR (WSAECONNRESET = "cancelamento ... pelo host remoto").
func isConnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.ECONNABORTED) ||
		errors.Is(err, syscall.EPIPE) {
		return true
	}
	s := strings.ToLower(err.Error())
	for _, frag := range []string{
		"forçado o cancelamento", "cancelamento de uma conexão", // Winsock pt-BR
		"connection reset", "forcibly closed", "broken pipe",
		"wsasend", "wsarecv", "use of closed", "eof",
	} {
		if strings.Contains(s, frag) {
			return true
		}
	}
	return false
}

// reconnect descarta a sessão morta e reestabelece o registro EtherNet-IP. O
// parâmetro stale é o *eip.EIPTCP que a operação estava usando quando falhou: se
// c.raw já mudou (outra goroutine reconectou nesse meio tempo), não faz nada,
// evitando reconexões redundantes concorrentes.
func (c *OmronNX1P2) reconnect(ctx context.Context, stale *eip.EIPTCP) error {
	c.mu.Lock()
	if c.raw != stale {
		c.mu.Unlock()
		return nil // já reconectado por outro caminho
	}
	if c.raw != nil {
		_ = c.raw.UnRegisterSession() // best-effort; a sessão pode já estar morta
	}
	c.raw = nil
	c.tags = nil
	c.connected = false
	c.mu.Unlock()
	return c.Connect(ctx)
}

// withRetry executa op; se falhar com um erro de conexão, reconecta uma única vez
// e repete. Erros que não são de conexão (ex.: GeneralStatus CIP != 0) passam direto.
func (c *OmronNX1P2) withRetry(ctx context.Context, op func() error) error {
	c.mu.Lock()
	stale := c.raw
	c.mu.Unlock()

	err := op()
	if err == nil || !isConnErr(err) {
		return err
	}
	if rerr := c.reconnect(ctx, stale); rerr != nil {
		return fmt.Errorf("%w (reconexão falhou: %v)", err, rerr)
	}
	return op()
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
	// O NX1P2 trata o BOOL como elemento de 2 bytes (a leitura devolve 2 bytes),
	// então o valor vai como UInt (16 bits): 0x0001=TRUE, 0x0000=FALSE.
	io := bufferx.New(nil)
	io.WL(types.UInt(0x00C1)) // type code BOOL
	io.WL(types.UInt(1))      // 1 elemento
	var b types.UInt
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

// ReadTagRaw lê UMA tag publicada por nome (symbolic + UCMM) e devolve o type
// code CIP (0xC1=BOOL, 0xC3=INT, 0xC4=DINT, 0xCA=REAL, 0xFCE=STRING) e os bytes
// crus do valor. Útil para descobrir o tipo real de uma tag antes de escrever.
func (c *OmronNX1P2) ReadTagRaw(ctx context.Context, name string) (typeCode uint16, value []byte, err error) {
	if !c.connected || c.raw == nil {
		return 0, nil, ErrNotConnected
	}
	_ = ctx

	var paths []byte
	for _, seg := range strings.Split(name, ".") {
		paths = packet.Paths(paths, path.DataBuild(path.DataTypeANSI, []byte(seg), true))
	}

	io := bufferx.New(nil)
	io.WL(types.UInt(1)) // número de elementos

	mr := packet.NewMessageRouter(packet.ServiceReadTag, paths, io.Bytes())
	res, err := c.raw.SendRRData(packet.NewUCMM(mr), 10)
	if err != nil {
		return 0, nil, fmt.Errorf("readTagRaw send %q: %w", name, err)
	}

	mrres := new(packet.MessageRouterResponse)
	mrres.Decode(res.Packet.Items[1].Data)
	if mrres.GeneralStatus != 0x00 {
		return 0, nil, fmt.Errorf("readTagRaw %q: GeneralStatus=%#x addStatus=%x", name, mrres.GeneralStatus, mrres.AdditionalStatus)
	}
	if len(mrres.ResponseData) < 2 {
		return 0, nil, fmt.Errorf("readTagRaw %q: resposta curta (%d bytes)", name, len(mrres.ResponseData))
	}
	typeCode = binary.LittleEndian.Uint16(mrres.ResponseData[:2])
	return typeCode, mrres.ResponseData[2:], nil
}

// ---------------------------------------------------------------------------
// Escrita genérica por nome (UCMM-direto) — funciona para todos os tipos.
//
// A escrita de alto nível da lib (Tag.Write) quebra no NX1P2 porque monta o path
// por InstanceID/Class 0x6B (exige browse). Aqui usamos o MESMO caminho comprovado
// do WriteBoolDebug/ReadTagRaw: ANSI Extended Symbolic Segments + Write Tag Service
// + UCMM direto. O payload do Write Tag é sempre {typeCode, nº de elementos, valor}.
// ---------------------------------------------------------------------------

// writeTagUCMM é o núcleo: monta o Request Path simbólico ("a.b.c" vira um segmento
// ANSI por nível), anexa o payload já serializado {typeCode, count, value...} e envia.
func (c *OmronNX1P2) writeTagUCMM(ctx context.Context, name string, typeCode types.UInt, count uint16, value []byte) error {
	if !c.connected || c.raw == nil {
		return ErrNotConnected
	}

	var paths []byte
	for _, seg := range strings.Split(name, ".") {
		paths = packet.Paths(paths, path.DataBuild(path.DataTypeANSI, []byte(seg), true))
	}

	buf := bufferx.New(nil)
	buf.WL(typeCode)          // type code CIP (2 bytes)
	buf.WL(types.UInt(count)) // nº de elementos (1 para escalar, N para array)
	buf.WL(value)             // bytes do valor, já em little-endian
	mr := packet.NewMessageRouter(packet.ServiceWriteTag, paths, buf.Bytes())

	// Envolto em withRetry: se a sessão tiver sido derrubada por ociosidade, reconecta
	// e repete o envio uma vez antes de propagar o erro.
	return c.withRetry(ctx, func() error {
		res, err := c.raw.SendRRData(packet.NewUCMM(mr), 10)
		if err != nil {
			return fmt.Errorf("writeTag send %q: %w", name, err)
		}
		mrres := new(packet.MessageRouterResponse)
		mrres.Decode(res.Packet.Items[1].Data)
		if mrres.GeneralStatus != 0x00 {
			return fmt.Errorf("writeTag %q: GeneralStatus=%#x addStatus=%x",
				name, mrres.GeneralStatus, mrres.AdditionalStatus)
		}
		return nil
	})
}

// cipTypeStruct é o type code CIP de um tipo ESTRUTURADO (UDT/array de UDT) no
// NX1P2 (0x02A0). Um read de um struct volta com este tipo seguido do "structure
// handle" (CRC do template do UDT) nos 2 primeiros bytes do valor.
const cipTypeStruct = types.UInt(0x02A0)

// WriteStructRaw escreve um tipo ESTRUTURADO (UDT ou ARRAY OF UDT) por nome, via
// UCMM-direto. Diferente dos tipos atômicos, o Write Tag de um struct usa o formato:
//
//	[tipo 0x02A0 (2B)][structure handle (2B)][count (2B)][bytes dos membros...]
//
// O structure handle (CRC do template do UDT) é lido do próprio controlador antes de
// escrever (ReadTagRaw devolve o handle nos 2 primeiros bytes do valor de um struct),
// então não precisa ser hardcoded. 'count' é o nº de elementos do array (1 para um
// struct único). 'data' são os bytes dos membros já serializados em little-endian, na
// ordem e no packing exatos do UDT — descubra o layout via ReadTagRaw (ex.: ST_Item =
// Numero INT(2) + Estado INT(2) + Ligado BOOL(2, padded) = 6 bytes por elemento).
func (c *OmronNX1P2) WriteStructRaw(ctx context.Context, name string, count uint16, data []byte) error {
	typeCode, cur, err := c.ReadTagRaw(ctx, name)
	if err != nil {
		return fmt.Errorf("writeStructRaw %q: leitura do handle falhou: %w", name, err)
	}
	if types.UInt(typeCode) != cipTypeStruct || len(cur) < 2 {
		return fmt.Errorf("writeStructRaw %q: tag não é estruturada (tipo=%#04x, %d bytes)", name, typeCode, len(cur))
	}
	handle := binary.LittleEndian.Uint16(cur[:2])
	return c.writeStructUCMM(ctx, name, handle, count, data)
}

// writeStructUCMM monta e envia o Write Tag de um tipo estruturado (ver WriteStructRaw
// para o formato). Mesmo transporte comprovado do writeTagUCMM (ANSI symbolic + UCMM),
// só muda o cabeçalho do payload, que inclui o structure handle.
func (c *OmronNX1P2) writeStructUCMM(ctx context.Context, name string, structHandle, count uint16, data []byte) error {
	if !c.connected || c.raw == nil {
		return ErrNotConnected
	}

	var paths []byte
	for _, seg := range strings.Split(name, ".") {
		paths = packet.Paths(paths, path.DataBuild(path.DataTypeANSI, []byte(seg), true))
	}

	buf := bufferx.New(nil)
	buf.WL(cipTypeStruct)            // 0x02A0: tipo estruturado
	buf.WL(types.UInt(structHandle)) // handle do template do UDT
	buf.WL(types.UInt(count))        // nº de elementos
	buf.WL(data)                     // membros serializados em little-endian
	mr := packet.NewMessageRouter(packet.ServiceWriteTag, paths, buf.Bytes())

	return c.withRetry(ctx, func() error {
		res, err := c.raw.SendRRData(packet.NewUCMM(mr), 10)
		if err != nil {
			return fmt.Errorf("writeStruct send %q: %w", name, err)
		}
		mrres := new(packet.MessageRouterResponse)
		mrres.Decode(res.Packet.Items[1].Data)
		if mrres.GeneralStatus != 0x00 {
			return fmt.Errorf("writeStruct %q: GeneralStatus=%#x addStatus=%x",
				name, mrres.GeneralStatus, mrres.AdditionalStatus)
		}
		return nil
	})
}

// cipTypeString é o type code CIP do STRING neste NX1P2 (0xD0), confirmado por
// read-back. Não usar eip.STRING (0xFCE) no write: o controlador o recusa (0x20).
const cipTypeString = types.UInt(0x00D0)

// boolAsUInt: o NX1P2 trata BOOL como elemento de 2 bytes (a leitura devolve 2
// bytes), então TRUE=0x0001, FALSE=0x0000.
func boolAsUInt(b bool) types.UInt {
	if b {
		return 1
	}
	return 0
}

// WriteValue escreve qualquer tipo suportado numa tag por nome, via UCMM-direto.
// Descobre o type code CIP e serializa o payload conforme o Go type de v:
//
//	bool     -> BOOL   (2 bytes no NX1P2)     []bool    -> array de BOOL
//	int8     -> SINT                          []int8    -> array de SINT
//	uint8    -> USINT                         []uint8   -> array de USINT
//	int16    -> INT                           []int16   -> array de INT
//	uint16   -> UINT                          []uint16  -> array de UINT
//	int32    -> DINT                          []int32   -> array de DINT
//	uint32   -> UDINT                         []uint32  -> array de UDINT
//	int64    -> LINT                          []int64   -> array de LINT
//	uint64   -> ULINT                         []uint64  -> array de ULINT
//	float32  -> REAL                          []float32 -> array de REAL
//	float64  -> LREAL                         []float64 -> array de LREAL
//	string   -> STRING
//
// Atenção aos tamanhos: uma tag INT no Sysmac exige int16 (não int32), senão o
// CLP recusa por descasamento de tamanho (GeneralStatus 0x1F). Para os campos do
// handshake: Action/Robot/Finger são INT (int16); Id/AckId são DINT (int32).
func (c *OmronNX1P2) WriteValue(ctx context.Context, name string, v any) error {
	typeCode, count, payload, err := encodeCIP(v)
	if err != nil {
		return fmt.Errorf("writeValue %q: %w", name, err)
	}
	return c.writeTagUCMM(ctx, name, typeCode, count, payload)
}

// encodeCIP serializa v no formato de valor do Write Tag Service e devolve o
// type code CIP e o nº de elementos. Suporta escalares e slices (arrays).
func encodeCIP(v any) (typeCode types.UInt, count uint16, payload []byte, err error) {
	io := bufferx.New(nil)
	switch x := v.(type) {
	// ---- escalares ----
	case bool:
		io.WL(boolAsUInt(x))
		return eip.BOOL, 1, io.Bytes(), nil
	case int8:
		io.WL(x)
		return eip.SINT, 1, io.Bytes(), nil
	case uint8:
		io.WL(x)
		return eip.USINT, 1, io.Bytes(), nil
	case int16:
		io.WL(x)
		return eip.INT, 1, io.Bytes(), nil
	case uint16:
		io.WL(x)
		return eip.UINT, 1, io.Bytes(), nil
	case int32:
		io.WL(x)
		return eip.DINT, 1, io.Bytes(), nil
	case uint32:
		io.WL(x)
		return eip.UDINT, 1, io.Bytes(), nil
	case int64:
		io.WL(x)
		return eip.LINT, 1, io.Bytes(), nil
	case uint64:
		io.WL(x)
		return eip.ULINT, 1, io.Bytes(), nil
	case float32:
		io.WL(x)
		return eip.REAL, 1, io.Bytes(), nil
	case float64:
		io.WL(x)
		return eip.LREAL, 1, io.Bytes(), nil
	case string:
		// STRING no NX1P2 = tipo CIP 0xD0, com layout {tamanho UInt (2 bytes), chars}.
		// Confirmado por read-back: uma tag STRING volta com tipo=0x00d0 e valor
		// "05 00 53 54 41 52 54" = len(5) + "START". O type code da lib (eip.STRING
		// = 0xfce) é recusado com GeneralStatus 0x20 — este controlador usa 0xD0.
		io.WL(types.UInt(len(x)))
		io.WL([]byte(x))
		return cipTypeString, 1, io.Bytes(), nil

	// ---- arrays (slices) ----
	case []bool:
		for _, e := range x {
			io.WL(boolAsUInt(e))
		}
		return eip.BOOL, uint16(len(x)), io.Bytes(), nil
	case []int8:
		io.WL(x)
		return eip.SINT, uint16(len(x)), io.Bytes(), nil
	case []uint8:
		io.WL(x)
		return eip.USINT, uint16(len(x)), io.Bytes(), nil
	case []int16:
		for _, e := range x {
			io.WL(e)
		}
		return eip.INT, uint16(len(x)), io.Bytes(), nil
	case []uint16:
		for _, e := range x {
			io.WL(e)
		}
		return eip.UINT, uint16(len(x)), io.Bytes(), nil
	case []int32:
		for _, e := range x {
			io.WL(e)
		}
		return eip.DINT, uint16(len(x)), io.Bytes(), nil
	case []uint32:
		for _, e := range x {
			io.WL(e)
		}
		return eip.UDINT, uint16(len(x)), io.Bytes(), nil
	case []int64:
		for _, e := range x {
			io.WL(e)
		}
		return eip.LINT, uint16(len(x)), io.Bytes(), nil
	case []float32:
		for _, e := range x {
			io.WL(e)
		}
		return eip.REAL, uint16(len(x)), io.Bytes(), nil
	case []float64:
		for _, e := range x {
			io.WL(e)
		}
		return eip.LREAL, uint16(len(x)), io.Bytes(), nil

	default:
		return 0, 0, nil, fmt.Errorf("%w: %T", ErrUnsupportedKind, v)
	}
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

// KeepAlive mantém a sessão EtherNet-IP aquecida fazendo uma leitura leve a cada
// `every`, evitando que o NX1P2 derrube a conexão CIP por ociosidade. Como ReadTag
// se auto-recupera, isso também reconecta proativamente caso a sessão já tenha caído.
// Bloqueia até ctx ser cancelado; rode em uma goroutine.
func (c *OmronNX1P2) KeepAlive(ctx context.Context, every time.Duration, log *slog.Logger) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if _, err := c.ReadTag(ctx, TagCmdAck); err != nil && log != nil {
				log.Warn("keepalive falhou", "err", err)
			}
		}
	}
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
	// getTag é chamado dentro do withRetry porque a reconexão zera o cache de tags:
	// após reconectar, precisamos de um *eip.Tag reinicializado contra a nova sessão.
	var t *eip.Tag
	if err := c.withRetry(ctx, func() error {
		t = c.getTag(tag)
		if err := t.Read(); err != nil {
			return fmt.Errorf("read %s: %w", tag, err)
		}
		return nil
	}); err != nil {
		return TagValue{}, err
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

// WriteTag escreve uma tag pela abstração TagValue. Delega para WriteValue (UCMM-
// direto), escolhendo o tamanho Go que casa com o tipo Sysmac de cada Kind:
// INT=int16, DINT=int32, REAL=float32, BOOL=bool (2 bytes), STRING=string.
func (c *OmronNX1P2) WriteTag(ctx context.Context, tag string, v TagValue) error {
	switch v.Kind {
	case KindBool:
		return c.WriteValue(ctx, tag, v.Bool)
	case KindInt:
		return c.WriteValue(ctx, tag, int16(v.Int))
	case KindDInt:
		return c.WriteValue(ctx, tag, int32(v.DInt))
	case KindReal:
		return c.WriteValue(ctx, tag, v.Real)
	case KindString:
		return c.WriteValue(ctx, tag, v.String)
	default:
		return ErrUnsupportedKind
	}
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
