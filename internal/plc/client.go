package plc

import (
	"context"
	"errors"
)

type ValueKind int

const (
	KindUnknown ValueKind = iota
	KindBool
	KindInt
	KindDInt
	KindReal
	KindString
)

type TagValue struct {
	Kind   ValueKind
	Bool   bool
	Int    int32
	DInt   int64
	Real   float32
	String string
}

type Status struct {
	Mode    string // RUN | PROGRAM | UNKNOWN
	Faulted bool
	Detail  string
}

// Client é a abstração sobre o CLP. Trocar a impl não deve afetar o gRPC layer.
type Client interface {
	Connect(ctx context.Context) error
	Close() error

	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Reset(ctx context.Context) error

	ReadTag(ctx context.Context, tag string) (TagValue, error)
	WriteTag(ctx context.Context, tag string, v TagValue) error

	// WriteStructRaw escreve um UDT / array de UDT num único frame. 'data' são os
	// bytes de todos os elementos já no packing do UDT (ver EncodeBatch).
	WriteStructRaw(ctx context.Context, name string, data []byte) error

	Status(ctx context.Context) (Status, error)
}

var ErrNotConnected = errors.New("plc: not connected")
var ErrUnsupportedKind = errors.New("plc: unsupported value kind")
