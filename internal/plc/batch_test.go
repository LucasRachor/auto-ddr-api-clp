package plc

import (
	"encoding/binary"
	"testing"
)

// O frame do lote tem de bater com o layout do UDT no Sysmac: 5 elementos de 11
// INT little-endian. Errar o packing aqui grava valores no membro errado do CLP.
func TestEncodeBatch_Layout(t *testing.T) {
	data, err := EncodeBatch([]BatchItem{
		{Acao: 1, Dedo: 2, Dedo2: 3, Estande: 4, Bandeja: 5, Fileira: 6, Coluna: 7, Rack: 8, Placa: 9, Slot: 10},
	})
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	if len(data) != BatchSize*tcomandoBytes {
		t.Fatalf("len = %d, esperava %d (5 × 22)", len(data), BatchSize*tcomandoBytes)
	}

	// Ordem exata da declaração; o 11º membro (status) é do CLP e vai 0.
	want := []int16{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, ItemStatusPending}
	for i, w := range want {
		if got := int16(binary.LittleEndian.Uint16(data[i*2:])); got != w {
			t.Errorf("membro %d = %d, esperava %d", i, got, w)
		}
	}

	// Os elementos além dos itens enviados ficam zerados.
	for i, b := range data[tcomandoBytes:] {
		if b != 0 {
			t.Fatalf("byte %d do padding = %#x, esperava 0", tcomandoBytes+i, b)
		}
	}
}

func TestEncodeBatch_RejectsOversize(t *testing.T) {
	if _, err := EncodeBatch(make([]BatchItem, BatchSize+1)); err == nil {
		t.Fatal("esperava erro para lote acima de BatchSize")
	}
}

func TestTagNames(t *testing.T) {
	cases := map[string]string{
		TagLote(1):              "gLote_Robo1",
		TagLoteId(2):            "gLote_Id_Robo2",
		TagLoteQtd(1):           "gLote_Qtd_Robo1",
		TagLoteReq(2):           "gLote_Req_Robo2",
		TagLoteStart(1):         "gLote_Start_Robo1",
		TagLoteAck(2):           "gLote_Ack_Robo2",
		TagLoteItemStatus(1, 3): "gLote_Robo1[3].status",
	}
	for got, want := range cases {
		if got != want {
			t.Errorf("tag = %q, esperava %q", got, want)
		}
	}
}

func TestTagSerial(t *testing.T) {
	if got := TagSerial(1, 3); got != "gSerial_Robo1[3]" {
		t.Errorf("TagSerial(1,3) = %q, esperava gSerial_Robo1[3]", got)
	}
	if got := TagSerial(2, 5); got != "gSerial_Robo2[5]" {
		t.Errorf("TagSerial(2,5) = %q, esperava gSerial_Robo2[5]", got)
	}
}

// O leitor preenche com NUL e o CLP pode deixar espaço; lixo com caractere de
// controle vira vazio (memória anônima) em vez de ser ligado a alguém.
func TestNormalizeSerial(t *testing.T) {
	cases := map[string]string{
		"8F9407260600477159": "8F9407260600477159",
		"ABC\x00\x00\x00":    "ABC",
		"  X ":               "X",
		"\x00":               "",
		"":                   "",
		"A\x01B":             "",
		"\x00\x00SN12\r\n":   "SN12",
		"0000000000":         "0000000000", // "0" (48) não é NUL (0)
	}
	for in, want := range cases {
		if got := NormalizeSerial(in); got != want {
			t.Errorf("NormalizeSerial(%q) = %q, esperava %q", in, got, want)
		}
	}
}

func TestDecodeBatch_RoundTrip(t *testing.T) {
	in := []BatchItem{
		{Acao: 5, Dedo: 2, Dedo2: 3, Rack: 8, Placa: 5, Slot: 4},
		{Acao: 5, Dedo: 1, Dedo2: 4, Rack: 1, Placa: 1, Slot: 1},
	}
	data, err := EncodeBatch(in)
	if err != nil {
		t.Fatalf("EncodeBatch: %v", err)
	}
	out, err := DecodeBatch(data)
	if err != nil {
		t.Fatalf("DecodeBatch: %v", err)
	}
	if len(out) != BatchSize {
		t.Fatalf("len = %d, esperava %d", len(out), BatchSize)
	}
	for i, want := range in {
		if out[i] != want {
			t.Errorf("item %d = %+v, esperava %+v", i, out[i], want)
		}
	}
	for i := len(in); i < BatchSize; i++ {
		if out[i] != (BatchItem{}) {
			t.Errorf("item %d = %+v, esperava zerado", i, out[i])
		}
	}
	if _, err := DecodeBatch(data[:10]); err == nil {
		t.Fatal("esperava erro para frame curto")
	}
}
