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
