package queue

import "testing"

// baseRecord é um lote mínimo e válido, usado como ponto de partida nos testes.
func baseRecord() Record {
	return Record{BatchID: 1, Robot: 1, Acao: acaoColeta, Qtd: 1, Items: []Item{
		{Position: 1, Dedo: 1, Estande: 1, Bandeja: 1, Fileira: 1, Coluna: 1},
	}}
}

// Coleta de centralização: Qtd=0 e sem itens é aceita — o CLP só centraliza.
func TestValidate_EmptyColetaCentralizes(t *testing.T) {
	rec := baseRecord()
	rec.Qtd = 0
	rec.Items = nil
	if err := Validate(rec); err != nil {
		t.Fatalf("coleta vazia (Qtd=0) deveria ser válida, veio: %v", err)
	}
}

// Só a coleta pode vir vazia: as demais ações ainda exigem 1..5 itens.
func TestValidate_EmptyNonColetaRejected(t *testing.T) {
	rec := baseRecord()
	rec.Acao = 4 // GUARDA
	rec.Qtd = 0
	rec.Items = nil
	if err := Validate(rec); err == nil {
		t.Fatal("lote vazio de GUARDA deveria ser rejeitado")
	}
}

// A coleta vazia continua sujeita ao teto e à coerência Qtd == len(Items).
func TestValidate_ColetaQtdMismatchRejected(t *testing.T) {
	rec := baseRecord()
	rec.Qtd = 0
	// Items não-vazio com Qtd=0 é incoerente.
	if err := Validate(rec); err == nil {
		t.Fatal("Qtd=0 com itens presentes deveria ser rejeitado")
	}
}
