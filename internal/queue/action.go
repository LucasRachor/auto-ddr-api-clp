package queue

import (
	"fmt"
	"math"

	"go_auto_ddr_clp/internal/plc"
)

// Ações suportadas pelo ladder de lote (gLote_RoboN[i].acao). O lote é homogêneo:
// todos os itens carregam a mesma ação, a do Record.
const (
	acaoMin = 1
	acaoMax = 5
	// acaoColeta aceita um lote vazio (Qtd=0): o CLP não pega nada, só executa o
	// fluxo de centralização da garra (gLote_Qtd=0 -> só CENTRALIZANDO).
	acaoColeta = 1
)

// Aplicabilidade dos campos do TComando por ação (docs/mudancas-batch.md §6). As
// ações de bandeja endereçam estande/bandeja/fileira/coluna; as de placa,
// rack/placa/slot. Os campos do grupo que não se aplica têm de vir zerados — é
// assim que eles chegam ao CLP (EncodeBatch serializa o Item como está).
func isBandejaAcao(acao int32) bool { return acao == 1 || acao == 4 }

// Validate aplica as regras de domínio do lote antes de enfileirar.
func Validate(req Record) error {
	if req.BatchID <= 0 || req.BatchID > math.MaxInt32 {
		return fmt.Errorf("batch_id %d inválido: deve ser 1..%d (cabe em DINT)", req.BatchID, math.MaxInt32)
	}
	if req.Robot < 1 || req.Robot > 2 {
		return fmt.Errorf("robot %d fora do intervalo 1..2", req.Robot)
	}
	if req.Acao < acaoMin || req.Acao > acaoMax {
		return fmt.Errorf("acao %d fora do intervalo %d..%d", req.Acao, acaoMin, acaoMax)
	}
	// Coleta de centralização é o único lote que pode vir vazio (Qtd=0): sem itens,
	// o CLP só centraliza. As demais ações exigem de 1 a 5 itens.
	qtdMin := int32(1)
	if req.Acao == acaoColeta {
		qtdMin = 0
	}
	if req.Qtd < qtdMin || req.Qtd > plc.BatchSize {
		return fmt.Errorf("qtd %d fora do intervalo %d..%d", req.Qtd, qtdMin, plc.BatchSize)
	}
	if len(req.Items) != int(req.Qtd) {
		return fmt.Errorf("qtd = %d mas o lote traz %d itens", req.Qtd, len(req.Items))
	}
	seen := make(map[int32]bool, len(req.Items))
	for _, it := range req.Items {
		if it.Position < 1 || it.Position > req.Qtd {
			return fmt.Errorf("position %d fora do intervalo 1..%d", it.Position, req.Qtd)
		}
		if seen[it.Position] {
			return fmt.Errorf("position %d duplicada", it.Position)
		}
		seen[it.Position] = true
		if err := validateItem(req.Acao, it); err != nil {
			return fmt.Errorf("item position %d: %w", it.Position, err)
		}
	}
	return nil
}

func validateItem(acao int32, it Item) error {
	if err := inRange("dedo", it.Dedo, 1, 5); err != nil {
		return err
	}
	// dedo_2 só é obrigatório na ação 5 (transferência entre dedos).
	if acao == 5 {
		if err := inRange("dedo_2", it.Dedo2, 1, 5); err != nil {
			return err
		}
	} else if it.Dedo2 != 0 {
		return fmt.Errorf("dedo_2 = %d: só se aplica à acao 5", it.Dedo2)
	}

	if isBandejaAcao(acao) {
		return errs(
			inRange("estande", it.Estande, 1, 2),
			inRange("bandeja", it.Bandeja, 1, 4),
			inRange("fileira", it.Fileira, 1, 3),
			inRange("coluna", it.Coluna, 1, 25),
			mustZero(acao, "rack", it.Rack),
			mustZero(acao, "placa", it.Placa),
			mustZero(acao, "slot", it.Slot),
		)
	}
	return errs(
		inRange("rack", it.Rack, 1, 8),
		inRange("placa", it.Placa, 1, 5),
		inRange("slot", it.Slot, 1, 4),
		mustZero(acao, "estande", it.Estande),
		mustZero(acao, "bandeja", it.Bandeja),
		mustZero(acao, "fileira", it.Fileira),
		mustZero(acao, "coluna", it.Coluna),
	)
}

func inRange(field string, v, min, max int32) error {
	if v < min || v > max {
		return fmt.Errorf("%s = %d fora do intervalo %d..%d", field, v, min, max)
	}
	return nil
}

func mustZero(acao int32, field string, v int32) error {
	if v != 0 {
		return fmt.Errorf("%s = %d: não se aplica à acao %d (envie 0)", field, v, acao)
	}
	return nil
}

// errs devolve o primeiro erro não-nulo.
func errs(list ...error) error {
	for _, err := range list {
		if err != nil {
			return err
		}
	}
	return nil
}
