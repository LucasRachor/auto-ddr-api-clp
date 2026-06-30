package queue

import (
	"fmt"
	"math"
)

// actionCodes mapeia a ação textual (vinda do Nest) para o código INT escrito
// na tag gCmd_Action do CLP. Adicione novas ações aqui conforme o ladder evoluir.
var actionCodes = map[string]int32{
	"insert": 1,
}

func actionCode(action string) (int32, bool) {
	c, ok := actionCodes[action]
	return c, ok
}

// Validate aplica as regras de domínio do comando antes de enfileirar.
func Validate(id int64, action string, motherboard string, robot, finger int32) error {
	if id <= 0 || id > math.MaxInt32 {
		return fmt.Errorf("command_id %d inválido: deve ser 1..%d (cabe em DINT)", id, math.MaxInt32)
	}
	if _, ok := actionCode(action); !ok {
		return fmt.Errorf("action %q não suportada", action)
	}
	if motherboard == "" {
		return fmt.Errorf("motherboard obrigatório")
	}
	if robot < 1 || robot > 2 {
		return fmt.Errorf("robot %d fora do intervalo 1..2", robot)
	}
	if finger < 1 || finger > 5 {
		return fmt.Errorf("finger %d fora do intervalo 1..5", finger)
	}
	return nil
}
