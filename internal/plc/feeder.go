package plc

import "fmt"

// Tags de presença de bandeja (gBandejaPresente), uma por robô — CLP -> API,
// SEM handshake: o CLP mantém a struct TStatusAlimentador sempre atualizada a
// partir do sensor de presença de cada posição, e a API lê livremente quando
// precisar (poll). Não há _Req/_Ack.
//
// O UDT tem 8 BOOLs (Estande1_Bandeja1..4, Estande2_Bandeja1..4), mesmas
// coordenadas estande/bandeja do TComando. A leitura é feita membro a membro:
// decodeTagValue recusa UDT inteiro (0x02A0) e o acesso por nome de membro já é
// o caminho comprovado no NX1P2 (mesmo mecanismo de gLote_RoboN[i].status).
// A tag precisa de Network Publish no Sysmac, como as demais.
const (
	// FeederEstandes é o nº de estandes do alimentador de um robô.
	FeederEstandes = 2

	// FeederBandejasPorEstande é o nº de posições de bandeja por estande.
	FeederBandejasPorEstande = 4
)

func TagBandejaPresente(robot int32) string {
	return fmt.Sprintf("gBandejaPresente_Robo%d", robot)
}

// TagBandejaPresenteMember endereça um BOOL do UDT TStatusAlimentador pelo nome
// do membro (estande 1..2, bandeja 1..4).
func TagBandejaPresenteMember(robot, estande, bandeja int32) string {
	return fmt.Sprintf("%s.Estande%d_Bandeja%d", TagBandejaPresente(robot), estande, bandeja)
}
