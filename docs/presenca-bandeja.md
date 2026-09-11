# Presença de bandeja (`gBandejaPresente`)

O CLP mantém `gBandejaPresente_Robo1/2` (UDT `TStatusAlimentador`, 8 BOOLs
`Estande1_Bandeja1`…`Estande2_Bandeja4`) sempre atualizada a partir dos sensores
de presença do alimentador. **Sem handshake** — nada de `_Req`/`_Ack`: a API lê
livremente. A tag exige **Network Publish** no Sysmac.

## Leitura

Membro a membro (`gBandejaPresente_RoboN.EstandeE_BandejaB`), nunca o UDT
inteiro: `decodeTagValue` recusa `0x02A0`, e o acesso por nome de membro é o
caminho comprovado no NX1P2 (mesmo mecanismo de `gLote_RoboN[i].status`). Cada
BOOL volta como `0xC1` de 2 bytes, como sempre. Nomes em
`internal/plc/feeder.go` (`TagBandejaPresente`, `TagBandejaPresenteMember`).

## RPCs (`internal/server/feeder.go`)

- `GetFeederStatus` — snapshot único dos 2 robôs (debug/teste).
- `SubscribeFeederStatus` — stream primário: poll de 500 ms (mín. 100 ms,
  `poll_ms` do request), emissão **on-change por robô**. Cada frame carrega o
  snapshot COMPLETO das 8 posições (nunca delta) e um frame inicial sai na
  assinatura — o consumidor (Nest) reconcilia tudo em qualquer reconexão, sem
  Get de bootstrap. Falha de leitura de um robô: warn + mantém o último estado,
  o stream não cai.

## Mock

`Mock.ReadTag`/`WriteTag` funcionam por nome de tag, então os membros já
funcionam sem simulador dedicado: o harness e2e escreve
`gBandejaPresente_Robo1.Estande2_Bandeja1` via RPC `WriteTag` para "colocar/
retirar" bandeja. Tag desconhecida devolve zero = ausente (default seguro).
Seed opcional na subida: `CLP_MOCK_FEEDER_PRESENT="1:2:1,1:2:2"`
(robô:estande:bandeja).

Testes: `internal/server/feeder_test.go` (bufconn, mesmo padrão do lote).
