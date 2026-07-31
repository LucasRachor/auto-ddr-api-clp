# Testar a fila de comandos + mudanças do driver

Este documento reúne (1) o que foi implementado/corrigido no acesso ao CLP e
(2) o roteiro para testar a fila de comandos fim-a-fim contra o hardware real.

Docs relacionados:
- `docs/sysmac-studio-passo-a-passo.md` — o que o programador faz no CLP.
- `docs/codigos-status-clp.md` — significado dos códigos de retorno do CLP.
- `docs/fila-de-comandos.md` — visão geral da fila (implementado/pendente).

---

## Parte 1 — Implementações e correções

### 1.1 Escrita genérica por nome (UCMM-direto)

O write de alto nível da lib (`Tag.Write`) **não funciona no NX1P2**: ele monta o
path por `InstanceID`/Class `0x6B`, que exige *browse* — e este controlador não
suporta *browse* de tags (só acesso por nome). A leitura de alto nível funciona por
acaso (cai no ramo de *ANSI symbolic segments*), mas a escrita não.

Solução: generalizamos o caminho **UCMM-direto** (ANSI Extended Symbolic Segments +
Write Tag Service), que já era comprovado no `WriteBoolDebug`, para **todos os tipos**.
Em `internal/plc/omron_nx1p2.go`:

- **`writeTagUCMM(name, typeCode, count, value)`** — núcleo: monta o path simbólico
  (`"a.b.c"` vira um segmento ANSI por nível) e envia o payload
  `{typeCode, nº de elementos, valor}`. Checa o `GeneralStatus`.
- **`WriteValue(ctx, name, v any)`** — descobre o type code CIP e serializa conforme
  o Go type de `v` (escalares e slices/arrays):

  | Go | CIP | | Go slice | CIP |
  |----|-----|--|----------|-----|
  | `bool` | BOOL (2 bytes) | | `[]bool` | array BOOL |
  | `int8/16/32/64` | SINT/INT/DINT/LINT | | `[]int32` etc. | array respectivo |
  | `uint8/16/32/64` | USINT/UINT/UDINT/ULINT | | | |
  | `float32/64` | REAL/LREAL | | `[]float32` | array REAL |
  | `string` | STRING (tipo `0xD0`) | | | |

- **`WriteTag(ctx, tag, TagValue)`** (método da interface `Client`) foi **religado**
  para delegar ao `WriteValue`, casando o tamanho Go com o tipo Sysmac:
  `KindInt→int16` (INT), `KindDInt→int32` (DINT), `KindReal→float32` (REAL),
  `KindBool→bool` (BOOL 2 bytes), `KindString→string`. **É isto que faz a fila de
  comandos funcionar contra o CLP real.**

### 1.2 Pegadinhas do NX1P2 descobertas no hardware

- **BOOL = 2 bytes.** A leitura de um BOOL devolve 2 bytes (`00 00`). Escrever 1 byte
  é recusado com `GeneralStatus=0x1F`. Corrigido: BOOL vai como `UInt` (16 bits),
  `0x0001`=TRUE / `0x0000`=FALSE.
- **STRING = tipo CIP `0xD0`, layout `[tamanho UInt (2 bytes)][chars]`.** Confirmado
  por read-back: uma tag STRING volta `tipo=0x00d0` e valor `05 00 53 54 41 52 54` =
  `len(5)+"START"`. O type code da lib (`eip.STRING`=`0xFCE`) é recusado com `0x20`
  (Invalid Parameter). Usamos a constante `cipTypeString = 0x00D0`.
- **Tamanho importa.** Uma tag `INT` no Sysmac exige `int16`; mandar `int32` numa
  tag INT dá descasamento (`0x1F`). Por isso o mapeamento de `Kind` acima.
- **STRING de tamanho fixo.** Se a tag é `STRING[16]`, escrever mais que 16 chars é
  recusado pelo CLP (`0x15`/`0x20`). Declarar as tags STRING com folga no Sysmac.

### 1.3 Comandos de diagnóstico (CLIs)

- **`cmd/readtag`** — lê uma tag e mostra tipo CIP + bytes + valor decodificado.
  Agora reconhece/decodifica `0xD0` (STRING).
- **`cmd/writetag`** — write genérico para qualquer tipo/array:
  `go run ./cmd/writetag <tag> <tipo> <valor[,valor,...]>`.
- **`cmd/writebool`** — write rápido de BOOL (legado do primeiro teste).

### 1.4 Docs novos

- `docs/codigos-status-clp.md` — tabela dos General Status CIP (`0x00`=OK,
  `0x05`=tag inexistente, `0x1F`=erro Omron/tipo, `0x20`=parâmetro inválido, …) e a
  diferença entre o status do CIP e o `gCmd_AckStatus` da aplicação.
- `docs/sysmac-studio-passo-a-passo.md` — guia do lado do CLP (variáveis + ST).

---

## Parte 2 — Roteiro de teste (fim-a-fim)

Pré-requisito: o programador do CLP já implementou o ST e publicou as variáveis
(ver `docs/sysmac-studio-passo-a-passo.md`). Teste em camadas, do isolado ao completo.

### Fase 0 — Conferir as variáveis no CLP

```powershell
$env:CLP_HOST="192.168.1.14"
go run ./cmd/readtag gCmd_Id           # tipo=0x00c4 (DINT)
go run ./cmd/readtag gCmd_Action       # 0x00c3 (INT)
go run ./cmd/readtag gCmd_Robot        # 0x00c3 (INT)
go run ./cmd/readtag gCmd_Finger       # 0x00c3 (INT)
go run ./cmd/readtag gCmd_Req          # 0x00c1 (BOOL)
go run ./cmd/readtag gCmd_AckId        # 0x00c4 (DINT)
go run ./cmd/readtag gCmd_AckStatus    # 0x00c3 (INT)
go run ./cmd/readtag gCmd_Ack          # 0x00c1 (BOOL)
go run ./cmd/readtag gCmd_Motherboard  # 0x00d0 (STRING)
```

Qualquer `0x05` = nome errado ou sem *Network Publish*.

### Fase 1 — Handshake na mão (isola o ladder, sem a fila)

Prova que o ST responde, simulando o que a API faz:

```powershell
go run ./cmd/writetag gCmd_Req bool 0          # slot ocioso

go run ./cmd/writetag gCmd_Id     dint 101     # escreve os campos...
go run ./cmd/writetag gCmd_Action int  1
go run ./cmd/writetag gCmd_Robot  int  1
go run ./cmd/writetag gCmd_Finger int  1
go run ./cmd/writetag gCmd_Req    bool 1       # ...e o Req por último (dispara)

go run ./cmd/readtag gCmd_Ack                  # espera TRUE
go run ./cmd/readtag gCmd_AckId                # espera 101 (eco)
go run ./cmd/readtag gCmd_AckStatus            # espera 1 (OK)

go run ./cmd/writetag gCmd_Req bool 0          # fecha o handshake
go run ./cmd/readtag  gCmd_Ack                 # o CLP deve baixar para FALSE
```

Se esta sequência funcionar, o lado do CLP está correto.

### Fase 2 — Subir a API

```powershell
$env:CLP_HOST="192.168.1.14"
$env:GRPC_ADDR=":50051"
$env:QUEUE_DB_PATH="./data/queue.db"
$env:CMD_ACK_TIMEOUT_MS="5000"
$env:CMD_MAX_RETRIES="3"
$env:CMD_POLL_MS="50"
go run ./cmd/server
```

Deve logar `plc connected` e `plc-api listening addr=:50051`.

### Fase 3 + 4 — Assinar o status e enfileirar

Serviço gRPC: `plc.PLCService`. Sem *reflection* registrada, passe o proto ao
`grpcurl` (`-import-path proto -proto plc/v1/plc.proto`).

**Terminal A** (escuta o stream de status):

```powershell
grpcurl -plaintext -import-path proto -proto plc/v1/plc.proto `
  -d '{}' localhost:50051 plc.PLCService/SubscribeCommandStatus
```

**Terminal B** (envia o comando; `command_id` é o id que o Nest manda):

```powershell
grpcurl -plaintext -import-path proto -proto plc/v1/plc.proto `
  -d '{\"command_id\":101,\"action\":\"insert\",\"motherboard\":\"MB09\",\"robot\":1,\"finger\":1}' `
  localhost:50051 plc.PLCService/EnqueueCommand
```

No Terminal A, a progressão esperada:

```
QUEUED → DISPATCHED → ACKED_OK
```

(ou `ACKED_ERROR` se `gCmd_AckStatus` voltar `2`.)

### Fase 5 — Durabilidade ("nenhum comando se perde")

**a) API cai com comando em voo → replay/reconciliação**
Enfileire e mate o server (Ctrl+C). Suba de novo: no boot o `Recover()` reconcilia
pelo `gCmd_AckId` e o `SubscribeCommandStatus` faz replay do status pendente ao
reconectar — o comando não some.

**b) CLP não confirma → retry → FAILED (não descartado)**
Com o ST sem setar `gCmd_Ack` (ou tag travada), enfileire: verá `DISPATCHED` e,
após `CMD_ACK_TIMEOUT_MS × CMD_MAX_RETRIES`, vai a `FAILED` — persistido, visível.

**c) Nest confirma entrega → para o replay**

```powershell
grpcurl -plaintext -import-path proto -proto plc/v1/plc.proto `
  -d '{\"command_id\":101}' localhost:50051 plc.PLCService/ConfirmStatus
```

Reabra o `SubscribeCommandStatus`: o 101 não deve reaparecer no replay.

---

## Checklist rápido

- [ ] Fase 0: todas as `gCmd_*` respondem com o tipo esperado.
- [ ] Fase 1: handshake manual completa IDLE→BUSY→DONE→IDLE.
- [ ] Fase 2: API sobe e conecta no CLP.
- [ ] Fase 4: `EnqueueCommand` → `QUEUED → DISPATCHED → ACKED_OK`.
- [ ] Fase 5a: matar/reabrir a API não perde o comando (replay).
- [ ] Fase 5b: sem ack → `FAILED` após os retries.
- [ ] Fase 5c: `ConfirmStatus` para o replay.
