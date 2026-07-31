# API Go — Mudanças para suportar Lote (gLote/TComando)

Documento de especificação das mudanças necessárias no serviço `go_auto_ddr_clp`
para atender a proposta de comunicação de **lote** do CLP
(ver [../comunicacoes/clp-software.md](../comunicacoes/clp-software.md)).

## Objetivo

Estender o serviço da fila de **comando único** (`gCmd_*`, 1 slot serial,
`action=insert`) para o modelo de **lote** definido em
[../comunicacoes/clp-software.md](../comunicacoes/clp-software.md):
`gLote : ARRAY[1..5] OF TComando`, homogêneo por `acao` (1–5),
**duplicado por robô** (`_Robo1`/`_Robo2`), com handshake `gLote_Req/Start/Ack`
e **2 dispatchers em paralelo** (um por robô). A durabilidade (bbolt), retry,
replay e `Recover()` são mantidos — passam a operar **por lote/robô**.

O backend NestJS consome apenas os novos RPCs; ele **não** dirige tags.

## O que se mantém (reaproveitar)

- Sessão CIP persistente e acesso por nome (`internal/plc/omron_nx1p2.go`), com os
  quirks já resolvidos: BOOL=2 bytes, STRING=`0xD0`, larguras INT/DINT
  (ver [testar-fila-de-comandos.md](testar-fila-de-comandos.md) §1.2).
- **`WriteStructRaw` / escrita por membro** (ver [escrita-struct-clp.md](escrita-struct-clp.md)) —
  já escreve `ARRAY OF STRUCT` num frame único (`[0x02A0][handle][count=1][data]`)
  e por membro (`gLote_Robo1[1].acao`).
- Fila durável bbolt, broker de status, dispatcher com timeout/retry/backoff/dead-letter,
  `Recover()`, mock para testes.
- Config CIP (`CLP_HOST/PORT`), gRPC (`GRPC_ADDR`), variáveis de ambiente da fila.

## O que muda

### 1. Proto (`proto/plc/v1/plc.proto`) — adicionar RPCs de lote

Manter a mesma `plc.v1.PLCService`. Os RPCs `gCmd` de comando único podem ser
**removidos** ou mantidos como legado (ver "Compatibilidade").

```proto
enum BatchState {
  BQ_QUEUED = 0; BQ_DISPATCHED = 1; BQ_STARTED = 2;
  BQ_ACKED_OK = 3; BQ_ACKED_ERROR = 4; BQ_FAILED = 5;
}

message TComandoMsg {            // 1 item do lote; campos não-aplicáveis = 0
  int32 position = 1;           // 1..5 (índice em gLote, base-1)
  int32 dedo = 2; int32 dedo_2 = 3;
  int32 estande = 4; int32 bandeja = 5; int32 fileira = 6; int32 coluna = 7;
  int32 rack = 8; int32 placa = 9; int32 slot = 10;
}
message EnqueueBatchRequest {
  int32 batch_id = 1;           // gLote_Id (DINT), único por robô, correlação do ack
  int32 robot   = 2;            // 1..2 -> _Robo1/_Robo2
  int32 acao    = 3;            // 1..5 (lote homogêneo)
  int32 qtd     = 4;            // gLote_Qtd (1..5)
  repeated TComandoMsg items = 5;
}
message EnqueueBatchResponse { bool accepted = 1; int32 batch_id = 2; string message = 3; }
message ItemStatusMsg   { int32 position = 1; int32 status = 2; } // 0=pend,1=ok,2=erro
message BatchStatusUpdate {
  int32 batch_id = 1; int32 robot = 2; BatchState state = 3;
  repeated ItemStatusMsg items = 4; string detail = 5; int64 ts_unix_ms = 6;
}
message SubscribeBatchRequest {}                  // opcional: filtrar por robot
message ConfirmBatchRequest   { int32 batch_id = 1; int32 robot = 2; }

rpc EnqueueBatch         (EnqueueBatchRequest)   returns (EnqueueBatchResponse);
rpc SubscribeBatchStatus (SubscribeBatchRequest) returns (stream BatchStatusUpdate);
rpc ConfirmBatchStatus   (ConfirmBatchRequest)   returns (CommandResponse);
```

Regenerar stubs (`protoc ...`) e religar em `internal/server/server.go` e
`cmd/server/main.go`.

### 2. Tags no Sysmac (por robô — `_Robo1` e `_Robo2`)

Criar o UDT e as globais com **Network Publish = Publish Only** (base para o ladder
de lote, equivalente ao de `gCmd` em [handshake-clp-sysmac.md](handshake-clp-sysmac.md)).

**UDT `TComando`** (11 × INT, na ordem exata — importa para o packing):

| Membro | Tipo | Bytes |
|--------|------|-------|
| `acao` `dedo` `dedo_2` `estande` `bandeja` `fileira` `coluna` `rack` `placa` `slot` `status` | INT | 2 cada |
| **total por elemento** | | **22** |

`gLote_RoboN : ARRAY[1..5] OF TComando` → `2 (handle) + 5×22 = 112 bytes`.
Índice **base-1** (`gLote_Robo1[1]`). `status` é gravado pelo CLP (a API envia 0).

**Globais por robô (`N` = 1, 2):**

| Tag | Tipo | Direção | Papel |
|-----|------|---------|-------|
| `gLote_RoboN` | ARRAY[1..5] OF TComando | API→CLP | o lote (API escreve; CLP grava `status[i]`) |
| `gLote_Id_RoboN` | DINT | API→CLP | id do lote (eco no ack) |
| `gLote_Qtd_RoboN` | INT | API→CLP | itens válidos (1..5) |
| `gLote_Req_RoboN` | BOOL | API→CLP | requisição pronta |
| `gLote_Start_RoboN` | BOOL | CLP→API | CLP travou o lote / começou |
| `gLote_Ack_RoboN` | BOOL | CLP→API | lote concluído (todos `status` gravados) |

### 3. Driver (`internal/plc/omron_nx1p2.go`)

- **`WriteBatch(ctx, robot, req)`**: serializa os 5 elementos LE (22 bytes cada; itens
  além de `qtd` = zeros; `status`=0; `acao` de cada item = `req.acao`) e escreve
  `gLote_RoboN` via `WriteStructRaw` (frame único, `count=1`). Alternativa
  campo-a-campo (`gLote_RoboN[i].campo`) escrevendo só os `qtd` válidos.
- Escrever `gLote_Qtd_RoboN` e `gLote_Id_RoboN` (via `WriteValue`).
- **`ReadBatchStatus(ctx, robot, qtd)`**: lê `gLote_RoboN[i].status` (i=1..qtd) — leitura
  por membro já suportada.
- Constantes das tags parametrizadas por sufixo `_RoboN`.

### 4. Dispatcher (`internal/queue/dispatcher.go`) — de 1 slot serial para **2 slots (por robô)**

Rodar **um dispatcher por robô** (2 goroutines). Cada um consome apenas os lotes do seu
`robot` e executa o **handshake de lote**
([../comunicacoes/clp-software.md](../comunicacoes/clp-software.md) §"Protocolo de lote"),
que tem um sinal a mais que o `gCmd` (o `Start` intermediário):

```
1. WriteBatch(robot)  +  gLote_Qtd_RoboN  +  gLote_Id_RoboN      // tudo antes do Req
2. gLote_Req_RoboN = TRUE                        -> estado DISPATCHED
3. espera gLote_Start_RoboN = TRUE               -> estado STARTED   (CLP travou)
4. espera gLote_Ack_RoboN  = TRUE                                    (CLP terminou)
5. confere gLote_Id_RoboN (eco) e lê gLote_RoboN[i].status (i=1..qtd)
   -> ACKED_OK se todos 1; ACKED_ERROR se algum 2                 (emite items[])
6. gLote_Req_RoboN = FALSE
7. espera gLote_Start_RoboN=FALSE e gLote_Ack_RoboN=FALSE          // slot do robô livre
```

Timeout em (3)/(4) → retry/backoff → `BQ_FAILED` (persistido). Poll a cada `CMD_POLL_MS`.

### 5. Store / fila (`internal/queue/store.go`, `queue.go`)

- **Record de lote**: `{batch_id, robot, acao, qtd, items[], state, item_status[], detail, ts}`.
  Chave inclui `robot` (2 slots independentes).
- Idempotência por `(robot, batch_id)`.
- Estados: `QUEUED → DISPATCHED → STARTED → ACKED_OK | ACKED_ERROR | FAILED`.
- Broker publica `BatchStatusUpdate` (com `items[]`) por transição; replay no connect do
  que não teve `ConfirmBatchStatus`.
- `Recover()`: reconciliar `DISPATCHED/STARTED` lendo `gLote_Id_RoboN`+`Ack`
  (evita reexecução).

### 6. Validação (`internal/queue/action.go`)

- `acao ∈ {1,2,3,4,5}` (não mais só `insert`).
- `robot ∈ {1,2}`, `qtd ∈ {1..5}`, `len(items) == qtd`, `position` único em `1..qtd`.
- Ranges do TComando: `dedo 1–5`, `dedo_2 0/1–5` (obrigatório só `acao=5`),
  `estande 1–2`, `bandeja 1–4`, `fileira 1–3`, `coluna 1–25`, `rack 1–8`, `placa 1–5`,
  `slot 1–4`.
- Aplicabilidade: campos de bandeja só em `acao∈{1,4}`; de placa só em `acao∈{2,3,5}`
  (os demais = 0).

### 7. Config (`internal/config/config.go`)

Reusar `CMD_ACK_TIMEOUT_MS`, `CMD_MAX_RETRIES`, `CMD_POLL_MS`, `QUEUE_DB_PATH`.
Opcional `BATCH_START_TIMEOUT_MS` (timeout específico do `Start`).

### 8. Mock (`internal/plc/mock.go`)

Simular o lote: ao ver `gLote_Req_RoboN`, subir `Start`, preencher `status[i]=1`, subir
`Ack`; ao ver `Req=FALSE`, baixar `Start`/`Ack`. Base dos testes sem CLP.

## Compatibilidade

O modelo `gCmd_*` (comando único) e o `gLote_*` (lote) são **excludentes** no ladder.
Recomendação: **substituir** o `gCmd` pelo `gLote` (a fila single-command era protótipo).
Para transição gradual, é possível manter os dois conjuntos de RPCs por um tempo, mas o
ladder do CLP só implementa um.

## Testes (fim-a-fim, análogo a [testar-fila-de-comandos.md](testar-fila-de-comandos.md))

- `go test ./...`: ack OK/erro, timeout→retry→FAILED, idempotência por `(robot,batch_id)`,
  recovery, 2 robôs em paralelo, integração gRPC via bufconn contra o mock.
- Manual (grpcurl contra o mock):
  ```
  EnqueueBatch {"batch_id":101,"robot":1,"acao":1,"qtd":3,"items":[
    {"position":1,"dedo":1,"estande":1,"bandeja":1,"fileira":1,"coluna":1},
    {"position":2,"dedo":2,"estande":1,"bandeja":1,"fileira":1,"coluna":2},
    {"position":3,"dedo":3,"estande":1,"bandeja":1,"fileira":1,"coluna":3}]}
  SubscribeBatchStatus {}   -> QUEUED → DISPATCHED → STARTED → ACKED_OK (items: 1..3 = 1)
  ConfirmBatchStatus {"batch_id":101,"robot":1}
  ```
- Watch Window (só Sysmac): forçar `gLote_Req_Robo1` e validar `Start`/`status[i]`/`Ack`.

## Questões abertas (alinhar com a equipe)

- **Erro no meio do lote (C005/C008):** o CLP para e reporta estado parcial (quais itens
  OK/falho/não-tentados)? Isso define `BQ_ACKED_ERROR` vs um estado parcial e o `detail`.
  Ver [../docs/condicionais/C005.md](../docs/condicionais/C005.md) e
  [../docs/condicionais/C008.md](../docs/condicionais/C008.md).
- **Escrita:** `WriteStructRaw` (array inteiro) vs campo-a-campo só dos `qtd` válidos —
  validar qual é mais robusto no NX1P2 real (o TODO de [../TODO.md](../TODO.md)).
- **Escopo do `gLote_Id`:** confirmar que é por robô e cabe em `int32` (DINT).
- **Ladder do `gLote` no Sysmac:** documentar (equivalente ao
  [handshake-clp-sysmac.md](handshake-clp-sysmac.md), agora em lote com o `Start`
  intermediário).
