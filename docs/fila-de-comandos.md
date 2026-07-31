# Fila de comandos com confirmação (ack)

Documento de referência da fila durável de comandos entre o backend **NestJS** e o
**CLP Omron NX1P2**, com confirmação de execução (ack) e correlação por id.

> Garantia central: **nenhum comando se perde** — nem na entrada, nem em voo, nem no
> retorno do status, mesmo que a API ou o Nest reiniciem.

---

## Arquitetura

```
[ Nest ] --gRPC--> [ plc-api (Go) ] --EtherNet/IP (CIP)--> [ Omron NX1P2 ]
                         |
                    fila durável (bbolt) + dispatcher serial
```

Fluxo de um comando:

1. Nest chama `EnqueueCommand` com `{command_id, action, motherboard, robot, finger}`.
2. A API valida e **persiste** o comando como `QUEUED` no bbolt (commit/fsync antes
   de responder) e responde `accepted`.
3. O **dispatcher** (goroutine única) pega o próximo `QUEUED` em ordem FIFO, escreve
   os campos nas tags do CLP e executa o **handshake de 4 vias**.
4. O CLP confirma via tags de ack, correlacionadas pelo `command_id`.
5. A API publica o novo estado no stream `SubscribeCommandStatus`, que o Nest
   consome. Estados não confirmados são re-enviados no reconnect (replay).

Estados: `QUEUED → DISPATCHED → ACKED_OK | ACKED_ERROR | FAILED`.

---

## API gRPC (`proto/plc/v1/plc.proto`)

| RPC | Tipo | Descrição |
|-----|------|-----------|
| `EnqueueCommand` | unário | Valida e enfileira um comando. Resposta `{accepted, command_id, message}`. |
| `SubscribeCommandStatus` | server-stream | Emite `CommandStatusUpdate {command_id, state, detail, ts_unix_ms}` a cada transição. Faz **replay no connect** do que ainda não foi confirmado. |
| `ConfirmStatus` | unário | Nest confirma a entrega de um status (`command_id`) → para de ser re-enviado no replay. |

### Contrato do `command_id`
- Fornecido pelo **Nest**.
- Deve ser **numérico e caber em `int32`** (1..2147483647), pois é escrito como
  `DINT` no CLP para correlação do ack.
- Idempotente: reenfileirar o mesmo id **não** cria registro novo.

### Validações de `EnqueueCommand`
- `action` ∈ allowlist (`internal/queue/action.go`) — hoje: `insert` (=1).
- `motherboard` não-vazio.
- `robot` ∈ [1, 2].
- `finger` ∈ [1, 5].

---

## Handshake de tags com o CLP

Slot único — **um comando em execução por vez**. Tags a publicar no Sysmac Studio
(Network Publish = "Publish Only"):

**Requisição (API → CLP):**

| Tag | Tipo | Descrição |
|-----|------|-----------|
| `gCmd_Id` | DINT | id do comando (do Nest) |
| `gCmd_Action` | INT | código da ação (`insert`=1) |
| `gCmd_Motherboard` | STRING | ex.: "MB09" |
| `gCmd_Robot` | INT | 1..2 |
| `gCmd_Finger` | INT | 1..5 |
| `gCmd_Req` | BOOL | TRUE = comando válido presente |

**Resposta (CLP → API):**

| Tag | Tipo | Descrição |
|-----|------|-----------|
| `gCmd_AckId` | DINT | eco do id processado |
| `gCmd_AckStatus` | INT | 0=none, 1=OK, 2=erro |
| `gCmd_AckDetail` | STRING | detalhe/erro (opcional) |
| `gCmd_Ack` | BOOL | TRUE = ack pronto |

**Sequência:**

1. API garante `gCmd_Req=FALSE` e espera `gCmd_Ack=FALSE` (slot ocioso).
2. API escreve `Id, Action, Motherboard, Robot, Finger`.
3. API seta `gCmd_Req=TRUE`.
4. CLP detecta `Req`, trava os dados, executa, escreve `AckId/AckStatus/AckDetail`
   e seta `gCmd_Ack=TRUE`.
5. API faz poll; ao ver `Ack=TRUE` **e** `AckId==Id` → registra o status.
6. API seta `gCmd_Req=FALSE` (confirma recebimento).
7. CLP detecta `Req=FALSE` → limpa `gCmd_Ack=FALSE`.
8. API espera `gCmd_Ack=FALSE` → slot ocioso, segue para o próximo comando.

Timeout sem ack → o comando vai para `FAILED` (persistido, **não descartado**), após
`CMD_MAX_RETRIES` tentativas com backoff.

---

## Configuração (variáveis de ambiente)

| Variável | Default | Descrição |
|----------|---------|-----------|
| `QUEUE_DB_PATH` | `./data/queue.db` | Caminho do banco bbolt da fila |
| `CMD_ACK_TIMEOUT_MS` | `5000` | Timeout aguardando o ack por tentativa |
| `CMD_MAX_RETRIES` | `3` | Retentativas antes de `FAILED` |
| `CMD_POLL_MS` | `50` | Intervalo de poll das tags de ack |
| `GRPC_ADDR` | `:50051` | Endereço gRPC |
| `CLP_HOST` / `CLP_PORT` | `192.168.1.14` / `44818` | Endereço do CLP |

---

## Garantias de não-perda

- **Entrada:** `Enqueue` só responde `accepted` após o commit (fsync) do bbolt.
- **Em voo:** cada transição é persistida antes do efeito; no boot, `Recover()`
  reconcilia comandos em `DISPATCHED` lendo `gCmd_AckId` (evita execução dupla) ou
  re-enfileira.
- **Saída (status):** estados persistidos; `SubscribeCommandStatus` re-envia tudo que
  não foi confirmado via `ConfirmStatus`. Se o Nest cair, recebe os pendentes ao
  reconectar.
- **Falha de execução:** timeout/erro → `FAILED` (dead-letter visível), nunca silencioso.

---

## Mapa de arquivos

| Arquivo | Papel |
|---------|-------|
| `proto/plc/v1/plc.proto` | RPCs/mensagens da fila (gera `gen/plc/v1`) |
| `internal/queue/store.go` | Persistência bbolt (Record, Enqueue, NextQueued, etc.) |
| `internal/queue/queue.go` | `Queue` + broker de status (fan-out p/ streams) |
| `internal/queue/dispatcher.go` | Dispatcher serial, handshake, retry, `Recover` |
| `internal/queue/action.go` | Allowlist de ações + validação |
| `internal/plc/omron_nx1p2.go` | Constantes das tags `gCmd_*` |
| `internal/plc/mock.go` | Mock que simula o handshake (demo sem CLP) |
| `internal/server/commands.go` | Handlers `EnqueueCommand`/`SubscribeCommandStatus`/`ConfirmStatus` |
| `cmd/server/main.go` | Abre bbolt, `Recover`, sobe dispatcher, shutdown |
| `internal/config/config.go` | Variáveis de ambiente da fila |

---

## Verificação

```bash
go build ./...
go vet ./...
go test ./...
```

Testes cobrem: ack OK/erro, timeout→retry→FAILED, validação, idempotência, recovery
(com e sem ack) e integração gRPC end-to-end via bufconn (QUEUED→DISPATCHED→ACKED_OK
pelo stream, usando o Mock).

Demo manual (com `grpcurl`, contra o Mock que auto-confirma):

```bash
grpcurl -plaintext -d '{"command_id":101,"action":"insert","motherboard":"MB09","robot":1,"finger":1}' \
  localhost:50051 plc.v1.PLCService/EnqueueCommand

grpcurl -plaintext -d '{}' localhost:50051 plc.v1.PLCService/SubscribeCommandStatus
```

---

## Status da implementação

### ✅ Implementado

- [x] RPCs/mensagens no proto + código gerado (`EnqueueCommand`,
      `SubscribeCommandStatus`, `ConfirmStatus`, enum `CommandState`).
- [x] Fila durável em bbolt (idempotente, FIFO, replay de não-entregues).
- [x] Dispatcher serial com handshake de 4 vias, timeout, retry/backoff e dead-letter.
- [x] `Recover()` na inicialização (reconciliação de comandos em voo).
- [x] Constantes das tags `gCmd_*`.
- [x] Handlers gRPC + injeção da `Queue` no servidor.
- [x] Wiring no `main.go` + variáveis de configuração.
- [x] Mock com handshake simulado para demo/testes.
- [x] Testes unitários (fila) e de integração (gRPC via bufconn).

### ⏳ Pendente

- [ ] **Ladder no CLP (Sysmac Studio):** implementar a lógica do handshake nas tags
      `gCmd_*` (publish-only) conforme a sequência acima. Pré-requisito para o uso real.
- [ ] **Trocar o Mock pelo driver real:** em `cmd/server/main.go`, substituir
      `plc.NewMock()` por `plc.NewOmronNX1P2(cfg.CLPHost, cfg.CLPPort)` (depende do
      `Connect` real do `go-ethernet-ip` — há TODOs em `omron_nx1p2.go`).
- [ ] **Integração no Nest:** gerar os stubs gRPC a partir do `.proto`, manter um
      stream `SubscribeCommandStatus` aberto e chamar `ConfirmStatus` ao processar
      cada status.
- [ ] **Definir status > 2 do CLP** (se necessário): hoje 1=OK, 2=erro; mapear outros
      códigos em `dispatcher.go` se o ladder usar mais.
- [ ] **Política de `FAILED`:** decidir se haverá reprocessamento manual/automático de
      comandos em dead-letter (hoje ficam persistidos e visíveis, sem retry adicional).
- [ ] **`command_id` como STRING/UUID** (opcional): se o Nest passar a usar UUID,
      trocar `gCmd_Id`/`gCmd_AckId` para STRING e ajustar a correlação.
