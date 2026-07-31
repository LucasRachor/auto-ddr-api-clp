# Implementação — Template `go_auto_ddr_clp`

Documento descrevendo tudo que foi gerado neste template inicial da API
gRPC ↔ CLP Omron NX1P2 via EtherNet/IP.

## Visão geral

```
[ App cliente ] --gRPC--> [ plc-api (este repo) ] --EtherNet/IP/CIP--> [ Omron NX1P2 ]
```

A API tem dois lados:
- **Norte (gRPC):** recebe comandos da aplicação cliente.
- **Sul (EtherNet/IP):** sessão CIP persistente com o CLP, acessando variáveis
  por **tag simbólica** (publicadas no Sysmac Studio).

Stack: Go 1.23, `google.golang.org/grpc`, `github.com/loki-os/go-ethernet-ip`,
`log/slog`.

## Árvore de arquivos criados

```
go_auto_ddr_clp/
├── go.mod
├── README.md
├── IMPLEMENTACAO.md                 (este arquivo)
├── cmd/
│   └── server/
│       └── main.go                  bootstrap: config, conexão CLP, gRPC
├── proto/
│   └── plc/v1/
│       └── plc.proto                contrato gRPC
└── internal/
    ├── config/
    │   └── config.go                env loading
    ├── plc/
    │   ├── client.go                interface Client + tipos (TagValue, Status)
    │   ├── omron_nx1p2.go           impl com go-ethernet-ip (stubs)
    │   └── mock.go                  impl in-memory para testes
    ├── server/
    │   ├── server.go                struct PLCServer (handlers gRPC)
    │   ├── control.go               doStart / doStop / doReset
    │   ├── tags.go                  doReadTag / doWriteTag / SubscribeLoop
    │   └── health.go                doStatus / doHealthcheck
    └── pubsub/
        └── broker.go                fan-out de TagUpdate
```

## Arquivos e responsabilidades

### `go.mod`
Módulo `go_auto_ddr_clp`, Go 1.23. Dependências declaradas: `grpc`,
`protobuf`, `loki-os/go-ethernet-ip`. Rodar `go mod tidy` resolve as versões.

### `proto/plc/v1/plc.proto`
Contrato gRPC. Serviço `plc.v1.PLCService` com 8 RPCs:

| RPC             | Tipo            | Função                                     |
|-----------------|-----------------|--------------------------------------------|
| `Start`         | unary           | Dispara comando de start no CLP            |
| `Stop`          | unary           | Dispara comando de stop                    |
| `Reset`         | unary           | Limpa falhas                               |
| `ReadTag`       | unary           | Lê uma tag simbólica                       |
| `WriteTag`      | unary           | Escreve em uma tag simbólica               |
| `GetStatus`     | unary           | Modo (RUN/PROGRAM), faulted, detail        |
| `Healthcheck`   | unary           | Liveness do serviço                        |
| `SubscribeTags` | server-stream   | Stream de updates por mudança de valor     |

`TagValue` é um `oneof` cobrindo BOOL/INT/DINT/REAL/STRING — o conjunto típico
útil para Omron NX/NJ via CIP.

### `internal/config/config.go`
Carrega 4 variáveis de ambiente com defaults:

| Env         | Default           | Uso                            |
|-------------|-------------------|--------------------------------|
| `GRPC_ADDR` | `:50051`          | Porta de escuta do gRPC        |
| `CLP_HOST`  | `192.168.250.1`   | IP do CLP                      |
| `CLP_PORT`  | `44818`           | Porta EtherNet/IP padrão       |
| `LOG_LEVEL` | `info`            | Nível do `slog`                |

Sem dependências externas (`os.Getenv` puro).

### `internal/plc/client.go`
Define a fronteira de abstração entre handlers gRPC e o driver físico:

```go
type Client interface {
    Connect(ctx) error;  Close() error
    Start(ctx) error;    Stop(ctx) error;  Reset(ctx) error
    ReadTag(ctx, tag) (TagValue, error)
    WriteTag(ctx, tag, TagValue) error
    Status(ctx) (Status, error)
}
```

Tipos `TagValue` (com `Kind` enumerado) e `Status{Mode,Faulted,Detail}` são
neutros em relação ao protocolo — convertidos para protobuf na borda gRPC.

Sentinelas: `ErrNotConnected`, `ErrUnsupportedKind`.

### `internal/plc/omron_nx1p2.go`
Impl real (com stubs marcados `// TODO`) usando `go-ethernet-ip`. Mantém
sessão CIP persistente protegida por `sync.Mutex`. Convenções de tags no
programa do CLP:

| Tag                | Tipo   | Uso                                    |
|--------------------|--------|----------------------------------------|
| `gSysCmd_Start`    | BOOL   | Pulso TRUE→FALSE (Start)               |
| `gSysCmd_Stop`     | BOOL   | Pulso TRUE→FALSE (Stop)                |
| `gSysCmd_Reset`    | BOOL   | Pulso TRUE→FALSE (Reset)               |
| `gSys_Mode`        | STRING | `"RUN"` / `"PROGRAM"`                  |
| `gSys_Faulted`     | BOOL   | Flag de falha                          |
| `gSys_FaultDetail` | STRING | Detalhe da falha                       |

`Start/Stop/Reset` reusam o helper `pulse()` (TRUE → 50 ms → FALSE).

**Pontos com `// TODO` para você preencher:**
- Em `Connect`: `ethernetip.NewClient(...)` + `Connect()` + `RegisterSession()`.
- Em `Close`: `UnRegisterSession()` + `Disconnect()`.
- Em `ReadTag`/`WriteTag`: `GetTag(tag)` + `Read()`/`Write()` + conversão de
  tipo para/de `TagValue`.

### `internal/plc/mock.go`
Impl in-memory thread-safe (`sync.RWMutex`). Útil para subir o servidor sem
CLP e validar handlers gRPC. `Start` muda `Mode` para `RUN`, `Stop` para
`PROGRAM`, `Reset` zera `Faulted`/`Detail`.

### `internal/server/server.go`
Struct `PLCServer{ PLC plc.Client; Log *slog.Logger }`. Carrega o embed
`pb.UnimplementedPLCServiceServer` (comentado até gerar os stubs).

### `internal/server/control.go`
Helpers `doStart` / `doStop` / `doReset` que delegam para `s.PLC` e logam.
A intenção é que os métodos gerados (`Start(ctx, *pb.StartRequest)`) chamem
esses helpers e empacotem em `*pb.CommandResponse`.

### `internal/server/tags.go`
- `doReadTag` / `doWriteTag`: passa-faixa para `Client`.
- **`SubscribeLoop`**: motor do RPC streaming. Recebe um `send` callback com a
  assinatura genérica `func(tag, TagValue, time.Time) error`. Lógica:
  - Polling com `time.Ticker(poll)` (default 100 ms se `poll <= 0`).
  - Mantém `last` por tag e só emite quando o valor muda (deduplicação).
  - Erro de leitura é logado mas não interrompe o stream.
  - Encerra em `ctx.Done()` (cliente desconectou) ou erro do `send`.

### `internal/server/health.go`
`doStatus` delega para `Client.Status`. `doHealthcheck` retorna `true` se há
um cliente PLC injetado (você pode evoluir para checar a sessão CIP de fato).

### `internal/pubsub/broker.go`
Broker fan-out (`Subscribe`/`Unsubscribe`/`Publish`). Não está cabeado ao
`SubscribeLoop` ainda — disponível para uma futura otimização: rodar **um**
goroutine de polling compartilhado entre N subscribers em vez de cada stream
pollar o CLP separadamente. Estratégia de overflow: drop em subscriber lento
(`select { case ch <- u: default: }`).

### `cmd/server/main.go`
Bootstrap:
1. `slog` em stdout.
2. `config.Load()`.
3. `signal.NotifyContext` com SIGINT/SIGTERM.
4. `plc.NewOmronNX1P2(host, port).Connect(ctx)` — fail-fast se o CLP não
   responder no boot.
5. `grpc.NewServer` com `KeepaliveParams{Time: 30s, Timeout: 10s}`.
6. Goroutine de shutdown: `<-ctx.Done()` → `gs.GracefulStop()`.
7. `gs.Serve(lis)`.

Linhas com `pb.RegisterPLCServiceServer(gs, srv)` ficam comentadas até gerar
os stubs.

### `README.md`
Quickstart: comandos `protoc`/`go run`, smoke-test com `grpcurl`, tabela de
convenções de tags e lembrete de **Network Publish = Publish Only** no Sysmac.

## Fluxo de uma chamada (exemplo: `Start`)

```
App cliente
   │  gRPC plc.v1.PLCService/Start
   ▼
PLCServer.Start (gerado, a implementar)
   │  s.doStart(ctx)
   ▼
PLCServer.doStart  (internal/server/control.go)
   │  s.PLC.Start(ctx)
   ▼
OmronNX1P2.Start  (internal/plc/omron_nx1p2.go)
   │  pulse("gSysCmd_Start"): WriteTag(true) → 50ms → WriteTag(false)
   ▼
go-ethernet-ip → CIP frame → CLP NX1P2
```

## O que falta para rodar de verdade

1. `go mod tidy` para resolver a versão concreta da `go-ethernet-ip`.
2. Gerar protobuf:
   ```sh
   protoc --go_out=. --go_opt=module=go_auto_ddr_clp \
          --go-grpc_out=. --go-grpc_opt=module=go_auto_ddr_clp \
          proto/plc/v1/plc.proto
   ```
3. Descomentar imports e chamadas `pb.*` em `server.go` e `main.go`.
4. Implementar os métodos do `pb.PLCServiceServer` (8 RPCs) chamando os
   helpers `doStart/doStop/doReset/doReadTag/doWriteTag/doStatus/
   doHealthcheck/SubscribeLoop` e fazendo a conversão `plc.TagValue` ↔
   `pb.TagValue`.
5. Preencher os `// TODO` em `omron_nx1p2.go` com a API real da lib EIP.
6. Criar as variáveis globais no Sysmac Studio com **Network Publish** ligado.

## Decisões de design relevantes

- **Interface `Client` no meio:** isola o gRPC do driver. Permite trocar
  `go-ethernet-ip` por `libplctag` (cgo) ou outra lib futuramente sem mexer
  nos handlers, e habilita testes com `Mock`.
- **Start/Stop por tag de comando, não por CIP service code:** mudar o modo
  do controlador (RUN↔PROGRAM) via CIP exige service codes específicos e
  privilégios; escrever em uma tag BOOL que o programa Ladder/ST do CLP
  observa é portável e seguro.
- **`SubscribeLoop` com dedupe por valor:** evita inundar o stream quando o
  CLP é polled mais rápido que a taxa de mudança real das tags.
- **Sem cgo no caminho default:** mantém o build cross-platform fácil
  (Windows é seu ambiente atual).
- **Keepalive gRPC habilitado:** detecta conexões mortas e libera recursos.
