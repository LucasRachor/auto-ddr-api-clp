# go_auto_ddr_clp

Template em Go de uma API gRPC que traduz comandos para um CLP **Omron NX1P2**
via **EtherNet/IP (CIP, tags simbólicas)**.

```
[ App cliente ] --gRPC--> [ plc-api (este repo) ] --EtherNet/IP--> [ Omron NX1P2 ]
```

## Estrutura

- `cmd/server` — bootstrap do servidor gRPC.
- `proto/plc/v1/plc.proto` — contrato dos RPCs (Start/Stop/Reset/ReadTag/WriteTag/GetStatus/Healthcheck/SubscribeTags).
- `internal/plc` — interface `Client`, impl `OmronNX1P2` (stubs com `go-ethernet-ip`) e `Mock` para testes.
- `internal/server` — handlers gRPC; `tags.go` traz o motor do streaming (`SubscribeLoop`).
- `internal/pubsub` — broker fan-out para compartilhar polling entre múltiplos subscribers.
- `internal/config` — env (`CLP_HOST`, `CLP_PORT`, `GRPC_ADDR`, `LOG_LEVEL`).

## Gerar stubs protobuf

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

protoc \
  --go_out=. --go_opt=module=go_auto_ddr_clp \
  --go-grpc_out=. --go-grpc_opt=module=go_auto_ddr_clp \
  proto/plc/v1/plc.proto
```

Saída esperada em `gen/plc/v1/`. Depois disso descomente as linhas marcadas
com `pb.` em `internal/server/server.go` e `cmd/server/main.go`, e implemente
os métodos do `pb.PLCServiceServer` chamando os helpers `doStart`, `doStop`,
`doReset`, `doReadTag`, `doWriteTag`, `doStatus`, `doHealthcheck` e
`SubscribeLoop`.

## Rodar

```sh
go mod tidy
CLP_HOST=192.168.250.1 GRPC_ADDR=:50051 go run ./cmd/server
```

```ps1
go run .\cmd\server\main.go
```

Smoke-test:

```sh
grpcurl -plaintext localhost:50051 plc.v1.PLCService/Healthcheck
```

## Onde preencher a integração real

`internal/plc/omron_nx1p2.go` tem `// TODO` em todos os pontos onde a API
concreta da lib `github.com/loki-os/go-ethernet-ip` precisa entrar
(`Connect/RegisterSession`, `GetTag/Read/Write`, conversão de tipo).

Convenções no Sysmac Studio para as tags de comando do template:

| Tag                | Tipo   | Uso                         |
| ------------------ | ------ | --------------------------- |
| `gSysCmd_Start`    | BOOL   | Pulso TRUE→FALSE para Start |
| `gSysCmd_Stop`     | BOOL   | Pulso TRUE→FALSE para Stop  |
| `gSysCmd_Reset`    | BOOL   | Pulso TRUE→FALSE para Reset |
| `gSys_Mode`        | STRING | "RUN" / "PROGRAM"           |
| `gSys_Faulted`     | BOOL   | Flag de falha               |
| `gSys_FaultDetail` | STRING | Detalhe legível da falha    |

Marque todas com **Network Publish = Publish Only** no Sysmac Studio. NX1P2
escuta EtherNet/IP em **TCP/UDP 44818**.
