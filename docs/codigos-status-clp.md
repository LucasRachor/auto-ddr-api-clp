# Códigos de status do CLP (CIP General Status)

Quando a API lê/escreve uma tag, o CLP responde com um **CIP General Status** de 1
byte. `0x00` = sucesso; qualquer outro valor = falha. Esse é o `GeneralStatus` que
aparece nos logs dos comandos de diagnóstico, ex.:

```
[writeBoolDebug] tag="Pedido" reply=0xcd status=0x1f addStatus=02010310
                                            ^^^^^^^^^^  ^^^^^^^^^^^^^^^^
                                         General Status   Additional Status
```

- **General Status** (`status=`): o código da tabela abaixo.
- **Additional Status** (`addStatus=`): detalhe extra específico do fabricante
  (Omron). Só tem significado quando o General Status é de erro; ajuda a distinguir
  a causa (ex.: tag inexistente vs. tipo errado).

---

## Tabela — General Status mais comuns

| Código | Nome (CIP) | Significado prático no NX1P2 |
|--------|------------|------------------------------|
| `0x00` | Success | **OK** — leitura/escrita concluída. |
| `0x04` | Path Segment Error | Nome de tag mal formado no request path (ex.: sintaxe de `a.b.c` errada). |
| `0x05` | Path Destination Unknown | **Tag não existe** ou não está com *Network Publish*. Confira o nome exato (maiúsc./minúsc.) e o publish no Sysmac. |
| `0x06` | Partial Transfer | Resposta veio em partes (dado maior que um pacote). Normal em leituras grandes. |
| `0x08` | Service Not Supported | O serviço não é suportado por esse objeto. É o que o NX1P2 devolve ao tentar **enumerar/browse** de tags (`GetInstanceAttributeList`) — por isso o acesso é sempre por nome. |
| `0x09` | Invalid Attribute Value | Valor de atributo inválido no request. |
| `0x0A` | Attribute List Error | Erro na lista de atributos pedida. |
| `0x0C` | Object State Conflict | O objeto está num estado que não permite a operação agora. |
| `0x0E` | Attribute Not Settable | Tentativa de escrever numa tag **somente leitura** (publish "Publish Only" sem permissão de escrita, ou constante). |
| `0x10` | Device State Conflict | Estado do controlador impede a operação (ex.: em certos modos). |
| `0x13` | Not Enough Data | Faltaram bytes no request — payload menor que o esperado para o tipo. |
| `0x15` | Too Much Data | Bytes demais no request — payload maior que o esperado para o tipo. |
| `0x1E` | Embedded Service Error | Erro num serviço embutido (ex.: dentro de um Multiple Service Packet). |
| `0x1F` | Vendor Specific Error | **Erro específico da Omron.** O mais comum aqui é **descasamento de tipo/tamanho**: escrever um BOOL como 1 byte (o NX1P2 espera 2), ou escrever `int32` numa tag `INT`. Veja o `addStatus` para o detalhe. |
| `0x20` | Invalid Parameter | Parâmetro inválido no serviço. |
| `0x26` | Path Size Invalid | Tamanho do request path inconsistente. |
| `0xFF` | General Error | Erro genérico; consulte sempre o `addStatus`. |

> A lista completa está na *CIP Common Specification* (Volume 1, Apêndice de
> General Status Codes). A tabela acima cobre o que costuma aparecer no NX1P2.

---

## Como a API traduz isso (fila de comandos)

Não confundir dois níveis de "status":

1. **General Status do CIP** (esta tabela) — resultado do transporte da tag: a
   escrita/leitura chegou e foi aceita? Tratado no driver
   (`internal/plc/omron_nx1p2.go`); qualquer valor ≠ `0x00` vira erro Go.
2. **Status da aplicação** (`gCmd_AckStatus`) — resultado da **execução do comando**
   pelo ladder, já por cima do handshake:

   | `gCmd_AckStatus` | Significado |
   |------------------|-------------|
   | `0` | nenhum ack ainda |
   | `1` | comando executado com **sucesso** (→ estado `ACKED_OK`) |
   | `2` | comando falhou na máquina (→ estado `ACKED_ERROR`, detalhe em `gCmd_AckDetail`) |

   Esse mapeamento fica em `internal/queue/dispatcher.go`. Para usar outros códigos
   de erro da célula, é só ampliar essa tabela.

---

## Diagnóstico rápido por código

- **`0x05`** → o nome da tag está errado ou sem *Network Publish*. Rode
  `go run ./cmd/readtag <tag>`: se também der `0x05`, é o nome/publish.
- **`0x1F` numa escrita** → quase sempre tipo/tamanho. Leia a tag primeiro com
  `go run ./cmd/readtag <tag>` para ver o `tipo=` e o `bytes=`, e escreva com o tipo
  certo via `go run ./cmd/writetag <tag> <tipo> <valor>` (ex.: `int` para tag INT,
  `dint` para DINT — não troque um pelo outro).
- **`0x0E`** → a tag é somente leitura; ajuste o publish/atributo no Sysmac.
- **`0x08`** → você tentou listar tags; o NX1P2 não permite, acesse por nome.

---

## Additional Status (Omron)

O `addStatus` vem como bytes crus (ex.: `02010310`). Não há tabela pública única —
os valores são específicos da Omron e servem para diferenciar causas dentro do mesmo
General Status. Regra prática: **use o General Status para o diagnóstico principal**
e registre o `addStatus` no chamado de suporte/Sysmac quando precisar do detalhe fino.
