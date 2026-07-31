# Escrita de STRUCT / ARRAY OF STRUCT (UDT) no NX1P2

Este documento descreve como a API escreve tipos **estruturados** (UDTs do Sysmac
Studio) no CLP Omron NX1P2, o formato CIP descoberto por teste no equipamento, e os
comandos/funções adicionados para isso.

> Contexto: os tipos atômicos (BOOL, INT, DINT, REAL, STRING, e arrays deles) já eram
> cobertos por `WriteValue`/`encodeCIP`. Faltava escrever um **array de structs** — o
> caso do programa de teste do CLP (`Itens : ARRAY[1..3] OF ST_Item`).

---

## 1. Como o NX1P2 representa um struct no CIP

Ao **ler** uma tag estruturada (`ReadTagRaw`), o controlador responde com:

```
[tipo 0x02A0 (2B)][structure handle (2B)][elementos empacotados...]
```

- **`0x02A0`** — type code CIP de "tipo estruturado" (não é um tipo atômico como `0xC3`).
- **structure handle** — 2 bytes = CRC do *template* do UDT. Identifica a estrutura;
  é o único valor não-zero num array recém-zerado, o que confirma ser metadado.
- **elementos** — os membros de cada elemento do array, concatenados em little-endian.

### Exemplo real — `Itens : ARRAY[1..3] OF ST_Item`

O UDT `ST_Item` (offset **NJ**) tem o layout, confirmado por read-back:

| Membro | Tipo | Bytes |
|--------|------|-------|
| `Numero` | INT  | 2 (LE) |
| `Estado` | INT  | 2 (LE) |
| `Ligado` | BOOL | **2** (0x0001 = TRUE / 0x0000 = FALSE) |
| **total por elemento** | | **6** |

> ⚠️ Um `BOOL` **dentro do struct** ocupa **2 bytes**, igual a um BOOL escalar neste
> controlador. Não é bit-packed.

Leitura da tag zerada (`go run ./cmd/readtag Itens`):

```
tipo=0x02a0  bytes=20
89 09 | 00 00 00 00 00 00 | 00 00 00 00 00 00 | 00 00 00 00 00 00
handle  elem[1]             elem[2]             elem[3]
```

`20 bytes = 2 (handle) + 3 × 6 (elementos)`.

---

## 2. Formato de ESCRITA (confirmado por probe — 2026-07-13)

O Write Tag (serviço `0x4D`) de um tipo estruturado usa:

```
[tipo 0x02A0 (2B)][structure handle (2B)][count = 1 (2B)][TODOS os membros LE]
```

O array **inteiro** vai num **único frame**. O detalhe crítico:

> **`count` é SEMPRE 1.** O NX1P2 trata a tag estruturada como **um objeto único**, com
> todos os elementos concatenados no bloco de dados. Enviar `count = nº de elementos`
> (ex.: 3) é recusado com `GeneralStatus=0x20` (Invalid Parameter, addStatus `0x8017`).

Formatos testados pelo `cmd/structprobe` e seus resultados:

| Formato | Resultado |
|---------|-----------|
| `[tipo][handle][count=3][data]` | ❌ `0x20` |
| `[tipo][handle][data]` (sem count) | ❌ `0x20` |
| `[tipo][count=3][data]` (sem handle) | ❌ `0x20` |
| **`[tipo][handle][count=1][data]`** | ✅ **OK** |

---

## 3. Índice de array e acesso por membro

O acesso a um elemento/membro por nome (`Itens[1].Numero`) usa um segmento lógico
**Member/Element `0x28`** após o segmento simbólico ANSI do nome. Isso vale para
**leitura e escrita**.

- O índice CIP é o **declarado no Sysmac Studio**. Como `Itens` é `ARRAY[1..3]`
  (base 1), `Itens[1]` é o primeiro elemento; **`Itens[0]` dá `GeneralStatus=0x05`**
  (fora dos limites).
- Escrever um membro é uma escrita **atômica** normal:
  `WriteValue(ctx, "Itens[1].Numero", int16(111))`. É a alternativa ao write do struct
  inteiro (útil para atualizar um campo só).

---

## 4. O que foi adicionado no código

### `internal/plc/omron_nx1p2.go`

- **`symbolicPath(name)`** — monta o Request Path simbólico. Cada nível separado por
  `.` vira um segmento ANSI; um sufixo `[i]` vira um segmento Member (`0x28`).
  Ex.: `"Itens[1].Numero"` → `ANSI(Itens) + Member(1) + ANSI(Numero)`. Usado por
  `ReadTagRaw`, `writeTagUCMM` e a escrita de struct — read e write ganharam índice de
  array de graça.

- **`WriteStructRaw(ctx, name, data)`** — escreve um UDT / array-de-UDT num único
  frame. Lê o *structure handle* do próprio controlador antes de escrever (não precisa
  hardcode), monta `[0x02A0][handle][count=1][data]` e envia por UCMM-direto. `data`
  são os bytes de **todos** os elementos, já serializados no packing do UDT.

- **`writeStructUCMM`** — helper interno que monta/envia o payload acima.

- **`WriteRawDebug(ctx, name, payload)`** — primitivo de diagnóstico: envia um Write
  Tag com o payload completo já montado e devolve `GeneralStatus`/`AdditionalStatus`
  crus (não transforma status ≠ 0 em erro). Base do `structprobe`.

- Const **`cipTypeStruct = 0x02A0`**.

### `cmd/writeitens/` (novo)

Comando de teste que escreve 3 `ST_Item` na tag `Itens` num único frame:

```powershell
$env:CLP_HOST="192.168.1.14"; go run ./cmd/writeitens
```

### `cmd/structprobe/` (novo)

Comando de diagnóstico que, numa execução, testa vários cabeçalhos de escrita do array
inteiro **e** a escrita por membro (base 0 e base 1), imprimindo o status de cada. Foi
o que revelou o formato `count=1` e a indexação base-1. Mantido para referência futura.

---

## 5. Como escrever OUTRO UDT (ex.: `ST_Pedido_Comida` / `Pedido`)

1. **Publicar** a tag no Sysmac Studio (**Network Publish = Publish Only**), fazer
   **Rebuild** e **Transfer to Controller**. Sem isso, toda leitura/escrita simbólica
   volta `GeneralStatus=0x04` (ver [codigos-status-clp.md](codigos-status-clp.md)).
2. `go run ./cmd/readtag <Tag>` para confirmar `tipo=0x02a0`, capturar o **handle** e
   medir o **stride** por elemento (packing exato dos membros, incl. BOOLs).
3. Serializar os membros de **todos** os elementos em little-endian, na ordem do UDT.
4. `WriteStructRaw(ctx, "<Tag>", data)`.
5. Índice é **base declarada** (1-based nestes exemplos) se for acessar por membro.

---

## Referências

- [codigos-status-clp.md](codigos-status-clp.md) — tabela de `GeneralStatus`.
- [sysmac.md](sysmac.md) — publicação de variáveis no Sysmac Studio.
- Quirks de CIP do NX1P2 (roteamento UCMM-direto, sem browse) estão documentados na
  memória do projeto.
