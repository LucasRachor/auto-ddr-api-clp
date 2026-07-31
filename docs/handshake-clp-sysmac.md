# Handshake de comando no CLP (Sysmac Studio / NX1P2)

Guia para implementar, no programa do CLP, o lado que recebe os comandos da
`plc-api` e confirma a execução (ack). É o **pré-requisito** para usar a fila de
comandos com hardware real.

Princípio: a API e o CLP fazem um **aperto de mão de 4 vias** sobre tags. A API só
sobe `gCmd_Req` depois de escrever todos os campos, então o CLP nunca lê um comando
pela metade. Um comando em execução por vez (slot único).

---

## 1. Variáveis globais (com Network Publish = "Publish Only")

| Variável | Tipo | Publish | Papel |
|----------|------|---------|-------|
| `gCmd_Id` | DINT | sim | id do comando (vindo do Nest) |
| `gCmd_Action` | INT | sim | código da ação (`insert`=1) |
| `gCmd_Motherboard` | STRING[16] | sim | ex.: "MB09" |
| `gCmd_Robot` | INT | sim | 1..2 |
| `gCmd_Finger` | INT | sim | 1..5 |
| `gCmd_Req` | BOOL | sim | TRUE = comando válido presente |
| `gCmd_AckId` | DINT | sim | eco do id processado |
| `gCmd_AckStatus` | INT | sim | 0=none, 1=OK, 2=erro |
| `gCmd_AckDetail` | STRING[64] | sim | detalhe/erro (opcional) |
| `gCmd_Ack` | BOOL | sim | TRUE = ack pronto |
| `eCmdState` | INT | não | estado interno da máquina (0/1/2) |

Variáveis internas de trabalho (não precisam de publish): `wId`, `wAction`, `wMb`,
`wRobot`, `wFinger`.

---

## 2. Sequência do aperto de mão

```
API:  escreve Id/Action/Motherboard/Robot/Finger
API:  gCmd_Req = TRUE                ──►  "comando pronto"
CLP:  vê Req=TRUE → latcha campos → executa
CLP:  gCmd_AckId/AckStatus/AckDetail → gCmd_Ack = TRUE   ──►  "feito"
API:  vê Ack=TRUE e AckId==Id → registra status
API:  gCmd_Req = FALSE               ──►  "recebi, pode liberar"
CLP:  vê Req=FALSE → gCmd_Ack = FALSE ──►  "slot livre p/ o próximo"
```

---

## 3. Máquina de estados em Structured Text

```pascal
// Estados: 0=IDLE, 1=BUSY (executando), 2=DONE (aguarda API confirmar)
CASE eCmdState OF

  0: // IDLE — aguardando uma requisição
     gCmd_Ack := FALSE;                 // garante slot limpo
     IF gCmd_Req THEN
        // --- LATCHA os campos AGORA (todos já chegaram antes do Req) ---
        wId     := gCmd_Id;
        wAction := gCmd_Action;
        wMb     := gCmd_Motherboard;
        wRobot  := gCmd_Robot;
        wFinger := gCmd_Finger;

        // dispara a ação real da máquina (ex.: aciona robô/dedo)
        Exec_StartInsert(wRobot, wFinger);   // seu FB/rotina de execução
        eCmdState := 1;
     END_IF;

  1: // BUSY — executando o movimento
     IF Exec_Done THEN
        gCmd_AckId := wId;              // ECO do id processado
        IF Exec_Error THEN
           gCmd_AckStatus := 2;         // erro
           gCmd_AckDetail := Exec_ErrMsg;
        ELSE
           gCmd_AckStatus := 1;         // OK
           gCmd_AckDetail := '';
        END_IF;
        gCmd_Ack  := TRUE;              // sinaliza "ack pronto"
        eCmdState := 2;
     END_IF;

  2: // DONE — a API já leu o ack? Ela confirma baixando o Req.
     IF NOT gCmd_Req THEN
        gCmd_Ack  := FALSE;            // libera o slot
        eCmdState := 0;                // volta a IDLE p/ o próximo comando
     END_IF;

END_CASE;
```

`Exec_StartInsert`, `Exec_Done`, `Exec_Error` e `Exec_ErrMsg` representam a sua
rotina de execução (FB do movimento). Troque pela lógica real da célula.

---

## 4. Por que cada parte importa

- **Latch no IDLE, só com `gCmd_Req=TRUE`:** garante que os 5 campos já estão
  consistentes. A API escreve `gCmd_Req` por último, justamente para isso.
- **`gCmd_AckId := wId`:** o eco do id permite à API ter certeza de que o ack é
  deste comando (ela compara `AckId == Id`).
- **DONE espera `gCmd_Req` cair:** o CLP **não** zera o `Ack` sozinho; só limpa
  quando a API baixa o `Req`. Isso fecha o handshake — o CLP sabe que a API leu, e
  a API sabe que o slot será liberado.

---

## 5. Casos de borda cobertos

- **Reinício da API com `Req` preso em TRUE:** ao voltar, a API força `Req=FALSE`
  (`ensureIdle`). Se o CLP estava em DONE, vai para IDLE. Se estava em BUSY,
  termina, publica o ack, e a API reconcilia pelo `AckId` no `Recover()`.
- **Comando duplicado (proteção extra opcional):** guarde `gLastDoneId` e, no IDLE,
  ignore (ou reconfirme) um `Req` cujo `gCmd_Id == gLastDoneId`. A API já não
  reenfileira o mesmo id; isso protege contra reenvio após crash.

---

## 6. Detalhes específicos do NX1P2

- **STRING via CIP:** a API escreve string com `SetString`. Use `STRING` no Sysmac
  (não array de BYTE) para `gCmd_Motherboard`/`gCmd_AckDetail`, com folga de tamanho.
- **BOOL via CIP:** a API escreve BOOL como inteiro 0/1 (ver `WriteTag` em
  `internal/plc/omron_nx1p2.go`); no CLP, `BOOL` normal funciona (1 → TRUE).
- **Scan vs. poll:** a API faz poll do ack a cada `CMD_POLL_MS` (default 50 ms). O
  scan do NX1P2 costuma ser bem mais rápido, então não há corrida.
- **Códigos de status:** hoje a API entende 1=OK e 2=erro. Para usar outros códigos,
  ajuste o mapeamento em `internal/queue/dispatcher.go`.

---

## 7. Checklist de ativação

- [ ] Criar as variáveis globais da seção 1 com Network Publish.
- [ ] Implementar a máquina de estados (seção 3) chamada todo scan.
- [ ] Ligar `Exec_*` à rotina real de movimento da célula.
- [ ] Em `cmd/server/main.go`, trocar `plc.NewMock()` por
      `plc.NewOmronNX1P2(cfg.CLPHost, cfg.CLPPort)`.
- [ ] Testar fim-a-fim: `EnqueueCommand` → observar `QUEUED → DISPATCHED → ACKED_OK`
      em `SubscribeCommandStatus`.
