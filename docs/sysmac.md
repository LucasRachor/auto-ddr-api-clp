# Passo a passo no Sysmac Studio (NX1P2) — lado do CLP

Guia **exclusivo do CLP**. Aqui está tudo o que precisa ser feito **dentro do
Sysmac Studio** para o controlador conversar com a `plc-api`. Não é preciso saber
nada da API: basta criar as variáveis, publicar na rede e implementar a máquina
de estados abaixo, chamada todo scan.

O princípio é um **aperto de mão de 4 vias**: a API só liga `gCmd_Req` depois de
já ter escrito todos os campos do comando. Assim o CLP **nunca** lê um comando pela
metade. Só um comando é executado por vez (slot único).

---

## 1. Criar as variáveis globais

No Sysmac Studio: **Programming ▸ Data ▸ Global Variables**. Crie exatamente estes
nomes (maiúsculas/minúsculas importam — a API procura pelo nome exato):

| Nome (Name) | Data Type | Network Publish |
|-------------|-----------|-----------------|
| `gCmd_Id` | DINT | **Publish Only** |
| `gCmd_Action` | INT | **Publish Only** |
| `gCmd_Motherboard` | STRING[16] | **Publish Only** |
| `gCmd_Robot` | INT | **Publish Only** |
| `gCmd_Finger` | INT | **Publish Only** |
| `gCmd_Req` | BOOL | **Publish Only** |
| `gCmd_AckId` | DINT | **Publish Only** |
| `gCmd_AckStatus` | INT | **Publish Only** |
| `gCmd_AckDetail` | STRING[64] | **Publish Only** |
| `gCmd_Ack` | BOOL | **Publish Only** |
| `eCmdState` | INT | Do not publish |

> **Network Publish** fica na coluna da direita da tabela de variáveis globais.
> Selecione **Publish Only** para as 10 primeiras. A `eCmdState` é o estado interno
> da máquina — **não** publique.

Crie também estas variáveis internas de trabalho (na mesma tabela global ou como
locais do POU, **sem** publish). Elas guardam uma cópia "travada" do comando:

| Nome | Data Type |
|------|-----------|
| `wId` | DINT |
| `wAction` | INT |
| `wMb` | STRING[16] |
| `wRobot` | INT |
| `wFinger` | INT |

### O que cada campo significa

**Recebidos da API (a API escreve, o CLP lê):**
- `gCmd_Id` — identificador único do comando. Só o eco de volta importa (ver ack).
- `gCmd_Action` — código da ação. Por enquanto só existe `insert` = **1**.
- `gCmd_Motherboard` — texto, ex.: `"MB09"`.
- `gCmd_Robot` — robô alvo, **1** ou **2**.
- `gCmd_Finger` — dedo alvo, **1** a **5**.
- `gCmd_Req` — **gatilho**. Passa a TRUE quando há um comando completo pronto.

**Respondidos ao CLP (o CLP escreve, a API lê):**
- `gCmd_AckId` — **eco** do `gCmd_Id` que foi processado. A API confere se bate.
- `gCmd_AckStatus` — resultado: **0** = nenhum, **1** = OK, **2** = erro.
- `gCmd_AckDetail` — texto opcional com o motivo do erro (pode ficar vazio).
- `gCmd_Ack` — **confirmação**. Passa a TRUE quando o ack está pronto para leitura.

---

## 2. A sequência do aperto de mão

```
API:  escreve Id / Action / Motherboard / Robot / Finger
API:  gCmd_Req = TRUE                       ──►  "comando pronto"
CLP:  vê Req=TRUE → copia os campos → executa
CLP:  escreve AckId / AckStatus / AckDetail
CLP:  gCmd_Ack = TRUE                        ──►  "feito, pode ler o resultado"
API:  vê Ack=TRUE e AckId==Id → registra o status
API:  gCmd_Req = FALSE                       ──►  "recebi, pode liberar o slot"
CLP:  vê Req=FALSE → gCmd_Ack = FALSE        ──►  "slot livre para o próximo"
```

Regra de ouro para o CLP: **o CLP nunca zera `gCmd_Ack` sozinho** — só quando vê
`gCmd_Req` cair para FALSE. É isso que fecha o handshake e evita perder comando.

---

## 3. Programa em Structured Text

Crie um **Program** (ex.: `Prg_CmdHandshake`), em Structured Text, e **atribua-o a
uma Task** (a Primary Periodic Task já serve) para rodar **todo scan**.

```pascal
// Estados de eCmdState: 0=IDLE, 1=BUSY (executando), 2=DONE (aguarda API confirmar)
CASE eCmdState OF

  0: // IDLE — aguardando uma requisição
     gCmd_Ack := FALSE;                    // garante o slot limpo
     IF gCmd_Req THEN
        // --- COPIA os campos AGORA (a API já escreveu todos antes do Req) ---
        wId     := gCmd_Id;
        wAction := gCmd_Action;
        wMb     := gCmd_Motherboard;
        wRobot  := gCmd_Robot;
        wFinger := gCmd_Finger;

        // dispara a ação real da máquina (troque pela sua rotina/FB de movimento)
        Exec_StartInsert(wRobot, wFinger);
        eCmdState := 1;
     END_IF;

  1: // BUSY — executando o movimento
     IF Exec_Done THEN
        gCmd_AckId := wId;                 // ECO do id processado (obrigatório)
        IF Exec_Error THEN
           gCmd_AckStatus := 2;            // erro
           gCmd_AckDetail := Exec_ErrMsg;
        ELSE
           gCmd_AckStatus := 1;            // OK
           gCmd_AckDetail := '';
        END_IF;
        gCmd_Ack  := TRUE;                 // sinaliza "ack pronto"
        eCmdState := 2;
     END_IF;

  2: // DONE — a API já leu o ack? Ela confirma baixando o Req.
     IF NOT gCmd_Req THEN
        gCmd_Ack  := FALSE;                // libera o slot
        eCmdState := 0;                    // volta a IDLE para o próximo comando
     END_IF;

END_CASE;
```

### O que você precisa ligar à sua célula

Os símbolos `Exec_*` são a **sua** lógica de movimento — substitua pela rotina real:

- `Exec_StartInsert(wRobot, wFinger)` — dispara o movimento (aciona robô/dedo).
- `Exec_Done` — BOOL, TRUE quando o movimento terminou.
- `Exec_Error` — BOOL, TRUE se deu erro.
- `Exec_ErrMsg` — STRING com o motivo do erro (usada em `gCmd_AckDetail`).

Pode ser um Function Block seu, uma máquina de estados de movimento, ou o que já
existir. O importante é sinalizar `Exec_Done` (e opcionalmente `Exec_Error`).

---

## 4. Por que cada detalhe importa (não pule)

- **Copiar os campos só no IDLE, com `gCmd_Req=TRUE`:** garante que os 5 campos já
  estão consistentes. A API escreve `gCmd_Req` por último de propósito.
- **`gCmd_AckId := wId`:** o eco do id é o que permite à API ter certeza de que o
  ack é deste comando. **Sem esse eco, a API não fecha o comando.**
- **DONE só limpa o `Ack` quando `Req` cai:** o CLP não pode zerar `gCmd_Ack`
  sozinho. Ele espera a API baixar `gCmd_Req`. Isso confirma que a API leu o
  resultado antes de liberar o slot.

---

## 5. Casos de borda (recomendado tratar)

- **API reiniciou com `gCmd_Req` preso em TRUE:** ao voltar, a API força
  `gCmd_Req=FALSE`. Se o CLP estava em DONE, ele vai para IDLE normalmente. Se
  estava em BUSY, termina, publica o ack, e a API reconcilia pelo `gCmd_AckId`.
  **A máquina de estados acima já cobre esse caso** — não precisa de código extra.
- **Proteção contra comando duplicado (opcional):** guarde o último id concluído
  numa variável `gLastDoneId` e, no IDLE, ignore um `gCmd_Req` cujo
  `gCmd_Id == gLastDoneId`. A API já evita reenviar o mesmo id; isso é só um cinto
  de segurança extra.

---

## 6. Especificidades do NX1P2 (importante)

- **Use `STRING` de verdade**, não array de BYTE, para `gCmd_Motherboard` e
  `gCmd_AckDetail`. Deixe folga de tamanho (`STRING[16]` / `STRING[64]`).
- **`BOOL` normal funciona** para `gCmd_Req` / `gCmd_Ack` — a API lê/escreve como
  0/1 e o controlador entende (1 = TRUE). Não use uma word como flag.
- **Tipos numéricos:** mantenha `DINT` para os ids e `INT` para action/robot/
  finger/status. Não troque por LINT/UDINT — a API espera esses tamanhos.
- **Timing:** a API consulta o ack a cada ~50 ms. O scan do NX1P2 é muito mais
  rápido, então não há corrida — desde que a máquina de estados rode todo scan.
- **Códigos de status:** hoje a API entende **1 = OK** e **2 = erro**. Se quiser
  outros códigos, avise para ajustarmos o lado da API.

---

## 7. Checklist de ativação

- [ ] Criar as 10 variáveis globais publicadas + `eCmdState` (seção 1).
- [ ] Criar as variáveis de trabalho `w*` (sem publish).
- [ ] Criar o Program em ST e **atribuí-lo a uma Task** (roda todo scan).
- [ ] Ligar `Exec_StartInsert` / `Exec_Done` / `Exec_Error` / `Exec_ErrMsg` à
      rotina real de movimento da célula.
- [ ] **Build** e **Transfer** para o controlador.
- [ ] Confirmar que as variáveis aparecem na rede (Network Publish ativo).
- [ ] Teste guiado com a API (ver seção 8).

---

## 8. Como testar sem a API (só no Sysmac)

Dá para validar a máquina de estados forçando valores na **Watch Window** antes de
integrar com a API:

1. Coloque o controlador em RUN e abra uma **Watch Window** com as 11 variáveis.
2. Force `gCmd_Id := 101`, `gCmd_Action := 1`, `gCmd_Robot := 1`,
   `gCmd_Finger := 1`, `gCmd_Motherboard := 'MB09'`.
3. Force `gCmd_Req := TRUE`. → `eCmdState` deve ir para **1 (BUSY)**.
4. Quando seu `Exec_Done` ficar TRUE, `eCmdState` vai para **2 (DONE)**,
   `gCmd_AckId` deve valer **101**, `gCmd_AckStatus` **1** e `gCmd_Ack` **TRUE**.
5. Force `gCmd_Req := FALSE`. → `gCmd_Ack` volta a FALSE e `eCmdState` a **0 (IDLE)**.

Se essa sequência acontecer certinha na Watch Window, o handshake está pronto para
a API assumir o controle.
