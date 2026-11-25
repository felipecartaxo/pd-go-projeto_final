# Teste manual em múltiplos terminais

Este roteiro demonstra o ciclo de tópicos, Pub/Sub entre os dois brokers,
notificação de ausência de assinantes e failover da biblioteca cliente.

## Preparação

Abra pelo menos seis terminais PowerShell. Em cada terminal, navegue até a raiz
do repositório:

```powershell
Set-Location C:\Users\Neraky\Documents\neraky\courses\curso-sistemas_para_internet\periodo6\prog_distribuida\pd-go-projeto_final
```

Confirme Go e diretório:

```powershell
go version
Get-Location
```

Use as portas UDP `9001` e `9002`, que devem estar livres. Se forem usadas por
outro processo, escolha outras portas e atualize os endereços em todos os
comandos. Para cada execução do roteiro, recomenda-se um tópico novo, por
exemplo `demo/manual-20261125`; criar um tópico já existente retorna erro.

## 1. Iniciar os dois brokers

### Terminal 1 — Broker 1 (coordenador do catálogo)

```powershell
go run ./cmd/broker -id 1 -listen 127.0.0.1:9001 -peer 127.0.0.1:9002
```

Saída esperada:

```text
INFO broker listening broker_id=1 listen=127.0.0.1:9001 peer=127.0.0.1:9002
```

**O que testa:** inicialização do primeiro broker e abertura do socket
UDP. Deixe o processo executando.

### Terminal 2 — Broker 2

```powershell
go run ./cmd/broker -id 2 -listen 127.0.0.1:9002 -peer 127.0.0.1:9001
```

Saída esperada:

```text
INFO broker listening broker_id=2 listen=127.0.0.1:9002 peer=127.0.0.1:9001
```

**O que testa:** inicialização do segundo broker e configuração do canal
interbroker UDP. Deixe o processo executando.

## 2. Criar tópico pelo Broker 1

### Terminal 3 — Cliente administrativo

Defina um tópico exclusivo para a execução:

```powershell
$topic = 'demo/manual-20261125'
go run ./cmd/demo -mode create -client-id admin-manual -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic $topic
```

Saída esperada:

```text
CREATE_TOPIC OK topic=demo/manual-20261125
```

**O que testa:** confirmação da operação de controle `CREATE_TOPIC` pelo
coordenador B1. A resposta confirma o commit local no coordenador; a réplica
para B2 é assíncrona.

Se receber `TOPIC_ALREADY_EXISTS`, escolha outro valor para `$topic` e execute
novamente. Para usar essa variável nos demais terminais, defina o mesmo texto
em cada PowerShell:

```powershell
$topic = 'demo/manual-20261125'
```

## 3. Assinar pelo Broker 2 e publicar pelo Broker 1

### Terminal 4 — Assinante no Broker 2

```powershell
$topic = 'demo/manual-20261125'
go run ./cmd/demo -mode sub -client-id subscriber-b2 -prefer 2 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic $topic
```

Saída inicial esperada (deixe o comando em execução):

```text
subscribed client_id=subscriber-b2 topic=demo/manual-20261125 broker=2
```

**O que testa:** registro da sessão e inscrição
local no B2, além da divulgação de interesse entre brokers.

### Terminal 5 — Publicador no Broker 1

```powershell
$topic = 'demo/manual-20261125'
Set-Content -Path .\payload.json -Value '{"lance":120,"lote":3}' -NoNewline
Get-Content -Raw -Path .\payload.json
go run ./cmd/demo -mode pub -client-id publisher-b1 -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic $topic -payload-file .\payload.json
```

O comando `Get-Content` deve exibir um único objeto JSON:

```json
{"lance":120,"lote":3}
```

Saída esperada no Terminal 5:

```text
PUBLISH sent message_id=<message_id> topic=demo/manual-20261125
```

Saída esperada no Terminal 4:

```text
demo/manual-20261125 <message_id> {"lance":120,"lote":3}
```

O `<message_id>` deve ser o mesmo nas duas linhas.

**O que testa:** publicação
em B1 e encaminhamento seletivo para um assinante conectado ao B2. `PUBLISH
sent` confirma somente a tentativa de envio local do datagrama; a linha do
Terminal 4 é a evidência de entrega observada pelo cliente. UDP é best effort,
portanto perda eventual é possível.

## 4. Criar tópico por meio do Broker 2

### Terminal 6 — Cliente administrativo conectado ao B2

Com os dois brokers ainda em execução:

```powershell
go run ./cmd/demo -mode create -client-id admin-via-b2 -prefer 2 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'demo/criado-via-b2'
```

Saída esperada:

```text
CREATE_TOPIC OK topic=demo/criado-via-b2
```

**O que testa:** o B2 encaminha o pedido de criação ao coordenador B1, sem
assumir a coordenação do catálogo.

## 5. Publicar sem assinantes

Use um tópico separado, que nenhum cliente assinou. Isso evita depender da
expiração de uma sessão do teste anterior.

### Terminal 3 — Criar tópico sem assinantes

```powershell
go run ./cmd/demo -mode create -client-id admin-no-subscriber -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'demo/sem-assinantes'
```

Saída esperada:

```text
CREATE_TOPIC OK topic=demo/sem-assinantes
```

Se já existir, escolha outro nome. Não execute `-mode sub` para esse tópico.

### Terminal 5 — Publicar no tópico vazio

```powershell
Set-Content -Path .\payload.json -Value '{"lance":121,"lote":3}' -NoNewline
Get-Content -Raw -Path .\payload.json
go run ./cmd/demo -mode pub -client-id publisher-no-subscriber -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'demo/sem-assinantes' -payload-file .\payload.json
```

Saída esperada no Terminal 5 quando o broker notificar a ausência:

```text
PUBLISH sent message_id=<message_id> topic=demo/sem-assinantes
NO_SUBSCRIBERS topic=demo/sem-assinantes message_id=<message_id>
```

**O que testa:** descarte da publicação sem assinantes conhecidos e notificação
assíncrona `NO_SUBSCRIBERS`. A biblioteca envia `ACK` automaticamente ao
receber o aviso. Como o aviso também usa UDP, ele pode se perder; nesse caso,
pode aparecer somente `PUBLISH sent`. Isso não confirma que havia assinantes.
O teste automatizado `TestNoSubscribersRetriesUntilACKOrLimit` é a verificação
determinística da tentativa e dos retries limitados.

## 6. Testar falha do B1 e failover do assinante

Antes deste passo, o tópico criado no passo 2 precisa existir no catálogo
conhecido pelo B2.

### Terminal 7 — Novo assinante com preferência pelo B1

```powershell
$topic = 'demo/manual-20261125'
go run ./cmd/demo -mode sub -client-id subscriber-failover -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic $topic
```

Saída inicial esperada:

```text
subscribed client_id=subscriber-failover topic=demo/manual-20261125 broker=1
```

Deixe o assinante ativo e aguarde alguns segundos.

### Terminal 1 — Interromper somente o Broker 1

No Terminal 1, pressione `Ctrl+C`. Não encerre o Broker 2.

### Terminal 7 — Confirmar migração

Após detectar a falha e restaurar a inscrição, a CLI deve mostrar:

```text
FAILOVER_OK client_id=subscriber-failover migrated_to_broker=2 topic=demo/manual-20261125
```

**O que testa:** detecção de falha do broker preferido, novo registro no B2 e
restauração da inscrição. Essa linha confirma a migração; ela não é uma
publicação nem uma entrega.

### Terminal 5 — Enviar uma nova publicação após o failover

```powershell
$topic = 'demo/manual-20261125'
Set-Content -Path .\payload.json -Value '{"lance":130,"lote":3}' -NoNewline
Get-Content -Raw -Path .\payload.json
go run ./cmd/demo -mode pub -client-id publisher-after-failover -prefer 2 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic $topic -payload-file .\payload.json
```

Saída esperada no Terminal 5:

```text
PUBLISH sent message_id=<message_id> topic=demo/manual-20261125
```

Saída esperada no Terminal 7:

```text
demo/manual-20261125 <message_id> {"lance":130,"lote":3}
```

**O que testa:** recebimento de novas publicações após a inscrição ser
restaurada no B2. Publicações enviadas durante a janela de falha não são
reproduzidas.

### Terminal 6 — Confirmar que mutações ainda dependem do B1

Com B1 parado, execute:

```powershell
go run ./cmd/demo -mode create -client-id admin-b2-offline -prefer 2 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'demo/nao-deve-ser-criado'
```

Saída esperada: erro explícito `COORDINATOR_UNAVAILABLE` ou resultado de
controle inconclusivo (`INCONCLUSIVE`), conforme o momento em que a falha for
detectada. O processo termina com código diferente de zero.

**O que testa:** B2 continua atendendo Pub/Sub em tópicos conhecidos, mas não
passa a coordenar criação/exclusão se B1 estiver indisponível.

## 7. Reiniciar B1 e encerrar

### Terminal 1 — Reiniciar o Broker 1

```powershell
go run ./cmd/broker -id 1 -listen 127.0.0.1:9001 -peer 127.0.0.1:9002
```

Saída inicial esperada:

```text
INFO broker listening broker_id=1 listen=127.0.0.1:9001 peer=127.0.0.1:9002
```

**O que testa:** retorno do broker e sincronização/reconciliação de estado
volátil com o peer. Aguarde os heartbeats e a sincronização antes de testar
novas mutações.

Para encerrar, pressione `Ctrl+C` nos processos de assinantes e brokers que
continuarem ativos. Se ambos os brokers reiniciarem, o estado em memória pode
ser perdido; este projeto não usa persistência.

## Saídas e limitações a considerar

- `CREATE_TOPIC OK`, `DELETE_TOPIC OK` e `UNSUBSCRIBE OK` representam sucesso
  da operação de controle, não entrega de dados.
- `PUBLISH sent` representa tentativa de envio pelo socket UDP local, não
  confirmação do broker ou do assinante.
- A entrega é best effort. Perdas em sobrecarga, durante failover ou por
  descarte UDP são possíveis.
- `NO_SUBSCRIBERS` é uma notificação assíncrona sujeita a perda de UDP. A falta
  da linha não prova que havia assinantes.
- Os identificadores e endereços neste documento são exemplos. Adapte tópico,
  IDs e portas quando houver colisão com outro teste.
