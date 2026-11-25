# Teste manual em múltiplos terminais

Este roteiro demonstra o ciclo de tópicos, Pub/Sub entre os dois brokers,
notificação de ausência de assinantes e failover da biblioteca cliente.

## Preparação

Abra pelo menos sete terminais PowerShell. Em cada terminal, navegue até a raiz
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

## Verificação individual dos requisitos mínimos do middleware

Os passos abaixo detalham os sete requisitos, além do roteiro geral acima.
Mantenha os Brokers 1 e 2 nos Terminais 1 e 2. Abra os demais terminais na raiz
do repositório, e use tópicos exclusivos ou troque o sufixo se já existirem.
Os exemplos de publicação usam arquivo JSON para evitar problemas de aspas do
PowerShell.

### Requisito 1 — Mensagens carregam o tópico

**Terminal 3 — criar o tópico:**

```powershell
go run ./cmd/demo -mode create -client-id req1-admin -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/topico-na-mensagem'
```

Saída esperada:

```text
CREATE_TOPIC OK topic=professor/topico-na-mensagem
```

Como a atualização do catálogo para B2 é assíncrona, aguarde alguns segundos
antes de iniciar a assinatura no Terminal 4.

**Terminal 4 — inscrever no Broker 2 e mantê-lo executando:**

```powershell
go run ./cmd/demo -mode sub -client-id req1-subscriber -prefer 2 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/topico-na-mensagem'
```

Saída inicial esperada:

```text
subscribed client_id=req1-subscriber topic=professor/topico-na-mensagem broker=2
```

**Terminal 5 — publicar pelo Broker 1:**

```powershell
Set-Content -Path .\payload.json -Value '{"exemplo":"topico"}' -NoNewline
go run ./cmd/demo -mode pub -client-id req1-publisher -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/topico-na-mensagem' -payload-file .\payload.json
```

Saída esperada no Terminal 5:

```text
PUBLISH sent message_id=<message_id> topic=professor/topico-na-mensagem
```

Saída esperada no Terminal 4:

```text
professor/topico-na-mensagem <message_id> {"exemplo":"topico"}
```

**O que testa:** o tópico está presente na publicação e na entrega, e é
preservado no encaminhamento interbroker. Os dois `<message_id>` devem ser
iguais.

### Requisito 2 — Publicar, inscrever e remover inscrição

**Terminal 3 — criar um tópico exclusivo para este teste:**

```powershell
go run ./cmd/demo -mode create -client-id req2-admin -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/inscricao'
```

Saída esperada:

```text
CREATE_TOPIC OK topic=professor/inscricao
```

**Terminal 4 — inscrever:**

```powershell
go run ./cmd/demo -mode sub -client-id req2-client -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/inscricao'
```

Saída esperada:

```text
subscribed client_id=req2-client topic=professor/inscricao broker=1
```

Deixe o comando rodando. **Terminal 5 — publicar uma vez:**

```powershell
Set-Content -Path .\payload.json -Value '{"teste":"inscricao"}' -NoNewline
go run ./cmd/demo -mode pub -client-id req2-publisher -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/inscricao' -payload-file .\payload.json
```

Saída esperada no Terminal 5:

```text
PUBLISH sent message_id=<message_id> topic=professor/inscricao
```

Saída esperada no Terminal 4:

```text
professor/inscricao <message_id> {"teste":"inscricao"}
```

Para remover a inscrição, pressione `Ctrl+C` no Terminal 4 e **execute
imediatamente** neste terminal, usando o mesmo `client-id` e o mesmo broker:

```powershell
go run ./cmd/demo -mode unsubscribe -client-id req2-client -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/inscricao'
```

Saída esperada:

```text
UNSUBSCRIBE OK topic=professor/inscricao
```

A sessão padrão expira após quatro segundos; executar logo após parar o
assinante permite que o comando retome a sessão existente e remova sua
inscrição. Para verificar o estado final, publique no mesmo tópico pelo
Terminal 5:

```powershell
Set-Content -Path .\payload.json -Value '{"teste":"apos-unsubscribe"}' -NoNewline
go run ./cmd/demo -mode pub -client-id req2-publisher -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/inscricao' -payload-file .\payload.json
```

Saída esperada quando o broker notificar que já não há inscritos:

```text
PUBLISH sent message_id=<message_id> topic=professor/inscricao
NO_SUBSCRIBERS topic=professor/inscricao message_id=<message_id>
```

**O que testa:** publicar, inscrever e cancelar a inscrição do mesmo cliente.
A ausência da notificação continua sujeita a perda UDP. A verificação
determinística da remoção do índice de assinantes também pode ser feita com:

```powershell
go test ./internal/broker -run TestCatalogAndSubscriptionsFollowLocalControlRules -v
```

Saída esperada:

```text
--- PASS: TestCatalogAndSubscriptionsFollowLocalControlRules
PASS
```

### Requisito 3 — Criar e apagar tópicos durante a execução

**Terminal 3 — criar e, em seguida, excluir o tópico:**

```powershell
go run ./cmd/demo -mode create -client-id req3-admin -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/ciclo-topico'
go run ./cmd/demo -mode delete -client-id req3-admin -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/ciclo-topico'
```

Saídas esperadas:

```text
CREATE_TOPIC OK topic=professor/ciclo-topico
DELETE_TOPIC OK topic=professor/ciclo-topico
```

**O que testa:** ciclo de vida dinâmico sem reiniciar os brokers. Para testar a
criação encaminhada por B2, execute os mesmos comandos com `-prefer 2`; B1
precisa estar ativo. Para confirmar que o tópico foi excluído, tente excluí-lo
novamente:

```powershell
go run ./cmd/demo -mode delete -client-id req3-admin -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/ciclo-topico'
```

Saída esperada: erro `TOPIC_NOT_FOUND` e código de saída diferente de zero.

### Requisito 4 — Descartar publicação sem inscritos e notificar o publicador

**Terminal 3 — criar um tópico que ninguém assinará:**

```powershell
go run ./cmd/demo -mode create -client-id req4-admin -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/sem-inscritos'
```

Saída esperada:

```text
CREATE_TOPIC OK topic=professor/sem-inscritos
```

Não execute `-mode sub` para esse tópico.

**Terminal 5 — publicar e observar a notificação:**

```powershell
Set-Content -Path .\payload.json -Value '{"teste":"sem-inscritos"}' -NoNewline
go run ./cmd/demo -mode pub -client-id req4-publisher -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/sem-inscritos' -payload-file .\payload.json
```

Saída esperada:

```text
PUBLISH sent message_id=<message_id> topic=professor/sem-inscritos
NO_SUBSCRIBERS topic=professor/sem-inscritos message_id=<message_id>
```

**O que testa:** descarte quando não há assinantes conhecidos e tentativa de
dar ciência ao publicador. A biblioteca envia ACK ao receber a notificação;
como a notificação é UDP, ela pode se perder. Se aparecer somente `PUBLISH
sent`, isso não confirma que havia inscritos nem garante que o aviso chegou.

### Requisito 5 — Encaminhar a todos os inscritos e consumir a mensagem

**Terminal 3 — criar um tópico:**

```powershell
go run ./cmd/demo -mode create -client-id req5-admin -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/fanout'
```

Saída esperada:

```text
CREATE_TOPIC OK topic=professor/fanout
```

Como a réplica do catálogo para B2 é assíncrona, aguarde alguns segundos antes
de iniciar a assinatura no Terminal 6.

**Terminal 4 — primeiro assinante no Broker 1:**

```powershell
go run ./cmd/demo -mode sub -client-id req5-subscriber-b1 -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/fanout'
```

**Terminal 6 — segundo assinante no Broker 2:**

```powershell
go run ./cmd/demo -mode sub -client-id req5-subscriber-b2 -prefer 2 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/fanout'
```

Em cada terminal de assinante, espere a linha `subscribed ...` e aguarde alguns
segundos pela convergência do interesse remoto.

**Terminal 5 — publicar uma única vez:**

```powershell
Set-Content -Path .\payload.json -Value '{"teste":"fanout"}' -NoNewline
go run ./cmd/demo -mode pub -client-id req5-publisher -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/fanout' -payload-file .\payload.json
```

Saída esperada no Terminal 5:

```text
PUBLISH sent message_id=<message_id> topic=professor/fanout
```

Saída esperada **em cada** Terminal 4 e 6:

```text
professor/fanout <message_id> {"teste":"fanout"}
```

**O que testa:** fan-out para assinantes locais e remotos conhecidos. Para essa
publicação, cada cliente deve observar uma linha com o mesmo ID. Não devem
aparecer retransmissões de `DELIVER`; a fila de saída é consumida após a
tentativa UDP. UDP e filas limitadas significam que essa observação é
best-effort, não garantia universal de recebimento. Para verificar de forma
determinística que a tentativa local de entrega não é repetida, execute:

```powershell
go test ./internal/broker -run TestPublishDeliversBestEffortWithoutACK -v
```

Saída esperada:

```text
--- PASS: TestPublishDeliversBestEffortWithoutACK
PASS
```

### Requisito 6 — Bufferizar rajadas

**Terminal 7 — executar o gerador de carga**, com os dois brokers ativos:

```powershell
go run ./cmd/loadgen -scenario burst -duration 10s -publishers 2 -subscribers 20 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/rajada' -run-id professor-burst-001 -gomaxprocs 1 -output .\professor-burst.csv
```

Saída esperada ao terminar:

```text
wrote .\professor-burst.csv: offered=<n> sent=<n> received=<n> rate_limited=<n>
```

Confira o resumo registrado:

```powershell
Import-Csv .\professor-burst.csv | Format-List scenario,run_id,offered,sent,received,rate_limited,topic
```

**O que testa:** submissão de uma rajada e observação de tentativas e entregas
no cenário de carga; o `loadgen` cria o tópico e os assinantes do cenário. Os
números dependem da máquina e não são fixos. A CLI não exporta o contador de
drop-oldest do broker; para verificar deterministicamente que a fila limitada
descarta a mensagem mais antiga e mantém as recentes, execute:

```powershell
go test ./internal/broker -run TestInputDropOldestAndFairDispatch -v
```

Saída esperada:

```text
--- PASS: TestInputDropOldestAndFairDispatch
PASS
```

### Requisito 7 — Brokers múltiplos transparentes na biblioteca

**Terminal 3 — criar o tópico para o failover:**

```powershell
go run ./cmd/demo -mode create -client-id req7-admin -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/failover'
```

Saída esperada:

```text
CREATE_TOPIC OK topic=professor/failover
```

Aguarde alguns segundos para B2 receber a atualização de catálogo antes de
derrubar B1.

**Terminal 7 — inscrever preferindo B1:**

```powershell
go run ./cmd/demo -mode sub -client-id req7-failover -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/failover'
```

Saída inicial esperada:

```text
subscribed client_id=req7-failover topic=professor/failover broker=1
```

**Terminal 1 — parar somente B1** com `Ctrl+C`. Aguarde a detecção e a
restauração da inscrição.

Saída esperada no Terminal 7:

```text
FAILOVER_OK client_id=req7-failover migrated_to_broker=2 topic=professor/failover
```

**Terminal 5 — publicar uma nova mensagem através do B2:**

```powershell
Set-Content -Path .\payload.json -Value '{"teste":"apos-failover"}' -NoNewline
go run ./cmd/demo -mode pub -client-id req7-publisher -prefer 2 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'professor/failover' -payload-file .\payload.json
```

Saída esperada no Terminal 7:

```text
professor/failover <message_id> {"teste":"apos-failover"}
```

**O que testa:** a aplicação continua usando a mesma biblioteca e lista
inscrita enquanto o cliente migra do broker indisponível para o alternativo.
Publicações enviadas durante a troca podem se perder; o failover não faz replay.
Depois do teste, reinicie B1 como descrito no passo 7 do roteiro geral.

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
