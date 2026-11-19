# Guia de execução dos testes

Este guia é a evidência reproduzível das tarefas **T026–T030**. Ele usa somente
Go, PowerShell, UDP em localhost e os dois brokers fixos. Não exige banco,
Docker ou serviços externos.

## Pré-requisitos

- Windows com PowerShell e Go 1.26 disponíveis no `PATH`;
- portas UDP `127.0.0.1:9001` e `127.0.0.1:9002` livres;
- executar a partir da raiz do repositório;
- nenhum broker, `demo` ou `loadgen` anterior em execução.

O script testa essa condição no início. Caso informe portas ocupadas, volte aos
terminais onde os brokers foram iniciados manualmente e use `Ctrl+C`; ele não
encerra processos que não foram criados pelo próprio roteiro.

No PowerShell integrado do VS Code, confirme a raiz e o módulo:

```powershell
Get-Location
go env GOMOD
```

Ambos devem apontar para `pd-go-cenario_2_implementacao` e seu `go.mod`.

## Execução automatizada

O roteiro de aceitação inicia os brokers, cria tópicos exclusivos, executa os
testes funcionais/failover e encerra **somente os processos que ele próprio
iniciou**:

```powershell
.\scripts\Executar-Testes.ps1
```

Se a política local bloquear scripts, use uma execução pontual sem alterar a
política persistente:

```powershell
powershell -ExecutionPolicy Bypass -File .\scripts\Executar-Testes.ps1
```

Ele grava todos os comandos e resultados em
`test-results\AAAAmmdd-HHmmss\`.

Para incluir os workloads de `loadgen` e a geração de CSV, execute:

```powershell
.\scripts\Executar-Testes.ps1 -RunLoad
```

Os workloads são experimentais: o script conserva o log e continua os testes
funcionais se o runtime Go encerrar o processo. CSV só é evidência quando o
processo conclui e imprime `wrote ...`.

Para não tentar o detector de corridas:

```powershell
.\scripts\Executar-Testes.ps1 -SkipRace
```

Por padrão, fan-out e fairness usam 20 assinantes. Para o perfil de estresse
(100 no fan-out e 30 no fairness), execute:

```powershell
.\scripts\Executar-Testes.ps1 -Stress
```

No Go 1.26.1/Windows usado na validação, cargas UDP concorrentes reproduziram
uma falha fatal no runtime de rede do Go. O roteiro passa `-gomaxprocs 1` ao
`loadgen` para manter a execução reprodutível; registre essa condição junto às
métricas. O perfil `-Stress` continua dependente de um ambiente que suporte a
carga.

No Windows, `go test -race ./...` pode falhar antes de iniciar porque requer
CGO e um compilador C. O roteiro mantém `race.log` como evidência e não
esconde essa limitação; os demais resultados continuam válidos.

## Casos executados e resultados esperados

| Caso | O que/por que testa | Comando ou ação | Resultado esperado |
|---|---|---|---|
| Suite geral | Regressões de todos os pacotes | `go test ./...` | Todos os pacotes terminam em `ok`; pacotes CLI podem dizer `[no test files]`. |
| Integração UDP | Dois brokers, interesse remoto e encaminhamento sem loop | `go test ./integration -v` | `TestTwoUDPBrokersForwardOnlyForKnownRemoteInterest` passa. |
| Sem assinantes | Descarte e aviso com retry/ACK limitado, sem bloquear dados | `go test ./internal/broker -run TestNoSubscribersRetriesUntilACKOrLimit -v` | `PASS`. |
| Rate limit | Um publicador agressivo não impede outro | `go test ./internal/broker -run TestRateLimitIsPerPublisher -v` | `PASS`. |
| Drop-oldest/fairness | Filas por produtor preservam eventos recentes e atendem produtores alternadamente | `go test ./internal/broker -run TestInputDropOldestAndFairDispatch -v` | `PASS`. |
| Assinante lento | Fila individual limitada não bloqueia o assinante rápido | `go test ./internal/broker -run TestSlowSubscriberOutputQueueDropsOldestWithoutBlockingOthers -v` | `PASS`. |
| Pub/Sub entre brokers | Publicação em B1 chega a assinatura em B2 | B1/B2, `create`, `sub` no B2, `pub` no B1 | Log de `subscriber-b2` contém o tópico, um `message_id` e `{"valor":120,"lote":3}`. |
| Catálogo via B2 | B2 encaminha `CREATE_TOPIC` ao coordenador B1 | `demo -mode create -prefer 2` | Encerramento com código 0 enquanto B1 está vivo. |
| Rajada | Observa tentativas e entregas sob carga alta | `Executar-Testes.ps1 -RunLoad` | Se concluir, `results-burst.csv` contém `offered`, `sent`, `received` e `rate_limited`; se o runtime falhar, o log é a evidência da limitação. |
| Fan-out | Uma fonte para muitos consumidores | `-RunLoad`, `loadgen -scenario fanout` com 20 assinantes por padrão | Se concluir, `results-fanout.csv` é criado. `received` é observação local, não garantia global. |
| Fairness sob agressor | Compara carga de publicador agressivo e normal | `-RunLoad`, `loadgen -scenario fairness` com 20 assinantes por padrão | Se concluir, `results-fairness.csv` é criado. Interpretar junto aos testes unitários de rate/fila. |
| Failover | Cliente inscrito em B1 migra para B2 depois da falha | parar B1, esperar 3 s, publicar via B2 | Log de `subscriber-failover` contém payload com `"valor":130`. Perdas no intervalo são aceitáveis. |
| B1 indisponível | B2 continua Pub/Sub conhecido, mas não coordena mutações | `CREATE_TOPIC` preferindo B2 após parar B1 | Processo termina com `COORDINATOR_UNAVAILABLE` ou `INCONCLUSIVE`. |
| Retorno do B1 | B1 volta a escutar e pode sincronizar estado com B2 sem aceitar snapshot atrasado que apague catálogo já confirmado | reiniciar B1 e aguardar 2 s | Log `broker-1-restarted.stderr.log` contém `broker listening`; a sincronização é assíncrona. |
| Corridas | Acessos concorrentes a broker/cliente | `go test -race ./...` | `PASS`, ou limitação explícita de CGO registrada em `race.log`. |

## Comandos manuais equivalentes

Use estes comandos quando quiser observar o fluxo em terminais separados.
No PowerShell, o valor de `-brokers` deve permanecer entre aspas por conter
vírgula. Para JSON com chaves, use `\"` antes das aspas internas: a camada de
argumentos nativa do Windows pode removê-las caso contrário.

Terminal 1:

```powershell
go run ./cmd/broker -id 1
```

Terminal 2:

```powershell
go run ./cmd/broker -id 2
```

Terminal 3:

```powershell
go run ./cmd/demo -mode create -client-id admin-1 -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'leilao/lances'
```

`TOPIC_ALREADY_EXISTS` é esperado se o tópico já tiver sido criado; nesse
caso, prossiga sem recriá-lo.

Terminal 4 (permanece aberto):

```powershell
go run ./cmd/demo -mode sub -client-id sub-2 -prefer 2 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'leilao/lances'
```

Terminal 5:

```powershell
go run ./cmd/demo -mode pub -client-id pub-1 -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'leilao/lances' -payload '{\"valor\":120,\"lote\":3}'
```

O Terminal 4 deve imprimir uma linha como:

```text
leilao/lances <message_id> {"valor":120,"lote":3}
```

## Limitações que devem constar no relatório

- `PUBLISH` e `DELIVER` usam UDP best effort. Um envio bem-sucedido para o
  socket não prova recebimento pelo assinante.
- Dados podem se perder em sobrecarga, falha ou troca de broker; não há replay
  nem deduplicação obrigatória.
- `NO_SUBSCRIBERS` é um evento assíncrono. A CLI `demo -mode pub` termina logo
  após o envio e, por isso, não permanece ativa para exibi-lo; a evidência
  reproduzível desse caso é o teste automatizado indicado acima.
- `sent` nos CSVs significa tentativa UDP local; `received` significa entrega
  observada pelo cliente. Nenhum dos dois é uma garantia de entrega fim a fim.
- O cenário `loadgen` gera artefatos de carga; os testes unitários de rate
  limit e filas são a evidência determinística das políticas de isolamento.
