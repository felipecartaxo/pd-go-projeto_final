# Guia de execução dos testes

Este guia é a evidência reproduzível das tarefas **T026–T030**. Execute todos
os comandos manualmente em terminais PowerShell. O roteiro usa somente Go,
UDP em localhost e os dois brokers fixos; não exige banco, Docker ou serviços
externos.

> **Estado de validação:** a suíte `go test ./...` foi aprovada após
> `go clean -cache` e `go mod download`. Smoke cross-broker, falha/migração
> e carga curta com B1 parado foram executados nesta máquina. Ainda falta
> executar `go test -race ./...` em ambiente com compilador C/CGO.

## Pré-requisitos

- Windows com PowerShell e Go 1.26 disponíveis no `PATH`;
- portas UDP `127.0.0.1:9001` e `127.0.0.1:9002` livres;
- comandos executados a partir da raiz do repositório;
- nenhum broker, `demo` ou `loadgen` anterior em execução.

Confirme a raiz e o módulo:

```powershell
Get-Location
go env GOMOD
```

Mantenha cada broker ou assinante em seu próprio terminal. Para encerrá-los,
use `Ctrl+C` no terminal que você iniciou; não encerre processos de outro teste
sem confirmar sua origem.

## Testes automatizados do código

Execute os casos abaixo e guarde a saída quando ela for evidência da entrega:

```powershell
go test ./...
go test ./integration -v
go test ./internal/broker -run TestNoSubscribersRetriesUntilACKOrLimit -v
go test ./internal/broker -run TestRateLimitIsPerPublisher -v
go test ./internal/broker -run TestInputDropOldestAndFairDispatch -v
go test ./internal/broker -run TestSlowSubscriberOutputQueueDropsOldestWithoutBlockingOthers -v
go test -race ./...
```

Espera-se `PASS` nos casos direcionados e `ok` nos pacotes da suíte geral. No
Windows, o detector de corridas pode não iniciar sem CGO e um compilador C;
registre a saída como limitação do ambiente, sem tratá-la como aprovação.

## Pub/Sub entre brokers

Abra os brokers em terminais separados:

```powershell
# Terminal 1
go run ./cmd/broker -id 1
```

```powershell
# Terminal 2
go run ./cmd/broker -id 2
```

Crie o tópico pelo B1:

```powershell
# Terminal 3
go run ./cmd/demo -mode create -client-id admin-1 -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'leilao/lances'
```

`TOPIC_ALREADY_EXISTS` é esperado se o tópico já existir; nesse caso, prossiga.
Em outro terminal, assine pelo B2 e mantenha o processo aberto:

```powershell
# Terminal 4
go run ./cmd/demo -mode sub -client-id sub-2 -prefer 2 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'leilao/lances'
```

Publique pelo B1:

```powershell
# Terminal 5
Set-Content -Path .\payload.json -Value '{"valor":120,"lote":3}' -NoNewline
go run ./cmd/demo -mode pub -client-id pub-1 -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'leilao/lances' -payload-file .\payload.json
```

O Terminal 4 deve imprimir uma linha semelhante a:

```text
leilao/lances <message_id> {"valor":120,"lote":3}
```

Para verificar a mutação encaminhada por B2, execute com B1 ainda ativo:

```powershell
go run ./cmd/demo -mode create -client-id admin-2 -prefer 2 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'leilao/lances-b2'
```

## Failover manual

Com ambos os brokers ativos, em um novo terminal inicie uma assinatura que
prefira B1:

```powershell
go run ./cmd/demo -mode sub -client-id sub-failover -prefer 1 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'leilao/lances'
```

Depois de alguns segundos, interrompa B1 com `Ctrl+C` no Terminal 1. Aguarde
cerca de cinco segundos para a detecção e o re-registro no B2 e publique:

```powershell
Set-Content -Path .\payload.json -Value '{"valor":130,"lote":3}' -NoNewline
go run ./cmd/demo -mode pub -client-id pub-2 -prefer 2 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'leilao/lances' -payload-file .\payload.json
```

O assinante de failover deve mostrar o payload com `"valor":130`. Perdas no
intervalo de troca são aceitáveis. Ainda com B1 parado, a criação via B2 deve
falhar com `COORDINATOR_UNAVAILABLE` ou `INCONCLUSIVE`:

```powershell
go run ./cmd/demo -mode create -client-id admin-b2 -prefer 2 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'leilao/indisponivel'
```

Reinicie B1 no Terminal 1 e aguarde a sincronização assíncrona:

```powershell
go run ./cmd/broker -id 1
```

## Carga e métricas

Com os dois brokers ativos, execute cada cenário em um terminal. Use tópicos
distintos para evitar resultados de execuções anteriores:

```powershell
go run ./cmd/loadgen -scenario burst -duration 15s -publishers 5 -subscribers 20 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'loadgen/burst-manual' -run-id burst-001 -gomaxprocs 1 -output results-burst.csv
go run ./cmd/loadgen -scenario fanout -duration 30s -publishers 1 -subscribers 20 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'loadgen/fanout-manual' -run-id fanout-001 -gomaxprocs 1 -output results-fanout.csv
go run ./cmd/loadgen -scenario fairness -duration 30s -publishers 2 -subscribers 20 -brokers '127.0.0.1:9001,127.0.0.1:9002' -topic 'loadgen/fairness-manual' -run-id fairness-001 -gomaxprocs 1 -output results-fairness.csv
```

Para estresse, aumente explicitamente para 100 assinantes em `fanout` e 30 em
`fairness`, em ambiente capaz de suportar a carga. No Go 1.26.1/Windows
validado, cargas UDP concorrentes apresentaram falha fatal no runtime de rede;
`-gomaxprocs 1` mantém o perfil padrão reproduzível. Se isso ocorrer, guarde o
log como evidência e só trate um CSV como resultado se o processo terminar e
imprimir `wrote ...`.

## Resultados e limitações

- `PUBLISH` e `DELIVER` usam UDP best effort. Um envio bem-sucedido para o
  socket não prova recebimento pelo assinante.
- Dados podem se perder em sobrecarga, falha ou troca de broker; não há replay
  nem deduplicação obrigatória.
- `NO_SUBSCRIBERS` é assíncrono. A CLI `demo -mode pub` termina logo após o
  envio; a evidência reproduzível é o teste
  `TestNoSubscribersRetriesUntilACKOrLimit`.
- Nos CSVs, `sent` é tentativa UDP local e `received` é entrega observada pelo
  cliente; nenhum dos dois é garantia fim a fim.
- Os testes unitários de rate limit e filas são a evidência determinística das
  políticas de isolamento.
