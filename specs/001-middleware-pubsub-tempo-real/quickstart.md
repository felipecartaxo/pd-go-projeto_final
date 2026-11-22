# Quickstart — execução local e testes

> **Estado da implementação:** T001–T030 fornecem brokers UDP, biblioteca, demo e `loadgen`. Os comandos abaixo são executáveis em localhost com os dois brokers ativos. `go test ./...` foi aprovado; a execução com `-race` e a evidência em máquina limpa permanecem pendentes conforme o checklist de requisitos.

**ATENÇÃO:** use os comandos como estão documentados. Para o roteiro PowerShell completo, incluindo carga e failover manual, consulte `docs/GUIA_EXECUCAO_TESTES.md`. Não inferir entrega garantida de uma execução local bem-sucedida.

## Pré-requisitos

- Go instalado e disponível em `go version`.
- Três ou mais terminais em localhost; portas UDP 9001 e 9002 disponíveis.
- Sem Docker, DB, Node.js, frontend ou serviços externos obrigatórios.
- No PowerShell do Windows, use o sinalizador e seu valor como argumentos
  separados. Por exemplo, use `-listen 127.0.0.1:9001`, não
  `-listen=127.0.0.1:9001`.

## 1. Executar os dois brokers

Terminal 1:

```bash

Saída esperada após o modo iniciar:

```text
subscribed client_id=sub-failover topic=falha/teste broker=1
```

Após migrar para o Broker 2, o mesmo terminal deverá mostrar:

```text
FAILOVER_OK client_id=sub-failover migrated_to_broker=2 topic=falha/teste
```

No terminal do Broker 2, cada reinscrição confirmada será registrada como:

```text
subscription active broker_id=2 client_id=sub-failover topic=falha/teste
```bash
go run ./cmd/broker -id 1 -listen 127.0.0.1:9001 -peer 127.0.0.1:9002
```

Terminal 2:

```bash
go run ./cmd/broker -id 2 -listen 127.0.0.1:9002 -peer 127.0.0.1:9001
```

Ambos devem indicar id, endereço e peer e encerrar graciosamente com `Ctrl+C`. Eles trocam heartbeat, interesses e catálogo por UDP, além de processar Pub/Sub local/interbroker.

## 2. Criar tópico e assinar no outro broker

Terminal 3:

```bash
go run ./cmd/demo -mode create -client-id admin-1 -prefer 1 -brokers 127.0.0.1:9001,127.0.0.1:9002 -topic leilao/lances
```

Saída esperada:

```text
CREATE_TOPIC OK topic=leilao/lances
```

Terminal 4 (deixe executando):

```bash
go run ./cmd/demo -mode sub -client-id sub-2 -prefer 2 -brokers 127.0.0.1:9001,127.0.0.1:9002 -topic leilao/lances
```

## 3. Publicar evento no B1

Terminal 5:

```bash
Set-Content -Path .\payload.json -Value '{"valor":120,"lote":3}' -NoNewline
go run ./cmd/demo -mode pub -client-id pub-1 -prefer 1 -brokers 127.0.0.1:9001,127.0.0.1:9002 -topic leilao/lances -payload-file .\payload.json
```

Saída esperada no terminal 5:

```text
PUBLISH sent message_id=<message_id> topic=leilao/lances
```

Saída esperada no assinante conectado ao B2:

```text
leilao/lances <message_id> {"valor":120,"lote":3}
```

**Não** inferir garantia de entrega de um teste que funcionou.

## 4. Sem assinantes e limite de taxa

1. Pare o assinante ou faça unsubscribe com `-mode=unsubscribe` se implementado pela CLI (a API deve suportar `Unsubscribe`).
2. Publique novamente no tópico existente. Esperado no terminal do publicador:

```text
PUBLISH sent message_id=<message_id> topic=leilao/lances
NO_SUBSCRIBERS topic=leilao/lances message_id=<message_id>
```

A ausência do aviso pode representar perda UDP; o broker tenta ACK/retry limitado e a biblioteca ACKa o aviso recebido.
3. Execute o gerador de carga para rajadas e observe `RATE_LIMITED` e contadores de filas.

```bash
go run ./cmd/loadgen -scenario burst -duration 15s -publishers 5 -subscribers 20 -brokers 127.0.0.1:9001,127.0.0.1:9002 -topic loadgen/burst-manual -run-id burst-001 -gomaxprocs 1 -output results-burst.csv
```

## 5. Failover transparente

1. Recrie o tópico e inicie o assinante com `-prefer 1`.
2. Mantenha outro processo publicando periodicamente (ou use `loadgen -scenario=failover`).
3. Termine **somente B1** (`Ctrl+C` no Terminal 1).
4. Aguarde os ciclos de heartbeat. A biblioteca deve tentar registrar-se em B2 e refazer inscrição automaticamente.
5. Publique novo evento via B2; o assinante deve continuar recebendo **novas** publicações após restaurar inscrição. As perdidas no intervalo **não** serão reproduzidas.
6. Experimente `CREATE_TOPIC` com apenas B2 ativo; espera-se `COORDINATOR_UNAVAILABLE`/inconclusivo, conforme falha observada.
7. Reinicie B1 e valide `PEER_SYNC`/recuperação de catálogo, conforme D02.

## 6. Suite automatizada

```bash
go test ./...
go test -race ./...
go test ./integration -v
```

`go test -race ./...` requer CGO com compilador C. Se o compilador não estiver
disponível, registre a limitação; não marque o critério como aprovado.

Documentar testes de integração que dependam de portas efêmeras/injeção de falha; **evitar** testes que exigem matar manualmente processos durante CI padrão.

## 7. Execuções de desempenho sugeridas

```bash
go run ./cmd/loadgen -scenario fanout -duration 30s -publishers 1 -subscribers 20 -brokers 127.0.0.1:9001,127.0.0.1:9002 -topic loadgen/fanout-manual -run-id fanout-001 -gomaxprocs 1 -output results-fanout.csv

go run ./cmd/loadgen -scenario fairness -duration 30s -publishers 2 -subscribers 20 -brokers 127.0.0.1:9001,127.0.0.1:9002 -topic loadgen/fairness-manual -run-id fairness-001 -gomaxprocs 1 -output results-fairness.csv
```

Os argumentos efetivos, dados de hardware e resultados reais devem estar descritos no README final, com exemplos de CSV/JSON gerados pelo código, sem inventar métricas.
