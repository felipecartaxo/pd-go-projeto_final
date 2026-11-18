# Quickstart planejado — execução local e testes

**ATENÇÃO:** este repositório, ao receber apenas o pacote SDD, ainda **não contém código**. Os comandos abaixo são o **contrato de usabilidade da Etapa 2**. O Codex deverá implementá-los em `cmd/` e **executá-los de verdade** antes da entrega; atualizar flags conforme necessário, sem desalinhar os exemplos.

## Pré-requisitos

- Go instalado e disponível em `go version`.
- Três ou mais terminais em localhost; portas UDP 9001 e 9002 disponíveis.
- Sem Docker, DB, Node.js, frontend ou serviços externos obrigatórios.

## 1. Executar os dois brokers

Terminal 1:

```bash
go run ./cmd/broker -id=1 -listen=127.0.0.1:9001 -peer=127.0.0.1:9002
```

Terminal 2:

```bash
go run ./cmd/broker -id=2 -listen=127.0.0.1:9002 -peer=127.0.0.1:9001
```

Ambos devem indicar id, endereço, peer e estado do catálogo. **Criar tópicos apenas com B1 disponível**; B2 encaminha mutações para B1.

## 2. Criar tópico e assinar no outro broker

Terminal 3:

```bash
go run ./cmd/demo -mode=create -client-id=admin-1 -prefer=1 -brokers=127.0.0.1:9001,127.0.0.1:9002 -topic=leilao/lances
```

Terminal 4 (deixe executando):

```bash
go run ./cmd/demo -mode=sub -client-id=sub-2 -prefer=2 -brokers=127.0.0.1:9001,127.0.0.1:9002 -topic=leilao/lances
```

## 3. Publicar evento no B1

Terminal 5:

```bash
go run ./cmd/demo -mode=pub -client-id=pub-1 -prefer=1 -brokers=127.0.0.1:9001,127.0.0.1:9002 -topic=leilao/lances -payload='{"valor":120,"lote":3}'
```

Esperado: assinante conectado ao B2 vê `DELIVER` com `topic`, `message_id` e JSON. **Não** inferir garantia de entrega de um teste que funcionou.

## 4. Sem assinantes e limite de taxa

1. Pare o assinante ou faça unsubscribe com `-mode=unsubscribe` se implementado pela CLI (a API deve suportar `Unsubscribe`).
2. Publique novamente no tópico existente. Esperado: broker descarta e publica evento `NO_SUBSCRIBERS` no cliente publicador, com ACK automático.
3. Execute o gerador de carga para rajadas e observe `RATE_LIMITED` e contadores de filas.

```bash
go run ./cmd/loadgen -scenario=burst -duration=15s -publishers=5 -subscribers=20 -brokers=127.0.0.1:9001,127.0.0.1:9002 -output=results-burst.csv
```

## 5. Failover transparente

1. Recrie o tópico e inicie o assinante com `-prefer=1`.
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

Documentar testes de integração que dependam de portas efêmeras/injeção de falha; **evitar** testes que exigem matar manualmente processos durante CI padrão.

## 7. Execuções de desempenho sugeridas

```bash
go run ./cmd/loadgen -scenario=fanout -duration=30s -publishers=1 -subscribers=100 -brokers=127.0.0.1:9001,127.0.0.1:9002 -output=results-fanout.csv

go run ./cmd/loadgen -scenario=fairness -duration=30s -publishers=2 -subscribers=30 -brokers=127.0.0.1:9001,127.0.0.1:9002 -output=results-fairness.csv
```

Os argumentos efetivos, dados de hardware e resultados reais devem estar descritos no README final, com exemplos de CSV/JSON gerados pelo código, sem inventar métricas.
