# Plano de implementação (Go)

**Branch/feature sugerida:** `001-middleware-pubsub-tempo-real`  
**Entrada:** `spec.md`, `contracts/*`, `data-model.md`, `docs/DECISOES_E_RISCOS.md`.  
**Saída:** biblioteca Go, 2 brokers UDP, demo e load generator com testes e docs.

## 1. Resumo arquitetural

```text
                        datagramas UDP / JSON v1
[pub/sub CLI] -> [biblioteca Go] <-> [Broker 1: catálogo líder + pub/sub]
                     |                     ^           |
                     | failover            | UDP       | TOPIC_UPDATE, INTEREST_UPDATE,
                     +---------------------+----+      | FORWARD_PUBLICATION, PEER_SYNC
                                              |      v
                                              +-> [Broker 2: cópia catálogo + pub/sub]
                                      [assinantes locais de cada broker]
```

Broker 1 e 2 recebem dados e inscrições para tópicos conhecidos; B1 coordena alterações do catálogo. Cada broker conserva sessões e inscritos locais, um mapa de interesse remoto e filas de dados limitadas. Biblioteca esconde instância ativa e refaz inscrições no failover.

## 2. Stack, testes e dependências

- Go (versão fixada em `go.mod` criada pela tarefa T001, compatível com ambiente da equipe).
- Preferir somente padrão: `net`, `encoding/json`, `context`, `crypto/rand`, `sync`, `time`, `flag`, `log/slog`, `testing`.
- Testes de pacote `go test ./...`; corrida `go test -race ./...`; integração com `os/exec` quando processo externo ajudar, ou brokers reais em portas UDP efêmeras.
- **Sem** DB, serviços cloud, Docker obrigatório, UI web, métricas externas obrigatórias.

## 3. Organização sugerida

```text
cmd/
  broker/main.go                 # processo B1/B2, flags e lifecycle
  demo/main.go                   # terminal para create/delete/pub/sub
  loadgen/main.go                # cenários de carga e CSV/JSON
pkg/pubsub/                      # API pública importável por apps de exemplo
internal/protocol/               # envelopes JSON, tamanhos, codificação e validação
internal/broker/                 # sessões, tópicos, assinantes, fan-out
internal/peer/                   # heartbeat, interesses, replica e peer sync
internal/flow/                   # limiter, filas finitas e scheduling justo
internal/observability/          # contadores e snapshots estruturados
integration/                     # testes de 2 brokers/failover/carga reduzida
specs/001-middleware-pubsub-tempo-real/
.specify/memory/
docs/
```

A estrutura pode ser consolidada se o código ficar excessivamente fragmentado. Nunca criar diretórios vazios só para aparentar complexidade.

## 4. Fases de entrega

### Fase 0 — Fundação

- Projeto Go + CLI mínimo, config/flags, logs identificando instância.
- Protocolos `v1`: envelope, tipos e validações com testes (incluindo 1200 bytes).
- Modelos de sessão/catálogo e cache de correlação com TTL/capacidade.

### Fase 1 — Broker local funcional

- `REGISTER`, `HEARTBEAT`, expiração de sessão.
- B1 cria/deleta tópico; B2 retorna `COORDINATOR_UNAVAILABLE` até integrar peer.
- Subscribe/unsubscribe, validação publish, `NO_SUBSCRIBERS` correlacionado com ACK/retry.
- Token bucket per publisher, buffers de dados finitos e fan-out sem bloqueio de outros clientes.
- Testes para operação local e rajada.

### Fase 2 — Comunicação entre brokers

- `PEER_HEARTBEAT`, `INTEREST_UPDATE` com retry/idempotência/versionamento.
- Repasse `FORWARD_PUBLICATION` sem loops.
- `TOPIC_REQUEST` e `TOPIC_UPDATE` com B1 coordenador, B2 réplica.
- Snapshot `PEER_SYNC` chunked em UDP, consistente e limitado; startup/restart e perda de pacotes de controle.

### Fase 3 — Biblioteca cliente

- Um socket UDP por cliente; um loop receptor central correlaciona respostas, notificações e `DELIVER`.
- Métodos públicos de controle e publicação; `Events()` para notificações assíncronas.
- Heartbeats e failover; `REGISTER` e resubscribe idempotente no broker alternativo; backoff quando ambos indisponíveis.
- Testes de canais/Close, cancelamento e troca de brokers.

### Fase 4 — Exemplo, carga e documentação

- CLI `cmd/demo` para publicar/assinar/criar/apagar, com opção de preferência B1/B2.
- `cmd/loadgen` com workloads `fanout`, `burst`, `fairness`, `failover` e exportação de métricas CSV/JSON.
- Documentar API real, comandos reais, limitações, tradeoffs e como gerar gráficos na Etapa 3.

## 5. Decisões de desenho e tradeoffs

| Decisão | Razão | Risco/mitigação |
|---|---|---|
| Leitor UDP rápido + filas por publicador + scheduler fair | Evita que um burst monopolize workers | Overhead de filas; limites configuráveis |
| Fila por assinante | Consumidor lento não impede demais | Possível queda de mensagens desse consumidor |
| `Publish` sem aguardar confirmação positiva | Caminho rápido alinhado a best effort | Status final só inferido por notificações; jamais prometer entrega |
| Retry apenas em controle | Menor overhead em dados | Controle pode ficar inconclusivo após timeout |
| B1 autoridade de catálogo; B2 cópia | Simplicidade sem eleição | Catálogo não pode ser modificado sem B1 |
| Snapshot paginado no `PEER_SYNC` | Evita truncar catálogo em UDP | Mais mensagens de controle; timeout/limite explícitos |
| Reconciliar B1 de B2 após restart | Recuperar catálogo volátil quando B2 sobrevive | Se atualização nunca chegou a B2, ela pode se perder; ver D02 |

## 6. Interfaces entre módulos

- `protocol`: `Encode/Decode/Validate`, `Envelope`, `MaxDatagramSize`, códigos de erro.
- `broker`: `Handle(envelope, source)`, catálogos e assinaturas, roteamento e contadores.
- `flow`: limiter e filas finitas, scheduling de publicações; API sem dependência do peer.
- `peer`: endpoint e estado interbroker, recebimento da réplica, atualizações e sincronização.
- `pkg/pubsub`: `Client`, `Config`, `Message`, `Event`, métodos em `contracts/client-api.md`.
- `observability`: snapshots de métricas para testes/exportação, sem depender de Prometheus.

## 7. Regras de concorrência

1. Um único loop lê de cada socket, valida datagrama e despacha rapidamente por tipo.
2. Controle não sofre descarte de fila de dados; todas as requisições pendentes são limitadas e canceláveis.
3. Trabalho de fan-out nunca espera consumidor lento segurando lock global.
4. Saída por assinante com limite e descarte; custos de goroutine dimensionados pelo número de sessões, não pelo volume de datagramas.
5. A finalização de um processo cancela workers, solta recursos, sinaliza pedidos pendentes e termina em prazo finito.

## 8. Config e defaults propostos

| Config | Inicial | Por quê |
|---|---|---|
| broker1 UDP | `127.0.0.1:9001` | Execução local simples |
| broker2 UDP | `127.0.0.1:9002` | Execução local simples |
| `max_datagram_bytes` | 1200 | Não fragmentar datagramas de aplicação intencionalmente |
| `rate_limit_msgs_per_sec` | 200 | Mostrar justiça |
| `rate_burst` | 100 | Absorver rajadas curtas |
| `input_queue_capacity` | 64 por publicador | Limitar memória |
| `output_queue_capacity` | 64 por assinante | Isolar assinante lento |
| `heartbeat_interval` | 500ms | Teste local rápido |
| `heartbeat_timeout` | 1800ms | Detectar suspeita de broker |
| `session_ttl` | 4s | Expirar sessões mortas |
| `control_retry_interval` | 300ms | Retry de controle limitado |
| `control_max_attempts` | 3 envios no total | Bounded control |

Valores revisáveis após métricas; nunca tratá-los como exigência da Etapa 1.

## 9. Gate antes de integrar

- Não adicionar ACK de `DELIVER` ou retransmissão de `PUBLISH`.
- Não permitir `CREATE_TOPIC/DELETE_TOPIC` em B2 sem coordenação de B1.
- Não confundir UDP `Write` com confirmação de consumo.
- Não aceitar snapshots/interesses fora de ordem sem controle de versão.
- Não executar fila infinita, goroutine por publicação ou serialização JSON custosa sob lock geral.
- Consultar `docs/DECISOES_E_RISCOS.md` para D01–D04 antes de finalizar contratos correspondentes.

## 10. Estratégia de integração incremental

1. Teste 1 cliente/1 broker em memória + UDP localhost.
2. Teste B1 e B2 em portas efêmeras com interesse remoto, antes de adicionar failover.
3. Teste biblioteca pública com `Close`, timeout, retries e re-registro.
4. Teste falha matando processo B1 e observando B2, depois restart e peer sync.
5. Só então teste carga parametrizada e relatórios.
