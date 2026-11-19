# Rastreabilidade — documentos de origem → especificação → testes

Documentos de origem:

- **Etapa 1 — Projeto de Middleware Publish/Subscribe**, seções 1–4 (10 páginas), em especial p. 3–5 arquitetura, p. 7–9 protocolo/mecanismos, p. 10 tradeoffs.
- **Enunciado da disciplina**, p. 2–4: Cenário C, mínimos do middleware, aplicações de exemplo e Etapa 2; p. 5 orienta medições na Etapa 3.

| Fonte / decisão | Requisitos SDD | Tarefas | Testes |
|---|---|---|---|
| Cenário C: baixa latência, carga alta, audiência heterogênea | RNF01–RNF04, US04, US06 | T008–T012, T028–T029 | `internal/broker/data_test.go`; CT07–CT09, W01–W03 |
| Etapa 1 §2.1 item 1: Pub/Sub desacoplado | RF03–RF04, US02 | T007, T010, T022–T023 | `internal/broker/{control,data}_test.go`; CT03–CT04 |
| Etapa 1 §2.1 item 2: dois brokers fixos | RF07–RF09, US03 | T013–T020, T024 | `internal/broker/peer_test.go`; `integration/cross_broker_test.go`; CT10–CT16 |
| Etapa 1 §2.1 item 3: memória e best effort | RF04, RF06, P2 da constituição | T010–T011, T023 | CT03, CT05, CT19 |
| Etapa 1 §2.1 item 4: limitação e filas | RF10–RF11, US04 | T008–T009, T012 | `internal/broker/data_test.go`; CT07–CT09, W02–W03 |
| Etapa 1 §2.1 item 5: UDP + sessões | RF01, RF14 | T003, T005, T021 | `internal/broker/control_test.go` (sessões); CT01, CT19 |
| Etapa 1 §2.1 item 6: interesse e encaminhamento | RF07–RF08 | T014–T015 | `internal/broker/peer_test.go`; `integration/cross_broker_test.go`; CT10–CT12 |
| Etapa 1 §2.1 item 7: recuperação de inscrições | RF12, US05 | T024–T025 | CT14, W04 |
| Etapa 1 §2.1 item 8: B1 catálogo, B2 cópia | RF02, RF09, US01 | T006, T016–T019 | `internal/broker/peer_test.go`; CT02, CT13, CT15–CT17 |
| Etapa 1 §3.2: JSON, ACKs e mensagens | RF05, RF06, RF13–RF14 | T003–T004, T011, T016–T018 | `internal/control/retry_test.go`; `internal/broker/data_test.go`; CT05–CT06, CT17–CT19 |
| Etapa 1 §3.3: PEER_SYNC, TTL, drop-oldest | RF01, RF09–RF13 | T005, T008–T009, T018–T019 | CT01, CT08, CT15–CT17 |
| Enunciado item 3: tópicos, inscrições, descarte, buffer, transparência | RF02–RF05, RF11–RF12 | T006–T011, T021–T024 | CT02–CT09, CT14 |
| Enunciado item 4: clientes/medições | RF15–RF16 | T027–T029 | W01–W04 |
| Enunciado item 5.3: código e documentação | RF15–RF16, RNF06–RNF07 | T001–T002, T026–T030 | quickstart + checklist de entrega |

## Pontos além do texto literal (propostas, não fatos da fonte)

- **1200 B**, token bucket **200/s** com burst **100**, filas **64**, timeouts e TTL são **defaults propostos**.
- `catalog_epoch`, `interest_version`, cache TTL, chunks de PEER_SYNC são **detalhes de implementação** para viabilizar ACK, retry, reinícios e limites UDP.
- `Publish` com notificações assíncronas `Events()` é **design proposto de API**, coerente com no-ACK e `NO_SUBSCRIBERS`.
- Critério de `OK` de catálogo e recuperação de B1 têm decisão explícita em **D02 e D03**.
