# Tarefas de implementação — SDD / Codex

**Status inicial:** todas pendentes. Marcar `[x]` **somente após código + testes executados**.  
**Ordem:** seguir dependências; múltiplas tarefas podem compartilhar PR se forem pequenas.  
**Critério geral de cada tarefa:** testes pertinentes passam, código `gofmt`, mudanças de contratos documentadas, nenhum requisito da Etapa 1 alterado implicitamente.

## Fase 0 — Bootstrap e protocolo (pré-requisito de todas as demais)

- [x] **T001 — Criar módulo Go e estrutura mínima.** `go.mod`, `internal/protocol`, `internal/broker`, `pkg/pubsub`, `cmd/{broker,demo,loadgen}`, `integration`; compilar projetos mínimos sem serviço externo. **Aceite:** `go test ./...` executa; registrar versão Go e path do módulo no README.
- [x] **T002 — Configuração, flags e lifecycle de processos.** Config de B1/B2 e timeouts/limites por flag; validar id 1/2, endereços, sinal de encerramento e logs. **Aceite:** iniciar 2 processos UDP em localhost com portas diferentes; encerrar graciosamente, sem erro de data race.
- [x] **T003 — Implementar protocolo v1 e testes de contrato.** Envelope, types/codes, `Encode/Decode/Validate`, `request_id`, `message_id`, tamanho 1200 B **do JSON total**, UTF-8/payload RawMessage. **Aceite:** round-trip, limite exatamente no máximo, excesso, JSON malformado, campos ausentes e versão não suportada.
- [x] **T004 — Implementar controle genérico com retry/cache limitado.** Correlation ID, respostas OK/ERROR/ACK, reenvio da mesma requisição e idempotência por janela. **Aceite:** simular perda da primeira resposta e duplicata; operar exatamente uma vez **dentro da janela de cache**; timeout vira `ErrInconclusive` no cliente.

## Fase 1 — Broker local e isolamento

- [x] **T005 — Sessões UDP.** `REGISTER`, vínculo client_id/UDP, `HEARTBEAT` reply, TTL e expiração. **Dep.:** T003. **Aceite:** REGISTER OK, invalidação de origem errada, expiração apaga inscrições, testes com clock controlável onde viável.
- [x] **T006 — Catálogo local coordenado.** Criar/deletar tópico dinamicamente no B1 e tratar duplicata; B2 não cria localmente. **Dep.:** T003, T005. **Aceite:** CREATE/DELETE, `TOPIC_ALREADY_EXISTS`, `TOPIC_NOT_FOUND`, remoção das pendências ainda enfileiradas.
- [x] **T007 — Inscrições locais.** SUBSCRIBE, UNSUBSCRIBE e índice topic→clientes, resposta de controle. **Dep.:** T005, T006. **Aceite:** sub duplicado idempotente, unsubscribe, sessão expirada removida e sem fan-out a unsubscribe concluído.
- [x] **T008 — Rate limiter por publicador.** Token bucket separado por client_id, valores configuráveis, `RATE_LIMITED` e métricas. **Dep.:** T005. **Aceite:** publicador A recebe limitação; B abaixo do limite continua aceito.
- [x] **T009 — Buffers finitos de dados e scheduler fair.** Entrada por publisher e saída por subscriber, drop-oldest e contadores. **Dep.:** T005. **Aceite:** ao encher N, nova publicação mantém as N mais recentes; assinante lento não bloqueia outro; sem goroutine por datagrama.
- [x] **T010 — PUBLISH/DELIVER local.** Validar sessão, tópico, payload e rate; enfileirar/entregar best effort, sem ACK de DELIVER. **Dep.:** T007–T009. **Aceite:** 1 pub e N subscribers recebem no localhost; envio UDP não é tratado como recebimento confirmado.
- [x] **T011 — NO_SUBSCRIBERS e notificações.** Ausência de inscritos conhecidos causa descarte + notificação correlacionada, ACK/retry limitado; erros relevantes e RATE_LIMITED assíncronos. **Dep.:** T004, T010. **Aceite:** aviso/ACK, ACK perdido, timeout limitado, não travar publish em espera de confirmação.
- [x] **T012 — Testes de fluxo local e recursos.** Cobrir US01/US02/US04 local, segurança concorrente e filas; corrigir vazamentos. **Dep.:** T005–T011. **Aceite:** `go test ./...` + `go test -race ./...` e casos de overflow reproduzíveis.

## Fase 2 — Interbroker, sincronização e falhas

- [ ] **T013 — Canal peer UDP fixo e PEER_HEARTBEAT.** Validar endereço e `broker_id`, responder heartbeat, marcar peer indisponível após timeout. **Dep.:** T002, T003. **Aceite:** peer vivo/morto detectado; mensagem interna de origem não configurada rejeitada.
- [ ] **T014 — INTEREST_UPDATE versionado.** 0→1 e 1→0 de inscritos locais, `present`, `peer_epoch`, `interest_version`, ACK/retry. **Dep.:** T004, T007, T013. **Aceite:** convergência após retry; duplicata e versão antiga não restauram interesse obsoleto.
- [ ] **T015 — FORWARD_PUBLICATION sem loops.** Broker que recebe publicação local faz encaminhamento apenas ao peer vivo interessado; peer entrega só local. **Dep.:** T010, T013, T014. **Aceite:** pub em B1, sub em B2; sem interesse não encaminha; mensagem recebida como FORWARD nunca retorna a B1.
- [ ] **T016 — TOPIC_REQUEST e TOPIC_UPDATE.** B2 encaminha create/delete a B1, B1 atualiza catálogo e incrementa revisão; replica alteração e trata ACK/retry. **Dep.:** T004, T006, T013. **Aceite:** criar a partir de B2, catálogo convergido, B1 morto gera `COORDINATOR_UNAVAILABLE` ou timeout inconclusivo; B2 mantém publish/sub dos tópicos existentes.
- [ ] **T017 — Versões e detecção de lacunas do catálogo.** `catalog_epoch` e revision monotônica, ignore duplicata/versão velha, gap sinaliza sync. **Dep.:** T016. **Aceite:** atualização fora de ordem não corrompe catálogo; epoch trocado dispara reconciliação.
- [ ] **T018 — PEER_SYNC paginado, bounded e atômico.** Snapshot de catálogo e interesses em chunks que cabem em 1200 B, ACK/retry finito, rollback/timeout de snapshot incompleto. **Dep.:** T014, T017. **Aceite:** catálogo maior que 1 datagrama, perda de chunk, restart do B2 e interesses consistentes após reconvergência.
- [ ] **T019 — Recuperação do B1 e falhas combinadas.** Implementar política proposta **D02** após discussão da equipe; B1 recém-reiniciado restaura B2 vivo antes de novas mutações; B2 read-only sem B1; ambos reiniciados sem persistência começam vazios. **Dep.:** T018. **Aceite:** testes de reinício B1 com B2 vivo, indisponibilidade de create/delete sem B1 e recuperação do estado possível.
- [ ] **T020 — Integração de replicação.** Testes de 2 processos, perda/reorder de controles simulados, sincronização, expiração e falha peer. **Dep.:** T013–T019. **Aceite:** todos os testes interbroker passam sob race detector e timeouts finitos.

## Fase 3 — Biblioteca cliente Go

- [ ] **T021 — Cliente: socket único e dispatcher de respostas.** Implementar `NewClient`, `Connect`, `Close`, pending requests e `Events()` limitado. **Dep.:** T003–T005. **Aceite:** requests simultâneos não roubam replies; Close idempotente, sem goroutines vazadas.
- [ ] **T022 — API de tópicos/inscrições.** Implementar Create/Delete/Subscribe/Unsubscribe e canais por tópico com estado desejado; erros tipados. **Dep.:** T007, T021. **Aceite:** API importável com teste black-box, context cancel, tópicos e canais corretos.
- [ ] **T023 — API Publish e notificações assíncronas.** Publish uma única tentativa, gera IDs; processa `DELIVER`, `NO_SUBSCRIBERS` (ACK automático), `RATE_LIMITED` e erros; evita blocking do receive loop. **Dep.:** T010, T011, T021. **Aceite:** sem ACK de sucesso de dados, aviso correlacionado, consumo lento de Events não impede HEARTBEAT.
- [ ] **T024 — Failover e restauração de inscrições.** HEARTBEAT e timeout; migrar B1↔B2, REGISTER e resubscribe; quando ambos inacessíveis, backoff e erro sem reenvio de dados. **Dep.:** T022, T023, T020. **Aceite:** derrubar B1, reconectar B2 e receber novos eventos com a mesma API; medir intervalo e registrar no log.
- [ ] **T025 — Testes de biblioteca e concorrência.** Compilar exemplo público, testes race, perda/duplicata de ACK e encerramento com operações concorrentes. **Dep.:** T021–T024. **Aceite:** `go test -race ./...` sem condições de corrida e casos documentados.

## Fase 4 — Testes demonstráveis, métricas e entrega

- [ ] **T026 — CLI `cmd/broker`.** Flags conformes quickstart, contadores/logs, processamento de sinais. **Dep.:** T020. **Aceite:** dois terminais com B1 e B2 reais executam sem alterar código.
- [ ] **T027 — CLI `cmd/demo`.** Modes `create`, `delete`, `pub`, `sub`; selecionar broker preferido, mostrar IDs/feedback de erro. **Dep.:** T025. **Aceite:** comandos de `quickstart.md` executados manualmente para local e cross-broker.
- [ ] **T028 — CLI `cmd/loadgen` e exportação.** Cenários `fanout`, `burst`, `fairness`, `failover`, sementes/config fixas, timestamps, CSV/JSON com unidades e causas de descarte. **Dep.:** T025, T026. **Aceite:** testes com N produtores/M consumidores geram arquivo analisável.
- [ ] **T029 — Suite de aceitação, falhas e sobrecarga.** Implementar matriz de `docs/PLANO_DE_TESTES.md`, incluindo NO_SUBSCRIBERS, drop-oldest, B1 falho, B1 reiniciado e fairness. **Dep.:** T026–T028. **Aceite:** scripts/comandos reproduzíveis, resultados registrados, ausência de travamentos.
- [ ] **T030 — Documentação final da entrega.** Atualizar README, `docs/BIBLIOTECA.md`, quickstart, métricas e limitações para **código realmente testado**, não promessas. **Dep.:** T029. **Aceite:** colega em máquina limpa consegue instalar Go, rodar os brokers/demo/carga e importar biblioteca; explicar resultados e tradeoffs.

## Rastreabilidade resumida

| Requisitos | Tarefas de implementação/validação |
|---|---|
| RF01, RF03 | T005, T007, T021–T025 |
| RF02, RF09 | T006, T016–T020 |
| RF04, RF06, RF08 | T010, T015, T023 |
| RF05, RF13 | T004, T011, T023 |
| RF07 | T013–T015, T018 |
| RF10, RF11 | T008, T009, T012, T029 |
| RF12 | T024–T025, T029 |
| RF14 | T003, T012, T020 |
| RF15, RF16 | T026–T030 |

## Convenção de trabalho com Codex

Ao iniciar uma tarefa, citar seu ID e arquivos pertinentes; pedir teste primeiro, alteração pequena, executar testes e marcar checkbox **só se concluída**. Se encontrar decisão em `docs/DECISOES_E_RISCOS.md`, comunicar escolha proposta e impacto antes de alterar o contrato. **Não** executar todo o backlog em uma única conversa/prompt.
