# Plano de testes para a Etapa 2 (e insumos da Etapa 3)

**Princípio:** testar duas instâncias reais UDP, biblioteca e filas com comportamento reproduzível. Testes unitários validam invariantes; integração valida rede/concorrência. `go test -race` deve cobrir as rotas críticas quando o ambiente dispuser de CGO e compilador C.

**Estado atual:** `go test ./...` foi aprovado. O detector de corridas está
pendente por ausência de compilador C no ambiente atual; os roteiros manuais
ainda precisam ser registrados em máquina limpa.

## Ferramentas e execução

- `go test ./...` — unitários e de integração curtos (preferir portas efêmeras e dados determinísticos).
- `go test -race ./...` — detecção de races de estado de broker/cliente.
- `go test ./integration -run <Teste> -v` — executar caso selecionado, conforme nomes reais criados por Codex.
- `go run ./cmd/loadgen ...` — cenário prolongado e exportação para CSV/JSON.

Os comandos são implementados. Resultados de carga só devem ser tratados como
evidência após registrar os parâmetros e o ambiente da execução.

## Matriz de aceitação funcional

| ID | Caso | Passos essenciais | Evidência esperada |
|---|---|---|---|
| CT01 | Registro e heartbeat | REGISTER; heartbeat periódico; parar cliente | sessão ativa e expiração após TTL |
| CT02 | Ciclo de tópico | CREATE B1, CREATE via B2, DELETE | catálogo converge; tópico removido não publica |
| CT03 | Pub/Sub local | 1 pub + 3 subs no B1 | todos recebem em condições sem falha, sem ACK de DELIVER |
| CT04 | Sub/unsub | Inscrever e retirar um assinante | novas mensagens não são enfileiradas após unsubscribe confirmado |
| CT05 | Sem assinantes | PUBLISH tópico existente e vazio | descarte, notificação NO_SUBSCRIBERS, ACK do cliente |
| CT06 | Perdendo ACK | Dropar ACK do NO_SUBSCRIBERS | retries finitos, sem fila infinita, eventual expiração |
| CT07 | Rate limit | Publicar > limite com A, normal com B | A recebe RATE_LIMITED; B mantém throughput/latência saudável |
| CT08 | Drop-oldest | Encher fila de capacidade pequena com mensagens rastreáveis | mensagens mais antigas pendentes descartadas, contador correto |
| CT09 | Consumidor lento | um subscriber lento e outro rápido | rápido continua consumindo; fila do lento perde dados sem bloquear |
| CT10 | Cross-broker | Pub no B1, sub no B2 | B1 encaminha e B2 faz entrega local |
| CT11 | Sem loop/interesse | Repasse de B1→B2 e tópico não assinado em B2 | não volta a B1; sem repasse quando não há interesse |
| CT12 | Sync de interesses | Mudança 0→1 e 1→0, perda/reorder de update | converge com retry/sync, atualização antiga ignorada |
| CT13 | Catálogo B1 down | Derrubar B1 com B2 ativo | CREATE/DELETE no B2 indisponíveis; pub/sub de tópico existente funciona |
| CT14 | Failover cliente | cliente inscrito em B1, matar B1, publicar em B2 | biblioteca registra em B2 e restaura inscrição; medir interrupção |
| CT15 | B2 reinicia | B1 mantém catálogo; reiniciar B2 | B2 recebe catálogo via PEER_SYNC e interesses reconvergem |
| CT16 | B1 reinicia | B2 preserva catálogo; reiniciar B1 | B1 recupera snapshot de B2 antes de aceitar mutações (D02) |
| CT17 | PEER_SYNC grande/incompleto | catálogo maior que 1 datagrama; perder chunk | snapshot aplicado completo ou descartado por timeout |
| CT18 | Requisição duplicada | repetir mesmo request_id em CREATE/SUBSCRIBE | nenhuma duplicação de efeito dentro da janela do cache |
| CT19 | Protocolo inválido | datagrama >1200B, JSON inválido, falta ID, versão 2 | rejeição controlada, sem crash ou goroutine vazada |
| CT20 | Shutdown | parar cliente/broker sob carga e pedidos pendentes | cancelamento finito, sem deadlock e sem race |

## Workloads de desempenho

### W01 — Fan-out (maioria consome)

- 1 publicador e 20, 50 ou 100 assinantes; payload pequeno (ex. lance JSON com 100–200 B totais).
- Taxas ofertadas: 20, 100 e 200 mensagens/s (ajustar para não misturar limitação por publisher com capacidade de fan-out).
- Separar cenários assinantes todos em B1, todos em B2 e divisão meio a meio.
- Medir por assinante: mensagens recebidas, recebidas distintas, duplicatas, p50/p95/p99, descartes.

### W02 — Rajadas irregulares

- 5 publicadores, dos quais 1 envia rajadas acima do rate limit e os demais com taxa estável baixa.
- Exigir contadores `rate_limited` e `queue_drop_oldest`, throughput por publicador e efeito sobre latência dos saudáveis.

### W03 — Fairness de concorrentes

- **Baseline:** publicador B saudável sozinho com assinantes.
- **Com agressor:** B nas mesmas condições, A com rajadas. Comparar p95 de B, taxa observada de B, p95 de fan-out e perdas.
- Evitar alegar fairness apenas porque A foi limitado: é preciso observar B.

### W04 — Failover / retorno

- Inscrever grupo em B1; iniciar fluxo periódico no B2/B1; derrubar B1 durante execução (sinal ao processo).
- Medir: último DELIVER antes da falha, suspeita detectada, REGISTER no B2, re-SUBSCRIBE OK, primeiro DELIVER depois.
- Após retorno de B1, verificar `PEER_SYNC`. Separar tempo de detecção de indisponibilidade do tempo total de recuperação.

## Ambiente e reprodutibilidade

Registrar `run_id`, versão Go, SO, arquitetura, número de CPUs, `GOMAXPROCS`, tópico, portas, número de publishers/subscribers e duração. Use `-run-id` para repetir uma comparação com o mesmo identificador de experimento. O CSV exportado pelo `loadgen` já inclui esses metadados, exceto RAM e tamanho médio dos datagramas. **Não comparar timestamps de máquinas diferentes sem sincronização de relógios**; no localhost o medidor pode usar timestamps monotônicos relativos no mesmo processo para medição mais precisa.

## Critério de sucesso

- CT01–CT20 passam nas condições de teste sem falhas não injetadas.
- Under overload, perdas são esperadas, mas **contabilizadas** e restritas prioritariamente ao emissor agressivo/assinante lento.
- B2 continua operando pub/sub de tópicos conhecidos após queda de B1 e biblioteca pode migrar.
- Resultados demonstram comportamento, não SLA irreal de zero falhas/zero perdas.
