# Especificação — Middleware Publish/Subscribe em Tempo Real

**Feature:** `001-middleware-pubsub-tempo-real`  
**Estado:** pronto para desenvolvimento com **detalhamentos propostos** (ver `docs/DECISOES_E_RISCOS.md`)  
**Linguagem:** Go  
**Escopo:** middleware, biblioteca e aplicações de teste CLI (sem frontend e sem banco).

## 1. Problema e objetivo

O Cenário C exige transportar eventos de interação ao vivo para uma audiência majoritariamente consumidora, com produtores de rajadas heterogêneas. O sistema deve reduzir latência, suportar alto volume, não deixar um publicador prejudicar outros e manter disponibilidade **do serviço** quando um broker falha. A entrega individual não é crítica; mensagens de dados podem ser perdidas.

**Não faz parte deste trabalho:** streaming de vídeo real, garantia de continuidade absoluta sem perdas, armazenamento de usuários/mensagens, segurança de produção, escalabilidade para N brokers, interface gráfica.

## 2. Papéis

- **Publicador:** cliente Go que cria/seleciona tópico e envia eventos.
- **Assinante:** cliente Go que solicita inscrição e recebe eventos do tópico.
- **Broker 1 (B1):** Pub/Sub completo e coordenador exclusivo de criação/exclusão do catálogo.
- **Broker 2 (B2):** Pub/Sub completo para tópicos conhecidos, replica catálogo e encaminha modificações a B1.
- **Biblioteca cliente:** encapsula UDP, sessão, inscrições, timeouts e failover, sem exigir que a aplicação conheça qual broker está ativo.

## 3. Histórias e cenários de aceitação

### US01 — Ciclo de vida de tópicos (P0)

**Como** cliente, **quero** criar e remover tópicos em execução **para** organizar a interação.

1. **Dado** B1 acessível, **quando** o cliente solicita `CREATE_TOPIC`, **então** o tópico passa a existir no B1 e é propagado ao B2 pelo mecanismo de controle.
2. **Dado** cliente conectado ao B2, **quando** cria/exclui tópico, **então** o B2 envia `TOPIC_REQUEST` ao B1 e repassa o resultado.
3. **Dado** B1 indisponível, **quando** o cliente pede criação/exclusão no B2, **então** recebe erro explícito `COORDINATOR_UNAVAILABLE` (ou resultado inconclusivo em timeout), mas pode publicar/assinar tópicos conhecidos.
4. **Dado** tópico excluído, **então** o broker remove inscrições, interesses e publicações enfileiradas para esse tópico, respeitando que datagramas **já enviados** não podem ser retirados.

### US02 — Pub/Sub local (P0)

1. **Dado** tópico existente e assinante registrado nele no mesmo broker, **quando** ocorre `PUBLISH`, **então** a publicação é tentativamente entregue por `DELIVER` sem ACK individual e removida dos buffers após a tentativa.
2. **Dado** publicação em tópico inexistente, **então** o broker rejeita com `TOPIC_NOT_FOUND`.
3. **Dado** não existir assinante ativo **conhecido** em nenhum broker, **quando** o broker processa `PUBLISH`, **então** descarta a publicação e tenta notificar `NO_SUBSCRIBERS` com confirmação/retransmissão finita.
4. **Dado** `UNSUBSCRIBE` concluído, **então** não há novos enfileiramentos intencionais para essa inscrição, salvo datagramas já enviados ou concorrentes no momento da alteração.

### US03 — Pub/Sub entre dois brokers (P0)

1. **Dado** assinantes locais em B2 e publicador em B1, **quando** B1 recebe publicação para o tópico, **então** B1 envia `FORWARD_PUBLICATION` a B2 se B2 estiver ativo e com interesse conhecido. B2 tenta entregar aos seus locais.
2. **Dado** publicação recebida via `FORWARD_PUBLICATION`, **então** o broker **não** a reencaminha para o broker de origem.
3. **Dado** B2 sem assinantes para determinado tópico, **então** B1 não faz repasse desnecessário para B2 após a convergência dos interesses.
4. **Dado** atualização de interesse perdida, **então** mecanismos de retry/sincronização permitem convergência futura, sem prometer entrega durante a janela de inconsistência.

### US04 — Isolamento e sobrecarga (P0)

1. **Dado** um publicador acima da taxa configurada, **quando** publica, **então** o broker descarta/rejeita excedentes e pode responder `RATE_LIMITED`, sem bloquear o processamento de publicadores saudáveis.
2. **Dado** uma fila transitória cheia, **quando** um novo evento entra, **então** o mais antigo ainda pendente é descartado (`drop-oldest`) e o novo pode ocupar seu lugar.
3. **Dado** um assinante lento, **então** sua fila individual é limitada, e ele não bloqueia a entrega a outros assinantes.
4. **Dada** rajada sustentada de um cliente agressivo, **então** métricas devem permitir comparar a latência e o recebimento de outro cliente sob carga e em baseline.

### US05 — Sessão, failover e recuperação (P0)

1. **Dado** cliente conectado a B1, **quando** B1 não responde a HEARTBEAT dentro da política configurada, **então** a biblioteca tenta REGISTER no B2 e refaz inscrições ativas.
2. **Dado** inscrições restauradas no B2, **então** novas publicações para esses tópicos podem ser recebidas, sem replay de publicações perdidas.
3. **Dado** cliente sem broker disponível, **então** API comunica indisponibilidade/inconclusão, mantém lista desejada de inscrições e volta a tentar após recuperação (sem loop ocupado).
4. **Dado** broker reiniciado, **então** usa `PEER_SYNC` para reconciliar catálogo e estado de interesse. Se ambos perdem toda a memória, tópicos e inscrições não são recuperáveis automaticamente.
5. **Dado** sessão que deixa de enviar HEARTBEAT, **então** o broker remove sua inscrição quando o TTL expira e atualiza o interesse remoto.

### US06 — Biblioteca Go e demonstrações (P1)

1. **Dada** aplicação Go, **quando** importa a biblioteca, **então** pode registrar-se, criar/remover tópicos, assinar/cancelar inscrição, publicar e consumir mensagens sem operar diretamente datagramas.
2. **Dada** aplicação CLI, **então** deve demonstrar pub/sub local, cross-broker, `NO_SUBSCRIBERS`, limite de taxa e failover apenas com terminal.
3. **Dado** cenário de carga configurável, **então** executa testes com múltiplos assinantes/publicadores e exporta métricas em CSV/JSON.

## 4. Requisitos funcionais

| ID | Requisito obrigatório |
|---|---|
| RF01 | Operações `REGISTER`, `HEARTBEAT` e expiração de sessões lógicas UDP por client_id e origem registrada |
| RF02 | `CREATE_TOPIC` e `DELETE_TOPIC` dinâmicos, coordenados por B1 e replicados ao B2 |
| RF03 | `SUBSCRIBE` e `UNSUBSCRIBE` por sessão e tópico, com resposta de controle |
| RF04 | `PUBLISH` encaminha ao conjunto de assinantes ativos conhecidos localmente, com best effort |
| RF05 | `NO_SUBSCRIBERS` com ciência tentada via ACK/retry limitado e status inconclusivo no timeout |
| RF06 | `DELIVER` sem ACK/retransmissão, com `topic`, `message_id` e `payload` |
| RF07 | `INTEREST_UPDATE` e encaminhamento somente para broker ativo com interesse conhecido |
| RF08 | `FORWARD_PUBLICATION` não gera reencaminhamento circular |
| RF09 | `TOPIC_REQUEST`, `TOPIC_UPDATE`, `PEER_SYNC` e `PEER_HEARTBEAT` entre brokers |
| RF10 | Rate limit por publicador e resposta `RATE_LIMITED` para excesso de tráfego detectado |
| RF11 | Buffer finito e drop-oldest de publicações transitórias; isolamento de assinantes lentos |
| RF12 | Biblioteca cliente detecta falha, muda de broker e reenvia inscrições |
| RF13 | Controle tem `request_id` e retry limitado, com aplicação idempotente dentro de janela definida |
| RF14 | Validação de tamanho do datagrama e tratamento explícito de requisições inválidas |
| RF15 | Executáveis de terminal e ferramentas de testes configuráveis/documentados |
| RF16 | Bibliotecas e cenários devem produzir métricas mensuráveis e exportáveis |

## 5. Requisitos não funcionais / critérios observáveis

- **RNF01 — Latência:** medir p50, p95 e p99 de entrega nos testes; buscar valores baixos. **Não** declarar limite matemático de tempo real; alvos experimentais constam em `docs/METRICAS.md`.
- **RNF02 — Throughput:** medir publicações processadas/s, entregas tentadas/s, entregas recebidas/s e descartes/s.
- **RNF03 — Disponibilidade:** medir tempo entre falha detectável e restauração de inscrição/primeiro evento recebido; falha total simultânea não é coberta.
- **RNF04 — Fairness:** taxas individuais, latência do participante bem-comportado versus baseline e isolação de fila/limite de taxa.
- **RNF05 — Recursos:** memória limitada por número configurado de sessões/filas/caches; evitar goroutines não canceláveis.
- **RNF06 — Operação:** executar brokers/testes por terminal em localhost, sem serviços externos obrigatórios.
- **RNF07 — Documentação:** README, biblioteca importável, execução local/cross-broker, casos de falha e cargas reproduzíveis.

## 6. Semântica e bordas obrigatórias

- `REGISTER`: vincula um `client_id` à origem UDP; re-registro atualiza endereço da sessão no broker atendente. Sem autenticação no escopo acadêmico; recomenda-se operar localmente.
- `PUBLISH` **não recebe ACK de sucesso de entrega**. A tentativa de envio local da biblioteca não prova processamento remoto; erros e notificações podem ocorrer de forma assíncrona.
- `NO_SUBSCRIBERS` somente quando broker conhece **zero assinantes** após consultar estado local e remoto disponível/conhecido; estado remoto pode estar desatualizado.
- `SUBSCRIBE` confirma registro **local** no broker; propagação de interesse remoto é assíncrona com retry, não uma confirmação de entrega de publicações.
- B1 é coordenador; B2 não promove líder ao falhar B1. Somente publicação e inscrição em tópicos já existentes continuam em B2.
- Se B1 reinicia sem memória e B2 está vivo, a hipótese proposta é recuperar catálogo de B2 antes de voltar a coordenar; ver **D02**.
- Publicações com destinatários são tentadas para **todos os assinantes ativos conhecidos**, mas podem ser descartadas devido a saturação de filas ou perda de UDP.
- Dados publicados durante a transição entre brokers podem ser perdidos ou eventualmente duplicados (não há replay/dedup obrigatória).
- Operações de controle expiradas ficam **inconclusivas** e não devem ser presumidas como revertidas.

## 7. Fora de escopo

Streaming de vídeo, frontend, armazenamento permanente, replicação durável, entrega at-least-once/exactly-once de dados, segurança/criptografia, descoberta automática, coordenação por consenso, N brokers, clientes em linguagens além de Go, execução internet pública.

## 8. Pronto quando

- [ ] RF01–RF16 demonstráveis com testes automatizados ou comandos reproduzíveis.
- [ ] `go test ./...` e `go test -race ./...` passam (ou limitações claramente relatadas).
- [ ] Dois brokers são executáveis em processos separados e as aplicações clientes não precisam saber qual broker está selecionado.
- [ ] Testes mostram Pub/Sub, ausência de assinantes, rajada/isolamento, falha e retorno de broker.
- [ ] Guia de biblioteca e execução corresponde ao código real, não apenas às expectativas.
- [ ] Resultados não prometem entrega garantida nem zero downtime em todas as condições.

## 9. Referências cruzadas

- `contracts/wire-protocol.md` — tipos, campos, replies e deduplicação de controle.
- `contracts/client-api.md` — API utilizável pelas aplicações Go.
- `plan.md` / `data-model.md` — estrutura e invariantes.
- `docs/DECISOES_E_RISCOS.md` — escolhas de detalhamento, riscos e pontos para discussão.
- `docs/RASTREABILIDADE.md` — vínculo com a Etapa 1/enunciado.
