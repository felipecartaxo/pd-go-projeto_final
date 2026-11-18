# Instruções do projeto para agentes (Codex)

## Objetivo e fonte da verdade

Você está implementando, em **Go**, um middleware Pub/Sub UDP para o **Cenário C — Interação ao Vivo**, conforme o trabalho de Sistemas Distribuídos e a arquitetura aprovada na Etapa 1.

Leia, nesta ordem, antes de modificar código:

1. `.specify/memory/constitution.md`
2. `specs/001-middleware-pubsub-tempo-real/spec.md`
3. `specs/001-middleware-pubsub-tempo-real/contracts/wire-protocol.md`
4. `specs/001-middleware-pubsub-tempo-real/contracts/client-api.md`
5. `specs/001-middleware-pubsub-tempo-real/plan.md`
6. `specs/001-middleware-pubsub-tempo-real/tasks.md`
7. `docs/DECISOES_E_RISCOS.md` para decisões propostas/pendentes.

O enunciado e a Etapa 1 predominam sobre propostas novas. **Não mude a arquitetura silenciosamente.** Se uma proposta pendente alterar comportamento, explique a escolha antes de consolidá-la. Não trate defaults sugeridos como requisitos do professor.

## Regras inegociáveis

- Usar Go e preferir **biblioteca padrão** (`net`, `encoding/json`, `context`, `sync`, `time`, `testing`). Não introduzir dependências externas sem necessidade justificada.
- Pub/Sub por tópico, **dois brokers fixos** configurados previamente, cada um capaz de atender inscritos/publicadores para tópicos existentes.
- UDP entre cliente–broker e broker–broker. **Um envelope JSON por datagrama**; limite total de tamanho explicitamente verificado.
- **Dados:** best effort, sem persistência, sem ACK por `DELIVER`, sem retransmitir publicações e sem deduplicação obrigatória de dados.
- **Controle:** `request_id`, resultados explícitos, ACK, retry limitado e idempotência dentro de janela definida.
- Broker 1 coordena **criação/exclusão** de tópicos; Broker 2 apenas encaminha tais solicitações. Broker 2 continua servindo tópicos já conhecidos se B1 falhar.
- Rate limiting **por publicador**; buffers finitos e drop-oldest para eventos transitórios; isolar assinante lento e publicador agressivo.
- Biblioteca cliente mantém inscrições, detecta falha e migra entre brokers, registrando sessão novamente e restaurando inscrições.
- Não adicionar frontend, banco, Redis, Kafka, MQTT, TCP/WebSocket, autenticação de produção, service discovery ou eleição distribuída.

## Processo de trabalho

- Trabalhe **por IDs de tarefas** em `tasks.md`; implemente no máximo um grupo coeso por iteração.
- Antes da edição: resuma tarefa, arquivos, testes e pendências de decisão.
- Antes do código de comportamento: escreva testes para o contrato (ou explique quando for um pequeno bootstrap).
- Após cada grupo: executar `gofmt`, `go test ./...` e, quando couber, `go test -race ./...`; relatar exatamente o que passou e o que não pôde ser executado.
- Mantenha memória limitada: filas, caches, contextos e goroutines devem ter limite/cancelamento. Não bloquear a distribuição para todos por um assinante lento.
- Nunca confundir `WriteToUDP` bem-sucedido com entrega recebida. Nunca afirmar "sem interrupção" de forma absoluta.
- Registre escolhas novas em `docs/DECISOES_E_RISCOS.md` e mantenha `docs/RASTREABILIDADE.md` coerente.
- Ao completar tarefas, marque suas caixas em `tasks.md` e atualize `README.md`, `docs/BIBLIOTECA.md` e `quickstart.md` para refletirem a implementação **real**, não apenas o design.

## Prioridades (nesta ordem)

1. Correção semântica do middleware e independência de clientes.
2. Comportamento de falha e sobrecarga testável.
3. Latência e observabilidade mensuráveis.
4. Clareza da biblioteca pública e documentação.
5. Refinamentos não essenciais.

Quando houver conflito entre uma otimização de desempenho e a semântica/documentação, preserve a semântica e reporte o trade-off.
