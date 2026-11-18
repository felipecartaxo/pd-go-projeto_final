# Como usar Codex com este SDD

## Abordagem

A pasta já contém os artefatos essenciais do fluxo **constituição → especificação → planejamento → tarefas → implementação**. Não é necessário instalar a CLI do Spec Kit para aproveitar SDD com Codex: basta `AGENTS.md` na raiz, specs no repositório e prompts que indiquem tarefas específicas. **Este pacote não instala comandos/suportes oficiais `speckit-*`.**

## Preparação

1. Criar repositório Git vazio para o projeto Go ou usar o repositório já existente.
2. Copiar **todos** os arquivos deste pacote para a raiz, incluindo `.specify` (pasta oculta); confirmar que `AGENTS.md` ficou na raiz.
3. Ler `docs/DECISOES_E_RISCOS.md`. Se a equipe discordar de D02/D03, corrigir o contrato **antes** de implementar sincronização/gestão de tópicos.
4. Abrir Codex a partir da raiz; garantir que ele leu `AGENTS.md`.
5. Criar commits pequenos conforme fases de `tasks.md`. Preferir revisões locais após cada tarefa e `go test` em todas as iterações.

## Primeiro prompt recomendado (copiar para o Codex)

> Leia `AGENTS.md`, `.specify/memory/constitution.md`, `specs/001-middleware-pubsub-tempo-real/spec.md`, `plan.md`, `contracts/wire-protocol.md`, `contracts/client-api.md` e `docs/DECISOES_E_RISCOS.md`. Não altere as decisões da Etapa 1. Implemente **somente T001, T002 e T003** de `tasks.md`. Antes, resuma arquivos e testes. Utilize Go e biblioteca padrão, escreva testes do protocolo JSON/UDP, valide 1200 bytes para o envelope completo e mantenha `PUBLISH/DELIVER` best effort. Depois execute `gofmt`, `go test ./...` e registre quais tarefas foram concluídas. Não implemente o restante do backlog nesta iteração.

## Prompts seguintes

**Broker local (T004–T007):**

> Implemente T004–T007 conforme `tasks.md`, `data-model.md` e o contrato do protocolo. Crie testes de retry/idempotência de controle e de sessões/tópicos/inscrições. Não crie entrega de mensagens ainda. Relate arquivos alterados e resultados dos testes.

**Limitação e fan-out (T008–T012):**

> Implemente T008–T012: rate limit por publicador, filas de dados limitadas com drop-oldest, despacho justo e entrega local best effort, incluindo NO_SUBSCRIBERS/ACK. Priorize testes demonstrando isolamento de clientes, sem goroutines ilimitadas e sem ACK de DELIVER.

**Interbroker (T013–T020):**

> Revise D02/D03 antes de começar. Implemente T013–T020 com dois brokers fixos, UDP, interesse por tópico, encaminhamento seletivo sem loops e recuperação PEER_SYNC chunked. Execute testes de duplicação, reordenação e falha de broker; não promova B2 a coordenador de catálogo.

**Biblioteca e demo (T021–T027):**

> Implemente T021–T027. Use um único leitor UDP no cliente, controle correlacionado, notificações assíncronas e failover com re-REGISTER/re-SUBSCRIBE. Escreva exemplos compiláveis e faça as CLIs seguirem `quickstart.md`. Não reenviar publicações perdidas.

**Carga e entrega (T028–T030):**

> Implemente T028–T030 conforme `docs/PLANO_DE_TESTES.md` e `docs/METRICAS.md`. Demonstre fan-out, rajadas, fairness, falha e retorno de broker. Execute a suite e atualize README/guia da biblioteca com comandos reais e limitações.

## Regras de revisão com a equipe

Em toda rodada do Codex, pedir: (1) tarefas executadas, (2) testes adicionados, (3) comando/resultado real, (4) limites encontrados, (5) mudanças em contratos. Reverter mudanças que introduzam DB, TCP, garantias de dados, N brokers, eleição ou frontend sem aprovação.

## Se optar pelo GitHub Spec Kit oficial

Há ferramentas próprias de inicialização e skills para agentes. Este pacote segue **nomes usuais de artefatos**, mas não substitui a instalação dos comandos do Spec Kit. Se instalá-lo posteriormente, confira quais arquivos ele gerará, evite sobrescrever a especificação já escrita e use `AGENTS.md`/revisão humana como autoridade do projeto.
