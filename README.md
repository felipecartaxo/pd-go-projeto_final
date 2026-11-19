# Middleware Pub/Sub para Interação ao Vivo — SDD (Etapa 2)

> **Estado da implementação:** T001–T007 concluídas: módulo Go, lifecycle UDP de broker, protocolo JSON/UDP v1 e controle local de sessões, catálogo e inscrições. Entrega de dados (`PUBLISH`/`DELIVER`) e biblioteca cliente ainda pertencem às tarefas posteriores.

> **Status:** implementação incremental; T001–T007 estão concluídas nesta revisão.
> **Origem:** documento aprovado da Etapa 1 (10 páginas) e enunciado do projeto (5 páginas).
> **Objetivo:** código Go no repositório, biblioteca pública utilizável, duas instâncias UDP, CLI de demonstração e testes/medições reproduzíveis.

## O que este pacote contém

| Arquivo | Uso |
|---|---|
| `AGENTS.md` | Instruções persistentes para o Codex na raiz do repositório |
| `.specify/memory/constitution.md` | Princípios de projeto e critérios inegociáveis |
| `specs/001-middleware-pubsub-tempo-real/spec.md` | Requisitos, cenários e aceitação (O QUÊ) |
| `specs/001-middleware-pubsub-tempo-real/plan.md` | Desenho técnico e organização em Go (COMO) |
| `specs/001-middleware-pubsub-tempo-real/tasks.md` | Backlog em ordem implementável, com critérios de conclusão |
| `specs/001-middleware-pubsub-tempo-real/research.md` | Justificativas, escolhas técnicas e parâmetros propostos |
| `specs/001-middleware-pubsub-tempo-real/data-model.md` | Modelo de estado em memória e invariantes |
| `specs/001-middleware-pubsub-tempo-real/contracts/wire-protocol.md` | Contrato JSON/UDP, tipos, ACK, erros, replicação |
| `specs/001-middleware-pubsub-tempo-real/contracts/client-api.md` | Contrato da biblioteca Go para os consumidores do middleware |
| `specs/001-middleware-pubsub-tempo-real/quickstart.md` | Roteiro de execução planejado e testes manuais |
| `specs/001-middleware-pubsub-tempo-real/checklists/requirements.md` | Verificação de completude de especificação e entrega |
| `docs/DECISOES_E_RISCOS.md` | Lacunas técnicas da Etapa 1 e propostas explícitas, sem mudança silenciosa |
| `docs/PLANO_DE_TESTES.md` | Casos de teste funcionais e de falha/sobrecarga |
| `docs/METRICAS.md` | Instrumentação e análise dos resultados, já visando a Etapa 3 |
| `docs/BIBLIOTECA.md` | Guia público planejado para uso da biblioteca |
| `docs/GUIA_CODEX.md` | Como conduzir o Codex por pequenas entregas orientadas às specs |
| `docs/RASTREABILIDADE.md` | Requisitos do documento de origem → spec → teste |

## Como começar

1. Descompacte **o conteúdo desta pasta na raiz do repositório Go** (não crie uma pasta `sdd-pubsub-go` dentro do projeto se quiser que o Codex leia `AGENTS.md` automaticamente).
2. Leia `docs/DECISOES_E_RISCOS.md`, sobretudo **D01–D04**. São propostas transparentes de detalhamento, não fatos previamente aprovados pelo professor.
3. Abra o Codex na raiz do repositório, leia `docs/GUIA_CODEX.md` e solicite a execução de **T001–T003** em `tasks.md`.
4. Revise código e testes ao terminar cada conjunto de tarefas. Não solicite a implementação inteira em um único prompt.
5. Ao implementar, substitua os exemplos marcados como **planejados** pelos comandos e assinaturas reais e teste-os antes de entregar.

**Não é necessário instalar GitHub Spec Kit:** esta estrutura é **inspirada** nas convenções do Spec Kit, mas pode ser lida diretamente pelo Codex com `AGENTS.md`. Se a equipe decidir instalar a CLI oficial do Spec Kit, inicialize a integração separadamente e **não sobrescreva** estas especificações aprovadas.

## Base implementada (T001–T007)

- Módulo: `github.com/Neraky/pd-go-cenario-2-implementacao`; versão declarada: Go 1.26.
- `internal/protocol` codifica, decodifica e valida envelopes JSON v1 em um único datagrama de no máximo **1200 bytes**. `PUBLISH` e `DELIVER` são dados best effort; esta base não adiciona ACK ou retry para eles.
- `cmd/broker` inicia um listener UDP configurável e encerra graciosamente com `Ctrl+C`. Ele processa `REGISTER`, `HEARTBEAT`, `CREATE_TOPIC`, `DELETE_TOPIC`, `SUBSCRIBE` e `UNSUBSCRIBE`, com cache de respostas idempotentes limitado (20 s/1024 entradas por default).
- O Broker 1 mantém catálogo e inscrições locais em memória; Broker 2 responde `COORDINATOR_UNAVAILABLE` a mutações de catálogo até T016 integrar o encaminhamento a B1. Não há `PUBLISH`, `DELIVER`, filas de dados nem fan-out nesta etapa.

Para iniciar os dois listeners locais nesta etapa:

```bash
go run ./cmd/broker -id=1
go run ./cmd/broker -id=2
```

Execute os testes com `go test ./...`. Os comandos de demo, carga e os fluxos Pub/Sub descritos neste documento continuam planejados até suas tarefas correspondentes.

## Limites assumidos

- Projeto **acadêmico**, não um serviço pronto para produção.
- Apenas **dois brokers fixos**, sem eleição de líder ou armazenamento permanente.
- O objeto do projeto é o **middleware**; terminal/CLI serve somente de exemplo e ensaio.
- `UDP + best effort` admite perda de publicações em sobrecarga, falhas e troca de broker.
- `OK` de controle não implica entrega de dados a assinantes.

## Fontes e precedência

1. Descrição da Etapa 1, especialmente seções 2.1, 2.3, 3.2, 3.3 e 4.
2. Enunciado, sobretudo requisitos mínimos e descrição das Etapas 2 e 3.
3. Esta especificação, que **detalha** escolhas deixadas abertas e sinaliza conflitos em `docs/DECISOES_E_RISCOS.md`.

Se houver conflito real com a Etapa 1, **sinalizar e pedir decisão**, em vez de trocar UDP por TCP, adicionar banco, criar um terceiro broker ou implementar entrega garantida.
