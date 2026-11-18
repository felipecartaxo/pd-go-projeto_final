# Constituição do Projeto — Middleware Pub/Sub (v1.0)

**Origem:** arquitetura da Etapa 1, Cenário C; princípios reforçados para orientar SDD em Go.

## P1. Fidelidade à arquitetura aprovada

A implementação terá **exatamente dois brokers fixos**, protocolo Pub/Sub próprio sobre **UDP**, comunicação interbroker UDP, catálogo coordenado pelo Broker 1, replicação ao Broker 2, dados em memória, encaminhamento interbroker seletivo, rate limit por publicador, buffers finitos e failover na biblioteca. Alterações precisam ser explicitadas antes de implementação.

## P2. Correção do caminho crítico

Publicações são **best effort**: não exigir ACKs de consumidores, persistência, retransmissão ou ordenação. Envio para cada inscrito elegível deve ser tentado sem que um inscrito lento impeça os outros. A validade do tópico, da sessão, do datagrama e do limite de taxa é checada antes do fan-out.

## P3. Controle confiável apenas até o limite escolhido

Operações de controle precisam ser correlacionáveis, idempotentes dentro de um prazo delimitado e retransmitidas de maneira finita. Não prometer exatamente uma execução em falhas arbitrárias. Respostas incertas são **INCONCLUSIVE**, não confirmação de sucesso nem de falha garantida.

## P4. Boundedness e isolamento

Todas as filas e caches possuem tamanho máximo explícito; rotinas têm ciclo de vida gerenciável. Rate limiting e backpressure limitado devem impedir que um publicador ou assinante monopolize memória/execução. O descarte drop-oldest aplica-se **apenas a eventos transitórios**, não ao estado de controle sem tratamento explícito.

## P5. Transparência dos dois brokers

A biblioteca conhece os dois endereços, mantém `client_id` e inscrições, realiza HEARTBEAT, escolhe instância saudável e tenta restaurar estado após falha. Mensagens em trânsito podem se perder durante o failover; operações do catálogo só funcionam se o Broker 1 estiver disponível.

## P6. Contratos antes de implementação

O protocolo JSON/UDP e a API Go são contratos versionados. Toda tarefa funcional exige teste de comportamento normal e, quando aplicável, erro, timeout ou duplicata. Não implementar semânticas implícitas por conveniência.

## P7. Escopo acadêmico e entrega reproduzível

Evitar complexidade de produção (autenticação forte, persistência, cluster dinâmico). Entregar repositório com biblioteca importável, executáveis CLI e scripts/cenários de teste reproduzíveis, permitindo avaliações de latência, taxa de entrega, fairness e failover.

## P8. Visibilidade e honestidade dos resultados

Expor contadores e métricas separando recebidas, aceitas, encaminhadas, descartadas por fila, limitadas por taxa, falhas e latência. Declarar limites dos testes locais e não chamar sucesso de envio UDP de confirmação pelo receptor.

## Como aplicar

- Toda revisão/PR deve indicar tarefas de `tasks.md`, requisitos relacionados, testes executados e alterações de contrato.
- Se uma decisão implicar mudança dos princípios P1–P5, interromper e documentar a discussão em `docs/DECISOES_E_RISCOS.md`.
- Em caso de conflito, a Etapa 1 e os requisitos explícitos do enunciado têm precedência sobre escolhas sugeridas neste pacote.
