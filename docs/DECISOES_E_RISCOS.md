# Decisões complementares, incompatibilidades e riscos

**Leia antes de entregar instruções ao Codex.** A Etapa 1 **não está incorreta em sua essência** e pode ser implementada em Go. Entretanto, há combinações de propriedades que não podem ser garantidas simultaneamente e lacunas que o código precisa tratar. Abaixo: **decisão da Etapa 1**, **limitação técnica**, **proposta deste pacote** e o que deve ser validado pela equipe.

## D01 — "Transmissão não pode cair" versus UDP best effort

- **Etapa 1:** priorizar disponibilidade do **serviço**, UDP, mensagens voláteis, entrega best effort e failover com restauração de inscrições.
- **Limitação:** UDP não estabelece conexão nem garante entregas, e o failover leva algum tempo. É impossível garantir simultaneamente "todos sempre recebem tudo" e descartar mensagens sob sobrecarga. A frase da narrativa refere-se à continuidade da **experiência de serviço**, não garantia formal de cada datagrama.
- **Proposta:** medir **tempo de interrupção detectada**, **tempo de re-inscrição** e **primeira publicação recebida após falha**, admitindo perda na janela. Mensagens com assinantes conhecidos recebem tentativa de encaminhamento, não confirmação de consumo. Escrever isso no relatório e documentação.
- **Impacto:** nenhuma mudança arquitetural. Essencial para evitar promessas incorretas.
- **Situação:** interpretação recomendada; confirmá-la com professor se exigir "zero downtime" literalmente.

## D02 — Broker 1 reinicia e perde o catálogo volátil

- **Etapa 1:** B1 coordena criação/exclusão, B2 guarda cópia, não há persistência, e no retorno há `PEER_SYNC`.
- **Lacuna:** se B1 morre, reinicia sem memória e passa a coordenar antes de consultar B2, **apaga logicamente a visão do catálogo**. B2 pode conter a única cópia de tópicos sobrevivente.
- **Proposta:** B1 em restart recebe snapshot de B2 **antes de aceitar mutações** e cria novo `catalog_epoch`; B2 reiniciado recupera de B1. Se ambos perderem memória, o catálogo anterior é irrecuperável sem persistência — coerente com o tradeoff. Se B2 tinha estado desatualizado antes da falha de B1, mudanças mais recentes podem ser perdidas.
- **Impacto:** detalhamento importante para recuperação, sem terceiro broker nem banco.
- **Situação:** **VALIDADO**.

## D03 — O que significa sucesso ao criar/excluir tópicos?

- **Etapa 1:** B1 organiza o catálogo, B2 sincroniza usando confirmações e retransmissões limitadas; falha de B1 impede mutações.
- **Lacuna:** se B1 aplicou a mutação mas `TOPIC_UPDATE` ou ACK se perdeu, o cliente deve receber `OK`, `ERROR` ou `INCONCLUSIVE`?
- **Proposta para simplificar o projeto:** `OK` significa **commit local no B1**. Replicação é assíncrona, com retry e `PEER_SYNC` até convergir. Para request vindo de B2, B1 devolve seu resultado; se B2 perde resposta, o cliente pode receber `INCONCLUSIVE`. A aplicação não deve assumir réplica instantaneamente sincronizada.
- **Alternativa válida:** `OK` apenas após ACK da réplica; se peer estiver indisponível, degrada disponibilidade de administração de tópicos. Este pacote **não** escolheu a alternativa porque a Etapa 1 enfatiza continuidade do serviço e atualização assíncrona do estado.
- **Situação:** **VALIDADO**.

## D04 — "Publicador tem ciência" com NO_SUBSCRIBERS sobre UDP

- **Etapa 1/enunciado:** publicação sem inscritos é descartada e o publicador deve saber; notificação NO_SUBSCRIBERS recebe ACK e retry limitado; falta de resposta é inconclusiva.
- **Limitação:** mesmo com três tentativas, todos os datagramas do aviso podem se perder; não existe garantia absoluta de ciência. Também é possível que a visão de interesse remoto esteja desatualizada.
- **Proposta:** `Publish` retorna após tentativa de envio UDP (sem ACK de sucesso). `NO_SUBSCRIBERS` é **evento assíncrono**, correlacionado por `request_id`/`message_id`, ACKado automaticamente pela biblioteca. Retries limitados aumentam a probabilidade de ciência. Ausência do aviso **não significa** que havia inscritos. Testes devem verificar a tentativa de ciência e o comportamento no caso de timeout.
- **Situação:** **VALIDADO**.

## D05 — JSON UDP e tamanho do catálogo

- **Etapa 1:** um JSON compacto por datagrama, rejeitar publicações acima de limite, PEER_SYNC de catálogos.
- **Risco:** catálogos crescentes podem não caber num único datagrama. Fragmentação IP degrada previsibilidade e confiabilidade.
- **Proposta:** limite de **1200 bytes no envelope serializado**; `PUBLISH/DELIVER` acima do limite são rejeitados, mas **somente o controle `PEER_SYNC`** usa várias mensagens, cada uma em seu datagrama, com `sync_id`, número de parte e retry. Não altera o princípio um JSON/datagrama.
- **Situação:** **VALIDADO**.

## D06 — "Fila drop-oldest" e isolamento real

- **Etapa 1:** limitar taxa por publicador, buffers limitados e descartar dados antigos; evitar que cliente agressivo prejudique outros.
- **Risco:** **uma única fila global** com drop-oldest pode deixar um produtor agressivo expulsar mensagens de clientes bem-comportados; um único worker bloqueado por assinante lento paralisa o fan-out.
- **Proposta:** rate limiter independente + fila de entrada por publicador + despacho justo + fila individual por assinante. Aplicar drop-oldest a **dados** e nunca descartar silêncio de controle. Medir fairness para verificar se realmente funciona.
- **Situação:** **VALIDADO**.

## D07 — Sem persistência: reinício simultâneo e perda de estado

- **Etapa 1:** mensagens e sessões apenas em memória; B2 copia catálogo de B1.
- **Consequência:** se os dois brokers perdem memória, não é possível restaurar catálogo anterior ou inscrições de **clientes que não reconectem**, sem persistência. A biblioteca de um cliente vivo pode repor suas **inscrições desejadas** quando um tópico voltar a existir; não recria automaticamente tópicos apagados.
- **Proposta:** *documentar essa limitação e testar o caso; não introduzir banco, WAL, quorum ou serviço externo*.
- **Situação:** **VALIDADO**.

## D08 — Identidade de sessões UDP sem autenticação

- **Etapa 1:** `client_id` + endereço UDP + heartbeat.
- **Risco:** origem de UDP pode ser falsificada em redes não confiáveis; reuso de ID pode conflitar. Isso não é uma pilha de segurança para Internet pública.
- **Proposta:** testes em localhost, conferência de endereço do peer fixo e sessões por cliente; nenhuma promessa de autenticação. Segurança de produção está fora do escopo.
- **Situação:** **VALIDADO**.

## D09 — Exclusão de tópico e canais de subscribers

- **Etapa 1:** `DELETE_TOPIC` elimina tópicos, inscrições e pendências; não define aviso de deleção aos clientes de terceiros.
- **Risco:** um assinante remoto pode não descobrir instantaneamente que seu tópico foi removido. Adicionar `TOPIC_DELETED` mudaria o protocolo.
- **Proposta:** no broker remover estado e filas ainda pendentes; após reconexão a tentativa de re-assinar tópico excluído retorna `TOPIC_NOT_FOUND`, que biblioteca expõe. Não acrescentar novo tipo de mensagem no MVP sem aprovação.
- **Situação:** **VALIDADO**.

## D10 — Métricas e metas acadêmicas

- **Enunciado:** testar vários clientes, extrair métricas de desempenho e posteriormente criar gráficos.
- **Proposta:** registrar p50/p95/p99, taxa de entrega **observada**, perda/descartes, fairness e failover. Metas numéricas são **hipóteses locais de avaliação**, não SLA nem requisito docente; consultar `docs/METRICAS.md`.
- **Situação:** **VALIDADO**.

## D11 — Janela limitada de idempotência de controle

- **Contrato:** controle deve reutilizar `request_id`, sofrer retry finito e ser idempotente dentro de uma janela delimitada, sem prometer execução única para falhas arbitrárias.
- **Escolha de implementação:** o broker guarda a resposta por chave (`type`, origem UDP, `request_id`) em cache limitado e configurável; os defaults são TTL de 20 s e capacidade de 1024 entradas, já propostos no modelo de dados. A mesma capacidade limita `NO_SUBSCRIBERS` pendentes, evitando fila de controle ilimitada. O retry que esgota tentativas retorna `ErrInconclusive` localmente.
- **Impacto:** uma repetição dentro da janela devolve a resposta original sem reaplicar a mutação. Após expiração/evicção, não há promessa de deduplicação; o cache nunca cresce sem limite.
- **Situação:** implementado em T004; defaults revisáveis por flag, não requisito docente.

## Recomendação de revisão pela equipe

Antes de Codex finalizar `T016–T019`, aprovar explicitamente **D02 e D03**. As demais decisões são esclarecimentos que podem ser implementados enquanto a equipe testa. Se professor solicitar uma garantia incompatível com UDP + dados descartáveis, registrar a alteração em uma nova versão da spec e da constituição antes de mexer no código.
