# Research / fundamentação técnica

Este documento não substitui a Etapa 1: registra interpretações **operacionais propostas** para implementá-la sem componentes novos.

## Transporte e envelope

**Escolha preservada:** UDP para cliente/broker e entre brokers, um JSON compacto por datagrama, sem fragmentação intencional e sem retransmissão de `PUBLISH`/`DELIVER`.

**Proposta:** limite de **1200 bytes para o datagrama de aplicação codificado**; rejeitar requisições maiores com `DATAGRAM_TOO_LARGE` quando possível. É um limite conservador para minimizar fragmentação IP, não a capacidade máxima de UDP. `payload` é um valor JSON válido (`json.RawMessage`) que conta no limite total. Para testes acadêmicos em localhost, evita complexidade de chunking de publicações.

## Formato, IDs e ordenação

- `v=1` identifica a versão do protocolo; mensagens desconhecidas devem falhar claramente.
- `request_id` é único dentro da janela de retry para cada emissor e correlaciona resposta/ACK.
- `message_id` é único por publicação para medição (gerar 128 bits randômicos no cliente); não exige deduplicação dos dados.
- `broker_id` (1 ou 2) é obrigatório em mensagens entre brokers. Não inferir confiança apenas do campo: conferir também endereço UDP do peer configurado.
- `catalog_revision` monotônico em B1 enquanto o estado é preservado. Reboot/sincronização exige um novo `catalog_epoch` aleatório, evitando comparar revisions de reinícios diferentes.
- `interest_version` por tópico e origem controla reordenação/duplicata das mudanças de interesse; após reinício da instância, o `peer_epoch` muda.

## Controle: semântica de confirmação

**Solicitações**: gerar `request_id`, repetir a **mesma solicitação** até 3 envios totais em timeout configurável, não criar operações novas nos retries. **Destinatário**: cache limitado de respostas por `(source, client_id ou broker_id, type, request_id)`, com TTL sugerido de 20 s. Ao receber repetição durante o TTL, devolver o mesmo resultado sem reaplicar o controle. Se cache expirou, semântica de exactly-once **não é prometida**.

**Respostas**: `OK` e `ERROR` indicam resultado de controle observado; `INCONCLUSIVE` é resultado **local da biblioteca** após timeout (não é um ACK recebido nem prova de falha remota). `NO_SUBSCRIBERS` usa `request_id` da publicação de origem e é retransmitido até receber `ACK` ou acabar o limite; `RATE_LIMITED` pode ser melhor esforço.

## Concorrência/fairness

- Leitor UDP valida tamanho e tipo cedo; não deve executar fan-out demorado no loop de leitura.
- Rate limiter independente por publicador (token bucket). Proposta para testes iniciais: **200 publicações/s, burst de 100** tokens, valores configuráveis.
- Filas limitadas por publicador para processamento de dados e por assinante para `DELIVER` (proposta inicial: 64 eventos cada), com descarte **drop-oldest**; os eventos de **controle não compartilham** a mesma fila drop-oldest.
- Dispatcher fair (round-robin/quotas entre publicadores com fila pronta) evita que a fila de um produtor agressivo monopolize a etapa de fan-out. Evitar goroutine ilimitada por datagrama.
- Um único assinante lento não pode bloquear os demais; o `WriteToUDP` e os canais/fila devem tratar erro/local overflow como descarte com métrica, sem bloquear fan-out.
- As filas são de transientes; catálogos, pedidos/ACK e interest updates usam estado próprio, com sincronização e retry finitos.

## Vida útil / detecção de falhas

**Sugestões iniciais para desenvolvimento, não SLA de produção:** `heartbeat_interval=500ms`, `heartbeat_timeout=1800ms`, `session_ttl=4s`, `peer_timeout=2s`, `control_attempts=3`, `control_retry_interval=300ms`. É permitido sobrescrever por config para testes determinísticos. A recepção de heartbeat por si só não prova que pacotes de dados chegarão.

## Sincronização de catálogo e interesses

- B1 cria/deleta, B2 encaminha pedido e mantém réplica. `TOPIC_UPDATE` carrega `catalog_epoch + catalog_revision` e é reenviado com limite. Revisions ausentes disparam `PEER_SYNC`.
- `PEER_SYNC` transmite **snapshot paginado em vários datagramas** (cada envelope ainda é um datagrama UDP de até 1200 B), com identificador e confirmação de cada parte. A réplica só troca seu estado depois de receber todas as partes da mesma revisão; snapshot incompleto expira.
- `INTEREST_UPDATE` leva presença/ausência de assinante local e `interest_version`; mudança local de 0→1 e 1→0 gera atualização. Retentativa/reconciliação periódica reduzem estados remotos obsoletos.
- Em reinício de B1, recuperar snapshot do B2 sobrevivente antes de voltar a aceitar mutações no catálogo (**D02**, proposta). Se B2 também perdeu memória, não há garantia de restaurar tópicos antigos.

## Alternativas descartadas por escopo/arquitetura

- TCP/WebSocket: divergência do protocolo selecionado na Etapa 1.
- Banco/WAL: aumentaria confiabilidade, mas contraria a escolha de memória volátil.
- Entrega confiável de dados: ACKs/retries por consumidor elevam latência e complexidade sem exigência para o cenário.
- N brokers, eleição automática, consenso: não cabem no escopo de dois brokers fixos.
- gRPC/Protobuf/MQTT: não implementariam o protocolo JSON/UDP proposto pela equipe.

## Referências primárias

- Projeto da Etapa 1, seções 2.1 (decisões), 2.3 (fluxos), 3.2 (mensagens), 3.3 (mecanismos) e 4 (tradeoffs).
- Enunciado, itens 3 (mínimos), 4 (aplicações de exemplo) e 5.2/5.3/5.4 (etapas).
