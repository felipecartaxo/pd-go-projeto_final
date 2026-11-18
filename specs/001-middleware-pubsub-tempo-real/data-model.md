# Modelo de dados lógico (apenas memória)

Os tipos abaixo são **conceituais**, não interfaces Go obrigatórias; manter nomes/semânticas compatíveis com `contracts/*`.

## Broker

| Entidade | Campos/índices | Invariantes |
|---|---|---|
| `BrokerConfig` | `id`, `listen_addr`, `peer_addr`, limites, timeouts | id somente 1 ou 2; peer fixo |
| `Catalog` | `topics: map[Topic]struct{}`, `catalog_epoch`, `catalog_revision` | B1 coordena mutações; B2 mantém réplica; update é ordenado pela versão |
| `Session` | `client_id`, `udp_addr`, `last_seen`, `subscriptions`, `publisher_limiter` | chave de cliente única por broker; endereço atualizado no REGISTER/HEARTBEAT válido |
| `LocalSubscriptions` | `topic -> set[client_id]` | compatível com `Session.subscriptions`; apenas sessões ativas |
| `RemoteInterest` | `topic -> {present, interest_version, peer_epoch, updated_at}` | reflete interesse **conhecido**, não compromisso de entrega; inválido quando peer falhou |
| `PeerState` | `peer_alive`, `last_heartbeat`, `peer_epoch`, filas de retry | estado limitado e atualizado por heartbeat válido |
| `InputQueue` | fila finita por publicador | somente dados; drop-oldest contabilizado; consumo fair |
| `OutputQueue` | fila finita por assinante | somente `DELIVER`; drop-oldest independente; não bloqueia outras sessões |
| `ReplyCache` | chave de request + resposta + expiração | limitada por capacidade/TTL; controle idempotente na janela |
| `PendingControl` | `request_id`, `attempts`, `deadline`, `payload` | retry limitado; não usar drop-oldest de dados |
| `PendingSnapshot` | `sync_id`, `epoch`, `revision`, chunks recebidos, deadline | só aplicar snapshot completo; memória e chunks limitados |
| `Metrics` | contadores/gauges por broker e causa | métricas de tentativas, não comprovantes de entrega |

## Biblioteca cliente

| Entidade | Campos | Invariantes |
|---|---|---|
| `ClientConfig` | `[2]broker_addresses`, `client_id`, timeouts, limites locais | ambos os endereços conhecidos |
| `ClientState` | `active_broker`, `conn`, `status`, `subscriptions`, `pending`, `events` | uma instância ativa por vez; migrar internamente |
| `DesiredSubscription` | `topic`, `delivery_channel` | persiste no cliente entre troca de brokers, não em banco |
| `PendingRequest` | `request_id`, `deadline`, `attempts`, `response_channel` | resposta de controle roteada por correlation ID |
| `ClientEvent` | notificação de não-inscritos, rate limit, erro ou status de failover | nunca omitir sinais importantes por saturação silenciosa |
| `ReceivedMessage` | `topic`, `message_id`, `payload`, `broker_id` (opcional) | client recebe best effort; pode haver perdas e duplicatas |

## Regras de vida útil

1. `REGISTER` cria/renova sessão; `HEARTBEAT` renova. Uma sessão expira após TTL e suas inscrições são removidas.
2. `SUBSCRIBE/UNSUBSCRIBE` alteram inscrição local e só enviam `INTEREST_UPDATE` nas transições de 0↔1 assinantes locais daquele tópico.
3. `DELETE_TOPIC` remove tópico, inscrições, interesses e eventos **ainda em fila**; não desfaz datagramas já enviados.
4. Se peer ficar indisponível, não encaminhar para ele e ignorar seu interesse no cálculo de roteamento até reconvergir.
5. No retorno de peer, catálogos e interesses são sincronizados **antes de declarar convergência**. O broker pode atender localmente, com possíveis lacunas até lá.
6. Bloqueios (`mutex`) protegem catálogos/índices compartilhados. **Não** manter lock de estado global enquanto faz I/O UDP ou aguarda ACK.
7. `Close()` cancela todas as goroutines, fecha UDP, conclui pedidos pendentes como inconclusivos/cancelados e não vaza canais.

## Estado e transições

**Cliente:** `DISCONNECTED → REGISTERING → ACTIVE(B1|B2) → SUSPECT → FAILING_OVER → ACTIVE(outro broker)`; se nenhum acessível: `UNAVAILABLE → REGISTERING` com backoff. `Close()` leva a `CLOSED` (terminal).

**Broker:** `STARTING → SYNCING → ACTIVE`; B1 que reinicia sem catálogo, e encontra B2 vivo, permanece `SYNCING` para operações de catálogo até recuperar snapshot. B2 sem B1 pode ficar `ACTIVE_READ_ONLY_CATALOG`: atende inscrições/publicações existentes, mas não cria/deleta tópicos.

## Limites iniciais propostos

- Datagrama UDP de aplicação: **1200 B** (JSON completo).
- Publicação: **200 msg/s**, burst **100** por cliente, configurável.
- Fila de entrada por publicador: **64** eventos; fila de saída por consumidor: **64** eventos.
- Sessão inativa: **4 s**; dedup de controle: **20 s** e tamanho máximo configurável.
- Sincronização: snapshot chunked, mas **com limites** de catálogo, chunks pendentes, bytes totais e prazo (estabelecer nos testes; não aceitar snapshot ilimitado).

Os números são **defaults recomendados para demonstrar o projeto**, não valores aprovados na Etapa 1.
