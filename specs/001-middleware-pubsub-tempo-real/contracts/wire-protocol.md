# Contrato v1 — Protocolo Pub/Sub JSON sobre UDP

> **Contrato de implementação proposto a partir da Etapa 1.** Tipos de mensagens e campos principais vêm da Etapa 1; limites, versões, campos de correlação adicionais e códigos de erro abaixo detalham pontos em aberto. Mudanças substantivas devem ser registradas em `docs/DECISOES_E_RISCOS.md`.

## 1. Transporte e envelope

- UDP entre biblioteca/broker e entre os brokers, com endereços fixos.
- **Cada mensagem = 1 datagrama UDP** com **um JSON compacto** (UTF-8), sem delimitador de linha.
- Limite recomendado `max_datagram_bytes = 1200` para o **JSON serializado completo**. Sem fragmentação em nível da aplicação para `PUBLISH`/`DELIVER`; mensagem maior deve ser rejeitada.
- Objeto sempre tem `v` (inteiro `1`) e `type` (string enum abaixo). Campos não listados podem ser ignorados para compatibilidade, mas campos obrigatórios faltantes implicam `INVALID_MESSAGE` quando puder ser respondido.
- IDs são strings não vazias (até 64 caracteres recomendado), `topic` é string UTF-8 com tamanho limitado (até 64 bytes recomendado), `payload` é **qualquer valor JSON válido**. Todas as medidas contam no limite total de 1200 B.
- Mensagens desconhecidas ou `v!=1` recebem `ERROR` quando correlação/origem forem confiáveis; caso contrário são descartadas e contabilizadas.

### 1.1 Campos

| Campo | Uso |
|---|---|
| `v` | versão do protocolo; sempre 1 |
| `type` | operação/envelope |
| `request_id` | identificador da solicitação/controle e da resposta correspondente |
| `client_id` | identificação lógica da sessão e da origem de eventos de cliente |
| `topic` | nome do tópico |
| `payload` | valor JSON da publicação, apenas em PUBLISH/DELIVER/FORWARD_PUBLICATION |
| `message_id` | ID único da publicação, usado para rastrear perdas/duplicações, sem retransmissão de dados |
| `broker_id` | ID 1 ou 2 do emissor de mensagens internas; não prova identidade isoladamente |
| `code` | código de erro/notificação |
| `reply` | booleano opcional para marcar resposta de heartbeat |
| `action` | `create` ou `delete` para pedido/atualização de catálogo |
| `catalog_epoch`, `catalog_revision` | versão de catálogo, permitindo identificar restart e alterações |
| `interest_version`, `peer_epoch`, `present` | controle de atualização de interesse remoto |
| `sync_id`, `scope`, `part`, `total` | identificação e paginação do PEER_SYNC |
| `status` | em ACK interno: `OK` ou `ERROR`; resultado de controle, não entrega |

### 1.2 Regras gerais de correlação

- O cliente gera **novo `request_id`** para cada operação de controle; retries reutilizam o ID.
- Publicações também possuem `request_id` (para correlacionar `NO_SUBSCRIBERS`) e `message_id`, mas **não** recebem `OK` para confirmar entrega.
- O `ACK` de `NO_SUBSCRIBERS` reutiliza o mesmo `request_id` da publicação original e leva `client_id`. `ACK` interno é distinguido pela origem interbroker e carrega status de controle.
- Brokers validam origem UDP de mensagens internas contra o endereço do peer configurado, além do `broker_id`. A verificação não é autenticação criptográfica; escopo acadêmico/local.
- Resposta de controle deve ser emitida ao **endereço UDP de origem registrado** da sessão, não a um endereço arbitrário declarado pelo cliente.

## 2. Operações cliente ↔ broker

| Tipo | Campos obrigatórios (além de `v`, `type`) | Comportamento e resposta |
|---|---|---|
| `REGISTER` | `request_id`, `client_id` | Cria/renova sessão vinculada à origem; `OK` ou `ERROR` |
| `CREATE_TOPIC` | `request_id`, `client_id`, `topic` | B1 coordena; B2 encaminha; `OK` ou `ERROR` (timeout local pode resultar inconclusivo) |
| `DELETE_TOPIC` | mesmos de CREATE | B1 coordena e invalida assinaturas e filas daquele tópico; `OK` ou `ERROR` |
| `SUBSCRIBE` | `request_id`, `client_id`, `topic` | Guarda inscrição local; interesse remoto é propagado assincronamente; `OK` ou `ERROR` |
| `UNSUBSCRIBE` | idem | Remove inscrição local; `OK` ou `ERROR` |
| `PUBLISH` | `request_id`, `client_id`, `topic`, `message_id`, `payload` | Sem ACK de sucesso; entrega se houver inscritos conhecidos; pode enviar `NO_SUBSCRIBERS`, `RATE_LIMITED` ou `ERROR` |
| `DELIVER` | `client_id`, `topic`, `message_id`, `payload` | Broker→assinante, **sem ACK** |
| `HEARTBEAT` | `request_id`, `client_id` | Requisição/resposta com `reply=false|true`; monitora sessão/broker |
| `NO_SUBSCRIBERS` | `request_id`, `client_id`, `topic`, `message_id` | Broker→publicador; correlaciona publicação descartada; aguarda ACK e faz retry limitado |
| `RATE_LIMITED` | `request_id`, `client_id`, `topic`, `message_id` | Notificação best effort do descarte por taxa; sem retry obrigatório |
| `OK` / `ERROR` | `request_id`, `client_id`, opcional `code` | Responde solicitação de controle ou notifica erro de publicação; `OK` não é confirmação de `DELIVER` |
| `ACK` | `request_id`, `client_id` | Cliente→broker para `NO_SUBSCRIBERS`; também existe ACK interno entre brokers |

**Política local:** todas as operações exceto `REGISTER` requerem sessão ativa com `client_id` compatível com endereço UDP observado. Em caso de sessão inválida: `ERROR code=NOT_REGISTERED`. `SUBSCRIBE` repetido na mesma sessão/tópico é idempotente. `UNSUBSCRIBE` de inscrição ausente responde OK (estado final já alcançado). `CREATE_TOPIC` com nome já existente por solicitação **distinta** responde `TOPIC_ALREADY_EXISTS`; `DELETE_TOPIC` inexistente responde `TOPIC_NOT_FOUND`.

### 2.1 Exemplos

Registro:

```json
{"v":1,"type":"REGISTER","request_id":"r-101","client_id":"pub-7"}
```

Confirmação de controle:

```json
{"v":1,"type":"OK","request_id":"r-101","client_id":"pub-7"}
```

Publicação (JSON válido em `payload`):

```json
{"v":1,"type":"PUBLISH","request_id":"r-102","client_id":"pub-7","topic":"leilao/lances","message_id":"msg-9283","payload":{"valor":120,"lote":3}}
```

Entrega sem ACK:

```json
{"v":1,"type":"DELIVER","client_id":"sub-4","topic":"leilao/lances","message_id":"msg-9283","payload":{"valor":120,"lote":3}}
```

Notificação de ausência de inscritos (repetir até ACK ou limite):

```json
{"v":1,"type":"NO_SUBSCRIBERS","request_id":"r-102","client_id":"pub-7","topic":"leilao/lances","message_id":"msg-9283"}
```

ACK do publicador:

```json
{"v":1,"type":"ACK","request_id":"r-102","client_id":"pub-7"}
```

Excedente de taxa:

```json
{"v":1,"type":"RATE_LIMITED","request_id":"r-102","client_id":"pub-7","topic":"leilao/lances","message_id":"msg-9283"}
```

## 3. Operações broker ↔ broker

| Tipo | Origem/uso | Confirmação |
|---|---|---|
| `TOPIC_REQUEST` | B2→B1, `action` create/delete, tópico e requisição interna correlacionável | B1 devolve `ACK status=OK|ERROR`, com `code` se erro; B2 mapeia para resposta ao cliente original |
| `TOPIC_UPDATE` | B1→B2, `action`, `topic`, `catalog_epoch`, `catalog_revision` | ACK; B2 aplica somente revision seguinte dentro do epoch; lacuna pede sync |
| `INTEREST_UPDATE` | origem→peer, `topic`, `present`, `peer_epoch`, `interest_version` | ACK; atualização stale/duplicada não reverte interesse mais recente |
| `FORWARD_PUBLICATION` | broker→peer, inclui `topic`, `payload`, `message_id`, `broker_id` | **Sem ACK**; destinatário entrega apenas localmente, sem retornar para peer |
| `PEER_SYNC` | solicitação e chunks de snapshot de catálogo/interesses em retorno/reconciliação | ACK por parte; snapshot só é aplicado completo, dentro de limites |
| `PEER_HEARTBEAT` | broker↔broker, checa disponibilidade/epoch | Resposta correlacionada (`reply=true`) |
| `ACK` | resposta entre brokers à mensagem de controle | `request_id`, `broker_id`, `status`, opcional `code` |

### 3.1 Semântica de interesse remoto

- `INTEREST_UPDATE present=true` quando primeiro inscrito local surge; `false` quando último sai/expira (inclusive por deleção de tópico).
- Versão e epoch impedem retorno a estado antigo após reorder/retry. Atualização assíncrona pode sofrer atraso, portanto mapa remoto é **conhecimento eventualmente consistente**, não verdade instantânea.
- Se peer estiver indisponível, broker não faz `FORWARD_PUBLICATION` para ele. No retorno, `PEER_SYNC` reconstrói interesses.
- Publicação originada em A só é encaminhada a B quando há interesse remoto conhecido em B e B está ativo. **B nunca reenvia publicação recebida por FORWARD_PUBLICATION**.

### 3.2 Versão de catálogo

- B1 aplica create/delete e incrementa `catalog_revision`; gera `TOPIC_UPDATE` para B2 e tenta confirmá-lo. `OK` ao cliente significa **commit local pelo coordenador**, não garantia de que B2 já recebeu. Falta de ACK dispara reconciliação. **Ver D03**: semântica proposta para validação pela equipe.
- B2 rejeita update de epoch/revision anterior, confirma repetição idempotentemente e solicita snapshot se houver lacuna.
- B2 encaminha pedido de alteração ao B1. Se B1 está indisponível, `ERROR COORDINATOR_UNAVAILABLE`; se pedido enviado mas resposta não vem após tentativas, a biblioteca recebe **INCONCLUSIVE**, pois B1 pode ter aplicado.
- Em B1 indisponível, B2 aceita apenas `REGISTER`, `SUBSCRIBE`, `UNSUBSCRIBE`, `PUBLISH` para tópicos que conhece e heartbeats. Criação/exclusão não migra para B2.

### 3.3 `PEER_SYNC` fragmentado por mensagem (não por publicação)

Como snapshot de tópicos pode exceder 1200 B, **o protocolo de sincronização de controle** divide o conteúdo em *múltiplos envelopes*, cada um em um datagrama UDP válido. **Não** fazer isso com publicações.

Fases conceituais:

1. Peer envia `PEER_SYNC` com `scope="catalog"|"interest"`, `action="request"`, `request_id`.
2. Origem congela snapshot de uma versão e envia chunks `PEER_SYNC` com `action="chunk"`, `sync_id`, `scope`, `part` (base 0), `total`, versão/epoch, `items` e `request_id` próprio. `items` contém nomes de tópicos ou interesses por tópico, conforme `scope`.
3. Destino confirma cada chunk com `ACK`, acumula em estado temporário **limitado**, rejeita inconsistência de versões e expiração. Após todos os chunks, troca a réplica/visão de interesse **atomicamente**.
4. Destino envia `PEER_SYNC action="complete"` com `sync_id` e ACK correlacionado. Se faltarem partes, pede sincronização novamente.
5. Se mudanças ocorreram durante a captura, versões futuras de `TOPIC_UPDATE/INTEREST_UPDATE` são aplicadas em ordem ou novo snapshot é solicitado.

**Regra de recuperação proposta (D02):** B2 reiniciado restaura catálogo de B1. B1 reiniciado sem catálogo e B2 ativo restaura a última cópia conhecida de B2 antes de retomar mutações. Se ambos perderam memória, inicia-se catálogo vazio (sem alta disponibilidade absoluta).

## 4. Controle, idempotência e retries

| Operação | ACK/retry | Limite |
|---|---|---|
| `REGISTER`, `CREATE_TOPIC`, `DELETE_TOPIC`, `SUBSCRIBE`, `UNSUBSCRIBE` | `OK`/`ERROR`; cliente reenvia mesma `request_id` | 3 envios totais propostos |
| `TOPIC_REQUEST`, `TOPIC_UPDATE`, `INTEREST_UPDATE`, chunks de `PEER_SYNC` | ACK interno + retry da mesma solicitação | 3 envios totais propostos |
| `NO_SUBSCRIBERS` | ACK do publicador; retry da notificação | 3 envios totais propostos |
| `PUBLISH`, `DELIVER`, `FORWARD_PUBLICATION`, `RATE_LIMITED` | **Sem retry** de dados/notificação de taxa | 1 tentativa (best effort) |
| `HEARTBEAT`, `PEER_HEARTBEAT` | reply correlacionado; detector trabalha com múltiplos ticks | sem bloquear caminho de dados |

Cache de controle idempotente, por chave com tipo/origem/`request_id`, TTL e capacidade. Resultado do controle **não** comprova recebimento por cada assinante. Após timeout de operação, resultado local `INCONCLUSIVE`, não `ERROR` de falha confirmada. Para `NO_SUBSCRIBERS`, se cliente não recebe aviso por perda contínua de UDP, ele **não pode saber com certeza** se havia inscritos; explicitado no contrato da API.

## 5. Erros/códigos e tratamento

| `code` | Situação |
|---|---|
| `INVALID_MESSAGE` | JSON inválido, campo ausente, versão inválida, enum inválido |
| `DATAGRAM_TOO_LARGE` | tamanho acima do limite; se decodificação for inviável, descartar + métrica |
| `NOT_REGISTERED` | cliente sem sessão válida para operação |
| `TOPIC_NOT_FOUND` | referência a tópico inexistente |
| `TOPIC_ALREADY_EXISTS` | create distinto para tópico já existente |
| `COORDINATOR_UNAVAILABLE` | B1 inacessível ao B2 para mutação |
| `RATE_LIMITED` | excesso do rate limit do publicador |
| `CONTROL_BUSY` | fila/cache/limite de controle atingido, sem descarte silencioso |
| `SYNC_REQUIRED` | epoch ou revision de catálogo/interesse inconsistente; pedir snapshot |
| `INTERNAL_ERROR` | falha inesperada não recuperável no processamento |

`INCONCLUSIVE` é **resultado local da biblioteca**, não um erro de protocolo emitido por broker. Para operações com `ctx` cancelado, devolver cancelamento do contexto sem declarar sucesso ou falha remotos.

## 6. Ordem de validação (testável)

1. Verificar tamanho total recebido e JSON válido (`v/type`).
2. Para mensagem interna, verificar broker de origem no endereço configurado.
3. Para controle de cliente, validar identidade lógica e origem (exceto REGISTER).
4. Validar campos e tópico, checar rate limit quando for `PUBLISH`.
5. Para publicação válida, consultar assinantes locais e interesse remoto ativo conhecido.
6. Sem assinantes: descartar, registrar contador e emitir `NO_SUBSCRIBERS` correlacionado.
7. Havendo assinantes: enfileirar para assinantes locais (drop-oldest em suas filas) e enviar ao peer interessado uma única vez, sem aguardar ACK de consumidores.
8. Nunca manter lock de estado compartilhado durante envio UDP de grande fan-out.

## 7. Testes obrigatórios do contrato

- Serialização/deserialização e limite exato em bytes, com `payload` Unicode/JSON aninhado.
- `PUBLISH` sem resposta positiva; `DELIVER` sem ACK; `FORWARD_PUBLICATION` sem loop.
- `NO_SUBSCRIBERS` com ACK, retry, ACK perdido e resultado não garantido após timeout.
- Duplicatas de `CREATE_TOPIC`, `SUBSCRIBE`, `TOPIC_REQUEST`, `TOPIC_UPDATE` idempotentes na janela.
- `TOPIC_UPDATE` out-of-order, interest update fora de ordem, perda de chunk PEER_SYNC e rebuild.
- B2 read-only para mutações quando B1 caiu; B1 reiniciado sincronizando com B2 vivo.
- Rejeição de versões/tópicos/sessões inválidas e pacotes acima de 1200 B.
