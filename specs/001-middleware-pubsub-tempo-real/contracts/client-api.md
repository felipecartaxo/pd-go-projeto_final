# Contrato planejado da biblioteca Go (`pkg/pubsub`)

> **Design de API**, não afirmação de que o código já existe. O Codex deve implementar e depois atualizar `docs/BIBLIOTECA.md` com assinaturas e exemplos realmente compiláveis.

## 1. Tipos públicos sugeridos

```go
package pubsub

import (
    "context"
    "encoding/json"
    "time"
)

type Config struct {
    Brokers               [2]string // host:porta, B1 e B2
    ClientID              string
    PreferredBroker       int       // 1 ou 2; apenas preferência inicial
    HeartbeatInterval     time.Duration
    HeartbeatTimeout      time.Duration
    ControlRetryInterval  time.Duration
    ControlMaxAttempts    int
    DeliveryQueueCapacity int
    EventQueueCapacity    int
}

type Message struct {
    Topic     string
    MessageID string
    Payload   json.RawMessage
}

type EventType string // NoSubscribers, RateLimited, BrokerChanged, Unavailable, Error

type Event struct {
    Type      EventType
    RequestID string
    MessageID string
    Topic     string
    Code      string
    Err       error
}

type Client struct { /* estado privado e threads protegidas */ }

func NewClient(cfg Config) (*Client, error)
func (c *Client) Connect(ctx context.Context) error
func (c *Client) CreateTopic(ctx context.Context, topic string) error
func (c *Client) DeleteTopic(ctx context.Context, topic string) error
func (c *Client) Subscribe(ctx context.Context, topic string) (<-chan Message, error)
func (c *Client) Unsubscribe(ctx context.Context, topic string) error
func (c *Client) Publish(ctx context.Context, topic string, payload json.RawMessage) (messageID string, err error)
func (c *Client) Events() <-chan Event
func (c *Client) Close() error
```

As assinaturas são sugestões concretas para guiar implementação; eventuais pequenos ajustes idiomáticos devem ser documentados e refletidos em exemplos/testes. Não trocar a semântica sem registrar decisão.

## 2. Contrato de cada método

- `NewClient`: valida configuração, aplica defaults seguros para campos opcionais e cria estado **sem** iniciar I/O ativo antes de `Connect`.
- `Connect`: tenta broker preferido e alternativo; faz `REGISTER`, sobe receive loop e heartbeat. Exige contexto cancelável.
- `CreateTopic/DeleteTopic`: controle com request_id, limite de retry e erro explícito; se timeout sem confirmação, expõe erro `ErrInconclusive` (usável por `errors.Is`).
- `Subscribe`: solicita SUBSCRIBE antes de expor canal ativo; retorna **mesmo canal** se já inscrito, ou comportamento alternativo claramente documentado e testado. Guarda inscrição para failover.
- `Unsubscribe`: solicita remoção e retira inscrição da lista desejada; o canal anterior fecha com segurança. Se timeout, resultado remoto é inconclusivo e a política para lista desejada deve ser documentada (proposta: conservar até confirmação para evitar perda involuntária).
- `Publish`: gera `request_id`/`message_id`, valida JSON/tamanho total e faz **uma tentativa** de envio UDP. Retorna `messageID` após envio local ou erro. **Não confirma entrega nem processamento pelo broker**. As notificações `NO_SUBSCRIBERS`, `RATE_LIMITED` e `ERROR` chegam por `Events()` com correlação. Não retransmitir dados automaticamente, nem na troca de broker.
- `Events()`: canal de notificações assíncronas; `NO_SUBSCRIBERS` deve gerar `ACK` **mesmo se o consumidor da API estiver lento**, antes de entregar ao canal de eventos. O canal de eventos é limitado e não pode travar o loop de recepção. É obrigatório definir contadores/estratégia explícita de overflow; mensagens de controle não podem sumir silenciosamente.
- `Close`: idempotente, cancela retries e loops, fecha UDP e canais de forma segura; testes race detector.

## 3. Erros esperados

- `ErrInconclusive`: esgotado retry sem confirmação de controle; não presume reversão ou sucesso.
- `ErrUnavailable`: nenhum broker acessível dentro do prazo solicitado.
- `ErrClosed`: uso após `Close`.
- `ProtocolError{Code string}`: `TOPIC_NOT_FOUND`, `TOPIC_ALREADY_EXISTS`, `NOT_REGISTERED`, `COORDINATOR_UNAVAILABLE` etc.
- `context.Canceled` / `context.DeadlineExceeded`: controle cancelado; estado remoto pode ser desconhecido.

Sugestão: `var ErrInconclusive = errors.New(...)` e `errors.Is`, com `ProtocolError` tipado para as rejeições explícitas.

## 4. Exemplo pretendido (a validar quando houver implementação)

```go
cfg := pubsub.Config{
    Brokers: [2]string{"127.0.0.1:9001", "127.0.0.1:9002"},
    ClientID: "consumidor-01",
    PreferredBroker: 2,
}
c, err := pubsub.NewClient(cfg)
if err != nil { log.Fatal(err) }
defer c.Close()
if err := c.Connect(ctx); err != nil { log.Fatal(err) }
messages, err := c.Subscribe(ctx, "leilao/lances")
if err != nil { log.Fatal(err) }
for {
    select {
    case m, ok := <-messages:
        if !ok { return }
        fmt.Printf("%s %s %s\n", m.Topic, m.MessageID, m.Payload)
    case ev, ok := <-c.Events():
        if !ok { return }
        log.Printf("evento: %v: %s", ev.Type, ev.Code)
    case <-ctx.Done():
        return
    }
}
```

Para publicar (com `c` já conectado e `ctx` válido):

```go
payload := json.RawMessage(`{"valor":120}`)
id, err := c.Publish(ctx, "leilao/lances", payload)
```

Consulte `docs/BIBLIOTECA.md` após implementar.

## 5. Invariantes/testes de biblioteca

1. Um único leitor UDP separa mensagens `DELIVER`, respostas e eventos por `type`/`request_id`; não há dois leitores concorrentes roubando replies.
2. A biblioteca conserva a lista de inscrições ao trocar brokers; `REGISTER` no destino antecede re-`SUBSCRIBE`.
3. Failover transparente não reenviará publicações já emitidas.
4. `Events()` e canais de tópicos possuem capacidade finita; a biblioteca não deixa o loop UDP bloqueado.
5. Notificações repetidas `NO_SUBSCRIBERS` são ACKadas. Pode haver mais de uma observação do evento, mas recomendado dedup limitado no lado do cliente para o mesmo `request_id`.
6. Contexto/close interrompem loops e timeouts; testes concorrentes e `-race` passam.
