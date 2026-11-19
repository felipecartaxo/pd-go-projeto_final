# Biblioteca Go do middleware — documentação prevista

> **Estado da implementação:** `pkg/pubsub` implementa a API descrita abaixo: um leitor UDP por cliente ativo, retry limitado de controle, eventos e failover. `Publish` faz uma única tentativa e não é reenviado no failover.

> **Status:** API pública implementada; exemplos usam o módulo declarado em `go.mod`.

## Para que serve

A biblioteca `pkg/pubsub` simplifica o uso do middleware Pub/Sub UDP. A aplicação fornece dois endereços fixos de broker e um ID de cliente. A biblioteca gerencia `REGISTER`, HEARTBEAT, correlação de operações de controle, inscrições e troca de broker em falhas. A aplicação publica eventos em tópicos e recebe eventos com `MessageID` e `Payload` JSON.

Ela **não** promete entrega de todas as publicações, ordenação, deduplicação ou replay. Uma publicação pode ser descartada por rate limit, sem inscritos, fila saturada ou perda de UDP. Controle usa retry limitado, então um timeout pode gerar resultado inconclusivo.

## Instalação

O módulo Go é `github.com/felipecartaxo/pd-go-projeto_final`. O import é:

```go
import "github.com/felipecartaxo/pd-go-projeto_final/pkg/pubsub"
```

O pacote expõe `Client`, `Config`, `Message`, `Event`, erros e os métodos documentados neste guia.

## Uso esperado

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "time"

    "github.com/felipecartaxo/pd-go-projeto_final/pkg/pubsub"
)

func main() {
    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()

    client, err := pubsub.NewClient(pubsub.Config{
        Brokers:         [2]string{"127.0.0.1:9001", "127.0.0.1:9002"},
        ClientID:        "cliente-1",
        PreferredBroker: 1,
    })
    if err != nil { log.Fatal(err) }
    defer client.Close()

    if err := client.Connect(ctx); err != nil { log.Fatal(err) }
    // CREATE_TOPIC normalmente é operado pelo admin/CLI; se já existir,
    // tratar TOPIC_ALREADY_EXISTS conforme a API tipada.
    // _ = client.CreateTopic(ctx, "leilao/lances")

    id, err := client.Publish(ctx, "leilao/lances", json.RawMessage(`{"valor":120}`))
    if err != nil { log.Fatal(err) }
    fmt.Println("Envio local tentado; message_id:", id)
}
```

**Importante:** `Publish` não é prova de recebimento pelos assinantes; observar notificações assíncronas via `Events()` para `NO_SUBSCRIBERS`, `RATE_LIMITED` e erros. O exemplo de consumidor está no contrato `client-api.md`.

## Semântica da API prevista

| Operação | Comportamento |
|---|---|
| `Connect(ctx)` | Escolhe um broker e registra sessão lógica. Se ambos inacessíveis, erro/timeout. |
| `CreateTopic(ctx, name)` | Pede criação sob coordenação de B1; aguarda confirmação de controle ou resultado inconclusivo. |
| `DeleteTopic(ctx, name)` | Exclui tópico, inscrições e pendências locais/replicadas conforme sincronização. |
| `Subscribe(ctx, name)` | Registra inscrição e recebe canal de mensagens; biblioteca guarda inscrição para failover. |
| `Unsubscribe(ctx, name)` | Remove inscrição; resultados incertos em timeout devem ser explicados. |
| `Publish(ctx, name, payload)` | Uma tentativa UDP, retorna message_id; notificações por Events; sem retransmissão de dados. |
| `Events()` | Canal assíncrono de eventos de status/erro/no subscribers/rate limited. |
| `Close()` | Libera socket/goroutines e recursos. |

## Limites e recomendações de uso

- `payload` deve ser JSON válido; mensagem toda precisa caber em **1200 bytes** por default, incluídos metadados.
- Use IDs de clientes distintos em processos concorrentes. Mesmo ID com diferentes origens não oferece autenticação de produção.
- Inscrição restaurada não reproduz mensagens perdidas durante o failover.
- Um assinante lento pode perder mensagens antigas quando sua fila encher.
- Para testar, use duas instâncias locais e `cmd/demo`; a integração `cmd/loadgen` mede fan-out, rajadas e fairness.

## Checklist antes de entrega

- [ ] Import correto foi substituído pelo nome do módulo real.
- [ ] Código de exemplo compila e funciona em dois terminais.
- [ ] Assinaturas, códigos de erro, flags e defaults batem com o código.
- [ ] Existe exemplo de consumidor e eventos assíncronos completos/compiláveis.
- [ ] Há explicação verificável de retry de controle vs best effort de dados.
