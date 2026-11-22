# Checklist de qualidade da especificação / entrega

## Antes de começar a codificar

- [x] Cenário C e restrições do enunciado constam na spec.
- [x] Dois brokers, UDP, JSON, memória volátil e B1 coordenador mantidos.
- [x] Histórias e requisitos têm IDs e condições verificáveis.
- [x] Protocolo cliente–broker e broker–broker documentado.
- [x] Data model, planejamento e tarefas com dependências.
- [x] Riscos/ambiguidade documentados sem mudança silenciosa.
- [X] Equipe revisou decisões complementares D01–D04 em `docs/DECISOES_E_RISCOS.md`.

## Contratos e testes de implementação

- [x] Operações de controle têm ID, ACK/retry limitado e resposta inconclusiva ao timeout.
- [x] Dados não usam ACK, retransmissão obrigatória, persistência ou dedup obrigatório.
- [x] `NO_SUBSCRIBERS` retorna tentativa de ciência ao publicador.
- [x] Tópico sem assinantes conhecidos descarta publicação.
- [x] Tópico com assinantes gera tentativa de entrega local e remota quando cabível.
- [x] Filas são limitadas, drop-oldest demonstrável e clientes lentos isolados.
- [x] Rate limit por publicador demonstrável em testes com dois participantes.
- [x] B2 não cria/deleta tópicos sem B1.
- [x] B1/B2 com interesse local/replicado e PEER_SYNC após restart.
- [x] Biblioteca recompõe inscrição após failover e não reenvia dados perdidos.
- [x] Sem goroutines ou filas ilimitadas, e `Close` cancela todos os loops.
- [x] Testes de versões, duplicatas, pacotes grandes e UDP inválido.

## Critérios de entrega da Etapa 2

### Itens pendentes

- `go test -race ./...`: não pôde ser executado neste ambiente porque o CGO não encontra o compilador `gcc` no `PATH`.
- Evidência física em máquina limpa não é necessária. Cenários repetíveis nesta máquina passaram após `go clean -cache` e `go mod download`; novas repetições devem registrar resultados e ambiente.

- [x] `go test ./...` aprovado.
- [ ] `go test -race ./...` aprovado.
- [x] Dois brokers rodando em terminais diferentes.
- [x] API da biblioteca importável por projeto Go de exemplo.
- [x] Comandos manuais executados nesta máquina após limpeza do cache: brokers reais, smoke cross-broker, falha/migração do subscriber e carga com B1 indisponível.
- [x] README e documentação da biblioteca correspondem ao código final.
- [x] Exportação de métricas com unidades e parâmetros de carga.
- [x] Limitações (best effort, período de failover, catálogo volátil) explícitas.
