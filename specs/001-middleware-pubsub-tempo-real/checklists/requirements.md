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

- [ ] Operações de controle têm ID, ACK/retry limitado e resposta inconclusiva ao timeout.
- [ ] Dados não usam ACK, retransmissão obrigatória, persistência ou dedup obrigatório.
- [ ] `NO_SUBSCRIBERS` retorna tentativa de ciência ao publicador.
- [ ] Tópico sem assinantes conhecidos descarta publicação.
- [ ] Tópico com assinantes gera tentativa de entrega local e remota quando cabível.
- [ ] Filas são limitadas, drop-oldest demonstrável e clientes lentos isolados.
- [ ] Rate limit por publicador demonstrável em testes com dois participantes.
- [ ] B2 não cria/deleta tópicos sem B1.
- [ ] B1/B2 com interesse local/replicado e PEER_SYNC após restart.
- [ ] Biblioteca recompõe inscrição após failover e não reenvia dados perdidos.
- [ ] Sem goroutines ou filas ilimitadas, e `Close` cancela todos os loops.
- [ ] Testes de versões, duplicatas, pacotes grandes e UDP inválido.

## Critérios de entrega da Etapa 2

- [ ] `go test ./...` aprovado.
- [ ] `go test -race ./...` aprovado.
- [ ] Dois brokers rodando em terminais diferentes.
- [ ] API da biblioteca importável por projeto Go de exemplo.
- [ ] Comandos manuais de cenários testados em máquina limpa.
- [ ] README e documentação da biblioteca correspondem ao código final.
- [ ] Exportação de métricas com unidades e parâmetros de carga.
- [ ] Limitações (best effort, período de failover, catálogo volátil) explícitas.
