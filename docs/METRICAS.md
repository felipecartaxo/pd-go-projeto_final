# Métricas e avaliação (instrumentadas na Etapa 2, analisar na Etapa 3)

## O que medir

`broker.Server.Metrics()` retorna contadores de processamento/tentativa. `cmd/loadgen` exporta observações locais em CSV/JSON (`scenario`, `run_id`, `started_at`, `duration_ms`, `publishers`, `subscribers`, `topic`, `offered`, `sent`, `received`, `rate_limited`, `go_version`, `goos`, `goarch`, `cpu_count`, `gomaxprocs` e `note`); `sent` só significa tentativa UDP local e `received` é observação no assinante, nunca prova de entrega completa. Cada execução recebe um `run_id` automático quando não informado por `-run-id`.

| Métrica | Definição | Unidade / origem |
|---|---|---|
| `publish_offered_total` | Chamadas de `Publish` iniciadas pela carga | mensagens/cliente |
| `publish_sent_total` | Datagramas PUBLISH enviados localmente pela biblioteca | mensagens/cliente; **não** entrega |
| `publish_accepted_total` | PUBLISH aceitos pelo broker após validação/rate limit | mensagens/broker |
| `rate_limited_total` | PUBLISH rejeitados por limite individual | mensagens/broker e publicador |
| `no_subscribers_total` | PUBLISH descartados sem assinantes conhecidos | mensagens/broker |
| `queue_drop_oldest_total` | mensagens transitórias antigas expulsas por fila | mensagens/broker, tipo de fila |
| `deliver_attempted_total` | tentativas de envio DELIVER para clientes | datagramas/broker |
| `deliver_received_total` | DELIVER efetivamente observado no cliente | datagramas/assinante |
| `forward_attempted_total` | FORWARD_PUBLICATION tentados | datagramas/broker |
| `latency_ms_p50/p95/p99` | tempo entre criação da publicação e observação no receptor sob hipótese de relógio coerente | milissegundos |
| `recovery_ms` | intervalo entre falha induzida/último evento e primeiro DELIVER após re-inscrição | milissegundos |
| `client_broker_switch_total` | trocas de broker feitas pela biblioteca | trocas/cliente |
| `peer_sync_duration_ms` | duração da sincronização completa de catálogo/interesses | milissegundos |
| `active_sessions`, `active_topics` | gauges de sessões e tópicos conhecidos | quantidade |

## Cálculos recomendados

- **Taxa de recebimento observada** = mensagens distintas recebidas pelo assinante / mensagens publicadas **aplicáveis** no intervalo. Com assinaturas alteradas, não usar denominador global sem ajustar período.
- **Taxa de perda observada** = 1 − taxa de recebimento (com caveat de duplicates e início/fim de teste).
- **Throughput recebido** = mensagens efetivamente recebidas / duração em segundos.
- **Deterioração de p95 sob agressor** = p95(B com A) / p95(B baseline), se baseline > 0; reportar valores absolutos também.
- **Fairness complementar:** throughput de B e drop counters quando A publica bursts; não simplificar a um único índice sem evidências.

## Alvos experimentais sugeridos (não exigência da Etapa 1)

- No **localhost** e em carga leve, observar p95 de entrega baixo (por exemplo, **abaixo de 50 ms** como hipótese de experimento), a ajustar conforme hardware/Go/JSON.
- Com B1 inativo, buscar failover na **ordem de poucos segundos** com heartbeat de 500 ms e timeout de 1800 ms; testar e relatar resultados em vez de prometer máximo.
- Com um publicador agressivo, preservar fluxo do publicador saudável e demonstrar contabilização dos descartes. Não estabelecer limiar de fairness sem baseline.

Metas acima são **pontos de partida para avaliação acadêmica**; se testes falharem, registrar condições e justificar mudanças.

## Formato mínimo para CSV/JSON

CSV `results.csv` (sugestão):

```csv
scenario,run_id,started_at,broker,client_id,topic,offered,sent,received,rate_limited,queue_dropped,p50_ms,p95_ms,p99_ms,recovery_ms
fairness,exp-001,2026-11-18T12:00:00-03:00,1,normal-01,leilao/lances,3000,3000,2890,0,20,3.2,8.9,15.0,
```

Os números da linha são **meramente ilustrativos**; nunca apresentá-los como medição real. Registrar parâmetros da execução em `run.json` para reprodução.

## Integridade da medição

- `message_id` mede unicidade e duplicatas, mas não torna entrega confiável.
- Envio UDP bem-sucedido apenas significa que o sistema operacional aceitou tentativa local.
- Latência one-way entre hosts requer relógios sincronizados; sem isso, use medições dentro do mesmo host ou reporte aproximação.
- Relatar intervalos de aquecimento, número de rodadas, percentis por assinante, e perda de observações.
- Métricas de `NO_SUBSCRIBERS` e `RATE_LIMITED` não equivalem a falha da rede; são resultados esperados de política.
