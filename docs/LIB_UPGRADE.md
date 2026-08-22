# Guia de Upgrade: v1.0.2 → v2.x

Este documento lista todas as mudancas que podem impactar aplicacoes que atualizam de `@vortus-solutions/kafka-service@1.0.2` (kafkajs) para a versao 2.x (@confluentinc/kafka-javascript).

---

## Requisitos

| Item | v1.0.2 | v2.x |
|------|--------|------|
| Node.js | >= 14 | **>= 18** |
| Dependencia Kafka | kafkajs ^2.2.4 | @confluentinc/kafka-javascript ^1.0.0 |

> A lib Confluent usa bindings nativos (librdkafka). Certifique-se de que o ambiente de build suporta compilacao nativa (gcc, make, python).

---

## Breaking Changes

### 1. `send()` — parametro `timeout` removido

**Antes (v1.0.2):**
```js
await kafka.send('topic', messages, { timeout: 5000 });
```

**Agora (v2.x):**
```js
await kafka.send('topic', messages);
```

O terceiro argumento ainda pode ser passado sem causar erro (JS ignora args extras), mas o `timeout` **nao tem mais efeito per-call**. O timeout agora e definido globalmente no config do producer (`config.producer.timeout`, default 30000ms).

**Acao necessaria:** Se voce usa timeout customizado por chamada, mova para o config do producer:
```js
const kafka = new KafkaService({
    producer: { timeout: 5000 }
});
```

---

### 2. `sendBatch()` — parametro `timeout` removido

Mesma situacao do `send()`. O segundo argumento `{ timeout }` nao existe mais.

**Antes:**
```js
await kafka.sendBatch(batchMessages, { timeout: 10000 });
```

**Agora:**
```js
await kafka.sendBatch(batchMessages);
```

**Acao necessaria:** Mesma do item 1 — mover timeout para `config.producer.timeout`.

---

### 3. `consumerSubscribe()` — `fromBeginning` configurado no consumer

**Antes (v1.0.2):**
```js
await kafka.consumerSubscribe({
    topics: ['my-topic'],
    fromBeginning: true
});
```

**Agora (v2.x):**
O Confluent nao aceita `fromBeginning` no `subscribe()`. A lib remove o campo antes de encaminhar a assinatura, mas ele deve ser igual ao valor configurado no consumer antes de `init()`.

**Acao necessaria:** Mover `fromBeginning` para o config do consumer na criacao do KafkaService:
```js
const kafka = new KafkaService({
    consumer: { fromBeginning: true }
});
```

> **Nota:** `fromBeginning: false` continua sendo o default. Para `true`, configure `consumer.fromBeginning: true` e mantenha o mesmo valor em `consumerSubscribe()`.

---

### 4. Eventos `producer.disconnected` e `consumer.disconnected` nao sao mais emitidos

**Antes (v1.0.2):**
```js
kafka.on('producer.disconnected', () => console.log('Producer desconectou'));
kafka.on('consumer.disconnected', () => console.log('Consumer desconectou'));
```

**Agora (v2.x):**
Esses eventos nunca disparam. A lib Confluent nao expoe `.on()` nos objetos producer/consumer como o kafkajs fazia.

**Acao necessaria:** Usar o evento `disconnected` do KafkaService (que continua funcionando):
```js
kafka.on('disconnected', () => console.log('Kafka desconectou'));
```

---

### 5. `compression` nao e mais configuravel per-call

**Antes (v1.0.2):**
A compressao GZIP era aplicada em cada chamada `send()` e `sendBatch()` internamente.

**Agora (v2.x):**
A compressao e definida uma vez no config do producer (default: GZIP). Nao ha mudanca se voce usava o default, mas se dependia de comportamento diferente por chamada, nao e mais possivel.

---

### 6. Config `sasl: null` tratado automaticamente

**Antes (v1.0.2):**
O kafkajs aceitava `sasl: null` sem problemas.

**Agora (v2.x):**
O Confluent tenta ler `sasl.mechanism` mesmo quando `sasl` e `null`, causando crash. A lib agora remove `sasl` automaticamente quando for `null` ou `undefined`, entao **nao e necessaria nenhuma acao**. Listado aqui apenas para conhecimento.

---

### 7. `autoCommit` nao e mais aceito em `consumer.run()`

**Antes (v1.0.2):**
O kafkajs aceitava `autoCommit` como opcao de `consumer.run()`:
```js
await consumer.run({ autoCommit: true, eachMessage: ... });
```

**Agora (v2.x):**
O Confluent rejeita `autoCommit` no `run()` com erro `ERR__INVALID_ARG`. Essa propriedade deve ser passada no config do consumer na criacao.

**Acao necessaria:** Nenhuma — a lib ja trata isso internamente. O `autoCommit` e configurado no consumer config (default: `true`) e foi removido das chamadas `consumeEach()` e `consumeBatch()`. Listado aqui para conhecimento caso voce interaja diretamente com o consumer.

---

### 8. Mensagens podem chegar sem `headers`

**Antes (v1.0.2):**
O kafkajs sempre incluia `headers` como objeto nas mensagens, mesmo que vazio (`{}`).

**Agora (v2.x):**
O Confluent pode entregar mensagens com `headers` como `undefined` ou `null`.

**Acao necessaria:** Verificar se o seu consumer acessa headers sem checagem previa:
```js
// ANTES — pode dar TypeError no v2.x
if (message.headers['gtw-simulator'].toString() === 'true') { ... }

// DEPOIS — seguro
if (message.headers && message.headers['gtw-simulator']
    && message.headers['gtw-simulator'].toString() === 'true') { ... }
```

---

### 9. Configs removidos do default (kafkajs-only)

Os seguintes configs existiam no default da v1.0.2 e foram removidos por serem exclusivos do kafkajs:

| Config removido | Onde estava | Motivo |
|---|---|---|
| `createPartitioner: Partitioners.DefaultPartitioner` | producer | librdkafka gerencia particionamento nativamente |
| `enforceRequestTimeout: true` | kafka | nao existe no Confluent |
| `retry.factor: 0.2` | kafka.retry, consumer.retry | nao existe no Confluent |

**Acao necessaria:** Nenhuma, a menos que voce referenciava esses valores via `KafkaService.DEFAULT_CONFIG`.

---

## O que continua funcionando sem alteracao

- `new KafkaService(config)` — mesma API de construcao
- `kafka.init(createProducer, createConsumer)` — mesma assinatura
- `kafka.send(topic, messages)` — funciona (sem terceiro arg)
- `kafka.sendBatch(batchMessages)` — funciona (sem segundo arg)
- `kafka.consumerSubscribe({ topics: [...] })` — funciona (sem `fromBeginning`)
- `kafka.consumeEach(callback)` — mesma assinatura de callback
- `kafka.consumeBatch(callback)` — mesma assinatura de callback
- `kafka.disconnect()` — mesmo comportamento
- `kafka.getHealth()` — mesmo retorno
- Eventos: `ready`, `error`, `disconnected`, `producer.connected`, `producer.ready`, `consumer.connected`, `consumer.ready`, `consumer.subscribed`
- Todas as env vars (`KAFKA_BROKERS`, `KAFKA_CLIENT_ID`, `KAFKA_CONSUMER_GROUP_ID`, etc.)
- Config override por env var continua tendo prioridade sobre config do constructor
- Configuracoes nativas podem ser passadas em `rdKafka` dentro de `kafka`, `producer` ou `consumer`

---

## Checklist de upgrade

- [ ] Node.js >= 18 no ambiente de deploy
- [ ] Ambiente suporta compilacao nativa (librdkafka)
- [ ] Buscar por `send(` com terceiro argumento `{ timeout }` — mover para config.producer
- [ ] Buscar por `sendBatch(` com segundo argumento `{ timeout }` — mover para config.producer
- [ ] Buscar por `fromBeginning` em chamadas `consumerSubscribe()` — mover para config.consumer
- [ ] Buscar por listeners de `producer.disconnected` e `consumer.disconnected` — substituir por `disconnected`
- [ ] Buscar por referencias a `KafkaService.DEFAULT_CONFIG` que usem `createPartitioner`, `enforceRequestTimeout` ou `retry.factor`
- [ ] Mover configuracoes exclusivas do librdkafka para `rdKafka`, por exemplo `producer: { rdKafka: { 'queue.buffering.max.ms': 10 } }`
- [ ] Rodar testes da aplicacao
