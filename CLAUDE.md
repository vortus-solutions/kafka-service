# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
npm run build      # Compile src/ → lib/ via Babel
npm run lint       # ESLint
npm run format     # Prettier
npm test           # Jest
```

## Architecture

This is a Node.js npm library (`@vortus-solutions/kafka-service`) that wraps [@confluentinc/kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript) (KafkaJS compatibility layer) with opinionated defaults, health monitoring, and environment-variable-based config.

**Source:** `src/` (compiled to `lib/` for publishing — never edit `lib/` directly)

### Two core files

- **`src/index.js`** — `KafkaService extends EventEmitter`. Manages producer/consumer lifecycle, message counting, health state, and typed error events.
- **`src/kafkaConfig.js`** — Three-layer config merging: *defaults → user-provided constructor arg → ENV vars* (ENV vars win). Maps `KAFKA_*` env vars onto nested kafkajs config objects.

### Config priority

ENV vars override everything. The mapping lives in `kafkaConfig.js` as a flat `envMapping` object that navigates nested config paths via dot notation.

### Public API surface

```js
await kafka.init(createProducer?, createConsumer?)
await kafka.send(topic, messages)          // extra args ignored → console.warn
await kafka.sendBatch(batchMessages)       // extra args ignored → console.warn
await kafka.consumerSubscribe(opts)
await kafka.consumeEach(callback)
await kafka.consumeBatch(callback)
await kafka.disconnect()
kafka.getHealth()  // → { connected, messagesSent, messagesReceived, partitionsAssigned, lastProducerError, lastConsumerError, timestamp }
```

Events emitted: `ready`, `disconnected`, `error`, `producer.connected`, `consumer.connected`.

### Notable defaults

- GZIP compression on all sends
- Consumer auto-commit every 5 s, offset reset to `latest`
- 8 retries with exponential backoff (300 ms initial, 30 s max)
- Health object updated on every send/receive and on errors

### Examples

`examples/example1.js` shows a full producer + consumer + health-monitoring + graceful shutdown flow.
