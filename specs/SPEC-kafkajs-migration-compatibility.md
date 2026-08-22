# SPEC — KafkaJS migration compatibility

**Status:** draft

## Goal

Keep the package’s KafkaJS-facing API usable while exposing the minimum native librdkafka configuration required by the Confluent backend.

## Behavior

- `rdKafka` may be supplied under `kafka`, `producer`, or `consumer`; its keys are passed outside the matching `kafkaJS` block.
- Existing KafkaJS options remain in their current locations and are still passed in `kafkaJS`.
- `consumerSubscribe({ fromBeginning })` removes that unsupported per-subscription option. `consumer.fromBeginning` remains the supported creation-time setting. If both are supplied with different values, subscription rejects with a clear error because the backend cannot change reset behavior after connection.
- Environment mappings are not modified while applying values, so repeated instances receive identical parsed config.

## API / Interface contracts

```js
new KafkaService({
  kafka: { rdKafka: { 'ssl.ca.location': '/path/ca.pem' } },
  producer: { rdKafka: { 'queue.buffering.max.ms': 10 } },
  consumer: { rdKafka: { 'enable.partition.eof': true } },
});
```

`rdKafka` is not forwarded inside `kafkaJS`.

## Implementation Outline

- Split each config scope into `kafkaJS` and `rdKafka` in `src/index.js` immediately before constructing the Confluent clients.
- Normalize `fromBeginning` in `consumerSubscribe()`.
- Avoid mutation in `_applyEnvVariables()`.
- Add focused mocked tests, repair the lint failure, and update `docs/LIB_UPGRADE.md`.

## Acceptance criteria

- [ ] Native values reach each Confluent constructor outside `kafkaJS`.
- [ ] Existing KafkaJS config remains under `kafkaJS`.
- [ ] `consumerSubscribe({ fromBeginning: true })` no longer forwards that field and reports conflicts with `consumer.fromBeginning`.
- [ ] Two service instances with the same transformed environment variable get the same config.
- [ ] Tests and lint pass.
