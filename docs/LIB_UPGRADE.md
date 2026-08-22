# Playbook de migração para IA: v1.0.2 → v2.0.7

Use este documento para migrar uma aplicação que usa
`@vortus-solutions/kafka-service@1.0.2` para `2.0.7`. O baseline legado é o
commit `9b5627181c114946fc624adf555e7e7de28b1176`.

Para todos os parâmetros, envio, consumo, compressão e offsets da v2.0.7, use
a [referência operacional para IA](API_REFERENCE.md).

## Instruções para o agente

1. Não altere a semântica de tópicos, `groupId`, autenticação ou garantias de
   entrega sem evidência no código da aplicação.
2. Faça as transformações desta página somente quando o padrão correspondente
   existir. Preserve configurações não relacionadas.
3. Configure o consumidor antes de `init()`. Não tente reconfigurá-lo depois
   da conexão.
4. Ao terminar, execute os testes da aplicação e valide produção com um
   producer e consumer no mesmo tópico/grupo de homologação.

## Compatibilidade de runtime

| Item          | v1.0.2           | v2.0.7                                   |
| ------------- | ---------------- | ---------------------------------------- |
| Node.js       | `>=14`           | `>=18`                                   |
| Cliente Kafka | `kafkajs ^2.2.4` | `@confluentinc/kafka-javascript ^1.10.0` |
| Implementação | JavaScript       | bindings nativos do `librdkafka`         |

O cliente Confluent fornece binários pré-compilados para plataformas suportadas.
Em uma plataforma fora da matriz suportada, o ambiente de build precisa das
ferramentas de compilação nativa.

## Transformações obrigatórias

### 1. Remover timeout por envio

Na v1.0.2, `send()` e `sendBatch()` aceitavam um timeout por chamada. Na v2.0.7,
argumentos extras são ignorados pelo JavaScript: o timeout efetivo é o de
`producer.timeout`.

```js
// v1.0.2
await kafka.send('orders', messages, { timeout: 5000 });
await kafka.sendBatch(batchMessages, { timeout: 10000 });

// v2.0.7
const kafka = new KafkaService({ producer: { timeout: 5000 } });
await kafka.send('orders', messages);
await kafka.sendBatch(batchMessages);
```

Procure por chamadas de três argumentos a `send` e de dois argumentos a
`sendBatch`. Não converta um timeout variável por mensagem para um valor global
sem confirmar que essa perda de granularidade é aceitável.

### 2. Mover `fromBeginning` para a configuração do consumidor

O backend Confluent não aceita `fromBeginning` em `consumer.subscribe()`. A
v2.0.7 remove o campo antes de encaminhar a chamada, porém, se ele for informado
na assinatura, seu valor precisa ser igual a `consumer.fromBeginning` configurado
antes de `init()`.

```js
// v1.0.2
await kafka.consumerSubscribe({ topics: ['orders'], fromBeginning: true });

// v2.0.7
const kafka = new KafkaService({
    consumer: { fromBeginning: true },
});
await kafka.init(false, true);
await kafka.consumerSubscribe({ topics: ['orders'] });
```

O default é `false`. Para reduzir risco, remova `fromBeginning` da chamada de
`consumerSubscribe()` depois de movê-lo para o construtor.

### 3. Substituir listeners de desconexão específicos

Os eventos `producer.disconnected` e `consumer.disconnected` eram derivados de
listeners do KafkaJS na v1.0.2. Eles não existem na v2.0.7. Use o evento único
`disconnected`, emitido após `KafkaService.disconnect()` concluir.

```js
// Remover
kafka.on('producer.disconnected', onDisconnect);
kafka.on('consumer.disconnected', onDisconnect);

// Usar
kafka.on('disconnected', onDisconnect);
```

`producer.connected` e `consumer.connected` continuam disponíveis e são emitidos
após cada conexão bem-sucedida durante `init()`.

### 4. Tornar acesso a headers tolerante a ausência

O Confluent pode entregar `message.headers` como `undefined` ou `null`. Não
acesse uma chave de header sem proteção.

```js
// Inseguro na v2.0.7
message.headers['trace-id'].toString();

// Seguro
const traceId = message.headers?.['trace-id']?.toString();
```

## Configuração: mapa de conversão

| Configuração v1.0.2                            | Ação na v2.0.7                                                                                                   |
| ---------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| `kafka.sasl: null`                             | Pode manter. A lib remove esse valor antes de criar o cliente.                                                   |
| `kafka.enforceRequestTimeout`                  | Remover; é exclusivo do KafkaJS.                                                                                 |
| `kafka.retry.factor` e `consumer.retry.factor` | Remover; não são usados pelo Confluent.                                                                          |
| `producer.createPartitioner`                   | Remover; o `librdkafka` escolhe o particionamento.                                                               |
| `send(..., { timeout })`                       | Mover para `producer.timeout`.                                                                                   |
| `consumer.run({ autoCommit })`                 | Configurar `consumer.autoCommit` no construtor. `consumeEach()` e `consumeBatch()` já não encaminham essa opção. |
| Configuração nativa do librdkafka              | Colocar em `rdKafka` no escopo correto.                                                                          |

Exemplo de configuração nativa. As chaves dentro de `rdKafka` não são KafkaJS:

```js
const kafka = new KafkaService({
    kafka: {
        rdKafka: { 'ssl.ca.location': '/run/secrets/ca.pem' },
    },
    producer: {
        rdKafka: { 'queue.buffering.max.ms': 10 },
    },
    consumer: {
        rdKafka: { 'enable.partition.eof': true },
    },
});
```

## O que preservar

Não reescreva chamadas que já usam estas APIs; elas continuam com a mesma forma:

```js
new KafkaService(config);
await kafka.init(createProducer, createConsumer);
await kafka.send(topic, messages);
await kafka.sendBatch(batchMessages);
await kafka.consumerSubscribe({ topics });
await kafka.consumeEach(callback);
await kafka.consumeBatch(callback);
await kafka.disconnect();
kafka.getHealth();
```

Também permanecem: precedência de variáveis `KAFKA_*` sobre o construtor,
contadores de saúde, `ready`, `error`, `disconnected`, `producer.ready`,
`consumer.ready` e `consumer.subscribed`.

## Armadilhas a evitar

-   Não passe configurações nativas diretamente em `kafka`, `producer` ou
    `consumer`; use `rdKafka`.
-   Não suponha que o terceiro argumento de `send()` ainda controla timeout. Ele
    não produz erro, mas também não altera o comportamento.
-   Não habilite `fromBeginning: true` sem verificar o impacto de reprocessar o
    histórico do tópico para aquele `groupId`.
-   Não trate a ausência de `producer.disconnected` como falha de conexão; o
    evento não é emitido na v2.0.7.
-   Não use as variáveis de ambiente SASL comentadas no código como interface
    pública. Passe `kafka.sasl` no construtor até que a aplicação tenha uma
    configuração própria e validada para credenciais.

## Checklist executável

-   [ ] Atualizar Node.js para 18 ou superior.
-   [ ] Atualizar a dependência para `@vortus-solutions/kafka-service@2.0.7`.
-   [ ] Buscar `send(` com opções de timeout e mover o valor necessário para
        `producer.timeout`.
-   [ ] Buscar `sendBatch(` com opções de timeout e fazer a mesma migração.
-   [ ] Buscar `consumerSubscribe(` com `fromBeginning`; mover o valor para
        `consumer.fromBeginning` antes de `init()`.
-   [ ] Buscar listeners de `producer.disconnected` e `consumer.disconnected`;
        trocar por `disconnected` quando o objetivo for observar o desligamento
        explícito do serviço.
-   [ ] Buscar acessos a `message.headers[...]`; torná-los opcionais.
-   [ ] Remover `createPartitioner`, `enforceRequestTimeout` e `retry.factor` de
        configurações herdadas ou de usos de `KafkaService.DEFAULT_CONFIG`.
-   [ ] Mover opções nativas para `rdKafka`.
-   [ ] Executar os testes da aplicação e um smoke test real de produzir, consumir
        e desconectar com as credenciais e o cluster de homologação.
