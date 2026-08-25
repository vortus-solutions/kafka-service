# Referência operacional para IA — Kafka Service v2.1.9

Este é o contrato operacional de `@vortus-solutions/kafka-service@2.1.9`.
Use-o para gerar ou revisar código. Para migrar de 1.0.2, consulte também
[LIB_UPGRADE.md](LIB_UPGRADE.md).

## Regras de uso

-   Instancie uma vez por processo/fluxo e chame `init()` antes de produzir ou
    consumir.
-   Escolha explicitamente se o processo cria producer, consumer, ou ambos:
    `init(true, false)`, `init(false, true)` ou `init()`.
-   Registre `error` antes de operações assíncronas. Em `EventEmitter`, emitir
    `error` sem listener encerra o fluxo com erro.
-   Finalize com `await kafka.disconnect()` no encerramento do processo.
-   Prioridade de configuração: **defaults → construtor → variáveis de ambiente**.

```js
const KafkaService = require('@vortus-solutions/kafka-service');

const kafka = new KafkaService({
    kafka: { clientId: 'billing', brokers: ['broker-1:9092'] },
    producer: { timeout: 10000 },
});

kafka.on('error', ({ type, error, timestamp }) => {
    console.error({ type, error, timestamp });
});

await kafka.init(true, false);
try {
    await kafka.send('billing-events', [{ key: 'invoice-42', value: 'paid' }]);
} finally {
    await kafka.disconnect();
}
```

## Configuração do construtor

```js
new KafkaService({ kafka, producer, consumer });
```

### `kafka`

| Campo                    |                Default | Finalidade                                                       |
| ------------------------ | ---------------------: | ---------------------------------------------------------------- |
| `clientId`               | `default-kafka-client` | Identificador do cliente.                                        |
| `brokers`                |   `['localhost:9092']` | Lista de brokers `host:porta`.                                   |
| `ssl`                    |                `false` | Habilita TLS na API KafkaJS compatível.                          |
| `sasl`                   |                ausente | Objeto de autenticação KafkaJS; `null` é removido com segurança. |
| `connectionTimeout`      |                 `3000` | Timeout de conexão, em ms.                                       |
| `requestTimeout`         |                `30000` | Timeout de requisição, em ms.                                    |
| `logLevel`               |        `logLevel.INFO` | Nível de log do cliente Confluent.                               |
| `retry.initialRetryTime` |                  `300` | Espera inicial entre tentativas, em ms.                          |
| `retry.maxRetryTime`     |                `30000` | Espera máxima entre tentativas, em ms.                           |
| `retry.retries`          |                    `8` | Quantidade de tentativas.                                        |
| `rdKafka`                |                   `{}` | Opções nativas do `librdkafka`; veja a seção específica.         |

Exemplo SASL/TLS. Não use credenciais em variáveis de ambiente documentadas pela
lib: as variáveis SASL não fazem parte da interface pública.

```js
const kafka = new KafkaService({
    kafka: {
        brokers: ['cluster.example.com:9092'],
        ssl: true,
        sasl: {
            mechanism: 'plain',
            username: process.env.KAFKA_USERNAME,
            password: process.env.KAFKA_PASSWORD,
        },
    },
});
```

### `producer`

| Campo                                           |                 Default | Finalidade                                                    |
| ----------------------------------------------- | ----------------------: | ------------------------------------------------------------- |
| `allowAutoTopicCreation`                        |                 `false` | Permite criação automática de tópico.                         |
| `transactionTimeout`                            |                 `30000` | Timeout transacional, em ms.                                  |
| `compression`                                   | `CompressionTypes.GZIP` | Codec aplicado a todos os envios.                             |
| `timeout`                                       |                 `30000` | Timeout do request de produção, em ms.                        |
| `acks`                                          |            não definido | Confirmações exigidas pelo broker; configure por producer.    |
| `maxInFlightRequests`                           |            não definido | Suportado por configuração ou `KAFKA_PRODUCER_MAX_IN_FLIGHT`. |
| `idempotent`                                    |            não definido | Suportado por configuração ou `KAFKA_PRODUCER_IDEMPOTENT`.    |
| `transactionalId`                               |            não definido | Habilita producer transacional no cliente subjacente.         |
| `metadataMaxAge`, `retry`, `logLevel`, `logger` |           não definidos | Opções KafkaJS compatíveis repassadas ao producer.            |
| `rdKafka`                                       |                    `{}` | Opções nativas específicas do producer.                       |

Use `CompressionTypes`, e não strings, ao definir compressão no construtor:

```js
const { CompressionTypes } = require('@confluentinc/kafka-javascript').KafkaJS;

const kafka = new KafkaService({
    producer: {
        compression: CompressionTypes.ZSTD,
        idempotent: true,
        maxInFlightRequests: 5,
    },
});
```

Codecs disponíveis: `NONE`, `GZIP`, `SNAPPY`, `LZ4` e `ZSTD`. A compressão é
global ao producer: `send()` e `sendBatch()` não aceitam compressão ou timeout
por chamada.

### `consumer`

| Campo                                      |                  Default | Finalidade                                                     |
| ------------------------------------------ | -----------------------: | -------------------------------------------------------------- |
| `groupId`                                  | `default-consumer-group` | Grupo de consumidores. Defina um valor próprio em produção.    |
| `allowAutoTopicCreation`                   |                  `false` | Permite criação automática de tópico.                          |
| `maxInFlightRequests`                      |                     `20` | Máximo de requisições em voo.                                  |
| `metadataMaxAge`, `rebalanceTimeout`       |            não definidos | Idade de metadata e tempo máximo de rebalanceamento.           |
| `maxBytes`                                 |               `10485760` | Máximo de bytes por fetch.                                     |
| `maxBytesPerPartition`, `minBytes`         |            não definidos | Limites de fetch por partição e resposta mínima.               |
| `sessionTimeout`                           |                  `60000` | Tempo de sessão, em ms.                                        |
| `heartbeatInterval`                        |                  `30000` | Intervalo de heartbeat, em ms.                                 |
| `maxWaitTimeInMs`                          |                   `5000` | Espera máxima do broker para completar fetch.                  |
| `fromBeginning`                            |                  `false` | Lê do início somente quando não há offset válido para o grupo. |
| `autoCommit`                               |                   `true` | Habilita commit periódico automático.                          |
| `autoCommitInterval`                       |                   `5000` | Intervalo do commit automático, em ms.                         |
| `readUncommitted`, `rackId`                |            não definidos | Leitura de transações pendentes e afinidade de rack.           |
| `partitionAssigners`, `partitionAssignors` |            não definidos | Assignors: `range`, `roundRobin`, `cooperativeSticky`.         |
| `logLevel`, `logger`                       |            não definidos | Opções KafkaJS compatíveis repassadas ao consumer.             |
| `retry.initialRetryTime`                   |                    `100` | Espera inicial entre tentativas, em ms.                        |
| `retry.maxRetryTime`                       |                  `30000` | Espera máxima entre tentativas, em ms.                         |
| `retry.retries`                            |                      `8` | Quantidade de tentativas.                                      |
| `rdKafka`                                  |                     `{}` | Opções nativas específicas do consumer.                        |

`fromBeginning` é uma propriedade do consumer, portanto deve ser definida antes
de `init()`. Não a use como mecanismo de reprocessamento contínuo: offsets já
commitados pelo `groupId` têm precedência.

### Campos KafkaJS adicionais

O wrapper faz merge profundo e encaminha os campos não nativos de `kafka`,
`producer` e `consumer` ao bloco `kafkaJS` do Confluent. Além das tabelas acima,
o escopo `kafka` aceita `authenticationTimeout`, `logger` e `retry`. Evite
`enforceRequestTimeout`: ele é uma opção herdada do KafkaJS e não deve ser usada
para definir a política de timeout neste cliente.

Para novas opções, use a documentação e os tipos da mesma versão instalada de
`@confluentinc/kafka-javascript`. Não invente campos: a biblioteca rejeita
configurações incompatíveis durante a inicialização.

### Variáveis de ambiente suportadas

| Variável                              | Destino                        | Conversão                                                       |
| ------------------------------------- | ------------------------------ | --------------------------------------------------------------- |
| `KAFKA_CLIENT_ID`                     | `kafka.clientId`               | texto                                                           |
| `KAFKA_BROKERS`                       | `kafka.brokers`                | separados por vírgula                                           |
| `KAFKA_SSL_ENABLED`                   | `kafka.ssl`                    | `true` ativa; outro valor desativa                              |
| `KAFKA_CONNECTION_TIMEOUT`            | `kafka.connectionTimeout`      | inteiro                                                         |
| `KAFKA_REQUEST_TIMEOUT`               | `kafka.requestTimeout`         | inteiro                                                         |
| `KAFKA_MAX_RETRIES`                   | `kafka.retry.retries`          | inteiro                                                         |
| `KAFKA_INITIAL_RETRY_TIME`            | `kafka.retry.initialRetryTime` | inteiro                                                         |
| `KAFKA_LOG_LEVEL`                     | `kafka.logLevel`               | nome do `logLevel`; inválido vira `INFO`                        |
| `KAFKA_PRODUCER_TRANSACTION_TIMEOUT`  | `producer.transactionTimeout`  | inteiro                                                         |
| `KAFKA_PRODUCER_MAX_IN_FLIGHT`        | `producer.maxInFlightRequests` | inteiro                                                         |
| `KAFKA_PRODUCER_IDEMPOTENT`           | `producer.idempotent`          | `true` ativa                                                    |
| `KAFKA_PRODUCER_COMPRESSION`          | `producer.compression`         | `NONE`, `GZIP`, `SNAPPY`, `LZ4` ou `ZSTD`; inválido vira `GZIP` |
| `KAFKA_CONSUMER_GROUP_ID`             | `consumer.groupId`             | texto                                                           |
| `KAFKA_CONSUMER_MAX_BYTES`            | `consumer.maxBytes`            | inteiro                                                         |
| `KAFKA_CONSUMER_MAX_WAIT_TIME`        | `consumer.maxWaitTimeInMs`     | inteiro                                                         |
| `KAFKA_CONSUMER_SESSION_TIMEOUT`      | `consumer.sessionTimeout`      | inteiro                                                         |
| `KAFKA_CONSUMER_HEARTBEAT_INTERVAL`   | `consumer.heartbeatInterval`   | inteiro                                                         |
| `KAFKA_CONSUMER_AUTO_COMMIT`          | `consumer.autoCommit`          | `true` ativa                                                    |
| `KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL` | `consumer.autoCommitInterval`  | inteiro                                                         |

Não há variáveis públicas para SASL, `fromBeginning`, `rdKafka` ou tamanho de
batch. Configure-os no construtor.

### Opções nativas `rdKafka`

O wrapper envia os campos normais dentro de `kafkaJS` e somente o objeto
`rdKafka` ao nível nativo. Use esse escape hatch para opções do `librdkafka`.

```js
const kafka = new KafkaService({
    kafka: {
        rdKafka: { 'ssl.ca.location': '/run/secrets/ca.pem' },
    },
    producer: {
        rdKafka: { 'queue.buffering.max.ms': 10 },
    },
    consumer: {
        rdKafka: {
            'js.consumer.max.batch.size': 100,
            'js.consumer.max.cache.size.per.worker.ms': 1500,
        },
    },
});
```

Valide cada opção nativa contra a versão instalada de
`@confluentinc/kafka-javascript`; chaves inválidas podem falhar durante
`init()`.

## Producer

### Envio individual ou múltiplas mensagens para um tópico

```js
await kafka.send('orders', [
    { key: 'order-42', value: JSON.stringify({ status: 'created' }) },
    { key: 'order-43', value: 'created', headers: { source: 'checkout' } },
]);
```

`send(topic, messages)` encaminha `topic` e `messages` ao producer. Em caso de
sucesso, incrementa `getHealth().messagesSent` pela quantidade de mensagens. O
terceiro argumento é ignorado; não o use para timeout, acks ou compressão. Se
ele for passado, a lib emite um `console.warn` uma única vez por método
(`send` e `sendBatch` avisam separadamente) e segue com a configuração global do
producer.

### Envio em batch para vários tópicos

```js
await kafka.sendBatch([
    {
        topic: 'orders',
        messages: [{ key: 'order-42', value: 'created' }],
    },
    {
        topic: 'audit',
        messages: [{ value: JSON.stringify({ event: 'order.created' }) }],
    },
]);
```

`sendBatch(batchMessages)` recebe `[{ topic, messages }]`, encaminha como
`topicMessages` e soma todas as mensagens em `messagesSent`. O batching físico é
feito pelo `librdkafka`; o método não cria uma transação. Um segundo argumento é
ignorado e gera o mesmo `console.warn` descrito em `send()`.

## Consumer

### Assinatura

```js
await kafka.consumerSubscribe({ topics: ['orders', 'audit'] });
```

Chame somente após `init(false, true)` ou `init()`. A assinatura aceita
`topics` (strings ou expressões regulares compatíveis com Confluent) e `replace`.
Use `fromBeginning` exclusivamente no construtor; se ainda for passado na
assinatura, deve coincidir com a configuração do consumer.

### Consumo individual

```js
await kafka.consumeEach(async ({ topic, partition, message, heartbeat, pause }) => {
    const value = message.value?.toString();
    const source = message.headers?.source?.toString();
    await processEvent({ topic, partition, offset: message.offset, value, source });
});
```

O callback recebe `topic`, `partition`, `message`, `heartbeat` e `pause`. O
contador `messagesReceived` sobe somente quando o callback termina sem lançar.
Uma exceção no callback emite `message_processing_error` e preserva a falha para
o consumer.

### Coleta e processamento em batch

```js
await kafka.consumeBatch(async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
    for (const message of batch.messages) {
        if (!isRunning() || isStale()) return;
        await processEvent(message);
        resolveOffset(message.offset);
    }
    await heartbeat();
});
```

O callback recebe um batch lógico, não necessariamente o mesmo lote recebido do
broker. O cliente Confluent limita esse batch a 32 mensagens por padrão; ajuste
`consumer.rdKafka['js.consumer.max.batch.size']` se necessário. O wrapper fixa
`eachBatchAutoResolve: true`, portanto uma conclusão bem-sucedida resolve o
batch inteiro. `resolveOffset()` é útil para marcar progresso antes de uma falha,
mas não substitui commit quando `autoCommit` está desligado.

## Offsets: automático, manual e seek

### Commit automático

É o default: `consumer.autoCommit: true` e intervalo de 5 segundos. O commit
ocorre após processamento bem-sucedido e pode ocorrer também ao desconectar ou
rebalancear. Use-o quando reprocessar uma mensagem após falha é aceitável.

### Commit manual

O `KafkaService` não possui método `commitOffsets`; o consumer criado fica
disponível como `kafka.consumer` depois de `init(false, true)`. Desligue o
commit automático no construtor e faça o commit somente após persistir o efeito
da mensagem.

```js
const kafka = new KafkaService({
    consumer: { groupId: 'billing-workers', autoCommit: false },
});

kafka.on('error', onKafkaError);
await kafka.init(false, true);
await kafka.consumerSubscribe({ topics: ['billing'] });

await kafka.consumeEach(async ({ topic, partition, message }) => {
    await persistDurably(message);

    await kafka.consumer.commitOffsets([
        {
            topic,
            partition,
            // Kafka recebe o próximo offset a ler, não o offset já processado.
            offset: (BigInt(message.offset) + 1n).toString(),
        },
    ]);
});
```

`await kafka.consumer.commitOffsets()` sem argumentos comita os offsets já
processados/conhecidos pelo consumer. Use commit explícito por partição quando a
aplicação controla a durabilidade. Não converta offsets com `Number`, pois eles
podem ultrapassar o limite seguro de inteiros JavaScript.

### Ajustar posição com `seek`

`seek` também é uma operação do consumer subjacente. Use offsets como strings.
O valor `'-2'` representa o menor offset disponível no broker.

```js
await kafka.init(false, true);
await kafka.consumerSubscribe({ topics: ['orders'] });

// Reprocessar a partição 0 a partir do offset 1200.
kafka.consumer.seek({ topic: 'orders', partition: 0, offset: '1200' });

// Ou reprocessar a partir do menor offset ainda disponível.
kafka.consumer.seek({ topic: 'orders', partition: 0, offset: '-2' });
```

Em uma partição atribuída, o ajuste é imediato; em uma não atribuída, o cliente
o aplica quando houver atribuição. Com `autoCommit: true`, fazer `seek` também
comita o offset buscado. Desligue auto commit quando essa alteração não deve ser
persistida para o grupo e trate o commit de forma explícita.

## Eventos, erros e saúde

| Evento                                 | Quando ocorre                                          |
| -------------------------------------- | ------------------------------------------------------ |
| `producer.connected`, `producer.ready` | Producer conectado em `init()`.                        |
| `consumer.connected`, `consumer.ready` | Consumer conectado em `init()`.                        |
| `consumer.subscribed`                  | Assinatura concluída.                                  |
| `ready`                                | Todos os clientes solicitados por `init()` conectaram. |
| `disconnected`                         | `disconnect()` terminou.                               |
| `error`                                | Erro tipado do wrapper.                                |

Tipos de erro possíveis incluem `client_creation_error`, `producer_creation_error`,
`consumer_creation_error`, `initialization_error`, `message_send_error`,
`batch_send_error`, `subscription_error`, `message_processing_error`,
`batch_processing_error`, `message_consumption_error`, `batch_consumption_error`
e `disconnect_error`.

Não há evento de crash do consumer. O cliente Confluent não expõe listeners de
instrumentação, portanto uma parada do loop de consumo não gera `error`. Use
`getHealth().partitionsAssigned` para detectar esse estado.

```js
kafka.on('error', ({ type, error, timestamp }) => {
    logger.error({ type, err: error, timestamp }, 'KafkaService failure');
});

const health = kafka.getHealth();
// { connected, messagesSent, messagesReceived, partitionsAssigned,
//   lastProducerError, lastConsumerError, timestamp }
```

`lastProducerError` só é preenchido por tipos iniciados por `producer`; o mesmo
vale para `lastConsumerError` e tipos iniciados por `consumer`.

### Campos de `getHealth()`

| Campo                | Tipo             | Significado                                                     |
| -------------------- | ---------------- | --------------------------------------------------------------- |
| `connected`          | boolean          | `true` após `init()`; volta a `false` apenas em `disconnect()`. |
| `messagesSent`       | number           | Mensagens enviadas com sucesso por `send()` e `sendBatch()`.    |
| `messagesReceived`   | number           | Mensagens cujo callback de consumo terminou sem lançar.         |
| `partitionsAssigned` | number ou `null` | Partições atribuídas ao consumer no instante da chamada.        |
| `lastProducerError`  | objeto ou `null` | Último `error` com `type` iniciado por `producer`.              |
| `lastConsumerError`  | objeto ou `null` | Último `error` com `type` iniciado por `consumer`.              |
| `timestamp`          | string ISO       | Momento da chamada a `getHealth()`.                             |

`connected` reflete o ciclo de vida explícito do wrapper, não a saúde do
consumo: ele continua `true` mesmo se o consumer parar de receber partições.

`partitionsAssigned` (a partir da v2.1.9) lê `consumer.assignment().length` a
cada chamada. Vale `null` quando o processo não criou consumer (`init(true,
false)`) ou quando o consumer ainda não está conectado — `null` significa
"desconhecido", não "zero". Em operação normal o valor fica estável; ele cai a
`0` apenas durante um rebalance, por poucos milissegundos.

```js
// Health check de um serviço consumidor.
app.get('/health', (req, res) => {
    const { connected, partitionsAssigned } = kafka.getHealth();
    const ok = connected && partitionsAssigned !== 0;
    res.status(ok ? 200 : 503).json(kafka.getHealth());
});
```

Um único `partitionsAssigned === 0` pode ser um rebalance em curso. Para
alarmar ou reiniciar, exija leituras consecutivas em zero em vez de agir na
primeira.

## Limites do wrapper

-   Não há API wrapper para `commitOffsets`, `seek`, `pause` ou `resume`; use
    `kafka.consumer` depois de `init()` e trate isso como uma integração
    avançada com o cliente Confluent. De `assignment()` o wrapper expõe apenas a
    contagem, em `getHealth().partitionsAssigned`; para saber quais tópicos e
    partições, chame `kafka.consumer.assignment()` diretamente.
-   `consumeBatch()` não permite trocar `eachBatchAutoResolve` nem configurar
    concorrência por chamada. Para esses controles, use diretamente
    `kafka.consumer.run(...)`; isso não atualiza os contadores de saúde do wrapper.
-   Não há API wrapper para transações, admin client ou schema registry. Use o
    cliente Confluent diretamente quando essas capacidades forem necessárias.
