# Kafka Service Client

A robust Kafka service client for Node.js applications with built-in error handling, retries, and health monitoring.

## Table of Contents

-   [Installation](#installation)
-   [Usage](#usage)
-   [Configuration](#configuration)
-   [API Reference](#api-reference)
    -   [KafkaService](#kafkaservice)
    -   [Methods](#methods)
-   [Error Handling](#error-handling)
-   [Health Monitoring](#health-monitoring)
-   [Testing](#testing)
-   [Contributing](#contributing)
-   [License](#license)

## Installation

To install the Kafka Service Client, run the following command:

bash
npm install kafka-service-client

## Usage

Here’s a basic example of how to use the Kafka Service Client:

javascript
const KafkaService = require('kafka-service-client');

// Create an instance with custom configuration
const kafka = new KafkaService({
kafka: {
clientId: 'my-app',
brokers: ['localhost:9092']
}
});

// Initialize the service
(async () => {
try {
await kafka.init();

        // Produce messages
        await kafka.send('my-topic', [
        {
            key: 'key1',
            value: JSON.stringify({ message: 'Hello World' })
        }
        ]);

        // Consume messages
        await kafka.consumerSubscribe({ topic: 'my-topic' });
        await kafka.consumeEach(async ({ topic, partition, message }) => {
            console.log(`Received message: ${message.value.toString()}`);
        });
    } catch (error) {
        console.error('Error initializing Kafka service:', error);
    }

})();

## Configuration

The service comes with sensible defaults that can be overridden. You can configure the Kafka service using the following options:

### Default Configuration

javascript
const config = {
kafka: {
clientId: 'default-kafka-client',
brokers: ['localhost:9092'],
ssl: false,
sasl: null,
connectionTimeout: 3000,
requestTimeout: 30000,
enforceRequestTimeout: true,
maxInFlightRequests: 10,
retry: {
initialRetryTime: 300,
maxRetryTime: 30000,
retries: 8,
factor: 0.2,
},
logLevel: 'INFO'
},
producer: {
allowAutoTopicCreation: false,
transactionTimeout: 30000,
maxInFlightRequests: 5,
idempotent: true,
compression: 'GZIP',
batchSize: 16384,
acks: -1, // all
timeout: 30000
},
consumer: {
groupId: 'default-consumer-group',
allowAutoTopicCreation: false,
maxInFlightRequests: 20,
sessionTimeout: 60000,
heartbeatInterval: 3000,
maxBytes: 10485760, // 10MB
maxWaitTimeInMs: 5000,
retry: {
initialRetryTime: 100,
maxRetryTime: 30000,
retries: 8,
factor: 0.2,
},
autoCommit: true,
autoCommitInterval: 5000,
autoOffsetReset: 'latest'
}
};

### Environment Variables

You can configure the service using environment variables:

-   `KAFKA_CLIENT_ID`: Kafka client ID
-   `KAFKA_BROKERS`: Comma-separated list of brokers
-   `KAFKA_SSL_ENABLED`: Enable SSL (true/false)
-   `KAFKA_CONNECTION_TIMEOUT`: Connection timeout in milliseconds
-   `KAFKA_REQUEST_TIMEOUT`: Request timeout in milliseconds
-   `KAFKA_MAX_RETRIES`: Maximum number of retries
-   `KAFKA_INITIAL_RETRY_TIME`: Initial retry time in milliseconds
-   `KAFKA_LOG_LEVEL`: Log level (e.g., INFO, DEBUG)

## API Reference

### KafkaService

The main class for interacting with Kafka.

#### Constructor

javascript
const kafka = new KafkaService(userConfig);

-   `userConfig` (Object): Custom configuration to override defaults.

### Methods

#### `init(createProducer = true, createConsumer = true)`

Initializes the Kafka service.

-   `createProducer` (boolean): Whether to create a producer (default: true).
-   `createConsumer` (boolean): Whether to create a consumer (default: true).

Returns a promise that resolves when the service is ready.

#### `send(topic, messages, options)`

Sends messages to a Kafka topic.

-   `topic` (string): The topic to send messages to.
-   `messages` (Array): An array of message objects, each containing `key` and `value`.
-   `options` (Object): Optional settings, including `timeout`.

Returns a promise that resolves with the result of the send operation.

#### `consumerSubscribe(opts)`

Subscribes the consumer to a topic.

-   `opts` (Object): Options for subscription, including `topic`.

Returns a promise that resolves when the subscription is successful.

#### `consumeEach(callback)`

Processes each message received by the consumer.

-   `callback` (function): A function that takes an object with `topic`, `partition`, and `message`.

Returns a promise that resolves when the consumer is running.

#### `getHealth()`

Returns the health status of the service.

Returns an object containing health metrics, including connection status and message counts.

#### `disconnect()`

Disconnects the producer and consumer.

Returns a promise that resolves when the disconnection is complete.

## Error Handling

The service includes comprehensive error handling and retry mechanisms. You can listen for error events:

javascript
kafka.on('error', ({ type, error }) => {
console.error(Kafka error: ${type}, error);
});

## Health Monitoring

Monitor the service health using the `getHealth()` method:

javascript
const health = kafka.getHealth();
console.log(health);

The health object includes:

-   `connected`: Boolean indicating if the service is connected.
-   `lastProducerError`: Details of the last producer error.
-   `lastConsumerError`: Details of the last consumer error.
-   `messagesSent`: Total messages sent.
-   `messagesReceived`: Total messages received.
-   `timestamp`: The current timestamp.

## Testing

To run tests, ensure you have Jest installed and run:

bash
npm test

You can add your tests in the `__tests__` directory. The tests should follow the naming convention `*.test.js`.

## Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository.
2. Create your feature branch (`git checkout -b feature/amazing-feature`).
3. Commit your changes (`git commit -m 'Add amazing feature'`).
4. Push to the branch (`git push origin feature/amazing-feature`).
5. Open a Pull Request.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

For more information, visit the [GitHub repository](https://github.com/yourusername/kafka-service-client).
