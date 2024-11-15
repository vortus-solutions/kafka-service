'use strict';

const { CompressionTypes, logLevel, Partitioners } = require('kafkajs');

const DEFAULT_CONFIG = {
    kafka: {
        clientId: 'default-kafka-client',
        brokers: ['localhost:9092'],
        ssl: false,
        sasl: null,
        connectionTimeout: 3000,
        requestTimeout: 30000,
        enforceRequestTimeout: true,
        logLevel: logLevel.INFO,
        retry: {
            initialRetryTime: 300,
            maxRetryTime: 30000,
            retries: 8,
            factor: 0.2,
        },
    },
    producer: {
        createPartitioner: Partitioners.DefaultPartitioner,
        allowAutoTopicCreation: false,
        transactionTimeout: 30000,
        compression: CompressionTypes.GZIP,
        // maxInFlightRequests: 5,
        // idempotent: true,
    },
    consumer: {
        groupId: 'default-consumer-group',
        allowAutoTopicCreation: false,
        maxInFlightRequests: 20,
        maxBytes: 10485760,
        sessionTimeout: 60000,
        heartbeatInterval: 30000,
        maxWaitTimeInMs: 5000,
        autoCommit: true,
        autoCommitInterval: 5000,
        retry: {
            initialRetryTime: 100,
            retries: 8,
            maxRetryTime: 30000,
            factor: 0.2,
        },
    },
};

const ENV_MAPPING = {
    KAFKA_CLIENT_ID: ['kafka', 'clientId'],
    KAFKA_BROKERS: ['kafka', 'brokers', (v) => v.split(',')],
    KAFKA_SSL_ENABLED: ['kafka', 'ssl', (v) => v === 'true'],
    KAFKA_CONNECTION_TIMEOUT: ['kafka', 'connectionTimeout', parseInt],
    KAFKA_REQUEST_TIMEOUT: ['kafka', 'requestTimeout', parseInt],
    KAFKA_MAX_RETRIES: ['kafka', 'retry', 'retries', parseInt],
    KAFKA_INITIAL_RETRY_TIME: ['kafka', 'retry', 'initialRetryTime', parseInt],
    KAFKA_LOG_LEVEL: ['kafka', 'logLevel', (v) => logLevel[v] || logLevel.INFO],

    // KAFKA_SASL_MECHANISM: ['kafka', 'sasl', 'mechanism'],
    // KAFKA_SASL_USERNAME: ['kafka', 'sasl', 'username'],
    // KAFKA_SASL_PASSWORD: ['kafka', 'sasl', 'password'],

    KAFKA_PRODUCER_TRANSACTION_TIMEOUT: ['producer', 'transactionTimeout', parseInt],
    KAFKA_PRODUCER_MAX_IN_FLIGHT: ['producer', 'maxInFlightRequests', parseInt],
    KAFKA_PRODUCER_IDEMPOTENT: ['producer', 'idempotent', (v) => v === 'true'],
    KAFKA_PRODUCER_COMPRESSION: [
        'producer',
        'compression',
        (v) => CompressionTypes[v] || CompressionTypes.GZIP,
    ],

    KAFKA_CONSUMER_GROUP_ID: ['consumer', 'groupId'],
    KAFKA_CONSUMER_MAX_BYTES: ['consumer', 'maxBytes', parseInt],
    KAFKA_CONSUMER_MAX_WAIT_TIME: ['consumer', 'maxWaitTimeInMs', parseInt],
    KAFKA_CONSUMER_SESSION_TIMEOUT: ['consumer', 'sessionTimeout', parseInt],
    KAFKA_CONSUMER_HEARTBEAT_INTERVAL: ['consumer', 'heartbeatInterval', parseInt],
    KAFKA_CONSUMER_AUTO_COMMIT: ['consumer', 'autoCommit', (v) => v === 'true'],
    KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL: ['consumer', 'autoCommitInterval', parseInt],
};

module.exports = {
    DEFAULT_CONFIG,
    ENV_MAPPING,
};