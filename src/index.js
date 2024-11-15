'use strict';

const { Kafka, CompressionTypes, logLevel } = require('kafkajs');
const EventEmitter = require('events');

class KafkaService extends EventEmitter {
    // Static default configuration
    static DEFAULT_CONFIG = {
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
            logLevel: logLevel.INFO,
        },
        producer: {
            allowAutoTopicCreation: false,
            transactionTimeout: 30000,
            maxInFlightRequests: 5,
            idempotent: true,
            compression: CompressionTypes.GZIP,
            batchSize: 16384,
            acks: -1, // all
            timeout: 30000,
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
            autoOffsetReset: 'latest',
        },
    };

    // Environment variable mapping
    static ENV_MAPPING = {
        // Kafka options
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

        // Producer options
        KAFKA_PRODUCER_TRANSACTION_TIMEOUT: ['producer', 'transactionTimeout', parseInt],
        KAFKA_PRODUCER_MAX_IN_FLIGHT: ['producer', 'maxInFlightRequests', parseInt],
        KAFKA_PRODUCER_IDEMPOTENT: ['producer', 'idempotent', (v) => v === 'true'],
        KAFKA_PRODUCER_COMPRESSION: [
            'producer',
            'compression',
            (v) => CompressionTypes[v] || CompressionTypes.GZIP,
        ],
        KAFKA_PRODUCER_BATCH_SIZE: ['producer', 'batchSize', parseInt],
        KAFKA_PRODUCER_ACKS: ['producer', 'acks', parseInt],

        // Consumer options
        KAFKA_CONSUMER_GROUP_ID: ['consumer', 'groupId'],
        KAFKA_CONSUMER_MAX_BYTES: ['consumer', 'maxBytes', parseInt],
        KAFKA_CONSUMER_MAX_WAIT_TIME: ['consumer', 'maxWaitTimeInMs', parseInt],
        KAFKA_CONSUMER_SESSION_TIMEOUT: ['consumer', 'sessionTimeout', parseInt],
        KAFKA_CONSUMER_HEARTBEAT_INTERVAL: ['consumer', 'heartbeatInterval', parseInt],
        KAFKA_CONSUMER_AUTO_COMMIT: ['consumer', 'autoCommit', (v) => v === 'true'],
        KAFKA_CONSUMER_AUTO_COMMIT_INTERVAL: ['consumer', 'autoCommitInterval', parseInt],
        KAFKA_CONSUMER_AUTO_OFFSET_RESET: ['consumer', 'autoOffsetReset'],
    };

    constructor(userConfig = {}) {
        super();
        this.config = this._buildConfig(userConfig);
        this.health = {
            connected: false,
            lastProducerError: null,
            lastConsumerError: null,
            messagesSent: 0,
            messagesReceived: 0,
        };
    }

    _buildConfig(userConfig) {
        // Start with default config
        const config = JSON.parse(JSON.stringify(KafkaService.DEFAULT_CONFIG));

        // Apply environment variables
        this._applyEnvVariables(config);

        // Apply user config (overrides both defaults and env vars)
        this._mergeConfigs(config, userConfig);

        return config;
    }

    _applyEnvVariables(config) {
        for (const [envVar, mapping] of Object.entries(KafkaService.ENV_MAPPING)) {
            const value = process.env[envVar];
            if (value !== undefined) {
                let target = config;
                const transformer =
                    typeof mapping[mapping.length - 1] === 'function' ? mapping.pop() : (v) => v;

                // Navigate to the correct nested property
                for (let i = 0; i < mapping.length - 1; i++) {
                    target = target[mapping[i]];
                }

                // Set the transformed value
                target[mapping[mapping.length - 1]] = transformer(value);
            }
        }
    }

    _mergeConfigs(target, source) {
        for (const key in source) {
            if (
                typeof source[key] === 'object' &&
                source[key] !== null &&
                !Array.isArray(source[key])
            ) {
                if (!(key in target)) {
                    target[key] = {};
                }
                this._mergeConfigs(target[key], source[key]);
            } else {
                target[key] = source[key];
            }
        }
    }

    async _createClient() {
        try {
            this.kafka = new Kafka(this.config.kafka);
            return this.kafka;
        } catch (error) {
            this._handleError('client_creation_error', error);
            throw error;
        }
    }

    async _createProducer() {
        try {
            this.producer = this.kafka.producer(this.config.producer);

            // Add producer event listeners
            this.producer.on('producer.connect', () => this.emit('producer.connected'));
            this.producer.on('producer.disconnect', () => this.emit('producer.disconnected'));
            this.producer.on('producer.network.request_timeout', (error) =>
                this._handleError('producer_timeout', error)
            );

            return this.producer;
        } catch (error) {
            this._handleError('producer_creation_error', error);
            throw error;
        }
    }

    async _createConsumer() {
        try {
            this.consumer = this.kafka.consumer(this.config.consumer);

            // Add consumer event listeners
            this.consumer.on('consumer.connect', () => this.emit('consumer.connected'));
            this.consumer.on('consumer.disconnect', () => this.emit('consumer.disconnected'));
            this.consumer.on('consumer.crash', (error) =>
                this._handleError('consumer_crash', error)
            );

            return this.consumer;
        } catch (error) {
            this._handleError('consumer_creation_error', error);
            throw error;
        }
    }

    async init(createProducer = true, createConsumer = true) {
        try {
            await this._createClient();

            if (createProducer) {
                await this._createProducer();
                await this.producer.connect();
                this.emit('producer.ready');
            }

            if (createConsumer) {
                await this._createConsumer();
                await this.consumer.connect();
                this.emit('consumer.ready');
            }

            this.health.connected = true;
            this.emit('ready');
            return true;
        } catch (error) {
            this._handleError('initialization_error', error);
            throw error;
        }
    }

    async sendBatch(batchMessages, { timeout = 30000 } = {}) {
        try {
            const result = await this.producer.sendBatch({
                compression: CompressionTypes.GZIP,
                timeout,
                topicMessages: batchMessages,
            });
            this.health.messagesSent += batchMessages.reduce(
                (acc, batch) => acc + batch.messages.length,
                0
            );
            return result;
        } catch (error) {
            this._handleError('batch_send_error', error);
            throw error;
        }
    }

    async send(topic, messages, { timeout = 30000 } = {}) {
        try {
            const result = await this.producer.send({
                topic,
                messages,
                compression: CompressionTypes.GZIP,
                timeout,
            });
            this.health.messagesSent += messages.length;
            return result;
        } catch (error) {
            this._handleError('message_send_error', error);
            throw error;
        }
    }

    async consumerSubscribe(opts) {
        try {
            await this.consumer.subscribe(opts);
            this.emit('consumer.subscribed', opts);
        } catch (error) {
            this._handleError('subscription_error', error);
            throw error;
        }
    }

    async consumeBatch(callback) {
        try {
            await this.consumer.run({
                eachBatchAutoResolve: true,
                autoCommit: true,
                eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
                    try {
                        await callback({ batch, resolveOffset, heartbeat, isRunning, isStale });
                        this.health.messagesReceived += batch.messages.length;
                    } catch (error) {
                        this._handleError('batch_processing_error', error);
                        throw error;
                    }
                },
            });
        } catch (error) {
            this._handleError('batch_consumption_error', error);
            throw error;
        }
    }

    async consumeEach(callback) {
        try {
            await this.consumer.run({
                autoCommit: true,
                eachMessage: async ({ topic, partition, message, heartbeat, pause }) => {
                    try {
                        await callback({ topic, partition, message, heartbeat, pause });
                        this.health.messagesReceived++;
                    } catch (error) {
                        this._handleError('message_processing_error', error);
                        throw error;
                    }
                },
            });
        } catch (error) {
            this._handleError('message_consumption_error', error);
            throw error;
        }
    }

    async disconnect() {
        try {
            const tasks = [];
            if (this.producer) {
                tasks.push(this.producer.disconnect());
                this.producer = null;
            }
            if (this.consumer) {
                tasks.push(this.consumer.disconnect());
                this.consumer = null;
            }
            await Promise.all(tasks);
            this.kafka = null;
            this.health.connected = false;
            this.emit('disconnected');
        } catch (error) {
            this._handleError('disconnect_error', error);
            throw error;
        }
    }

    _handleError(type, error) {
        const errorEvent = {
            type,
            error,
            timestamp: new Date().toISOString(),
        };

        if (type.startsWith('producer')) {
            this.health.lastProducerError = errorEvent;
        } else if (type.startsWith('consumer')) {
            this.health.lastConsumerError = errorEvent;
        }

        this.emit('error', errorEvent);
    }

    getHealth() {
        return {
            ...this.health,
            timestamp: new Date().toISOString(),
        };
    }
}

module.exports = KafkaService;
