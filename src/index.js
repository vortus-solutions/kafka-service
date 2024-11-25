'use strict';

const { Kafka, CompressionTypes } = require('kafkajs');
const EventEmitter = require('events');
const { DEFAULT_CONFIG, ENV_MAPPING } = require('./kafkaConfig');

/**
 * @class KafkaService
 * @extends EventEmitter
 * @description A service class for managing Kafka producer and consumer operations with built-in error handling and health monitoring
 * 
 * @fires KafkaService#producer.connected
 * @fires KafkaService#producer.disconnected
 * @fires KafkaService#producer.ready
 * @fires KafkaService#consumer.connected
 * @fires KafkaService#consumer.disconnected
 * @fires KafkaService#consumer.ready
 * @fires KafkaService#consumer.subscribed
 * @fires KafkaService#ready
 * @fires KafkaService#disconnected
 * @fires KafkaService#error
 */
class KafkaService extends EventEmitter {
    static DEFAULT_CONFIG = DEFAULT_CONFIG;
    static ENV_MAPPING = ENV_MAPPING;

    /**
     * @constructor
     * @param {Object} [userConfig={}] - Custom configuration to override defaults
     */
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

    /**
     * @private
     * @param {Object} userConfig - User provided configuration
     * @returns {Object} Complete configuration with defaults and overrides
     */
    _buildConfig(userConfig) {
        // Start with default config using object spread for shallow clone
        const config = {
            kafka: { ...KafkaService.DEFAULT_CONFIG.kafka },
            producer: { ...KafkaService.DEFAULT_CONFIG.producer },
            consumer: { ...KafkaService.DEFAULT_CONFIG.consumer }
        };

        // Apply user config (overrides both defaults and env vars)
        this._mergeConfigs(config, userConfig);

        // Apply environment variables
        this._applyEnvVariables(config);

        return config;
    }

    /**
     * @private
     * @param {Object} config - Configuration object to apply environment variables to
     */
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

    /**
     * @private
     * @param {Object} target - Target configuration object
     * @param {Object} source - Source configuration object to merge from
     */
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

    /**
     * @private
     * @async
     * @returns {Object} Kafka client instance
     * @throws {Error} If client creation fails
     */
    async _createClient() {
        try {
            this.kafka = new Kafka(this.config.kafka);
            return this.kafka;
        } catch (error) {
            this._handleError('client_creation_error', error);
            throw error;
        }
    }

    /**
     * @private
     * @async
     * @returns {Object} Kafka producer instance
     * @throws {Error} If producer creation fails
     */
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

    /**
     * @private
     * @async
     * @returns {Object} Kafka consumer instance
     * @throws {Error} If consumer creation fails
     */
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

    /**
     * Initialize the Kafka service
     * @async
     * @param {boolean} [createProducer=true] - Whether to create a producer
     * @param {boolean} [createConsumer=true] - Whether to create a consumer
     * @returns {Promise<boolean>} Success status
     * @throws {Error} If initialization fails
     */
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

    /**
     * Send a batch of messages to Kafka
     * @async
     * @param {Array} batchMessages - Array of messages to send
     * @param {Object} [options] - Send options
     * @param {number} [options.timeout=30000] - Timeout in milliseconds
     * @returns {Promise<Object>} Send result
     * @throws {Error} If batch send fails
     */
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

    /**
     * Send messages to a specific topic
     * @async
     * @param {string} topic - Kafka topic
     * @param {Array} messages - Array of messages
     * @param {Object} [options] - Send options
     * @param {number} [options.timeout=30000] - Timeout in milliseconds
     * @returns {Promise<Object>} Send result
     * @throws {Error} If send fails
     */
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

    /**
     * Subscribe consumer to topics
     * @async
     * @param {Object} opts - Subscription options
     * @throws {Error} If subscription fails
     */
    async consumerSubscribe(opts) {
        try {
            await this.consumer.subscribe(opts);
            this.emit('consumer.subscribed', opts);
        } catch (error) {
            this._handleError('subscription_error', error);
            throw error;
        }
    }

    /**
     * Consume messages in batches
     * @async
     * @param {Function} callback - Batch processing callback
     * @throws {Error} If batch consumption fails
     */
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

    /**
     * Consume messages individually
     * @async
     * @param {Function} callback - Message processing callback
     * @throws {Error} If message consumption fails
     */
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

    /**
     * Disconnect from Kafka
     * @async
     * @throws {Error} If disconnect fails
     */
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

    /**
     * Handle and emit error events
     * @private
     * @param {string} type - Error type
     * @param {Error} error - Error object
     */
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

    /**
     * Get current health status
     * @returns {Object} Health status object
     */
    getHealth() {
        return {
            ...this.health,
            timestamp: new Date().toISOString(),
        };
    }
}

module.exports = KafkaService;