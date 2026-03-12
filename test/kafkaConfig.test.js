'use strict';

const { DEFAULT_CONFIG, ENV_MAPPING } = require('../src/kafkaConfig');
const { CompressionTypes, logLevel } = require('@confluentinc/kafka-javascript').KafkaJS;

describe('kafkaConfig', () => {
	describe('DEFAULT_CONFIG', () => {
		it('should have kafka, producer and consumer sections', () => {
			expect(DEFAULT_CONFIG).toHaveProperty('kafka');
			expect(DEFAULT_CONFIG).toHaveProperty('producer');
			expect(DEFAULT_CONFIG).toHaveProperty('consumer');
		});

		it('should set default kafka broker to localhost:9092', () => {
			expect(DEFAULT_CONFIG.kafka.brokers).toEqual(['localhost:9092']);
		});

		it('should set default clientId', () => {
			expect(DEFAULT_CONFIG.kafka.clientId).toBe('default-kafka-client');
		});

		it('should disable ssl by default', () => {
			expect(DEFAULT_CONFIG.kafka.ssl).toBe(false);
		});

		it('should set kafka retry config', () => {
			expect(DEFAULT_CONFIG.kafka.retry).toEqual({
				initialRetryTime: 300,
				maxRetryTime: 30000,
				retries: 8,
			});
		});

		it('should set logLevel to INFO', () => {
			expect(DEFAULT_CONFIG.kafka.logLevel).toBe(logLevel.INFO);
		});

		it('should set producer compression to GZIP', () => {
			expect(DEFAULT_CONFIG.producer.compression).toBe(CompressionTypes.GZIP);
		});

		it('should disable auto topic creation for producer and consumer', () => {
			expect(DEFAULT_CONFIG.producer.allowAutoTopicCreation).toBe(false);
			expect(DEFAULT_CONFIG.consumer.allowAutoTopicCreation).toBe(false);
		});

		it('should set default consumer groupId', () => {
			expect(DEFAULT_CONFIG.consumer.groupId).toBe('default-consumer-group');
		});

		it('should enable consumer autoCommit with 5s interval', () => {
			expect(DEFAULT_CONFIG.consumer.autoCommit).toBe(true);
			expect(DEFAULT_CONFIG.consumer.autoCommitInterval).toBe(5000);
		});

		it('should set consumer session timeout and heartbeat', () => {
			expect(DEFAULT_CONFIG.consumer.sessionTimeout).toBe(60000);
			expect(DEFAULT_CONFIG.consumer.heartbeatInterval).toBe(30000);
		});
	});

	describe('ENV_MAPPING', () => {
		it('should map KAFKA_CLIENT_ID to kafka.clientId', () => {
			expect(ENV_MAPPING.KAFKA_CLIENT_ID).toEqual(['kafka', 'clientId']);
		});

		it('should map KAFKA_BROKERS with split transformer', () => {
			const mapping = [...ENV_MAPPING.KAFKA_BROKERS];
			const transformer = mapping.pop();
			expect(mapping).toEqual(['kafka', 'brokers']);
			expect(transformer('broker1,broker2')).toEqual(['broker1', 'broker2']);
		});

		it('should map KAFKA_SSL_ENABLED with boolean transformer', () => {
			const mapping = [...ENV_MAPPING.KAFKA_SSL_ENABLED];
			const transformer = mapping.pop();
			expect(transformer('true')).toBe(true);
			expect(transformer('false')).toBe(false);
		});

		it('should map KAFKA_CONNECTION_TIMEOUT with parseInt', () => {
			const mapping = [...ENV_MAPPING.KAFKA_CONNECTION_TIMEOUT];
			const transformer = mapping.pop();
			expect(transformer('5000')).toBe(5000);
		});

		it('should map KAFKA_LOG_LEVEL with logLevel lookup', () => {
			const mapping = [...ENV_MAPPING.KAFKA_LOG_LEVEL];
			const transformer = mapping.pop();
			expect(transformer('ERROR')).toBe(logLevel.ERROR);
			expect(transformer('INVALID')).toBe(logLevel.INFO);
		});

		it('should map KAFKA_PRODUCER_COMPRESSION with CompressionTypes lookup', () => {
			const mapping = [...ENV_MAPPING.KAFKA_PRODUCER_COMPRESSION];
			const transformer = mapping.pop();
			expect(transformer('GZIP')).toBe(CompressionTypes.GZIP);
			expect(transformer('INVALID')).toBe(CompressionTypes.GZIP);
		});

		it('should map KAFKA_CONSUMER_GROUP_ID to consumer.groupId', () => {
			expect(ENV_MAPPING.KAFKA_CONSUMER_GROUP_ID).toEqual(['consumer', 'groupId']);
		});

		it('should map KAFKA_CONSUMER_AUTO_COMMIT with boolean transformer', () => {
			const mapping = [...ENV_MAPPING.KAFKA_CONSUMER_AUTO_COMMIT];
			const transformer = mapping.pop();
			expect(transformer('true')).toBe(true);
			expect(transformer('false')).toBe(false);
		});

		it('should map nested retry config via KAFKA_MAX_RETRIES', () => {
			expect(ENV_MAPPING.KAFKA_MAX_RETRIES.slice(0, -1)).toEqual([
				'kafka',
				'retry',
				'retries',
			]);
		});
	});
});
