'use strict';

const { Kafka } = require('@confluentinc/kafka-javascript').KafkaJS;

jest.mock('@confluentinc/kafka-javascript', () => {
	const mockProducer = {
		connect: jest.fn().mockResolvedValue(),
		disconnect: jest.fn().mockResolvedValue(),
		send: jest.fn().mockResolvedValue([{ topicName: 'test', partition: 0 }]),
		sendBatch: jest.fn().mockResolvedValue([{ topicName: 'test', partition: 0 }]),
	};

	const mockConsumer = {
		connect: jest.fn().mockResolvedValue(),
		disconnect: jest.fn().mockResolvedValue(),
		subscribe: jest.fn().mockResolvedValue(),
		run: jest.fn().mockResolvedValue(),
	};

	const mockKafka = {
		producer: jest.fn(() => mockProducer),
		consumer: jest.fn(() => mockConsumer),
	};

	return {
		KafkaJS: {
			Kafka: jest.fn(() => mockKafka),
			CompressionTypes: { GZIP: 1, SNAPPY: 2, LZ4: 3, ZSTD: 4, NONE: 0 },
			logLevel: { NOTHING: 0, ERROR: 1, WARN: 2, INFO: 4, DEBUG: 5 },
		},
	};
});

const KafkaService = require('../src/index');
const { DEFAULT_CONFIG } = require('../src/kafkaConfig');

function getMocks() {
	const kafka = new KafkaService();
	const mockKafkaInstance = Kafka.mock.results[Kafka.mock.results.length - 1].value;
	const mockProducer = mockKafkaInstance.producer();
	const mockConsumer = mockKafkaInstance.consumer();
	return { kafka, mockKafkaInstance, mockProducer, mockConsumer };
}

beforeEach(() => {
	jest.clearAllMocks();
});

describe('KafkaService', () => {
	describe('constructor', () => {
		it('should create instance with default config', () => {
			const service = new KafkaService();
			expect(service.config.kafka.clientId).toBe('default-kafka-client');
			expect(service.config.kafka.brokers).toEqual(['localhost:9092']);
		});

		it('should merge user config over defaults', () => {
			const service = new KafkaService({
				kafka: { clientId: 'my-app', requestTimeout: 5000 },
			});
			expect(service.config.kafka.clientId).toBe('my-app');
			expect(service.config.kafka.requestTimeout).toBe(5000);
			expect(service.config.kafka.brokers).toEqual(['localhost:9092']);
		});

		it('should deep merge nested config', () => {
			const service = new KafkaService({
				kafka: { retry: { retries: 3 } },
			});
			expect(service.config.kafka.retry.retries).toBe(3);
			expect(service.config.kafka.retry.maxRetryTime).toBe(30000);
		});

		it('should initialize health object', () => {
			const service = new KafkaService();
			expect(service.health).toEqual({
				connected: false,
				lastProducerError: null,
				lastConsumerError: null,
				messagesSent: 0,
				messagesReceived: 0,
			});
		});

		it('should be an EventEmitter', () => {
			const service = new KafkaService();
			expect(typeof service.on).toBe('function');
			expect(typeof service.emit).toBe('function');
		});

		it('should override consumer groupId', () => {
			const service = new KafkaService({
				consumer: { groupId: 'custom-group' },
			});
			expect(service.config.consumer.groupId).toBe('custom-group');
			expect(service.config.consumer.autoCommit).toBe(true);
		});
	});

	describe('environment variable overrides', () => {
		const envBackup = { ...process.env };

		afterEach(() => {
			process.env = { ...envBackup };
		});

		it('should override clientId from KAFKA_CLIENT_ID', () => {
			process.env.KAFKA_CLIENT_ID = 'env-client';
			const service = new KafkaService();
			expect(service.config.kafka.clientId).toBe('env-client');
		});

		it('should override brokers from KAFKA_BROKERS (comma-separated)', () => {
			process.env.KAFKA_BROKERS = 'broker1:9092,broker2:9092';
			const service = new KafkaService();
			expect(service.config.kafka.brokers).toEqual(['broker1:9092', 'broker2:9092']);
		});

		it('should override ssl from KAFKA_SSL_ENABLED', () => {
			process.env.KAFKA_SSL_ENABLED = 'true';
			const service = new KafkaService();
			expect(service.config.kafka.ssl).toBe(true);
		});

		it('should override consumer groupId from KAFKA_CONSUMER_GROUP_ID', () => {
			process.env.KAFKA_CONSUMER_GROUP_ID = 'env-group';
			const service = new KafkaService();
			expect(service.config.consumer.groupId).toBe('env-group');
		});

		it('should prioritize env vars over user config', () => {
			process.env.KAFKA_CLIENT_ID = 'env-wins';
			const service = new KafkaService({ kafka: { clientId: 'user-config' } });
			expect(service.config.kafka.clientId).toBe('env-wins');
		});

		it('should parse integer env vars', () => {
			process.env.KAFKA_CONNECTION_TIMEOUT = '10000';
			const service = new KafkaService();
			expect(service.config.kafka.connectionTimeout).toBe(10000);
		});

		it('should override nested retry config', () => {
			process.env.KAFKA_MAX_RETRIES = '3';
			const service = new KafkaService();
			expect(service.config.kafka.retry.retries).toBe(3);
		});
	});

	describe('init()', () => {
		it('should create client, producer, and consumer by default', async () => {
			const service = new KafkaService();
			await service.init();

			expect(Kafka).toHaveBeenCalledWith({ kafkaJS: service.config.kafka });
			expect(service.kafka.producer).toHaveBeenCalledWith({
				kafkaJS: service.config.producer,
			});
			expect(service.kafka.consumer).toHaveBeenCalledWith({
				kafkaJS: service.config.consumer,
			});
			expect(service.health.connected).toBe(true);
		});

		it('should emit ready event', async () => {
			const service = new KafkaService();
			const readyHandler = jest.fn();
			service.on('ready', readyHandler);
			await service.init();
			expect(readyHandler).toHaveBeenCalledTimes(1);
		});

		it('should emit producer.connected and producer.ready when producer is created', async () => {
			const service = new KafkaService();
			const connHandler = jest.fn();
			const readyHandler = jest.fn();
			service.on('producer.connected', connHandler);
			service.on('producer.ready', readyHandler);
			await service.init(true, false);
			expect(connHandler).toHaveBeenCalledTimes(1);
			expect(readyHandler).toHaveBeenCalledTimes(1);
		});

		it('should emit consumer.connected and consumer.ready when consumer is created', async () => {
			const service = new KafkaService();
			const connHandler = jest.fn();
			const readyHandler = jest.fn();
			service.on('consumer.connected', connHandler);
			service.on('consumer.ready', readyHandler);
			await service.init(false, true);
			expect(connHandler).toHaveBeenCalledTimes(1);
			expect(readyHandler).toHaveBeenCalledTimes(1);
		});

		it('should skip producer when createProducer=false', async () => {
			const service = new KafkaService();
			await service.init(false, true);
			expect(service.producer).toBeUndefined();
			expect(service.consumer).toBeDefined();
		});

		it('should skip consumer when createConsumer=false', async () => {
			const service = new KafkaService();
			await service.init(true, false);
			expect(service.producer).toBeDefined();
			expect(service.consumer).toBeUndefined();
		});

		it('should return true on success', async () => {
			const service = new KafkaService();
			const result = await service.init();
			expect(result).toBe(true);
		});

		it('should throw and emit error on failure', async () => {
			Kafka.mockImplementationOnce(() => {
				throw new Error('connection failed');
			});

			const service = new KafkaService();
			const errorHandler = jest.fn();
			service.on('error', errorHandler);

			await expect(service.init()).rejects.toThrow('connection failed');
			expect(errorHandler).toHaveBeenCalledWith(
				expect.objectContaining({
					type: 'client_creation_error',
				})
			);
		});
	});

	describe('send()', () => {
		let service;

		beforeEach(async () => {
			service = new KafkaService();
			await service.init(true, false);
		});

		it('should send messages to a topic', async () => {
			const messages = [{ key: 'k1', value: 'v1' }];
			await service.send('test-topic', messages);

			expect(service.producer.send).toHaveBeenCalledWith({
				topic: 'test-topic',
				messages,
			});
		});

		it('should increment messagesSent count', async () => {
			const messages = [{ value: 'a' }, { value: 'b' }, { value: 'c' }];
			await service.send('topic', messages);
			expect(service.health.messagesSent).toBe(3);
		});

		it('should accumulate messagesSent across multiple sends', async () => {
			await service.send('topic', [{ value: 'a' }]);
			await service.send('topic', [{ value: 'b' }, { value: 'c' }]);
			expect(service.health.messagesSent).toBe(3);
		});

		it('should return producer.send result', async () => {
			const result = await service.send('topic', [{ value: 'a' }]);
			expect(result).toEqual([{ topicName: 'test', partition: 0 }]);
		});

		it('should throw and emit error on failure', async () => {
			service.producer.send.mockRejectedValueOnce(new Error('send failed'));
			const errorHandler = jest.fn();
			service.on('error', errorHandler);

			await expect(service.send('topic', [{ value: 'a' }])).rejects.toThrow('send failed');
			expect(errorHandler).toHaveBeenCalledWith(
				expect.objectContaining({ type: 'message_send_error' })
			);
		});
	});

	describe('sendBatch()', () => {
		let service;

		beforeEach(async () => {
			service = new KafkaService();
			await service.init(true, false);
		});

		it('should send batch messages with topicMessages', async () => {
			const batch = [
				{ topic: 't1', messages: [{ value: 'a' }] },
				{ topic: 't2', messages: [{ value: 'b' }, { value: 'c' }] },
			];
			await service.sendBatch(batch);

			expect(service.producer.sendBatch).toHaveBeenCalledWith({
				topicMessages: batch,
			});
		});

		it('should count all messages across topics', async () => {
			const batch = [
				{ topic: 't1', messages: [{ value: 'a' }] },
				{ topic: 't2', messages: [{ value: 'b' }, { value: 'c' }] },
			];
			await service.sendBatch(batch);
			expect(service.health.messagesSent).toBe(3);
		});

		it('should throw and emit error on failure', async () => {
			service.producer.sendBatch.mockRejectedValueOnce(new Error('batch failed'));
			const errorHandler = jest.fn();
			service.on('error', errorHandler);

			await expect(
				service.sendBatch([{ topic: 't', messages: [{ value: 'a' }] }])
			).rejects.toThrow('batch failed');
			expect(errorHandler).toHaveBeenCalledWith(
				expect.objectContaining({ type: 'batch_send_error' })
			);
		});
	});

	describe('consumerSubscribe()', () => {
		let service;

		beforeEach(async () => {
			service = new KafkaService();
			await service.init(false, true);
		});

		it('should subscribe with given options', async () => {
			const opts = { topics: ['topic-a', 'topic-b'] };
			await service.consumerSubscribe(opts);
			expect(service.consumer.subscribe).toHaveBeenCalledWith(opts);
		});

		it('should emit consumer.subscribed event', async () => {
			const handler = jest.fn();
			service.on('consumer.subscribed', handler);
			const opts = { topics: ['topic-a'] };
			await service.consumerSubscribe(opts);
			expect(handler).toHaveBeenCalledWith(opts);
		});

		it('should throw and emit error on failure', async () => {
			service.consumer.subscribe.mockRejectedValueOnce(new Error('sub failed'));
			const errorHandler = jest.fn();
			service.on('error', errorHandler);

			await expect(service.consumerSubscribe({ topics: ['t'] })).rejects.toThrow(
				'sub failed'
			);
			expect(errorHandler).toHaveBeenCalledWith(
				expect.objectContaining({ type: 'subscription_error' })
			);
		});
	});

	describe('consumeEach()', () => {
		let service;

		beforeEach(async () => {
			service = new KafkaService();
			await service.init(false, true);
		});

		it('should call consumer.run with eachMessage', async () => {
			const callback = jest.fn();
			await service.consumeEach(callback);

			expect(service.consumer.run).toHaveBeenCalledWith(
				expect.objectContaining({
					autoCommit: true,
					eachMessage: expect.any(Function),
				})
			);
		});

		it('should pass message data to callback and increment count', async () => {
			const callback = jest.fn();
			service.consumer.run.mockImplementationOnce(async ({ eachMessage }) => {
				await eachMessage({
					topic: 'test',
					partition: 0,
					message: { value: Buffer.from('hello') },
					heartbeat: jest.fn(),
					pause: jest.fn(),
				});
			});

			await service.consumeEach(callback);

			expect(callback).toHaveBeenCalledWith(
				expect.objectContaining({
					topic: 'test',
					partition: 0,
				})
			);
			expect(service.health.messagesReceived).toBe(1);
		});

		it('should emit error when callback throws', async () => {
			const errorHandler = jest.fn();
			service.on('error', errorHandler);

			service.consumer.run.mockImplementationOnce(async ({ eachMessage }) => {
				await eachMessage({
					topic: 'test',
					partition: 0,
					message: { value: Buffer.from('hello') },
					heartbeat: jest.fn(),
					pause: jest.fn(),
				});
			});

			const failingCallback = jest.fn().mockRejectedValue(new Error('process failed'));
			await expect(service.consumeEach(failingCallback)).rejects.toThrow('process failed');
			expect(errorHandler).toHaveBeenCalledWith(
				expect.objectContaining({ type: 'message_processing_error' })
			);
		});
	});

	describe('consumeBatch()', () => {
		let service;

		beforeEach(async () => {
			service = new KafkaService();
			await service.init(false, true);
		});

		it('should call consumer.run with eachBatch', async () => {
			const callback = jest.fn();
			await service.consumeBatch(callback);

			expect(service.consumer.run).toHaveBeenCalledWith(
				expect.objectContaining({
					eachBatchAutoResolve: true,
					autoCommit: true,
					eachBatch: expect.any(Function),
				})
			);
		});

		it('should pass batch data to callback and count messages', async () => {
			const callback = jest.fn();
			service.consumer.run.mockImplementationOnce(async ({ eachBatch }) => {
				await eachBatch({
					batch: {
						messages: [{ value: 'a' }, { value: 'b' }],
					},
					resolveOffset: jest.fn(),
					heartbeat: jest.fn(),
					isRunning: jest.fn(() => true),
					isStale: jest.fn(() => false),
				});
			});

			await service.consumeBatch(callback);

			expect(callback).toHaveBeenCalledWith(
				expect.objectContaining({
					batch: expect.objectContaining({ messages: expect.any(Array) }),
				})
			);
			expect(service.health.messagesReceived).toBe(2);
		});
	});

	describe('disconnect()', () => {
		it('should disconnect producer and consumer', async () => {
			const service = new KafkaService();
			await service.init();

			const producer = service.producer;
			const consumer = service.consumer;

			await service.disconnect();

			expect(producer.disconnect).toHaveBeenCalled();
			expect(consumer.disconnect).toHaveBeenCalled();
			expect(service.producer).toBeNull();
			expect(service.consumer).toBeNull();
			expect(service.kafka).toBeNull();
			expect(service.health.connected).toBe(false);
		});

		it('should emit disconnected event', async () => {
			const service = new KafkaService();
			await service.init();
			const handler = jest.fn();
			service.on('disconnected', handler);

			await service.disconnect();
			expect(handler).toHaveBeenCalledTimes(1);
		});

		it('should handle disconnect when only producer exists', async () => {
			const service = new KafkaService();
			await service.init(true, false);
			await expect(service.disconnect()).resolves.not.toThrow();
			expect(service.producer).toBeNull();
		});

		it('should handle disconnect when only consumer exists', async () => {
			const service = new KafkaService();
			await service.init(false, true);
			await expect(service.disconnect()).resolves.not.toThrow();
			expect(service.consumer).toBeNull();
		});

		it('should handle disconnect when nothing was initialized', async () => {
			const service = new KafkaService();
			service.kafka = {};
			await expect(service.disconnect()).resolves.not.toThrow();
			expect(service.health.connected).toBe(false);
		});

		it('should throw and emit error on failure', async () => {
			const service = new KafkaService();
			await service.init();
			service.producer.disconnect.mockRejectedValueOnce(new Error('dc failed'));

			const errorHandler = jest.fn();
			service.on('error', errorHandler);

			await expect(service.disconnect()).rejects.toThrow('dc failed');
			expect(errorHandler).toHaveBeenCalledWith(
				expect.objectContaining({ type: 'disconnect_error' })
			);
		});
	});

	describe('getHealth()', () => {
		it('should return health with timestamp', () => {
			const service = new KafkaService();
			const health = service.getHealth();
			expect(health).toHaveProperty('timestamp');
			expect(health.connected).toBe(false);
			expect(health.messagesSent).toBe(0);
			expect(health.messagesReceived).toBe(0);
		});

		it('should reflect updated state after init', async () => {
			const service = new KafkaService();
			await service.init();
			expect(service.getHealth().connected).toBe(true);
		});

		it('should reflect message counts', async () => {
			const service = new KafkaService();
			await service.init(true, false);
			await service.send('topic', [{ value: 'a' }, { value: 'b' }]);
			const health = service.getHealth();
			expect(health.messagesSent).toBe(2);
		});
	});

	describe('_handleError()', () => {
		it('should store producer errors in health', () => {
			const service = new KafkaService();
			const handler = jest.fn();
			service.on('error', handler);

			service._handleError('producer_timeout', new Error('timeout'));

			expect(service.health.lastProducerError).toMatchObject({
				type: 'producer_timeout',
				error: expect.any(Error),
				timestamp: expect.any(String),
			});
			expect(service.health.lastConsumerError).toBeNull();
		});

		it('should store consumer errors in health', () => {
			const service = new KafkaService();
			const handler = jest.fn();
			service.on('error', handler);

			service._handleError('consumer_crash', new Error('crash'));

			expect(service.health.lastConsumerError).toMatchObject({
				type: 'consumer_crash',
			});
			expect(service.health.lastProducerError).toBeNull();
		});

		it('should not store non-producer/consumer errors in health', () => {
			const service = new KafkaService();
			const handler = jest.fn();
			service.on('error', handler);

			service._handleError('initialization_error', new Error('init'));

			expect(service.health.lastProducerError).toBeNull();
			expect(service.health.lastConsumerError).toBeNull();
		});

		it('should emit error event with errorEvent object', () => {
			const service = new KafkaService();
			const handler = jest.fn();
			service.on('error', handler);

			const err = new Error('test');
			service._handleError('some_error', err);

			expect(handler).toHaveBeenCalledWith({
				type: 'some_error',
				error: err,
				timestamp: expect.any(String),
			});
		});
	});

	describe('static properties', () => {
		it('should expose DEFAULT_CONFIG', () => {
			expect(KafkaService.DEFAULT_CONFIG).toBe(DEFAULT_CONFIG);
		});

		it('should expose ENV_MAPPING', () => {
			expect(KafkaService.ENV_MAPPING).toBeDefined();
			expect(KafkaService.ENV_MAPPING).toHaveProperty('KAFKA_CLIENT_ID');
		});
	});
});
