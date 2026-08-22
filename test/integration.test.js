'use strict';

const KafkaService = require('../src');

const describeKafka = process.env.KAFKA_INTEGRATION === 'true' ? describe : describe.skip;

describeKafka('KafkaService integration', () => {
    test('produces and consumes a message through Kafka', async () => {
        const suffix = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
        const topic = `kafka-service-${suffix}`;
        const value = `message-${suffix}`;
        const kafka = { clientId: `kafka-service-${suffix}`, brokers: ['localhost:9092'] };
        const producer = new KafkaService({ kafka, producer: { allowAutoTopicCreation: true } });
        const consumer = new KafkaService({
            kafka,
            consumer: {
                groupId: `kafka-service-${suffix}`,
                fromBeginning: true,
                allowAutoTopicCreation: true,
                autoCommit: false,
            },
        });
        producer.on('error', () => {});
        consumer.on('error', () => {});

        try {
            await producer.init(true, false);
            await producer.send(topic, [{ value }]);
            await consumer.init(false, true);
            await consumer.consumerSubscribe({ topics: [topic], fromBeginning: true });

			await new Promise((resolve, reject) => {
                const timeout = setTimeout(
                    () => reject(new Error('Timed out waiting for Kafka message')),
                    30000
                );
                consumer
                    .consumeEach(({ message }) => {
                        clearTimeout(timeout);
                        expect(message.value.toString()).toBe(value);
                        resolve();
                    })
					.catch(reject);
			});
			await new Promise(setImmediate);

			expect(producer.getHealth().messagesSent).toBe(1);
            expect(consumer.getHealth().messagesReceived).toBe(1);
        } finally {
            await Promise.all([producer.disconnect(), consumer.disconnect()]);
        }
    }, 45000);
});
