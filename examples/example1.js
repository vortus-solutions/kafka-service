const KafkaService = require('../src/index');

async function main() {
    const kafka = new KafkaService({
        kafka: {
            requestTimeout: 25000,
        },
    });

    // Event listeners
    kafka.on('ready', () => console.log('Kafka service is ready'));
    kafka.on('error', (error) => console.error('Kafka error:', error));
    kafka.on('producer.connected', () => console.log('Producer connected'));
    kafka.on('consumer.connected', () => console.log('Consumer connected'));

    try {
        await kafka.init(true, true);

        // Producer example
        await kafka.send('my-topic', [{ key: 'key1', value: JSON.stringify({ data: 'test' }) }]);

        // Producer example: Send batch messages
        const batchMessages = [
            {
                topic: 'test-topic',
                messages: [
                    { key: 'key2', value: JSON.stringify({ message: 'Batch message 1' }) },
                    { key: 'key3', value: JSON.stringify({ message: 'Batch message 2' }) }
                ]
            }
        ];
        await kafka.sendBatch(batchMessages);

        // Consumer example: Subscribe to topics
        await kafka.consumerSubscribe({
            topics: ['test-topic'],
            fromBeginning: true
        });

        await kafka.consumeEach(async ({ topic, partition, message, heartbeat }) => {
            console.log({
                topic,
                partition,
                message: message.value.toString(),
            });

            await heartbeat();
        });

        // Alternative: Process messages in batches
        /*
        await kafka.consumeBatch(async ({ batch, resolveOffset, heartbeat }) => {
            console.log(`Received batch of ${batch.messages.length} messages`);
            for (const message of batch.messages) {
                console.log({
                    topic: batch.topic,
                    partition: batch.partition,
                    offset: message.offset,
                    key: message.key.toString(),
                    value: JSON.parse(message.value.toString())
                });
                resolveOffset(message.offset);
            }
            await heartbeat();
        });
        */

        // Check health status
        setInterval(() => {
            const health = kafka.getHealth();
            console.log('Kafka Service Health:', health);
        }, 30000);

        // Graceful shutdown example
        process.on('SIGTERM', async () => {
            console.log('Shutting down...');
            await kafka.disconnect();
            throw new Error('Shutdown complete');
        });
    } catch (error) {
        console.error('Error:', error);
    }
}

main();
