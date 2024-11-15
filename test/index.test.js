const KafkaService = require('../src/index');

async function main() {
    const kafka = new KafkaService({
        retryAttempts: 5,
    });

    // Event listeners
    kafka.on('ready', () => console.log('Kafka service is ready'));
    kafka.on('error', (error) => console.error('Kafka error:', error));
    kafka.on('producer.connected', () => console.log('Producer connected'));
    kafka.on('consumer.connected', () => console.log('Consumer connected'));

    try {
        await kafka.init();

        // Producer example
        await kafka.send('my-topic', [{ key: 'key1', value: JSON.stringify({ data: 'test' }) }]);

        // Consumer example
        await kafka.consumerSubscribe({ topic: 'my-topic' });
        await kafka.consumeEach(async ({ topic, partition, message }) => {
            console.log({
                topic,
                partition,
                message: message.value.toString(),
            });
        });

        // Health check
        console.log(kafka.getHealth());
    } catch (error) {
        console.error('Error:', error);
    }
}

main();
