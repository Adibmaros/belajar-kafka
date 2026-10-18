import { Kafka } from 'kafkajs';

const kafka = new Kafka({ clientId: 'belajar', brokers: ['192.168.54.202:9092'] });

// Producer
const producer = kafka.producer({ idempotent: true });
await producer.connect();
await producer.send({
    topic: 'orders',
    acks: -1, // all
    messages: [{ key: 'user1', value: JSON.stringify({ orderId: 'A', total: 50000 }) }],
});

// Consumer
const consumer = kafka.consumer({ groupId: 'grup-a' });
await consumer.connect();
await consumer.subscribe({ topic: 'orders', fromBeginning: true });
await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
        console.log({ partition, key: message.key?.toString(), value: message.value?.toString() });
        // offset di-commit otomatis setelah handler selesai tanpa error
    },
});