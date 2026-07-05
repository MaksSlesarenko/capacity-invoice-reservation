import { Kafka, type Consumer } from 'kafkajs';
import type { Pool } from 'pg';
import { routeMessage, type DeadLetterEnvelope } from './router';

export const DEAD_LETTER_TOPIC = 'capacity.dlq';

export async function startConsumer(
  pool: Pool,
  brokers: string[],
  groupId: string = 'capacity-consumer'
): Promise<Consumer> {
  const kafka = new Kafka({ clientId: 'capacity-consumer', brokers });
  const consumer = kafka.consumer({ groupId });
  const producer = kafka.producer();
  await Promise.all([consumer.connect(), producer.connect()]);

  const deadLetter = {
    publish: async (envelope: DeadLetterEnvelope) => {
      await producer.send({
        topic: DEAD_LETTER_TOPIC,
        messages: [
          {
            value: JSON.stringify({ ...envelope, failedAt: new Date().toISOString() }),
          },
        ],
      });
    },
  };

  await consumer.subscribe({
    topics: ['capacity.adjustments', 'capacity.reconciliation', 'fx.rates'],
    fromBeginning: false,
  });
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      await routeMessage(pool, { topic, value: message.value.toString() }, console, deadLetter);
    },
  });
  return consumer;
}
