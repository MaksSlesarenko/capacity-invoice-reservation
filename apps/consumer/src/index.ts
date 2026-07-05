import { createPool } from '@capacity/core';
import { startConsumer } from './consumer';

const pool = createPool(process.env.DATABASE_URL!);
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

startConsumer(pool, brokers)
  .then(() => console.log('consumer running'))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
