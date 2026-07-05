import { createPool } from '@capacity/core';
import { buildApp } from './server';

const pool = createPool(process.env.DATABASE_URL!);
const clients = JSON.parse(process.env.CLIENTS_JSON ?? '{}');
const app = buildApp({ pool, jwtSecret: process.env.JWT_SECRET!, clients });

app
  .listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' })
  .then(() => console.log(`api listening on ${process.env.PORT ?? 3000}`))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
