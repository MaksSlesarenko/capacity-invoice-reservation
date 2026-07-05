import fs from 'node:fs';
import path from 'node:path';
import { Pool } from 'pg';

export function createTestPool(): Pool {
  return new Pool({
    connectionString:
      process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/capacity_test',
  });
}

export async function applyMigrations(pool: Pool): Promise<void> {
  const sql = fs.readFileSync(
    path.join(__dirname, '../src/db/migrations/001_init.sql'),
    'utf-8'
  );
  await pool.query(sql);
}

export async function resetSchema(pool: Pool): Promise<void> {
  await pool.query(
    'TRUNCATE programs, reservations, fx_rates, processed_messages CASCADE'
  );
}
