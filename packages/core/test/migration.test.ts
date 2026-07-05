import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createTestPool, applyMigrations } from './testDb';
import type { Pool } from 'pg';

describe('migrations', () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = createTestPool();
    await applyMigrations(pool);
  });

  afterAll(async () => {
    await pool.end();
  });

  it('creates all four tables', async () => {
    const result = await pool.query(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public' ORDER BY table_name`
    );
    const names = result.rows.map((r) => r.table_name);
    expect(names).toEqual(
      expect.arrayContaining(['programs', 'reservations', 'fx_rates', 'processed_messages'])
    );
  });
});
