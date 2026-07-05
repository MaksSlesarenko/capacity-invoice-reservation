import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type { Pool } from 'pg';
import { createTestPool, applyMigrations, resetSchema } from './testDb';
import { seedDemoData } from '../src/seedData';

describe('seedDemoData', () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = createTestPool();
    await applyMigrations(pool);
  });

  beforeEach(async () => {
    await resetSchema(pool);
  });

  afterAll(async () => {
    await pool.end();
  });

  it('inserts demo programs and fx rates, and is safe to run twice', async () => {
    await seedDemoData(pool);
    await seedDemoData(pool);

    const programs = await pool.query('SELECT id, currency FROM programs ORDER BY name');
    expect(programs.rows.length).toBe(2);

    const rates = await pool.query('SELECT base_currency, quote_currency FROM fx_rates');
    expect(rates.rows.length).toBeGreaterThanOrEqual(4);
  });
});
