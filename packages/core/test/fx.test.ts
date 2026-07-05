import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type { Pool } from 'pg';
import { createTestPool, applyMigrations, resetSchema } from './testDb';
import { lookupRate, FxRateUnavailableError } from '../src/fx';

describe('lookupRate', () => {
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

  it('returns "1" when base equals quote without a DB lookup', async () => {
    const client = await pool.connect();
    try {
      const rate = await lookupRate(client, 'USD', 'USD');
      expect(rate).toBe('1');
    } finally {
      client.release();
    }
  });

  it('returns the stored rate for a known pair', async () => {
    await pool.query(
      `INSERT INTO fx_rates (base_currency, quote_currency, rate) VALUES ('USD', 'EUR', 0.92)`
    );
    const client = await pool.connect();
    try {
      const rate = await lookupRate(client, 'USD', 'EUR');
      expect(rate).toBe('0.92000000');
    } finally {
      client.release();
    }
  });

  it('throws FxRateUnavailableError for an unknown pair', async () => {
    const client = await pool.connect();
    try {
      await expect(lookupRate(client, 'USD', 'JPY')).rejects.toThrow(FxRateUnavailableError);
    } finally {
      client.release();
    }
  });
});
