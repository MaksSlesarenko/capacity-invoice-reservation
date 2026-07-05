import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type { Pool } from 'pg';
import { createTestPool, applyMigrations, resetSchema } from '../../../packages/core/test/testDb';
import { getProgram } from '@capacity/core';
import { routeMessage } from '../src/router';

const PROGRAM_ID = '11111111-1111-1111-1111-111111111111';

describe('routeMessage', () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = createTestPool();
    await applyMigrations(pool);
  });

  beforeEach(async () => {
    await resetSchema(pool);
    await pool.query(
      `INSERT INTO programs (id, name, currency, total_limit, reserved)
       VALUES ($1, 'Test Program', 'USD', '1000.00', 0)`,
      [PROGRAM_ID]
    );
  });

  afterAll(async () => {
    await pool.end();
  });

  it('routes capacity.adjustments to applyCapacityAdjustment', async () => {
    await routeMessage(pool, {
      topic: 'capacity.adjustments',
      value: JSON.stringify({ messageId: 'm1', programId: PROGRAM_ID, deltaAmount: '250.00' }),
    });
    const program = await getProgram(pool, PROGRAM_ID);
    expect(program.totalLimit).toBe('1250.00');
  });

  it('routes capacity.reconciliation to applyReconciliation', async () => {
    await routeMessage(pool, {
      topic: 'capacity.reconciliation',
      value: JSON.stringify({
        messageId: 'm2', programId: PROGRAM_ID, totalLimit: '2000.00', reserved: '100.00',
        asOf: '2026-07-10T00:00:00Z',
      }),
    });
    const program = await getProgram(pool, PROGRAM_ID);
    expect(program.totalLimit).toBe('2000.00');
  });

  it('routes fx.rates to applyFxRateUpdate', async () => {
    await routeMessage(pool, {
      topic: 'fx.rates',
      value: JSON.stringify({ messageId: 'm3', base: 'USD', quote: 'EUR', rate: '0.9', timestamp: '2026-07-10T00:00:00Z' }),
    });
    const rate = await pool.query('SELECT rate FROM fx_rates WHERE base_currency = $1 AND quote_currency = $2', ['USD', 'EUR']);
    expect(rate.rows[0].rate).toBe('0.90000000');
  });

  it('logs a warning and does not throw for an unknown topic', async () => {
    const warn: string[] = [];
    await routeMessage(
      pool,
      { topic: 'unknown.topic', value: '{}' },
      { warn: (msg: string) => warn.push(msg), info: () => {} }
    );
    expect(warn.length).toBe(1);
  });

  it('logs a warning and does not throw for malformed JSON', async () => {
    const warn: string[] = [];
    await routeMessage(
      pool,
      { topic: 'capacity.adjustments', value: '{not valid json' },
      { warn: (msg: string) => warn.push(msg), info: () => {} }
    );
    expect(warn.length).toBe(1);
    const program = await getProgram(pool, PROGRAM_ID);
    expect(program.totalLimit).toBe('1000.00');
  });

  it('logs a warning and skips a capacity.adjustments payload missing required fields', async () => {
    const warn: string[] = [];
    await routeMessage(
      pool,
      { topic: 'capacity.adjustments', value: JSON.stringify({ messageId: 'm4', programId: PROGRAM_ID }) },
      { warn: (msg: string) => warn.push(msg), info: () => {} }
    );
    expect(warn.length).toBe(1);
    const program = await getProgram(pool, PROGRAM_ID);
    expect(program.totalLimit).toBe('1000.00');
  });

  it('logs a warning and skips a capacity.reconciliation payload with an invalid asOf date', async () => {
    const warn: string[] = [];
    await routeMessage(
      pool,
      {
        topic: 'capacity.reconciliation',
        value: JSON.stringify({
          messageId: 'm5', programId: PROGRAM_ID, totalLimit: '2000.00', reserved: '100.00', asOf: 'not-a-date',
        }),
      },
      { warn: (msg: string) => warn.push(msg), info: () => {} }
    );
    expect(warn.length).toBe(1);
    const program = await getProgram(pool, PROGRAM_ID);
    expect(program.totalLimit).toBe('1000.00');
  });

  it('logs a warning and skips an fx.rates payload missing required fields', async () => {
    const warn: string[] = [];
    await routeMessage(
      pool,
      { topic: 'fx.rates', value: JSON.stringify({ messageId: 'm6', base: 'USD' }) },
      { warn: (msg: string) => warn.push(msg), info: () => {} }
    );
    expect(warn.length).toBe(1);
    const rate = await pool.query('SELECT 1 FROM fx_rates WHERE base_currency = $1', ['USD']);
    expect(rate.rows.length).toBe(0);
  });
});
