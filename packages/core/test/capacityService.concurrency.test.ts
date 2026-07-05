import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type { Pool } from 'pg';
import { createTestPool, applyMigrations, resetSchema } from './testDb';
import { reserveCapacity, getProgram } from '../src/capacityService';

const PROGRAM_ID = '11111111-1111-1111-1111-111111111111';

describe('reserveCapacity concurrency', () => {
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

  it('never over-reserves when 10 concurrent requests compete for capacity for 5', async () => {
    const attempts = Array.from({ length: 10 }, (_, i) =>
      reserveCapacity(pool, {
        programId: PROGRAM_ID,
        invoiceId: `inv-${i}`,
        currency: 'USD',
        amount: '200.00',
      }).then(
        (r) => ({ ok: true as const, result: r }),
        (err) => ({ ok: false as const, error: err })
      )
    );

    const outcomes = await Promise.all(attempts);
    const succeeded = outcomes.filter((o) => o.ok);
    const failed = outcomes.filter((o) => !o.ok);

    expect(succeeded.length).toBe(5);
    expect(failed.length).toBe(5);

    const program = await getProgram(pool, PROGRAM_ID);
    expect(program.reserved).toBe('1000.00');
  });
});
