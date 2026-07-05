import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from 'vitest';
import type { Pool } from 'pg';
import { createTestPool, applyMigrations, resetSchema } from './testDb';
import { applyCapacityAdjustment, applyReconciliation, applyFxRateUpdate } from '../src/kafkaHandlers';
import { getProgram, reserveCapacity, ProgramNotFoundError } from '../src/capacityService';

const PROGRAM_ID = '11111111-1111-1111-1111-111111111111';

async function seedProgram(pool: Pool) {
  // Insert without specifying updated_at to allow DEFAULT to be used,
  // then immediately update to a past value to make test dates non-stale
  await pool.query(
    `INSERT INTO programs (id, name, currency, total_limit, reserved)
     VALUES ($1, 'Test Program', 'USD', '1000.00', 0)`,
    [PROGRAM_ID]
  );
  // Update updated_at directly using a timestamp before all test asOf dates
  // Use interval arithmetic to bypass DEFAULT now() behavior
  await pool.query(
    `UPDATE programs SET updated_at = to_timestamp(0) + interval '1 year' WHERE id = $1`,
    [PROGRAM_ID]
  );
}

describe('kafkaHandlers', () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = createTestPool();
    await applyMigrations(pool);
  });

  beforeEach(async () => {
    await resetSchema(pool);
    await seedProgram(pool);
  });

  afterAll(async () => {
    await pool.end();
  });

  describe('applyCapacityAdjustment', () => {
    it('adds the delta to total_limit', async () => {
      const result = await applyCapacityAdjustment(pool, {
        messageId: 'adj-1', programId: PROGRAM_ID, deltaAmount: '500.00',
      });
      expect(result.applied).toBe(true);
      const program = await getProgram(pool, PROGRAM_ID);
      expect(program.totalLimit).toBe('1500.00');
    });

    it('is idempotent on a repeated messageId', async () => {
      await applyCapacityAdjustment(pool, { messageId: 'adj-2', programId: PROGRAM_ID, deltaAmount: '500.00' });
      const result = await applyCapacityAdjustment(pool, { messageId: 'adj-2', programId: PROGRAM_ID, deltaAmount: '500.00' });
      expect(result.applied).toBe(false);
      const program = await getProgram(pool, PROGRAM_ID);
      expect(program.totalLimit).toBe('1500.00');
    });

    it('throws and does not mark the message processed when the program does not exist', async () => {
      const unknownProgramId = '22222222-2222-2222-2222-222222222222';
      await expect(
        applyCapacityAdjustment(pool, { messageId: 'adj-unknown', programId: unknownProgramId, deltaAmount: '500.00' })
      ).rejects.toThrow(ProgramNotFoundError);
      const result = await pool.query(
        'SELECT 1 FROM processed_messages WHERE message_id = $1', ['adj-unknown']
      );
      expect(result.rows.length).toBe(0);
    });
  });

  describe('applyReconciliation', () => {
    it('overwrites total_limit and reserved when asOf is newer', async () => {
      const result = await applyReconciliation(pool, {
        messageId: 'rec-1',
        programId: PROGRAM_ID,
        totalLimit: '2000.00',
        reserved: '300.00',
        asOf: new Date('2026-02-01T00:00:00Z'),
      });
      expect(result).toEqual({ applied: true, reason: 'applied' });
      const program = await getProgram(pool, PROGRAM_ID);
      expect(program.totalLimit).toBe('2000.00');
      expect(program.reserved).toBe('300.00');
    });

    it('skips a stale snapshot older than the program updated_at', async () => {
      await applyReconciliation(pool, {
        messageId: 'rec-2', programId: PROGRAM_ID, totalLimit: '2000.00', reserved: '300.00',
        asOf: new Date('2026-02-01T00:00:00Z'),
      });
      const result = await applyReconciliation(pool, {
        messageId: 'rec-3', programId: PROGRAM_ID, totalLimit: '9999.00', reserved: '9999.00',
        asOf: new Date('2026-01-15T00:00:00Z'),
      });
      expect(result).toEqual({ applied: false, reason: 'stale' });
      const program = await getProgram(pool, PROGRAM_ID);
      expect(program.totalLimit).toBe('2000.00');
    });

    it('is idempotent on a repeated messageId', async () => {
      await applyReconciliation(pool, {
        messageId: 'rec-4', programId: PROGRAM_ID, totalLimit: '2000.00', reserved: '300.00',
        asOf: new Date('2026-02-01T00:00:00Z'),
      });
      const result = await applyReconciliation(pool, {
        messageId: 'rec-4', programId: PROGRAM_ID, totalLimit: '5000.00', reserved: '500.00',
        asOf: new Date('2026-03-01T00:00:00Z'),
      });
      expect(result).toEqual({ applied: false, reason: 'already_processed' });
      const program = await getProgram(pool, PROGRAM_ID);
      expect(program.totalLimit).toBe('2000.00');
    });

    it('logs a warning when reconciled reserved diverges from the local ledger', async () => {
      await reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-1', currency: 'USD', amount: '100.00' });
      const warn = vi.fn();
      await applyReconciliation(
        pool,
        { messageId: 'rec-5', programId: PROGRAM_ID, totalLimit: '1000.00', reserved: '900.00', asOf: new Date('2027-12-31T00:00:00Z') },
        { warn }
      );
      expect(warn).toHaveBeenCalledWith(
        'reconciliation diverges from local ledger',
        expect.objectContaining({ programId: PROGRAM_ID })
      );
    });
  });

  describe('applyFxRateUpdate', () => {
    it('upserts a new rate', async () => {
      await applyFxRateUpdate(pool, {
        messageId: 'fx-1', base: 'USD', quote: 'EUR', rate: '0.91', timestamp: new Date('2026-01-01T00:00:00Z'),
      });
      const result = await pool.query(
        'SELECT rate FROM fx_rates WHERE base_currency = $1 AND quote_currency = $2', ['USD', 'EUR']
      );
      expect(result.rows[0].rate).toBe('0.91000000');
    });

    it('does not overwrite a newer rate with an older message', async () => {
      await applyFxRateUpdate(pool, {
        messageId: 'fx-2', base: 'USD', quote: 'EUR', rate: '0.95', timestamp: new Date('2026-02-01T00:00:00Z'),
      });
      await applyFxRateUpdate(pool, {
        messageId: 'fx-3', base: 'USD', quote: 'EUR', rate: '0.80', timestamp: new Date('2026-01-01T00:00:00Z'),
      });
      const result = await pool.query(
        'SELECT rate FROM fx_rates WHERE base_currency = $1 AND quote_currency = $2', ['USD', 'EUR']
      );
      expect(result.rows[0].rate).toBe('0.95000000');
    });
  });
});
