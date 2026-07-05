import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type { Pool } from 'pg';
import { createTestPool, applyMigrations, resetSchema } from './testDb';
import {
  reserveCapacity,
  ProgramNotFoundError,
  InsufficientCapacityError,
  InvoiceConflictError,
  InvalidAmountError,
  InvalidCurrencyError,
  type ReserveResult,
} from '../src/capacityService';
import { FxRateUnavailableError } from '../src/fx';

const PROGRAM_ID = '11111111-1111-1111-1111-111111111111';

async function seedProgram(pool: Pool, overrides: Partial<{ currency: string; totalLimit: string }> = {}) {
  await pool.query(
    `INSERT INTO programs (id, name, currency, total_limit, reserved)
     VALUES ($1, 'Test Program', $2, $3, 0)`,
    [PROGRAM_ID, overrides.currency ?? 'USD', overrides.totalLimit ?? '1000.00']
  );
}

describe('reserveCapacity', () => {
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

  it('reserves capacity in the program currency when invoice currency matches', async () => {
    await seedProgram(pool);
    const result = await reserveCapacity(pool, {
      programId: PROGRAM_ID,
      invoiceId: 'inv-1',
      currency: 'USD',
      amount: '400.00',
    });

    expect(result.created).toBe(true);
    expect(result.reservation.reservedAmount).toBe('400.00');
    expect(result.reservation.status).toBe('RESERVED');

    const program = await pool.query('SELECT reserved FROM programs WHERE id = $1', [PROGRAM_ID]);
    expect(program.rows[0].reserved).toBe('400.00');
  });

  it('converts to program currency using the fx rate', async () => {
    await seedProgram(pool, { currency: 'USD' });
    await pool.query(
      `INSERT INTO fx_rates (base_currency, quote_currency, rate) VALUES ('EUR', 'USD', 1.10)`
    );

    const result = await reserveCapacity(pool, {
      programId: PROGRAM_ID,
      invoiceId: 'inv-2',
      currency: 'EUR',
      amount: '100.00',
    });

    expect(result.reservation.fxRateUsed).toBe('1.10000000');
    expect(result.reservation.reservedAmount).toBe('110.00');
  });

  it('rejects a reservation that exceeds available capacity', async () => {
    await seedProgram(pool, { totalLimit: '100.00' });

    await expect(
      reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-3', currency: 'USD', amount: '150.00' })
    ).rejects.toThrow(InsufficientCapacityError);

    const program = await pool.query('SELECT reserved FROM programs WHERE id = $1', [PROGRAM_ID]);
    expect(program.rows[0].reserved).toBe('0.00');
  });

  it('throws ProgramNotFoundError for an unknown program', async () => {
    await expect(
      reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-4', currency: 'USD', amount: '10.00' })
    ).rejects.toThrow(ProgramNotFoundError);
  });

  it('throws FxRateUnavailableError when no rate exists for the pair', async () => {
    await seedProgram(pool, { currency: 'USD' });
    await expect(
      reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-5', currency: 'JPY', amount: '10.00' })
    ).rejects.toThrow(FxRateUnavailableError);
  });

  it('is idempotent for an identical repeated invoiceId', async () => {
    await seedProgram(pool);
    const first = await reserveCapacity(pool, {
      programId: PROGRAM_ID, invoiceId: 'inv-6', currency: 'USD', amount: '200.00',
    });
    const second = await reserveCapacity(pool, {
      programId: PROGRAM_ID, invoiceId: 'inv-6', currency: 'USD', amount: '200.00',
    });

    expect(second.created).toBe(false);
    expect(second.reservation.id).toBe(first.reservation.id);

    const program = await pool.query('SELECT reserved FROM programs WHERE id = $1', [PROGRAM_ID]);
    expect(program.rows[0].reserved).toBe('200.00');
  });

  it('rejects a repeated invoiceId with a different amount', async () => {
    await seedProgram(pool);
    await reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-7', currency: 'USD', amount: '200.00' });

    await expect(
      reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-7', currency: 'USD', amount: '300.00' })
    ).rejects.toThrow(InvoiceConflictError);
  });

  it('rejects a lowercase currency code', async () => {
    await seedProgram(pool);

    await expect(
      reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-13', currency: 'usd', amount: '100.00' })
    ).rejects.toThrow(InvalidCurrencyError);
  });

  it('rejects a currency code that is not 3 letters', async () => {
    await seedProgram(pool);

    await expect(
      reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-14', currency: 'US', amount: '100.00' })
    ).rejects.toThrow(InvalidCurrencyError);
  });

  it('rejects a negative amount without mutating program state', async () => {
    await seedProgram(pool);

    await expect(
      reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-8', currency: 'USD', amount: '-100.00' })
    ).rejects.toThrow(InvalidAmountError);

    const program = await pool.query('SELECT reserved FROM programs WHERE id = $1', [PROGRAM_ID]);
    expect(program.rows[0].reserved).toBe('0.00');
  });

  it('rejects a zero amount', async () => {
    await seedProgram(pool);

    await expect(
      reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-9', currency: 'USD', amount: '0.00' })
    ).rejects.toThrow(InvalidAmountError);
  });

  it('rejects a non-numeric amount', async () => {
    await seedProgram(pool);

    await expect(
      reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-10', currency: 'USD', amount: 'not-a-number' })
    ).rejects.toThrow(InvalidAmountError);
  });

  it('resolves many truly concurrent identical-invoice reservations idempotently, none failing with a raw error', async () => {
    await seedProgram(pool);

    const attempts = Array.from({ length: 15 }, () =>
      reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-11', currency: 'USD', amount: '250.00' }).then(
        (r) => ({ ok: true as const, r }),
        (e) => ({ ok: false as const, e })
      )
    );
    const outcomes = await Promise.all(attempts);

    const failed = outcomes.filter((o) => !o.ok);
    expect(failed).toEqual([]);

    const succeeded = outcomes.filter((o): o is { ok: true; r: ReserveResult } => o.ok);
    const ids = new Set(succeeded.map((o) => o.r.reservation.id));
    expect(ids.size).toBe(1);
    expect(succeeded.filter((o) => o.r.created).length).toBe(1);

    const program = await pool.query('SELECT reserved FROM programs WHERE id = $1', [PROGRAM_ID]);
    expect(program.rows[0].reserved).toBe('250.00');
  });

  it('surfaces a genuine conflict for concurrent requests with the same invoiceId but different amounts', async () => {
    // Plenty of headroom for both amounts combined, so the only way either
    // request can fail is the invoice-conflict path -- not capacity math --
    // isolating the race this test targets from the unrelated one above.
    await seedProgram(pool, { totalLimit: '10000.00' });

    const outcomes = await Promise.all([
      reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-12', currency: 'USD', amount: '250.00' }).then(
        (r) => ({ ok: true as const, r }),
        (e) => ({ ok: false as const, e })
      ),
      reserveCapacity(pool, { programId: PROGRAM_ID, invoiceId: 'inv-12', currency: 'USD', amount: '999.00' }).then(
        (r) => ({ ok: true as const, r }),
        (e) => ({ ok: false as const, e })
      ),
    ]);

    const succeeded = outcomes.filter((o) => o.ok);
    const failed = outcomes.filter((o) => !o.ok);
    expect(succeeded.length).toBe(1);
    expect(failed.length).toBe(1);
    expect((failed[0] as { ok: false; e: unknown }).e).toBeInstanceOf(InvoiceConflictError);
  });
});
