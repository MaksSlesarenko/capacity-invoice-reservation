import type { Pool } from 'pg';
import Decimal from 'decimal.js';
import { isProcessed, markProcessed } from './idempotency';
import { ProgramNotFoundError } from './capacityService';

const DIVERGENCE_THRESHOLD = '0.01';

export interface AdjustmentInput {
  messageId: string;
  programId: string;
  deltaAmount: string;
}

export interface ReconciliationInput {
  messageId: string;
  programId: string;
  totalLimit: string;
  reserved: string;
  asOf: Date;
}

export interface ReconciliationResult {
  applied: boolean;
  reason?: 'already_processed' | 'stale' | 'applied';
}

export interface FxRateUpdateInput {
  messageId: string;
  base: string;
  quote: string;
  rate: string;
  timestamp: Date;
}

interface Logger {
  warn: (message: string, meta: Record<string, unknown>) => void;
}

export async function applyCapacityAdjustment(
  pool: Pool,
  input: AdjustmentInput
): Promise<{ applied: boolean }> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    if (await isProcessed(client, input.messageId)) {
      await client.query('COMMIT');
      return { applied: false };
    }
    const result = await client.query(
      'UPDATE programs SET total_limit = total_limit + $1, version = version + 1, updated_at = now() WHERE id = $2',
      [input.deltaAmount, input.programId]
    );
    if (result.rowCount === 0) {
      throw new ProgramNotFoundError(input.programId);
    }
    await markProcessed(client, input.messageId);
    await client.query('COMMIT');
    return { applied: true };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

export async function applyReconciliation(
  pool: Pool,
  input: ReconciliationInput,
  logger: Logger = console
): Promise<ReconciliationResult> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    if (await isProcessed(client, input.messageId)) {
      await client.query('COMMIT');
      return { applied: false, reason: 'already_processed' };
    }

    const programResult = await client.query(
      'SELECT * FROM programs WHERE id = $1 FOR UPDATE',
      [input.programId]
    );
    if (programResult.rows.length === 0) {
      throw new ProgramNotFoundError(input.programId);
    }
    const program = programResult.rows[0];

    const programUpdatedAtTime = program.updated_at instanceof Date
      ? program.updated_at.getTime()
      : new Date(program.updated_at).getTime();
    const asOfTime = input.asOf.getTime();

    if (asOfTime <= programUpdatedAtTime) {
      await markProcessed(client, input.messageId);
      await client.query('COMMIT');
      return { applied: false, reason: 'stale' };
    }

    const ledgerSum = await client.query(
      `SELECT COALESCE(SUM(reserved_amount), 0) AS sum FROM reservations
       WHERE program_id = $1 AND status = 'RESERVED'`,
      [input.programId]
    );
    const localReserved = new Decimal(ledgerSum.rows[0].sum);
    const treasuryReserved = new Decimal(input.reserved);
    if (localReserved.sub(treasuryReserved).abs().gt(DIVERGENCE_THRESHOLD)) {
      logger.warn('reconciliation diverges from local ledger', {
        programId: input.programId,
        localReserved: localReserved.toFixed(2),
        treasuryReserved: treasuryReserved.toFixed(2),
      });
    }

    await client.query(
      'UPDATE programs SET total_limit = $1, reserved = $2, updated_at = $3, version = version + 1 WHERE id = $4',
      [input.totalLimit, input.reserved, input.asOf, input.programId]
    );
    await markProcessed(client, input.messageId);
    await client.query('COMMIT');
    return { applied: true, reason: 'applied' };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

export async function applyFxRateUpdate(
  pool: Pool,
  input: FxRateUpdateInput
): Promise<{ applied: boolean }> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    if (await isProcessed(client, input.messageId)) {
      await client.query('COMMIT');
      return { applied: false };
    }
    await client.query(
      `INSERT INTO fx_rates (base_currency, quote_currency, rate, updated_at)
       VALUES ($1, $2, $3, $4)
       ON CONFLICT (base_currency, quote_currency)
       DO UPDATE SET rate = excluded.rate, updated_at = excluded.updated_at
       WHERE fx_rates.updated_at < excluded.updated_at`,
      [input.base, input.quote, input.rate, input.timestamp]
    );
    await markProcessed(client, input.messageId);
    await client.query('COMMIT');
    return { applied: true };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}
