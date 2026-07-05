import type { Pool } from 'pg';
import Decimal from 'decimal.js';
import { lookupRate } from './fx';
import type { Program, Reservation } from './domain/types';

export class ProgramNotFoundError extends Error {
  constructor(public programId: string) {
    super('program_not_found');
    this.name = 'ProgramNotFoundError';
  }
}

export class ReservationNotFoundError extends Error {
  constructor(public reservationId: string) {
    super('reservation_not_found');
    this.name = 'ReservationNotFoundError';
  }
}

export class InsufficientCapacityError extends Error {
  constructor(public available: string, public requested: string) {
    super('insufficient_capacity');
    this.name = 'InsufficientCapacityError';
  }
}

export class InvoiceConflictError extends Error {
  constructor(public invoiceId: string) {
    super('invoice_already_reserved');
    this.name = 'InvoiceConflictError';
  }
}

export class InvalidAmountError extends Error {
  constructor(public amount: string) {
    super('invalid_amount');
    this.name = 'InvalidAmountError';
  }
}

export class InvalidCurrencyError extends Error {
  constructor(public currency: string) {
    super('invalid_currency');
    this.name = 'InvalidCurrencyError';
  }
}

export interface ReserveInput {
  programId: string;
  invoiceId: string;
  currency: string;
  amount: string;
}

export interface ReserveResult {
  reservation: Reservation;
  created: boolean;
}

interface ReservationRow {
  id: string;
  program_id: string;
  invoice_id: string;
  invoice_currency: string;
  invoice_amount: string;
  fx_rate_used: string;
  reserved_amount: string;
  status: 'RESERVED' | 'RELEASED';
  created_at: Date;
  released_at: Date | null;
}

interface ProgramRow {
  id: string;
  name: string;
  currency: string;
  total_limit: string;
  reserved: string;
  version: string | number;
  updated_at: Date;
}

function rowToReservation(row: ReservationRow): Reservation {
  return {
    id: row.id,
    programId: row.program_id,
    invoiceId: row.invoice_id,
    invoiceCurrency: row.invoice_currency,
    invoiceAmount: row.invoice_amount,
    fxRateUsed: row.fx_rate_used,
    reservedAmount: row.reserved_amount,
    status: row.status,
    createdAt: row.created_at,
    releasedAt: row.released_at,
  };
}

function rowToProgram(row: ProgramRow): Program {
  return {
    id: row.id,
    name: row.name,
    currency: row.currency,
    totalLimit: row.total_limit,
    reserved: row.reserved,
    version: Number(row.version),
    updatedAt: row.updated_at,
  };
}

export function availabilityOf(program: Program): string {
  return new Decimal(program.totalLimit).sub(program.reserved).toFixed(2);
}

function isPositiveDecimal(value: string): boolean {
  try {
    return new Decimal(value).gt(0);
  } catch {
    return false;
  }
}

const CURRENCY_CODE = /^[A-Z]{3}$/;

function isValidCurrencyCode(value: string): boolean {
  return CURRENCY_CODE.test(value);
}

function matchesExisting(row: ReservationRow, input: ReserveInput): boolean {
  const sameAmount = new Decimal(row.invoice_amount).cmp(input.amount) === 0;
  return row.invoice_currency === input.currency && sameAmount;
}

interface PgUniqueViolation extends Error {
  code: '23505';
}

function isUniqueViolation(err: unknown): err is PgUniqueViolation {
  return typeof err === 'object' && err !== null && (err as { code?: unknown }).code === '23505';
}

// Two concurrent first-time requests for the same (programId, invoiceId) can
// both pass the pre-check below and both attempt the INSERT; the loser hits
// the DB's UNIQUE(program_id, invoice_id) constraint instead of a graceful
// idempotent return. Resolve it the same way the pre-check would have, using
// the row the winning transaction just committed.
async function resolveConcurrentDuplicate(pool: Pool, input: ReserveInput): Promise<ReserveResult> {
  const result = await pool.query<ReservationRow>(
    'SELECT * FROM reservations WHERE program_id = $1 AND invoice_id = $2',
    [input.programId, input.invoiceId]
  );
  const row = result.rows[0];
  if (!row || !matchesExisting(row, input)) {
    throw new InvoiceConflictError(input.invoiceId);
  }
  return { reservation: rowToReservation(row), created: false };
}

export async function reserveCapacity(pool: Pool, input: ReserveInput): Promise<ReserveResult> {
  if (!isValidCurrencyCode(input.currency)) {
    throw new InvalidCurrencyError(input.currency);
  }
  if (!isPositiveDecimal(input.amount)) {
    throw new InvalidAmountError(input.amount);
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const existing = await client.query(
      'SELECT * FROM reservations WHERE program_id = $1 AND invoice_id = $2',
      [input.programId, input.invoiceId]
    );
    if (existing.rows.length > 0) {
      const row = existing.rows[0];
      if (!matchesExisting(row, input)) {
        throw new InvoiceConflictError(input.invoiceId);
      }
      await client.query('COMMIT');
      return { reservation: rowToReservation(row), created: false };
    }

    const programResult = await client.query(
      'SELECT * FROM programs WHERE id = $1 FOR UPDATE',
      [input.programId]
    );
    if (programResult.rows.length === 0) {
      throw new ProgramNotFoundError(input.programId);
    }
    const program = rowToProgram(programResult.rows[0]);

    const rate = await lookupRate(client, input.currency, program.currency);
    const convertedAmount = new Decimal(input.amount).mul(rate);
    const available = new Decimal(program.totalLimit).sub(program.reserved);

    if (convertedAmount.gt(available)) {
      throw new InsufficientCapacityError(available.toFixed(2), convertedAmount.toFixed(2));
    }

    const inserted = await client.query(
      `INSERT INTO reservations
        (program_id, invoice_id, invoice_currency, invoice_amount, fx_rate_used, reserved_amount, status)
       VALUES ($1, $2, $3, $4, $5, $6, 'RESERVED')
       RETURNING *`,
      [input.programId, input.invoiceId, input.currency, input.amount, rate, convertedAmount.toFixed(2)]
    );

    await client.query(
      'UPDATE programs SET reserved = reserved + $1, version = version + 1, updated_at = now() WHERE id = $2',
      [convertedAmount.toFixed(2), input.programId]
    );

    await client.query('COMMIT');
    return { reservation: rowToReservation(inserted.rows[0]), created: true };
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    if (!isUniqueViolation(err)) {
      throw err;
    }
  } finally {
    client.release();
  }

  // Reaching here means the catch block swallowed a concurrent-duplicate
  // unique-violation (the only case it doesn't rethrow) — resolve it below.
  return resolveConcurrentDuplicate(pool, input);
}

export async function releaseReservation(pool: Pool, reservationId: string): Promise<Reservation> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const resResult = await client.query(
      'SELECT * FROM reservations WHERE id = $1 FOR UPDATE',
      [reservationId]
    );
    if (resResult.rows.length === 0) {
      throw new ReservationNotFoundError(reservationId);
    }
    const reservation = rowToReservation(resResult.rows[0]);

    if (reservation.status === 'RELEASED') {
      await client.query('COMMIT');
      return reservation;
    }

    await client.query(
      'UPDATE programs SET reserved = reserved - $1, version = version + 1, updated_at = now() WHERE id = $2',
      [reservation.reservedAmount, reservation.programId]
    );

    const updated = await client.query(
      `UPDATE reservations SET status = 'RELEASED', released_at = now() WHERE id = $1 RETURNING *`,
      [reservationId]
    );

    await client.query('COMMIT');
    return rowToReservation(updated.rows[0]);
  } catch (err) {
    await client.query('ROLLBACK').catch(() => {});
    throw err;
  } finally {
    client.release();
  }
}

export async function getProgram(pool: Pool, programId: string): Promise<Program> {
  const result = await pool.query('SELECT * FROM programs WHERE id = $1', [programId]);
  if (result.rows.length === 0) throw new ProgramNotFoundError(programId);
  return rowToProgram(result.rows[0]);
}

export async function getAvailability(
  pool: Pool,
  programId: string
): Promise<{ available: string; currency: string }> {
  const program = await getProgram(pool, programId);
  return { available: availabilityOf(program), currency: program.currency };
}

export async function getReservation(pool: Pool, reservationId: string): Promise<Reservation> {
  const result = await pool.query('SELECT * FROM reservations WHERE id = $1', [reservationId]);
  if (result.rows.length === 0) throw new ReservationNotFoundError(reservationId);
  return rowToReservation(result.rows[0]);
}
