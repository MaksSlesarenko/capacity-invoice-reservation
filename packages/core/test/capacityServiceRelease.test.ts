import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type { Pool } from 'pg';
import { createTestPool, applyMigrations, resetSchema } from './testDb';
import {
  reserveCapacity,
  releaseReservation,
  getProgram,
  getAvailability,
  getReservation,
  ReservationNotFoundError,
  ProgramNotFoundError,
} from '../src/capacityService';

const PROGRAM_ID = '11111111-1111-1111-1111-111111111111';

async function seedProgram(pool: Pool) {
  await pool.query(
    `INSERT INTO programs (id, name, currency, total_limit, reserved)
     VALUES ($1, 'Test Program', 'USD', '1000.00', 0)`,
    [PROGRAM_ID]
  );
}

describe('releaseReservation and reads', () => {
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

  it('releases a reservation and frees capacity', async () => {
    await seedProgram(pool);
    const { reservation } = await reserveCapacity(pool, {
      programId: PROGRAM_ID, invoiceId: 'inv-1', currency: 'USD', amount: '400.00',
    });

    const released = await releaseReservation(pool, reservation.id);
    expect(released.status).toBe('RELEASED');
    expect(released.releasedAt).not.toBeNull();

    const availability = await getAvailability(pool, PROGRAM_ID);
    expect(availability.available).toBe('1000.00');
  });

  it('is a no-op releasing an already-released reservation', async () => {
    await seedProgram(pool);
    const { reservation } = await reserveCapacity(pool, {
      programId: PROGRAM_ID, invoiceId: 'inv-2', currency: 'USD', amount: '400.00',
    });
    await releaseReservation(pool, reservation.id);
    const secondRelease = await releaseReservation(pool, reservation.id);

    expect(secondRelease.status).toBe('RELEASED');
    const availability = await getAvailability(pool, PROGRAM_ID);
    expect(availability.available).toBe('1000.00');
  });

  it('throws ReservationNotFoundError for an unknown reservation', async () => {
    await expect(releaseReservation(pool, '00000000-0000-0000-0000-000000000000')).rejects.toThrow(
      ReservationNotFoundError
    );
  });

  it('getProgram throws ProgramNotFoundError for an unknown program', async () => {
    await expect(getProgram(pool, '00000000-0000-0000-0000-000000000000')).rejects.toThrow(
      ProgramNotFoundError
    );
  });

  it('getReservation returns the stored reservation', async () => {
    await seedProgram(pool);
    const { reservation } = await reserveCapacity(pool, {
      programId: PROGRAM_ID, invoiceId: 'inv-3', currency: 'USD', amount: '50.00',
    });
    const fetched = await getReservation(pool, reservation.id);
    expect(fetched.invoiceId).toBe('inv-3');
  });
});
