import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type { Pool } from 'pg';
import { createTestPool, applyMigrations, resetSchema } from '../../../packages/core/test/testDb';
import { buildApp } from '../src/server';
import { signToken } from '../src/auth/jwt';

const SECRET = 'test-secret';
const PROGRAM_ID = '11111111-1111-1111-1111-111111111111';

describe('API routes', () => {
  let pool: Pool;
  let app: ReturnType<typeof buildApp>;
  let token: string;

  beforeAll(async () => {
    pool = createTestPool();
    await applyMigrations(pool);
    app = buildApp({ pool, jwtSecret: SECRET, clients: { 'demo-client': 'demo-secret' } });
    token = signToken({ clientId: 'demo-client' }, SECRET);
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
    await app.close();
    await pool.end();
  });

  function authed(opts: { method: 'GET' | 'POST'; url: string; payload?: unknown }) {
    return app.inject({ ...opts, headers: { authorization: `Bearer ${token}` } });
  }

  it('rejects unauthenticated requests', async () => {
    const response = await app.inject({ method: 'GET', url: `/programs/${PROGRAM_ID}` });
    expect(response.statusCode).toBe(401);
  });

  it('returns program state', async () => {
    const response = await authed({ method: 'GET', url: `/programs/${PROGRAM_ID}` });
    expect(response.statusCode).toBe(200);
    expect(response.json().available).toBe('1000.00');
  });

  it('returns 404 for an unknown program', async () => {
    const response = await authed({ method: 'GET', url: `/programs/00000000-0000-0000-0000-000000000000` });
    expect(response.statusCode).toBe(404);
  });

  it('creates a reservation and reflects it in availability', async () => {
    const create = await authed({
      method: 'POST',
      url: `/programs/${PROGRAM_ID}/reservations`,
      payload: { invoiceId: 'inv-1', currency: 'USD', amount: '400.00' },
    });
    expect(create.statusCode).toBe(201);
    const reservationId = create.json().id;

    const availability = await authed({ method: 'GET', url: `/programs/${PROGRAM_ID}/availability` });
    expect(availability.json().available).toBe('600.00');

    const release = await authed({ method: 'POST', url: `/reservations/${reservationId}/release` });
    expect(release.statusCode).toBe(200);

    const afterRelease = await authed({ method: 'GET', url: `/programs/${PROGRAM_ID}/availability` });
    expect(afterRelease.json().available).toBe('1000.00');
  });

  it('returns 409 with availability details when capacity is insufficient', async () => {
    const response = await authed({
      method: 'POST',
      url: `/programs/${PROGRAM_ID}/reservations`,
      payload: { invoiceId: 'inv-2', currency: 'USD', amount: '5000.00' },
    });
    expect(response.statusCode).toBe(409);
    expect(response.json()).toEqual({ error: 'insufficient_capacity', available: '1000.00', requested: '5000.00' });
  });

  it('returns 422 when the FX rate is unavailable', async () => {
    const response = await authed({
      method: 'POST',
      url: `/programs/${PROGRAM_ID}/reservations`,
      payload: { invoiceId: 'inv-3', currency: 'JPY', amount: '100.00' },
    });
    expect(response.statusCode).toBe(422);
  });

  it('returns 404 releasing an unknown reservation', async () => {
    const response = await authed({
      method: 'POST',
      url: `/reservations/00000000-0000-0000-0000-000000000000/release`,
    });
    expect(response.statusCode).toBe(404);
  });

  it('returns 400 for a negative reservation amount', async () => {
    const response = await authed({
      method: 'POST',
      url: `/programs/${PROGRAM_ID}/reservations`,
      payload: { invoiceId: 'inv-4', currency: 'USD', amount: '-100.00' },
    });
    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'invalid_amount' });

    const availability = await authed({ method: 'GET', url: `/programs/${PROGRAM_ID}/availability` });
    expect(availability.json().available).toBe('1000.00');
  });

  it('returns 400 for a malformed currency code', async () => {
    const response = await authed({
      method: 'POST',
      url: `/programs/${PROGRAM_ID}/reservations`,
      payload: { invoiceId: 'inv-5', currency: 'usd', amount: '100.00' },
    });
    expect(response.statusCode).toBe(400);
    expect(response.json()).toEqual({ error: 'invalid_currency' });
  });
});
