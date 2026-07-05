import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type { Pool } from 'pg';
import { createTestPool, applyMigrations, resetSchema } from './testDb';
import { isProcessed, markProcessed } from '../src/idempotency';

describe('idempotency', () => {
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

  it('reports unprocessed messages as not processed', async () => {
    const client = await pool.connect();
    try {
      expect(await isProcessed(client, 'msg-1')).toBe(false);
    } finally {
      client.release();
    }
  });

  it('reports a marked message as processed, and marking twice is safe', async () => {
    const client = await pool.connect();
    try {
      await markProcessed(client, 'msg-1');
      await markProcessed(client, 'msg-1');
      expect(await isProcessed(client, 'msg-1')).toBe(true);
    } finally {
      client.release();
    }
  });
});
