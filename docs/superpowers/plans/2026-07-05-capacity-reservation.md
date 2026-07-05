# Program Capacity & Invoice Reservation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Node/TypeScript service that tracks per-program financing capacity, accepts/releases invoice reservations over an authenticated HTTP API, and ingests capacity/FX state from Kafka (including bulk reconciliation), with multi-currency support and a local docker-compose run.

**Architecture:** npm workspaces monorepo — `packages/core` (DB access, `CapacityService`, FX lookup, Kafka message handlers, all money math via `decimal.js`) is shared by two thin process entrypoints: `apps/api` (Fastify HTTP server, JWT auth) and `apps/consumer` (kafkajs consumer). Both talk to the same Postgres instance; correctness under concurrency comes from row-level locking (`SELECT ... FOR UPDATE`) inside `packages/core`, not from anything in the process layer.

**Tech Stack:** TypeScript (strict), Node 20, npm workspaces, Fastify, kafkajs, `pg` (raw SQL, no ORM), `decimal.js`, `jsonwebtoken`, Postgres 16, Redpanda (local Kafka), vitest.

## Global Constraints

- TypeScript strict mode everywhere (`tsconfig.base.json`).
- All money/decimal arithmetic goes through `decimal.js` — never raw JS `number` for amounts, rates, or capacity math (floating-point rounding would corrupt capacity tracking).
- All SQL is raw parameterized queries via `pg` — no ORM. `SELECT ... FOR UPDATE` is required on every code path that reads-then-writes a `programs` row.
- Every Kafka-consumed message is deduped via `processed_messages` inside the same DB transaction as its state mutation (at-least-once delivery → effectively-once application).
- Programs are seeded via `scripts/seed.ts`, never created through the HTTP API.
- All HTTP endpoints except `POST /auth/token` require a valid `Authorization: Bearer <jwt>` header (HS256, secret from `JWT_SECRET` env var).
- Run via `docker compose up` locally: `postgres`, `redpanda`, `api`, `consumer`.
- Tests use a real Postgres (`capacity_test` database) — no mocked DB layer, since lock semantics can't be faithfully mocked.

---

## File Structure

```
package.json                          # npm workspaces root
tsconfig.base.json
vitest.config.ts
docker-compose.yml
.env.example
db/init/00-databases.sql              # creates capacity_test alongside capacity
scripts/migrate.ts
scripts/seed.ts

packages/core/
  package.json
  tsconfig.json
  src/
    db/
      pool.ts
      migrations/001_init.sql
    domain/types.ts
    fx.ts
    idempotency.ts
    capacityService.ts
    kafkaHandlers.ts
    index.ts
  test/
    testDb.ts
    fx.test.ts
    idempotency.test.ts
    capacityService.test.ts
    capacityService.concurrency.test.ts
    kafkaHandlers.test.ts
    seed.test.ts

apps/api/
  package.json
  tsconfig.json
  Dockerfile
  src/
    auth/jwt.ts
    auth/middleware.ts
    routes/auth.ts
    routes/programs.ts
    routes/reservations.ts
    server.ts
    index.ts
  test/
    auth.test.ts
    routes.test.ts

apps/consumer/
  package.json
  tsconfig.json
  Dockerfile
  src/
    router.ts
    consumer.ts
    index.ts
  test/
    router.test.ts

README.md
```

---

### Task 1: Monorepo scaffolding + Postgres/Redpanda local infra

**Files:**
- Create: `package.json`
- Create: `tsconfig.base.json`
- Create: `vitest.config.ts`
- Create: `.env.example`
- Create: `.gitignore`
- Create: `docker-compose.yml`
- Create: `db/init/00-databases.sql`
- Create: `packages/core/package.json`
- Create: `packages/core/tsconfig.json`
- Create: `apps/api/package.json`
- Create: `apps/api/tsconfig.json`
- Create: `apps/consumer/package.json`
- Create: `apps/consumer/tsconfig.json`

**Interfaces:**
- Produces: workspace resolution so `apps/api` and `apps/consumer` can `import ... from '@capacity/core'`; `docker compose up -d postgres redpanda` gives every later task a real DB/broker to run against.

- [ ] **Step 1: Root package.json**

```json
{
  "name": "capacity-reservation",
  "private": true,
  "workspaces": ["packages/*", "apps/*"],
  "scripts": {
    "test": "vitest run",
    "migrate": "tsx scripts/migrate.ts",
    "seed": "tsx scripts/seed.ts"
  },
  "devDependencies": {
    "typescript": "^5.5.4",
    "vitest": "^2.0.5",
    "tsx": "^4.16.5",
    "@types/node": "^20.14.15"
  }
}
```

- [ ] **Step 2: tsconfig.base.json**

```json
{
  "compilerOptions": {
    "target": "ES2022",
    "module": "commonjs",
    "moduleResolution": "node",
    "strict": true,
    "esModuleInterop": true,
    "skipLibCheck": true,
    "resolveJsonModule": true,
    "declaration": false
  }
}
```

- [ ] **Step 3: vitest.config.ts**

```ts
import { defineConfig } from 'vitest/config';

export default defineConfig({
  test: {
    include: ['packages/**/test/**/*.test.ts', 'apps/**/test/**/*.test.ts'],
    testTimeout: 20000,
    hookTimeout: 20000,
  },
});
```

- [ ] **Step 4: .env.example**

```
DATABASE_URL=postgres://postgres:postgres@localhost:5432/capacity
TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/capacity_test
KAFKA_BROKERS=localhost:9092
JWT_SECRET=dev-secret-change-me
CLIENTS_JSON={"demo-client":"demo-secret"}
PORT=3000
```

- [ ] **Step 5: .gitignore**

```
node_modules/
dist/
.env
```

- [ ] **Step 6: docker-compose.yml**

```yaml
version: '3.8'
services:
  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: capacity
    ports:
      - '5432:5432'
    volumes:
      - ./db/init:/docker-entrypoint-initdb.d
      - pgdata:/var/lib/postgresql/data

  redpanda:
    image: docker.redpanda.com/redpandadata/redpanda:v24.2.4
    command:
      - redpanda
      - start
      - --smp=1
      - --overprovisioned
      - --node-id=0
      - --kafka-addr=PLAINTEXT://0.0.0.0:9092
      - --advertise-kafka-addr=PLAINTEXT://redpanda:9092
    ports:
      - '9092:9092'

  api:
    build:
      context: .
      dockerfile: apps/api/Dockerfile
    environment:
      DATABASE_URL: postgres://postgres:postgres@postgres:5432/capacity
      JWT_SECRET: dev-secret-change-me
      CLIENTS_JSON: '{"demo-client":"demo-secret"}'
      PORT: '3000'
    ports:
      - '3000:3000'
    depends_on:
      - postgres

  consumer:
    build:
      context: .
      dockerfile: apps/consumer/Dockerfile
    environment:
      DATABASE_URL: postgres://postgres:postgres@postgres:5432/capacity
      KAFKA_BROKERS: redpanda:9092
    depends_on:
      - postgres
      - redpanda

volumes:
  pgdata:
```

- [ ] **Step 7: db/init/00-databases.sql**

```sql
CREATE DATABASE capacity_test;
```

- [ ] **Step 8: packages/core/package.json**

```json
{
  "name": "@capacity/core",
  "version": "0.1.0",
  "main": "src/index.ts",
  "types": "src/index.ts",
  "dependencies": {
    "pg": "^8.12.0",
    "decimal.js": "^10.4.3"
  },
  "devDependencies": {
    "@types/pg": "^8.11.6"
  }
}
```

- [ ] **Step 9: packages/core/tsconfig.json**

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "rootDir": "src", "outDir": "dist" },
  "include": ["src"]
}
```

- [ ] **Step 10: apps/api/package.json**

```json
{
  "name": "@capacity/api",
  "version": "0.1.0",
  "dependencies": {
    "@capacity/core": "*",
    "fastify": "^4.28.1",
    "jsonwebtoken": "^9.0.2"
  },
  "devDependencies": {
    "@types/jsonwebtoken": "^9.0.6"
  }
}
```

- [ ] **Step 11: apps/api/tsconfig.json**

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "rootDir": "src", "outDir": "dist" },
  "include": ["src"]
}
```

- [ ] **Step 12: apps/consumer/package.json**

```json
{
  "name": "@capacity/consumer",
  "version": "0.1.0",
  "dependencies": {
    "@capacity/core": "*",
    "kafkajs": "^2.2.4"
  }
}
```

- [ ] **Step 13: apps/consumer/tsconfig.json**

```json
{
  "extends": "../../tsconfig.base.json",
  "compilerOptions": { "rootDir": "src", "outDir": "dist" },
  "include": ["src"]
}
```

- [ ] **Step 14: Install and verify infra boots**

```bash
npm install
docker compose up -d postgres redpanda
docker compose ps
```

Expected: `npm install` completes with a single root `node_modules` and symlinked workspace packages; `docker compose ps` shows `postgres` and `redpanda` both `Up`.

- [ ] **Step 15: Commit**

```bash
git add package.json tsconfig.base.json vitest.config.ts .env.example .gitignore docker-compose.yml db/init packages/core/package.json packages/core/tsconfig.json apps/api/package.json apps/api/tsconfig.json apps/consumer/package.json apps/consumer/tsconfig.json
git commit -m "Scaffold monorepo workspaces and local Postgres/Redpanda infra"
```

---

### Task 2: Schema migration + migration runner + test DB helper

**Files:**
- Create: `packages/core/src/db/migrations/001_init.sql`
- Create: `packages/core/src/db/pool.ts`
- Create: `scripts/migrate.ts`
- Create: `packages/core/test/testDb.ts`
- Test: `packages/core/test/testDb.ts` (exercised directly, no separate test file — see Step 3)

**Interfaces:**
- Produces: `createPool(connectionString: string): Pool` from `packages/core/src/db/pool.ts`.
- Produces: `createTestPool(): Pool`, `applyMigrations(pool: Pool): Promise<void>`, `resetSchema(pool: Pool): Promise<void>` from `packages/core/test/testDb.ts` — every later test file uses these three.

- [ ] **Step 1: Write the migration SQL**

`packages/core/src/db/migrations/001_init.sql`:

```sql
CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE IF NOT EXISTS programs (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  name text NOT NULL,
  currency char(3) NOT NULL,
  total_limit numeric(18,2) NOT NULL,
  reserved numeric(18,2) NOT NULL DEFAULT 0,
  version bigint NOT NULL DEFAULT 0,
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS reservations (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  program_id uuid NOT NULL REFERENCES programs(id),
  invoice_id text NOT NULL,
  invoice_currency char(3) NOT NULL,
  invoice_amount numeric(18,2) NOT NULL,
  fx_rate_used numeric(18,8) NOT NULL,
  reserved_amount numeric(18,2) NOT NULL,
  status text NOT NULL CHECK (status IN ('RESERVED','RELEASED')),
  created_at timestamptz NOT NULL DEFAULT now(),
  released_at timestamptz,
  UNIQUE (program_id, invoice_id)
);

CREATE TABLE IF NOT EXISTS fx_rates (
  base_currency char(3) NOT NULL,
  quote_currency char(3) NOT NULL,
  rate numeric(18,8) NOT NULL,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (base_currency, quote_currency)
);

CREATE TABLE IF NOT EXISTS processed_messages (
  message_id text PRIMARY KEY,
  processed_at timestamptz NOT NULL DEFAULT now()
);
```

- [ ] **Step 2: DB pool helper**

`packages/core/src/db/pool.ts`:

```ts
import { Pool } from 'pg';

export function createPool(connectionString: string): Pool {
  return new Pool({ connectionString });
}
```

- [ ] **Step 3: Test DB helper (also serves as the migration smoke test)**

`packages/core/test/testDb.ts`:

```ts
import fs from 'node:fs';
import path from 'node:path';
import { Pool } from 'pg';

export function createTestPool(): Pool {
  return new Pool({
    connectionString:
      process.env.TEST_DATABASE_URL ?? 'postgres://postgres:postgres@localhost:5432/capacity_test',
  });
}

export async function applyMigrations(pool: Pool): Promise<void> {
  const sql = fs.readFileSync(
    path.join(__dirname, '../src/db/migrations/001_init.sql'),
    'utf-8'
  );
  await pool.query(sql);
}

export async function resetSchema(pool: Pool): Promise<void> {
  await pool.query(
    'TRUNCATE programs, reservations, fx_rates, processed_messages CASCADE'
  );
}
```

Add a real test file that exercises it, since every subsequent task depends on this working correctly:

`packages/core/test/migration.test.ts`:

```ts
import { describe, it, expect, beforeAll, afterAll } from 'vitest';
import { createTestPool, applyMigrations } from './testDb';
import type { Pool } from 'pg';

describe('migrations', () => {
  let pool: Pool;

  beforeAll(async () => {
    pool = createTestPool();
    await applyMigrations(pool);
  });

  afterAll(async () => {
    await pool.end();
  });

  it('creates all four tables', async () => {
    const result = await pool.query(
      `SELECT table_name FROM information_schema.tables
       WHERE table_schema = 'public' ORDER BY table_name`
    );
    const names = result.rows.map((r) => r.table_name);
    expect(names).toEqual(
      expect.arrayContaining(['programs', 'reservations', 'fx_rates', 'processed_messages'])
    );
  });
});
```

- [ ] **Step 4: Run to verify it passes**

Run: `TEST_DATABASE_URL=postgres://postgres:postgres@localhost:5432/capacity_test npx vitest run packages/core/test/migration.test.ts`
Expected: PASS (requires `docker compose up -d postgres` from Task 1 already running).

- [ ] **Step 5: Migration runner script for dev/prod DB**

`scripts/migrate.ts`:

```ts
import fs from 'node:fs';
import path from 'node:path';
import { Pool } from 'pg';

async function main() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  const sql = fs.readFileSync(
    path.join(__dirname, '../packages/core/src/db/migrations/001_init.sql'),
    'utf-8'
  );
  await pool.query(sql);
  await pool.end();
  console.log('migration applied');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

- [ ] **Step 6: Commit**

```bash
git add packages/core/src/db packages/core/test/testDb.ts packages/core/test/migration.test.ts scripts/migrate.ts
git commit -m "Add schema migration, DB pool helper, and test DB harness"
```

---

### Task 3: FX rate lookup

**Files:**
- Create: `packages/core/src/fx.ts`
- Test: `packages/core/test/fx.test.ts`

**Interfaces:**
- Consumes: `createTestPool`, `applyMigrations`, `resetSchema` from `packages/core/test/testDb.ts`.
- Produces: `lookupRate(client: PoolClient, base: string, quote: string): Promise<string>`, `class FxRateUnavailableError extends Error { base: string; quote: string }` — used by `capacityService.ts` (Task 5) and `kafkaHandlers.ts` (Task 8).

- [ ] **Step 1: Write the failing test**

`packages/core/test/fx.test.ts`:

```ts
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run packages/core/test/fx.test.ts`
Expected: FAIL — `Cannot find module '../src/fx'`

- [ ] **Step 3: Implement**

`packages/core/src/fx.ts`:

```ts
import type { PoolClient } from 'pg';

export class FxRateUnavailableError extends Error {
  constructor(public base: string, public quote: string) {
    super('fx_rate_unavailable');
    this.name = 'FxRateUnavailableError';
  }
}

export async function lookupRate(
  client: PoolClient,
  base: string,
  quote: string
): Promise<string> {
  if (base === quote) return '1';

  const result = await client.query(
    'SELECT rate FROM fx_rates WHERE base_currency = $1 AND quote_currency = $2',
    [base, quote]
  );
  if (result.rows.length === 0) {
    throw new FxRateUnavailableError(base, quote);
  }
  return result.rows[0].rate;
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run packages/core/test/fx.test.ts`
Expected: PASS (3 tests)

- [ ] **Step 5: Commit**

```bash
git add packages/core/src/fx.ts packages/core/test/fx.test.ts
git commit -m "Add FX rate lookup"
```

---

### Task 4: Kafka message idempotency helper

**Files:**
- Create: `packages/core/src/idempotency.ts`
- Test: `packages/core/test/idempotency.test.ts`

**Interfaces:**
- Produces: `isProcessed(client: PoolClient, messageId: string): Promise<boolean>`, `markProcessed(client: PoolClient, messageId: string): Promise<void>` — used by `kafkaHandlers.ts` (Task 8).

- [ ] **Step 1: Write the failing test**

`packages/core/test/idempotency.test.ts`:

```ts
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run packages/core/test/idempotency.test.ts`
Expected: FAIL — `Cannot find module '../src/idempotency'`

- [ ] **Step 3: Implement**

`packages/core/src/idempotency.ts`:

```ts
import type { PoolClient } from 'pg';

export async function isProcessed(client: PoolClient, messageId: string): Promise<boolean> {
  const result = await client.query(
    'SELECT 1 FROM processed_messages WHERE message_id = $1',
    [messageId]
  );
  return result.rows.length > 0;
}

export async function markProcessed(client: PoolClient, messageId: string): Promise<void> {
  await client.query(
    'INSERT INTO processed_messages (message_id) VALUES ($1) ON CONFLICT DO NOTHING',
    [messageId]
  );
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run packages/core/test/idempotency.test.ts`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add packages/core/src/idempotency.ts packages/core/test/idempotency.test.ts
git commit -m "Add Kafka message idempotency helper"
```

---

### Task 5: CapacityService — reserveCapacity

**Files:**
- Create: `packages/core/src/domain/types.ts`
- Create: `packages/core/src/capacityService.ts`
- Test: `packages/core/test/capacityService.test.ts`

**Interfaces:**
- Consumes: `lookupRate`, `FxRateUnavailableError` from `../src/fx`.
- Produces:
  - `interface Program { id, name, currency, totalLimit, reserved, version, updatedAt }`
  - `interface Reservation { id, programId, invoiceId, invoiceCurrency, invoiceAmount, fxRateUsed, reservedAmount, status, createdAt, releasedAt }`
  - `class ProgramNotFoundError`, `class InsufficientCapacityError { available, requested }`, `class InvoiceConflictError`
  - `reserveCapacity(pool: Pool, input: { programId, invoiceId, currency, amount }): Promise<{ reservation: Reservation; created: boolean }>`
  - These are consumed by Task 6 (release/read functions), Task 7 (concurrency test), and Task 11 (API routes).

- [ ] **Step 1: Domain types**

`packages/core/src/domain/types.ts`:

```ts
export type ReservationStatus = 'RESERVED' | 'RELEASED';

export interface Program {
  id: string;
  name: string;
  currency: string;
  totalLimit: string;
  reserved: string;
  version: number;
  updatedAt: Date;
}

export interface Reservation {
  id: string;
  programId: string;
  invoiceId: string;
  invoiceCurrency: string;
  invoiceAmount: string;
  fxRateUsed: string;
  reservedAmount: string;
  status: ReservationStatus;
  createdAt: Date;
  releasedAt: Date | null;
}
```

- [ ] **Step 2: Write the failing test**

`packages/core/test/capacityService.test.ts`:

```ts
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type { Pool } from 'pg';
import { createTestPool, applyMigrations, resetSchema } from './testDb';
import {
  reserveCapacity,
  ProgramNotFoundError,
  InsufficientCapacityError,
  InvoiceConflictError,
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
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `npx vitest run packages/core/test/capacityService.test.ts`
Expected: FAIL — `Cannot find module '../src/capacityService'`

- [ ] **Step 4: Implement reserveCapacity**

`packages/core/src/capacityService.ts`:

```ts
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

function rowToReservation(row: any): Reservation {
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

function rowToProgram(row: any): Program {
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

export async function reserveCapacity(pool: Pool, input: ReserveInput): Promise<ReserveResult> {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');

    const existing = await client.query(
      'SELECT * FROM reservations WHERE program_id = $1 AND invoice_id = $2',
      [input.programId, input.invoiceId]
    );
    if (existing.rows.length > 0) {
      const row = existing.rows[0];
      const sameAmount = new Decimal(row.invoice_amount).cmp(input.amount) === 0;
      if (row.invoice_currency !== input.currency || !sameAmount) {
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
    throw err;
  } finally {
    client.release();
  }
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `npx vitest run packages/core/test/capacityService.test.ts`
Expected: PASS (7 tests)

- [ ] **Step 6: Commit**

```bash
git add packages/core/src/domain packages/core/src/capacityService.ts packages/core/test/capacityService.test.ts
git commit -m "Add reserveCapacity with FX conversion and idempotent invoice handling"
```

---

### Task 6: CapacityService — releaseReservation and read functions

**Files:**
- Modify: `packages/core/src/capacityService.ts`
- Test: `packages/core/test/capacityServiceRelease.test.ts`

**Interfaces:**
- Consumes: everything from Task 5 (`rowToReservation`, `rowToProgram`, errors, types — same file).
- Produces: `releaseReservation(pool: Pool, reservationId: string): Promise<Reservation>`, `getProgram(pool: Pool, programId: string): Promise<Program>`, `getAvailability(pool: Pool, programId: string): Promise<{ available: string; currency: string }>`, `getReservation(pool: Pool, reservationId: string): Promise<Reservation>` — used by Task 11 (API routes).

- [ ] **Step 1: Write the failing test**

`packages/core/test/capacityServiceRelease.test.ts`:

```ts
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run packages/core/test/capacityServiceRelease.test.ts`
Expected: FAIL — `releaseReservation is not a function` (or similar import error)

- [ ] **Step 3: Implement, appending to `packages/core/src/capacityService.ts`**

Add to the end of `packages/core/src/capacityService.ts`:

```ts
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
  const available = new Decimal(program.totalLimit).sub(program.reserved).toFixed(2);
  return { available, currency: program.currency };
}

export async function getReservation(pool: Pool, reservationId: string): Promise<Reservation> {
  const result = await pool.query('SELECT * FROM reservations WHERE id = $1', [reservationId]);
  if (result.rows.length === 0) throw new ReservationNotFoundError(reservationId);
  return rowToReservation(result.rows[0]);
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run packages/core/test/capacityServiceRelease.test.ts`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add packages/core/src/capacityService.ts packages/core/test/capacityServiceRelease.test.ts
git commit -m "Add releaseReservation and program/reservation read functions"
```

---

### Task 7: Concurrency correctness test for reserveCapacity

**Files:**
- Test: `packages/core/test/capacityService.concurrency.test.ts`

**Interfaces:**
- Consumes: `reserveCapacity`, `getProgram` from `../src/capacityService` (Task 5/6) — no production code changes expected; this task exists to prove the `FOR UPDATE` locking in Task 5 is actually correct under contention.

- [ ] **Step 1: Write the test**

`packages/core/test/capacityService.concurrency.test.ts`:

```ts
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
```

- [ ] **Step 2: Run test**

Run: `npx vitest run packages/core/test/capacityService.concurrency.test.ts`
Expected: PASS. If this fails with `reserved` exceeding `1000.00`, the `SELECT ... FOR UPDATE` in Task 5's `reserveCapacity` is not actually serializing concurrent transactions — check that the lock is acquired before the availability check and that every write path goes through it.

- [ ] **Step 3: Commit**

```bash
git add packages/core/test/capacityService.concurrency.test.ts
git commit -m "Add concurrency test proving reserveCapacity never over-reserves"
```

---

### Task 8: Kafka message handlers (adjustment, reconciliation, FX rate update)

**Files:**
- Create: `packages/core/src/kafkaHandlers.ts`
- Test: `packages/core/test/kafkaHandlers.test.ts`

**Interfaces:**
- Consumes: `isProcessed`, `markProcessed` from `./idempotency`; `reserveCapacity` (for test setup only) from `./capacityService`.
- Produces:
  - `applyCapacityAdjustment(pool: Pool, input: { messageId, programId, deltaAmount }): Promise<{ applied: boolean }>`
  - `applyReconciliation(pool: Pool, input: { messageId, programId, totalLimit, reserved, asOf: Date }, logger?): Promise<{ applied: boolean; reason?: 'already_processed' | 'stale' | 'applied' }>`
  - `applyFxRateUpdate(pool: Pool, input: { messageId, base, quote, rate, timestamp: Date }): Promise<{ applied: boolean }>`
  - Used by `apps/consumer/src/router.ts` (Task 12).

- [ ] **Step 1: Write the failing test**

`packages/core/test/kafkaHandlers.test.ts`:

```ts
import { describe, it, expect, beforeAll, afterAll, beforeEach, vi } from 'vitest';
import type { Pool } from 'pg';
import { createTestPool, applyMigrations, resetSchema } from './testDb';
import { applyCapacityAdjustment, applyReconciliation, applyFxRateUpdate } from '../src/kafkaHandlers';
import { getProgram, reserveCapacity } from '../src/capacityService';

const PROGRAM_ID = '11111111-1111-1111-1111-111111111111';

async function seedProgram(pool: Pool) {
  await pool.query(
    `INSERT INTO programs (id, name, currency, total_limit, reserved, updated_at)
     VALUES ($1, 'Test Program', 'USD', '1000.00', 0, '2026-01-01T00:00:00Z')`,
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
        { messageId: 'rec-5', programId: PROGRAM_ID, totalLimit: '1000.00', reserved: '900.00', asOf: new Date('2026-02-01T00:00:00Z') },
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run packages/core/test/kafkaHandlers.test.ts`
Expected: FAIL — `Cannot find module '../src/kafkaHandlers'`

- [ ] **Step 3: Implement**

`packages/core/src/kafkaHandlers.ts`:

```ts
import type { Pool } from 'pg';
import Decimal from 'decimal.js';
import { isProcessed, markProcessed } from './idempotency';

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
    await client.query(
      'UPDATE programs SET total_limit = total_limit + $1, version = version + 1, updated_at = now() WHERE id = $2',
      [input.deltaAmount, input.programId]
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
      throw new Error(`program_not_found:${input.programId}`);
    }
    const program = programResult.rows[0];

    if (input.asOf <= program.updated_at) {
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
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run packages/core/test/kafkaHandlers.test.ts`
Expected: PASS (8 tests)

- [ ] **Step 5: Commit**

```bash
git add packages/core/src/kafkaHandlers.ts packages/core/test/kafkaHandlers.test.ts
git commit -m "Add Kafka handlers for capacity adjustment, reconciliation, and FX rate updates"
```

---

### Task 9: Core package index + seed script

**Files:**
- Create: `packages/core/src/index.ts`
- Create: `scripts/seed.ts`
- Test: `packages/core/test/seed.test.ts`

**Interfaces:**
- Consumes: everything exported from `fx.ts`, `idempotency.ts`, `capacityService.ts`, `kafkaHandlers.ts`, `domain/types.ts`, `db/pool.ts`.
- Produces: `@capacity/core` package entrypoint — this is what `apps/api` and `apps/consumer` import from (Tasks 11–12).

- [ ] **Step 1: Core index**

`packages/core/src/index.ts`:

```ts
export * from './domain/types';
export * from './fx';
export * from './idempotency';
export * from './capacityService';
export * from './kafkaHandlers';
export { createPool } from './db/pool';
```

- [ ] **Step 2: Write the failing test for the seed script's SQL**

`packages/core/test/seed.test.ts` runs the same inserts the seed script uses, against the test DB, to prove they're valid and idempotent — the actual script (Step 4) is a thin CLI wrapper around this SQL.

```ts
import { describe, it, expect, beforeAll, afterAll, beforeEach } from 'vitest';
import type { Pool } from 'pg';
import { createTestPool, applyMigrations, resetSchema } from './testDb';
import { seedDemoData } from '../src/seedData';

describe('seedDemoData', () => {
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

  it('inserts demo programs and fx rates, and is safe to run twice', async () => {
    await seedDemoData(pool);
    await seedDemoData(pool);

    const programs = await pool.query('SELECT id, currency FROM programs ORDER BY name');
    expect(programs.rows.length).toBe(2);

    const rates = await pool.query('SELECT base_currency, quote_currency FROM fx_rates');
    expect(rates.rows.length).toBeGreaterThanOrEqual(4);
  });
});
```

- [ ] **Step 3: Run test to verify it fails**

Run: `npx vitest run packages/core/test/seed.test.ts`
Expected: FAIL — `Cannot find module '../src/seedData'`

- [ ] **Step 4: Implement seed data module and CLI script**

`packages/core/src/seedData.ts`:

```ts
import type { Pool } from 'pg';

export async function seedDemoData(pool: Pool): Promise<void> {
  await pool.query(`
    INSERT INTO programs (id, name, currency, total_limit, reserved)
    VALUES
      ('11111111-1111-1111-1111-111111111111', 'Acme Supplier Financing', 'USD', 10000000.00, 0),
      ('22222222-2222-2222-2222-222222222222', 'Global Trade Program', 'EUR', 5000000.00, 0)
    ON CONFLICT (id) DO NOTHING
  `);
  await pool.query(`
    INSERT INTO fx_rates (base_currency, quote_currency, rate)
    VALUES
      ('USD', 'EUR', 0.92),
      ('EUR', 'USD', 1.087),
      ('GBP', 'USD', 1.27),
      ('USD', 'GBP', 0.79)
    ON CONFLICT (base_currency, quote_currency) DO NOTHING
  `);
}
```

Add the export to `packages/core/src/index.ts`:

```ts
export { seedDemoData } from './seedData';
```

`scripts/seed.ts`:

```ts
import { Pool } from 'pg';
import { seedDemoData } from '../packages/core/src/seedData';

async function main() {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL });
  await seedDemoData(pool);
  await pool.end();
  console.log('seed complete');
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
```

- [ ] **Step 5: Run test to verify it passes**

Run: `npx vitest run packages/core/test/seed.test.ts`
Expected: PASS (1 test)

- [ ] **Step 6: Commit**

```bash
git add packages/core/src/index.ts packages/core/src/seedData.ts packages/core/test/seed.test.ts scripts/seed.ts
git commit -m "Add core package entrypoint and demo data seeding"
```

---

### Task 10: API auth — JWT signing/verification, /auth/token, middleware

**Files:**
- Create: `apps/api/src/auth/jwt.ts`
- Create: `apps/api/src/auth/middleware.ts`
- Create: `apps/api/src/routes/auth.ts`
- Test: `apps/api/test/auth.test.ts`

**Interfaces:**
- Produces:
  - `signToken(payload: { clientId: string }, secret: string, expiresIn?: string): string`
  - `verifyToken(token: string, secret: string): { clientId: string }`
  - `requireAuth(secret: string): (request, reply) => Promise<void>` — a Fastify preHandler/onRequest hook
  - `registerAuthRoutes(app: FastifyInstance, opts: { jwtSecret: string; clients: Record<string,string> }): void`
  - Used by `apps/api/src/server.ts` (Task 11).

- [ ] **Step 1: Write the failing test**

`apps/api/test/auth.test.ts`:

```ts
import { describe, it, expect } from 'vitest';
import { signToken, verifyToken } from '../src/auth/jwt';
import Fastify from 'fastify';
import { requireAuth } from '../src/auth/middleware';
import { registerAuthRoutes } from '../src/routes/auth';

const SECRET = 'test-secret';

describe('jwt', () => {
  it('round-trips a signed token', () => {
    const token = signToken({ clientId: 'demo-client' }, SECRET);
    const payload = verifyToken(token, SECRET);
    expect(payload.clientId).toBe('demo-client');
  });

  it('rejects a token signed with a different secret', () => {
    const token = signToken({ clientId: 'demo-client' }, 'other-secret');
    expect(() => verifyToken(token, SECRET)).toThrow();
  });
});

describe('POST /auth/token', () => {
  function buildTestApp() {
    const app = Fastify();
    registerAuthRoutes(app, { jwtSecret: SECRET, clients: { 'demo-client': 'demo-secret' } });
    return app;
  }

  it('issues a token for valid client credentials', async () => {
    const app = buildTestApp();
    const response = await app.inject({
      method: 'POST',
      url: '/auth/token',
      payload: { clientId: 'demo-client', clientSecret: 'demo-secret' },
    });
    expect(response.statusCode).toBe(200);
    const body = response.json();
    expect(body.token).toBeDefined();
    expect(verifyToken(body.token, SECRET).clientId).toBe('demo-client');
  });

  it('rejects invalid client credentials', async () => {
    const app = buildTestApp();
    const response = await app.inject({
      method: 'POST',
      url: '/auth/token',
      payload: { clientId: 'demo-client', clientSecret: 'wrong-secret' },
    });
    expect(response.statusCode).toBe(401);
  });
});

describe('requireAuth middleware', () => {
  function buildTestApp() {
    const app = Fastify();
    app.addHook('onRequest', requireAuth(SECRET));
    app.get('/protected', async () => ({ ok: true }));
    return app;
  }

  it('rejects a missing Authorization header', async () => {
    const app = buildTestApp();
    const response = await app.inject({ method: 'GET', url: '/protected' });
    expect(response.statusCode).toBe(401);
  });

  it('allows a request with a valid bearer token', async () => {
    const app = buildTestApp();
    const token = signToken({ clientId: 'demo-client' }, SECRET);
    const response = await app.inject({
      method: 'GET', url: '/protected', headers: { authorization: `Bearer ${token}` },
    });
    expect(response.statusCode).toBe(200);
  });
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run apps/api/test/auth.test.ts`
Expected: FAIL — `Cannot find module '../src/auth/jwt'`

- [ ] **Step 3: Implement**

`apps/api/src/auth/jwt.ts`:

```ts
import jwt from 'jsonwebtoken';

export interface TokenPayload {
  clientId: string;
}

export function signToken(payload: TokenPayload, secret: string, expiresIn: string = '15m'): string {
  return jwt.sign(payload, secret, { expiresIn });
}

export function verifyToken(token: string, secret: string): TokenPayload {
  return jwt.verify(token, secret) as TokenPayload;
}
```

`apps/api/src/auth/middleware.ts`:

```ts
import type { FastifyRequest, FastifyReply } from 'fastify';
import { verifyToken } from './jwt';

export function requireAuth(secret: string) {
  return async (request: FastifyRequest, reply: FastifyReply) => {
    const header = request.headers.authorization;
    if (!header || !header.startsWith('Bearer ')) {
      return reply.code(401).send({ error: 'unauthorized' });
    }
    const token = header.slice('Bearer '.length);
    try {
      verifyToken(token, secret);
    } catch {
      return reply.code(401).send({ error: 'unauthorized' });
    }
  };
}
```

`apps/api/src/routes/auth.ts`:

```ts
import type { FastifyInstance } from 'fastify';
import { signToken } from '../auth/jwt';

export interface AuthRouteOptions {
  jwtSecret: string;
  clients: Record<string, string>;
}

export function registerAuthRoutes(app: FastifyInstance, opts: AuthRouteOptions) {
  app.post<{ Body: { clientId: string; clientSecret: string } }>('/auth/token', async (request, reply) => {
    const body = request.body ?? ({} as { clientId: string; clientSecret: string });
    const { clientId, clientSecret } = body;
    if (!clientId || !clientSecret || opts.clients[clientId] !== clientSecret) {
      return reply.code(401).send({ error: 'invalid_client_credentials' });
    }
    const token = signToken({ clientId }, opts.jwtSecret);
    return reply.send({ token, expiresIn: 900 });
  });
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run apps/api/test/auth.test.ts`
Expected: PASS (6 tests)

- [ ] **Step 5: Commit**

```bash
git add apps/api/src/auth apps/api/src/routes/auth.ts apps/api/test/auth.test.ts
git commit -m "Add JWT auth: token issuance and bearer-token middleware"
```

---

### Task 11: API program & reservation routes + server wiring

**Files:**
- Create: `apps/api/src/routes/programs.ts`
- Create: `apps/api/src/routes/reservations.ts`
- Create: `apps/api/src/server.ts`
- Create: `apps/api/src/index.ts`
- Test: `apps/api/test/routes.test.ts`

**Interfaces:**
- Consumes: `getProgram, getAvailability, reserveCapacity, releaseReservation, getReservation, ProgramNotFoundError, InsufficientCapacityError, InvoiceConflictError, ReservationNotFoundError, FxRateUnavailableError` from `@capacity/core`; `registerAuthRoutes`, `requireAuth` from Task 10.
- Produces: `buildApp(opts: { pool: Pool; jwtSecret: string; clients: Record<string,string> }): FastifyInstance` — the full HTTP surface, used directly by tests and by `apps/api/src/index.ts`.

- [ ] **Step 1: Write the failing test**

`apps/api/test/routes.test.ts`:

```ts
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
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run apps/api/test/routes.test.ts`
Expected: FAIL — `Cannot find module '../src/server'`

- [ ] **Step 3: Implement routes and server**

`apps/api/src/routes/programs.ts`:

```ts
import type { FastifyInstance } from 'fastify';
import type { Pool } from 'pg';
import { getProgram, getAvailability, ProgramNotFoundError } from '@capacity/core';

export function registerProgramRoutes(app: FastifyInstance, pool: Pool) {
  app.get<{ Params: { id: string } }>('/programs/:id', async (request, reply) => {
    try {
      const program = await getProgram(pool, request.params.id);
      const available = (
        await getAvailability(pool, request.params.id)
      ).available;
      return reply.send({
        id: program.id,
        name: program.name,
        currency: program.currency,
        totalLimit: program.totalLimit,
        reserved: program.reserved,
        available,
        version: program.version,
        updatedAt: program.updatedAt,
      });
    } catch (err) {
      if (err instanceof ProgramNotFoundError) return reply.code(404).send({ error: 'program_not_found' });
      throw err;
    }
  });

  app.get<{ Params: { id: string } }>('/programs/:id/availability', async (request, reply) => {
    try {
      const result = await getAvailability(pool, request.params.id);
      return reply.send(result);
    } catch (err) {
      if (err instanceof ProgramNotFoundError) return reply.code(404).send({ error: 'program_not_found' });
      throw err;
    }
  });
}
```

`apps/api/src/routes/reservations.ts`:

```ts
import type { FastifyInstance } from 'fastify';
import type { Pool } from 'pg';
import {
  reserveCapacity,
  releaseReservation,
  getReservation,
  ProgramNotFoundError,
  ReservationNotFoundError,
  InsufficientCapacityError,
  InvoiceConflictError,
  FxRateUnavailableError,
} from '@capacity/core';

interface ReserveBody {
  invoiceId: string;
  currency: string;
  amount: string;
}

export function registerReservationRoutes(app: FastifyInstance, pool: Pool) {
  app.post<{ Params: { id: string }; Body: ReserveBody }>(
    '/programs/:id/reservations',
    async (request, reply) => {
      const { invoiceId, currency, amount } = request.body ?? ({} as ReserveBody);
      if (!invoiceId || !currency || !amount) {
        return reply.code(400).send({ error: 'invalid_request' });
      }
      try {
        const result = await reserveCapacity(pool, {
          programId: request.params.id,
          invoiceId,
          currency,
          amount,
        });
        return reply.code(result.created ? 201 : 200).send(result.reservation);
      } catch (err) {
        if (err instanceof ProgramNotFoundError) return reply.code(404).send({ error: 'program_not_found' });
        if (err instanceof InsufficientCapacityError) {
          return reply
            .code(409)
            .send({ error: 'insufficient_capacity', available: err.available, requested: err.requested });
        }
        if (err instanceof InvoiceConflictError) {
          return reply.code(409).send({ error: 'invoice_already_reserved' });
        }
        if (err instanceof FxRateUnavailableError) {
          return reply.code(422).send({ error: 'fx_rate_unavailable' });
        }
        throw err;
      }
    }
  );

  app.post<{ Params: { id: string } }>('/reservations/:id/release', async (request, reply) => {
    try {
      const reservation = await releaseReservation(pool, request.params.id);
      return reply.send(reservation);
    } catch (err) {
      if (err instanceof ReservationNotFoundError) return reply.code(404).send({ error: 'reservation_not_found' });
      throw err;
    }
  });

  app.get<{ Params: { id: string } }>('/reservations/:id', async (request, reply) => {
    try {
      const reservation = await getReservation(pool, request.params.id);
      return reply.send(reservation);
    } catch (err) {
      if (err instanceof ReservationNotFoundError) return reply.code(404).send({ error: 'reservation_not_found' });
      throw err;
    }
  });
}
```

`apps/api/src/server.ts`:

```ts
import Fastify, { type FastifyInstance } from 'fastify';
import type { Pool } from 'pg';
import { requireAuth } from './auth/middleware';
import { registerAuthRoutes } from './routes/auth';
import { registerProgramRoutes } from './routes/programs';
import { registerReservationRoutes } from './routes/reservations';

export interface BuildAppOptions {
  pool: Pool;
  jwtSecret: string;
  clients: Record<string, string>;
}

export function buildApp(opts: BuildAppOptions): FastifyInstance {
  const app = Fastify({ logger: false });

  registerAuthRoutes(app, { jwtSecret: opts.jwtSecret, clients: opts.clients });

  app.addHook('onRequest', async (request, reply) => {
    if (request.url === '/auth/token') return;
    return requireAuth(opts.jwtSecret)(request, reply);
  });

  registerProgramRoutes(app, opts.pool);
  registerReservationRoutes(app, opts.pool);

  return app;
}
```

`apps/api/src/index.ts`:

```ts
import { createPool } from '@capacity/core';
import { buildApp } from './server';

const pool = createPool(process.env.DATABASE_URL!);
const clients = JSON.parse(process.env.CLIENTS_JSON ?? '{}');
const app = buildApp({ pool, jwtSecret: process.env.JWT_SECRET!, clients });

app
  .listen({ port: Number(process.env.PORT ?? 3000), host: '0.0.0.0' })
  .then(() => console.log(`api listening on ${process.env.PORT ?? 3000}`))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run apps/api/test/routes.test.ts`
Expected: PASS (7 tests)

- [ ] **Step 5: Commit**

```bash
git add apps/api/src/routes apps/api/src/server.ts apps/api/src/index.ts apps/api/test/routes.test.ts
git commit -m "Add API program and reservation routes with server wiring"
```

---

### Task 12: Kafka consumer router + process wiring

**Files:**
- Create: `apps/consumer/src/router.ts`
- Create: `apps/consumer/src/consumer.ts`
- Create: `apps/consumer/src/index.ts`
- Test: `apps/consumer/test/router.test.ts`

**Interfaces:**
- Consumes: `applyCapacityAdjustment, applyReconciliation, applyFxRateUpdate` from `@capacity/core`.
- Produces: `routeMessage(pool: Pool, message: { topic: string; value: string }, logger?): Promise<void>` — the only piece kafkajs wiring in `consumer.ts` depends on; keeping it separate from `consumer.ts` means it's testable without a real broker.

- [ ] **Step 1: Write the failing test**

`apps/consumer/test/router.test.ts`:

```ts
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
        asOf: '2026-05-01T00:00:00Z',
      }),
    });
    const program = await getProgram(pool, PROGRAM_ID);
    expect(program.totalLimit).toBe('2000.00');
  });

  it('routes fx.rates to applyFxRateUpdate', async () => {
    await routeMessage(pool, {
      topic: 'fx.rates',
      value: JSON.stringify({ messageId: 'm3', base: 'USD', quote: 'EUR', rate: '0.9', timestamp: '2026-05-01T00:00:00Z' }),
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
});
```

- [ ] **Step 2: Run test to verify it fails**

Run: `npx vitest run apps/consumer/test/router.test.ts`
Expected: FAIL — `Cannot find module '../src/router'`

- [ ] **Step 3: Implement**

`apps/consumer/src/router.ts`:

```ts
import type { Pool } from 'pg';
import { applyCapacityAdjustment, applyReconciliation, applyFxRateUpdate } from '@capacity/core';

export interface RouterLogger {
  warn: (message: string, meta?: Record<string, unknown>) => void;
  info: (message: string, meta?: Record<string, unknown>) => void;
}

export interface KafkaMessage {
  topic: string;
  value: string;
}

export async function routeMessage(
  pool: Pool,
  message: KafkaMessage,
  logger: RouterLogger = console
): Promise<void> {
  const payload = JSON.parse(message.value);

  switch (message.topic) {
    case 'capacity.adjustments':
      await applyCapacityAdjustment(pool, {
        messageId: payload.messageId,
        programId: payload.programId,
        deltaAmount: payload.deltaAmount,
      });
      return;

    case 'capacity.reconciliation': {
      const result = await applyReconciliation(
        pool,
        {
          messageId: payload.messageId,
          programId: payload.programId,
          totalLimit: payload.totalLimit,
          reserved: payload.reserved,
          asOf: new Date(payload.asOf),
        },
        logger
      );
      if (result.reason === 'stale') {
        logger.info('skipped stale reconciliation', { programId: payload.programId });
      }
      return;
    }

    case 'fx.rates':
      await applyFxRateUpdate(pool, {
        messageId: payload.messageId,
        base: payload.base,
        quote: payload.quote,
        rate: payload.rate,
        timestamp: new Date(payload.timestamp),
      });
      return;

    default:
      logger.warn(`unknown topic: ${message.topic}`);
  }
}
```

`apps/consumer/src/consumer.ts`:

```ts
import { Kafka, type Consumer } from 'kafkajs';
import type { Pool } from 'pg';
import { routeMessage } from './router';

export async function startConsumer(
  pool: Pool,
  brokers: string[],
  groupId: string = 'capacity-consumer'
): Promise<Consumer> {
  const kafka = new Kafka({ clientId: 'capacity-consumer', brokers });
  const consumer = kafka.consumer({ groupId });
  await consumer.connect();
  await consumer.subscribe({
    topics: ['capacity.adjustments', 'capacity.reconciliation', 'fx.rates'],
    fromBeginning: false,
  });
  await consumer.run({
    eachMessage: async ({ topic, message }) => {
      if (!message.value) return;
      await routeMessage(pool, { topic, value: message.value.toString() });
    },
  });
  return consumer;
}
```

`apps/consumer/src/index.ts`:

```ts
import { createPool } from '@capacity/core';
import { startConsumer } from './consumer';

const pool = createPool(process.env.DATABASE_URL!);
const brokers = (process.env.KAFKA_BROKERS ?? 'localhost:9092').split(',');

startConsumer(pool, brokers)
  .then(() => console.log('consumer running'))
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
```

- [ ] **Step 4: Run test to verify it passes**

Run: `npx vitest run apps/consumer/test/router.test.ts`
Expected: PASS (4 tests)

- [ ] **Step 5: Commit**

```bash
git add apps/consumer/src apps/consumer/test/router.test.ts
git commit -m "Add Kafka consumer message router and process wiring"
```

---

### Task 13: Dockerfiles, full docker-compose wiring, README

**Files:**
- Create: `apps/api/Dockerfile`
- Create: `apps/consumer/Dockerfile`
- Modify: `README.md`

**Interfaces:**
- Produces: `docker compose up --build` running the full stack end to end — the final deliverable a reviewer exercises manually.

- [ ] **Step 1: API Dockerfile**

`apps/api/Dockerfile`:

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY . .
RUN npm install
WORKDIR /app/apps/api
CMD ["npx", "tsx", "src/index.ts"]
```

- [ ] **Step 2: Consumer Dockerfile**

`apps/consumer/Dockerfile`:

```dockerfile
FROM node:20-alpine
WORKDIR /app
COPY . .
RUN npm install
WORKDIR /app/apps/consumer
CMD ["npx", "tsx", "src/index.ts"]
```

Trade-off, documented: these Dockerfiles copy the whole repo and run `npm install` uncached per image — fine for a local demo, not optimized for build speed or image size. A production Dockerfile would use multi-stage builds and only copy each workspace's `package.json` before `npm ci`.

- [ ] **Step 3: README**

`README.md`:

```markdown
# Program Capacity & Invoice Reservation

Tracks real-time credit capacity per financing program, accepts/releases
invoice reservations over an authenticated API, and ingests treasury state
(capacity adjustments, bulk reconciliation, FX rates) via Kafka.

Design doc: `docs/superpowers/specs/2026-07-05-capacity-reservation-design.md`

## Run locally

```bash
cp .env.example .env
npm install
docker compose up -d postgres redpanda
npm run migrate
npm run seed
docker compose up --build api consumer
```

Two demo programs are seeded:
- `11111111-1111-1111-1111-111111111111` — Acme Supplier Financing, USD, $10,000,000 limit
- `22222222-2222-2222-2222-222222222222` — Global Trade Program, EUR, €5,000,000 limit

## Walkthrough

```bash
# 1. Get a token
TOKEN=$(curl -s -X POST localhost:3000/auth/token \
  -H 'Content-Type: application/json' \
  -d '{"clientId":"demo-client","clientSecret":"demo-secret"}' | jq -r .token)

# 2. Check availability
curl -s localhost:3000/programs/11111111-1111-1111-1111-111111111111/availability \
  -H "Authorization: Bearer $TOKEN"

# 3. Reserve capacity for an invoice
curl -s -X POST localhost:3000/programs/11111111-1111-1111-1111-111111111111/reservations \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"invoiceId":"inv-001","currency":"EUR","amount":"50000.00"}'

# 4. Try to over-reserve — expect 409
curl -s -X POST localhost:3000/programs/11111111-1111-1111-1111-111111111111/reservations \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"invoiceId":"inv-002","currency":"USD","amount":"50000000.00"}'

# 5. Release the reservation (repayment)
curl -s -X POST localhost:3000/reservations/<reservation-id-from-step-3>/release \
  -H "Authorization: Bearer $TOKEN"

# 6. Publish a bulk reconciliation message from "treasury"
docker compose exec redpanda rpk topic produce capacity.reconciliation <<'EOF'
{"messageId":"recon-1","programId":"11111111-1111-1111-1111-111111111111","totalLimit":"12000000.00","reserved":"0.00","asOf":"2026-07-05T12:00:00Z"}
EOF

# Confirm it landed
curl -s localhost:3000/programs/11111111-1111-1111-1111-111111111111 \
  -H "Authorization: Bearer $TOKEN"
```

## Tests

Requires Postgres running (`docker compose up -d postgres`):

```bash
npm test
```

## Documented trade-offs

- **Auth** is a client-credentials stub (`POST /auth/token` against a hardcoded
  `CLIENTS_JSON` map). Production would front this with a real IdP or mTLS.
- **Reservations release in full only** — no partial repayment support.
- **Reconciliation trusts treasury's `reserved` figure wholesale**; local ledger
  divergence is logged, not rejected (see design doc).
- **No FX triangulation** — only direct currency pairs present in `fx_rates`
  are resolvable; missing pairs return `422 fx_rate_unavailable`.
- **Dockerfiles are demo-grade** (whole-repo copy, no multi-stage build).
- **No dead-letter queue** for malformed Kafka messages — a bad payload throws
  and kafkajs applies its default retry behavior.
```

- [ ] **Step 4: Verify the full stack**

```bash
docker compose up -d postgres redpanda
npm run migrate
npm run seed
docker compose up --build -d api consumer
curl -s -X POST localhost:3000/auth/token -H 'Content-Type: application/json' \
  -d '{"clientId":"demo-client","clientSecret":"demo-secret"}'
```

Expected: JSON response containing a `token` field, confirming `api` is up, authenticated, and talking to Postgres.

- [ ] **Step 5: Commit**

```bash
git add apps/api/Dockerfile apps/consumer/Dockerfile README.md
git commit -m "Add Dockerfiles, full docker-compose wiring, and README walkthrough"
```
