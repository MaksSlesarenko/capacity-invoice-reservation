# Program Capacity & Invoice Reservation — Design

Date: 2026-07-05
Status: Approved for planning

## Problem

A financing program has a total credit limit. Approving an invoice for early
payment reserves a slice of that limit; repayment releases it. We need a
service that:

- Tracks available capacity per program in real time.
- Accepts reservations and releases via an authenticated API.
- Ingests capacity state from an external treasury system via Kafka,
  including periodic full-state reconciliation messages.
- Handles programs and invoices denominated in different currencies.
- Runs locally end-to-end (API + Kafka + DB) via docker-compose.

## Scope

This is a take-home-grade demo built to production-quality coding standards
— correct concurrency handling, idempotency, documented trade-offs — but
without production infra concerns (no k8s, no real treasury Kafka cluster,
no real IdP). Where a decision was simplified for demo purposes, it's called
out explicitly below so it reads as a conscious trade-off, not an oversight.

## Architecture

Two Node.js/TypeScript services sharing one library, one Postgres database:

```
                 ┌──────────────┐
  HTTP clients → │   apps/api   │ ─┐
                 └──────────────┘  │
                                    ├─→ packages/core (CapacityService, FX, DB)
                 ┌──────────────┐  │
  Kafka topics → │ apps/consumer│ ─┘
                 └──────────────┘
                        │
                        ▼
                   Postgres (source of truth)
```

- **apps/api** — Fastify HTTP service. JWT-authenticated REST endpoints.
- **apps/consumer** — kafkajs consumer process. Ingests treasury events.
- **packages/core** — shared: DB access (Postgres via a lightweight query
  layer, e.g. `pg` + `slonik`/`kysely`), `CapacityService` (reservation,
  release, reconciliation logic), FX conversion helpers, domain types.

Splitting API and consumer into separate processes means each scales and
deploys independently and a slow/stuck Kafka consumer can't back-pressure
HTTP request handling (or vice versa). They coordinate only through
Postgres row locks — no in-process shared state.

## Data model (Postgres)

```sql
programs
  id            uuid primary key
  name          text
  currency      char(3)          -- program's base currency
  total_limit   numeric(18,2)    -- authoritative from treasury
  reserved      numeric(18,2)    -- running total, in program currency
  version       bigint           -- optimistic concurrency + reconciliation ordering
  updated_at    timestamptz

reservations
  id                 uuid primary key
  program_id         uuid references programs
  invoice_id         text        -- external ref
  invoice_currency   char(3)
  invoice_amount     numeric(18,2)
  fx_rate_used       numeric(18,8)
  reserved_amount    numeric(18,2)  -- invoice_amount * fx_rate_used, in program currency
  status             text check (status in ('RESERVED', 'RELEASED'))
  created_at         timestamptz
  released_at        timestamptz null
  unique (program_id, invoice_id)

fx_rates
  base_currency   char(3)
  quote_currency  char(3)
  rate            numeric(18,8)
  updated_at      timestamptz
  primary key (base_currency, quote_currency)

processed_messages       -- Kafka idempotency dedup
  message_id   text primary key
  processed_at timestamptz
```

`available = total_limit - reserved`. `reserved` is a running counter on
`programs`, updated transactionally alongside each reservation/release —
not summed from `reservations` on every read. This keeps availability
reads O(1) at the cost of needing every mutation path (API and consumer)
to update the counter correctly, which is why both paths funnel through
the same `CapacityService`. `reservations` remains the full audit ledger
and is used to cross-check reconciliation (see below).

Programs are **not** created via the API — they're seeded via a DB seed
script (`npm run seed`), matching the reality that program capacity
originates from treasury, not from API clients.

## API

All endpoints require `Authorization: Bearer <jwt>`.

```
POST   /auth/token
  body: { clientId, clientSecret }
  -> { token, expiresIn }
  Demo-only client-credentials stub. Real deployment would front this with
  a proper IdP (Auth0/Cognito/etc.) or mTLS; this endpoint exists so the
  service is self-contained and runnable without external dependencies.

GET    /programs/:id
  -> { id, name, currency, totalLimit, reserved, available, version, updatedAt }

GET    /programs/:id/availability
  -> { available, currency }
  Hot-path read, no join, single row fetch.

POST   /programs/:id/reservations
  body: { invoiceId, currency, amount }
  -> 201 { id, programId, invoiceId, reservedAmount, fxRateUsed, status }
  -> 409 { error: "insufficient_capacity", available, requested } if it doesn't fit
  Idempotent on (programId, invoiceId): retrying an identical request that
  already succeeded returns the existing reservation (200) rather than
  erroring or double-reserving.

POST   /reservations/:id/release
  -> 200 { id, status: "RELEASED", releasedAt }
  Idempotent: releasing an already-released reservation is a no-op 200,
  not an error.

GET    /reservations/:id
  -> reservation detail
```

### Reservation flow

```
BEGIN
  SELECT * FROM programs WHERE id = $1 FOR UPDATE
  rate := lookup fx_rates(invoice.currency -> program.currency)   -- 1.0 if same currency
  convertedAmount := invoice.amount * rate
  IF program.total_limit - program.reserved < convertedAmount:
    ROLLBACK, return 409
  INSERT INTO reservations (..., fx_rate_used = rate, reserved_amount = convertedAmount, status = 'RESERVED')
  UPDATE programs SET reserved = reserved + convertedAmount, version = version + 1
COMMIT
```

### Release flow

```
BEGIN
  SELECT reservation FOR UPDATE
  IF reservation.status = 'RELEASED': COMMIT, return 200 (no-op)
  SELECT program FOR UPDATE
  UPDATE programs SET reserved = reserved - reservation.reserved_amount, version = version + 1
  UPDATE reservations SET status = 'RELEASED', released_at = now()
COMMIT
```

Row-level locking (`FOR UPDATE`) serializes concurrent reservations against
the same program so two simultaneous requests can't both read stale
availability and over-reserve. The `version` column is bumped on every
mutation and doubles as the ordering guard for Kafka reconciliation
(below).

**FX trade-off**: the rate used at reservation time is frozen on the
reservation record (`fx_rate_used`). Release always reverses the exact
`reserved_amount` that was originally deducted — never re-converts at a
"current" rate. This avoids capacity leaking or phantom-freeing due to FX
drift between reservation and repayment.

## Kafka ingestion (apps/consumer)

Topics, keyed by `programId` for per-program ordering within a partition:

```
capacity.adjustments
  { messageId, programId, deltaAmount, reason, timestamp }
  Incremental change to total_limit (e.g. treasury increases the line).

capacity.reconciliation
  { messageId, programId, totalLimit, reserved, asOf, timestamp }
  Full-state snapshot; periodic bulk sync from treasury.

fx.rates
  { messageId, base, quote, rate, timestamp }
```

**Idempotency**: every message carries a `messageId`. The consumer inserts
it into `processed_messages` inside the same transaction as the state
mutation (`ON CONFLICT DO NOTHING`, skip if already present). This makes
at-least-once Kafka delivery effectively-once for state changes.

**Adjustment handling**:
```
BEGIN
  IF messageId already processed: COMMIT, skip
  SELECT program FOR UPDATE
  UPDATE programs SET total_limit = total_limit + deltaAmount, version = version + 1
  INSERT processed_messages
COMMIT
```

**Reconciliation handling** (bulk, authoritative):
```
BEGIN
  IF messageId already processed: COMMIT, skip
  SELECT program FOR UPDATE
  IF asOf <= program.updated_at: COMMIT, skip (stale/out-of-order snapshot, do not apply)
  ledgerSum := SUM(reserved_amount) WHERE program_id = programId AND status = 'RESERVED'
  IF abs(ledgerSum - message.reserved) > threshold:
    log warning "reconciliation diverges from local ledger" (programId, ledgerSum, message.reserved)
  UPDATE programs SET total_limit = message.totalLimit, reserved = message.reserved,
                       updated_at = message.asOf, version = version + 1
  INSERT processed_messages
COMMIT
```

**Trade-off, documented explicitly**: reconciliation trusts treasury's
`reserved` figure wholesale rather than rejecting on divergence. Treasury
is the system of record for capacity; our reservation ledger could be
behind for legitimate reasons (a release event not yet processed, a
reservation made through another channel). Hard-failing on divergence
would mean a single noisy reconciliation blocks all future state updates
for that program. We log the divergence for visibility instead. A stricter
mode (quarantine + alert) is a reasonable follow-up for real production use
but is out of scope here. Concretely, this means `programs.reserved` can end
up inconsistent with the sum of open `RESERVED` rows in the ledger — if
reconciliation and organic reservation/release activity disagree, a
subsequent release could, in principle, drive the counter below the true
outstanding total. We accept this as part of the "treasury is authoritative
for capacity" trade-off rather than treating it as a bug; a stricter mode
that reconciles the counter from the ledger (or clamps it) is a reasonable
production follow-up but is out of scope here.

**Ordering assumption**: we assume the treasury producer partitions by
`programId`, so messages for one program arrive in order at one consumer.
Cross-program ordering is not required. The `asOf`/`updated_at` guard on
reconciliation is what actually protects us if this assumption is ever
violated (e.g. a replay), not partition ordering alone.

**FX rate handling**:
```
UPSERT INTO fx_rates (base_currency, quote_currency, rate, updated_at)
VALUES (...) ON CONFLICT (base_currency, quote_currency) DO UPDATE ...
  WHERE fx_rates.updated_at < excluded.updated_at   -- ignore out-of-order rate updates
```

## Currency conversion

- `fx_rates` holds direct pairs. For the demo, seed data includes the pairs
  actually exercised by sample programs/invoices (e.g. USD/EUR, USD/GBP).
  Missing-pair lookups return a 422 (`fx_rate_unavailable`) rather than
  silently defaulting to 1.0.
- Same-currency reservations skip lookup entirely (rate = 1.0).
- No triangulation (converting via a third currency) — out of scope; a
  real system would need this if treasury doesn't publish every pair
  directly.

## Auth

- HS256 JWT, shared secret from `JWT_SECRET` env var.
- `POST /auth/token` validates `clientId`/`clientSecret` against values in
  `CLIENTS` env config (a small hardcoded map for the demo) and issues a
  token with a short expiry (e.g. 15 min).
- Middleware on every other route validates signature + expiry, rejects
  with 401 otherwise.
- Documented as a **stub**: production would replace this with a real
  IdP/OAuth2 client-credentials flow or mTLS between services. The token
  shape and verification middleware are written so swapping the issuer is
  a small, isolated change.

## Local run

`docker-compose.yml` brings up:
- `postgres` (with an init/migration step)
- `redpanda` (single-node, Kafka-API compatible — chosen over real
  Kafka+ZK for fast local startup; consumer code uses kafkajs against the
  standard Kafka protocol so swapping to real Kafka later is a config
  change, not a code change)
- `api`
- `consumer`

`npm run seed` loads demo programs and FX rates directly into Postgres.
`.env.example` documents required vars (`JWT_SECRET`, `CLIENTS`, DB/Kafka
connection strings).

README includes a curl walkthrough: get a token → check availability →
reserve → attempt an over-limit reservation (see the 409) → release →
publish a sample reconciliation message to `capacity.reconciliation` →
observe the program's state update.

## Error handling summary

| Condition | Response |
|---|---|
| Missing/invalid/expired JWT | 401 |
| Program not found | 404 |
| Reservation not found | 404 |
| Insufficient capacity | 409 with `{ available, requested }` |
| Duplicate `(programId, invoiceId)` reservation, identical payload | 200, existing reservation returned |
| Duplicate `(programId, invoiceId)` reservation, different payload | 409 `invoice_already_reserved` |
| Release of already-released reservation | 200, no-op |
| Missing FX pair | 422 `fx_rate_unavailable` |
| Kafka message already processed (`messageId` seen) | skipped silently (logged at debug) |
| Stale/out-of-order reconciliation (`asOf` <= current `updated_at`) | skipped, logged at info |

## Testing plan

- **Unit**: `CapacityService` reservation/release math, FX conversion,
  reconciliation staleness guard, divergence-logging threshold.
- **Concurrency test**: fire N concurrent reservation requests against a
  program with capacity for fewer than N, assert exactly the right number
  succeed and `reserved` never exceeds `total_limit`.
- **Idempotency test**: replay the same Kafka message twice, assert
  state changes exactly once.
- **Integration**: spin up Postgres + Redpanda (testcontainers or the same
  docker-compose), exercise the full API + Kafka path end to end.

## Out of scope

- Partial repayments / partial releases (a reservation is released in full,
  once).
- Multi-instance horizontal scaling considerations beyond what row-level
  locking already provides for a single Postgres instance.
- Real IdP integration, mTLS, secrets management (Vault/KMS).
- FX triangulation through intermediate currencies.
- Historical/point-in-time reporting beyond the reservation audit ledger.
