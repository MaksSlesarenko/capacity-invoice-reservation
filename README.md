# Program Capacity & Invoice Reservation

Tracks real-time credit capacity per financing program, accepts/releases
invoice reservations over an authenticated API, and ingests treasury state
(capacity adjustments, bulk reconciliation, FX rates) via Kafka.

Design doc: `docs/superpowers/specs/2026-07-05-capacity-reservation-design.md`

## Run locally

```bash
cp .env.example .env
set -a; source .env; set +a
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
# Note: --compression=none is required — kafkajs (used by the consumer) does
# not implement Snappy decompression, which is rpk's default.
docker compose exec redpanda rpk topic produce capacity.reconciliation --compression=none <<'EOF'
{"messageId":"recon-1","programId":"11111111-1111-1111-1111-111111111111","totalLimit":"12000000.00","reserved":"0.00","asOf":"2026-07-05T12:00:00Z"}
EOF

# Confirm it landed
curl -s localhost:3000/programs/11111111-1111-1111-1111-111111111111 \
  -H "Authorization: Bearer $TOKEN"
```

## Tests

Requires Postgres running (`docker compose up -d postgres`):

```bash
cp .env.example .env
set -a; source .env; set +a
npm test
```

## Documented trade-offs

- **Auth** is a client-credentials stub (`POST /auth/token` against a hardcoded
  `CLIENTS_JSON` map). Production would front this with a real IdP or mTLS.
- **Reservations release in full only** — no partial repayment support.
- **Reconciliation trusts treasury's `reserved` figure wholesale**; local ledger
  divergence is logged, not rejected (see design doc).
- **`programs.reserved` can drift from the ledger's open `RESERVED` rows** as a
  result of the above — a subsequent release could in principle drive the
  counter below the true outstanding total; accepted trade-off, not a bug
  (see design doc).
- **No FX triangulation** — only direct currency pairs present in `fx_rates`
  are resolvable; missing pairs return `422 fx_rate_unavailable`.
- **Dockerfiles are multi-stage** (`deps` installs via `npm ci` from a
  package.json-only layer for cache reuse, `runtime` copies `node_modules`
  + source and runs as the non-root `node` user).
- **Dead-letter queue covers malformed input only.** Malformed Kafka
  messages (invalid JSON, missing fields, unparseable dates, unknown topics)
  are published to `capacity.dlq` instead of just logged and skipped.
  A *well-formed* message referencing an unknown `programId` still
  deliberately throws so the data-integrity problem is loud rather than
  silently routed to the DLQ; kafkajs then retries it indefinitely, which
  blocks that partition until the message is resolved (a poison-pill by
  design).
- **No Snappy codec registered on the consumer** — kafkajs doesn't implement
  Snappy decompression out of the box, so producers must publish uncompressed
  (`rpk topic produce ... --compression=none`, as used in the walkthrough
  above). A production setup would register a Snappy codec or standardize on
  gzip/lz4/zstd.
