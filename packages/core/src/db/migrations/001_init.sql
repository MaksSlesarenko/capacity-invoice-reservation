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
