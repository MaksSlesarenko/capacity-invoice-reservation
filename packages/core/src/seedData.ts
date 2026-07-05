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
