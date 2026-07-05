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
