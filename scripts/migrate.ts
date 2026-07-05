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
