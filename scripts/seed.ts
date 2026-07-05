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
