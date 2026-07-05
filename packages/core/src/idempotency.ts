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
