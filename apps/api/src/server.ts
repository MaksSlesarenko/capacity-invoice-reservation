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
