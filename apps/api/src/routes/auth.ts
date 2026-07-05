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
