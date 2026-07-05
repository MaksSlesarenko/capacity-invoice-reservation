import type { FastifyRequest, FastifyReply } from 'fastify';
import { verifyToken } from './jwt';

export function requireAuth(secret: string) {
  return async (request: FastifyRequest, reply: FastifyReply) => {
    const header = request.headers.authorization;
    if (!header || !header.startsWith('Bearer ')) {
      return reply.code(401).send({ error: 'unauthorized' });
    }
    const token = header.slice('Bearer '.length);
    try {
      verifyToken(token, secret);
    } catch {
      return reply.code(401).send({ error: 'unauthorized' });
    }
  };
}
