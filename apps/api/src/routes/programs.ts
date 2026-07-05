import type { FastifyInstance } from 'fastify';
import type { Pool } from 'pg';
import { getProgram, getAvailability, availabilityOf, ProgramNotFoundError } from '@capacity/core';

export function registerProgramRoutes(app: FastifyInstance, pool: Pool) {
  app.get<{ Params: { id: string } }>('/programs/:id', async (request, reply) => {
    try {
      const program = await getProgram(pool, request.params.id);
      return reply.send({
        id: program.id,
        name: program.name,
        currency: program.currency,
        totalLimit: program.totalLimit,
        reserved: program.reserved,
        available: availabilityOf(program),
        version: program.version,
        updatedAt: program.updatedAt,
      });
    } catch (err) {
      if (err instanceof ProgramNotFoundError) return reply.code(404).send({ error: 'program_not_found' });
      throw err;
    }
  });

  app.get<{ Params: { id: string } }>('/programs/:id/availability', async (request, reply) => {
    try {
      const result = await getAvailability(pool, request.params.id);
      return reply.send(result);
    } catch (err) {
      if (err instanceof ProgramNotFoundError) return reply.code(404).send({ error: 'program_not_found' });
      throw err;
    }
  });
}
