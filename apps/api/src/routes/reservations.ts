import type { FastifyInstance } from 'fastify';
import type { Pool } from 'pg';
import {
  reserveCapacity,
  releaseReservation,
  getReservation,
  ProgramNotFoundError,
  ReservationNotFoundError,
  InsufficientCapacityError,
  InvoiceConflictError,
  InvalidAmountError,
  InvalidCurrencyError,
  FxRateUnavailableError,
} from '@capacity/core';

interface ReserveBody {
  invoiceId: string;
  currency: string;
  amount: string;
}

export function registerReservationRoutes(app: FastifyInstance, pool: Pool) {
  app.post<{ Params: { id: string }; Body: ReserveBody }>(
    '/programs/:id/reservations',
    async (request, reply) => {
      const { invoiceId, currency, amount } = request.body ?? ({} as ReserveBody);
      if (!invoiceId || !currency || !amount) {
        return reply.code(400).send({ error: 'invalid_request' });
      }
      try {
        const result = await reserveCapacity(pool, {
          programId: request.params.id,
          invoiceId,
          currency,
          amount,
        });
        return reply.code(result.created ? 201 : 200).send(result.reservation);
      } catch (err) {
        if (err instanceof ProgramNotFoundError) return reply.code(404).send({ error: 'program_not_found' });
        if (err instanceof InsufficientCapacityError) {
          return reply
            .code(409)
            .send({ error: 'insufficient_capacity', available: err.available, requested: err.requested });
        }
        if (err instanceof InvoiceConflictError) {
          return reply.code(409).send({ error: 'invoice_already_reserved' });
        }
        if (err instanceof InvalidAmountError) {
          return reply.code(400).send({ error: 'invalid_amount' });
        }
        if (err instanceof InvalidCurrencyError) {
          return reply.code(400).send({ error: 'invalid_currency' });
        }
        if (err instanceof FxRateUnavailableError) {
          return reply.code(422).send({ error: 'fx_rate_unavailable' });
        }
        throw err;
      }
    }
  );

  app.post<{ Params: { id: string } }>('/reservations/:id/release', async (request, reply) => {
    try {
      const reservation = await releaseReservation(pool, request.params.id);
      return reply.send(reservation);
    } catch (err) {
      if (err instanceof ReservationNotFoundError) return reply.code(404).send({ error: 'reservation_not_found' });
      throw err;
    }
  });

  app.get<{ Params: { id: string } }>('/reservations/:id', async (request, reply) => {
    try {
      const reservation = await getReservation(pool, request.params.id);
      return reply.send(reservation);
    } catch (err) {
      if (err instanceof ReservationNotFoundError) return reply.code(404).send({ error: 'reservation_not_found' });
      throw err;
    }
  });
}
