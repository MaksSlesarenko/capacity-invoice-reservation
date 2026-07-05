import { describe, it, expect } from 'vitest';
import { signToken, verifyToken } from '../src/auth/jwt';
import Fastify from 'fastify';
import { requireAuth } from '../src/auth/middleware';
import { registerAuthRoutes } from '../src/routes/auth';

const SECRET = 'test-secret';

describe('jwt', () => {
  it('round-trips a signed token', () => {
    const token = signToken({ clientId: 'demo-client' }, SECRET);
    const payload = verifyToken(token, SECRET);
    expect(payload.clientId).toBe('demo-client');
  });

  it('rejects a token signed with a different secret', () => {
    const token = signToken({ clientId: 'demo-client' }, 'other-secret');
    expect(() => verifyToken(token, SECRET)).toThrow();
  });
});

describe('POST /auth/token', () => {
  function buildTestApp() {
    const app = Fastify();
    registerAuthRoutes(app, { jwtSecret: SECRET, clients: { 'demo-client': 'demo-secret' } });
    return app;
  }

  it('issues a token for valid client credentials', async () => {
    const app = buildTestApp();
    const response = await app.inject({
      method: 'POST',
      url: '/auth/token',
      payload: { clientId: 'demo-client', clientSecret: 'demo-secret' },
    });
    expect(response.statusCode).toBe(200);
    const body = response.json();
    expect(body.token).toBeDefined();
    expect(verifyToken(body.token, SECRET).clientId).toBe('demo-client');
  });

  it('rejects invalid client credentials', async () => {
    const app = buildTestApp();
    const response = await app.inject({
      method: 'POST',
      url: '/auth/token',
      payload: { clientId: 'demo-client', clientSecret: 'wrong-secret' },
    });
    expect(response.statusCode).toBe(401);
  });
});

describe('requireAuth middleware', () => {
  function buildTestApp() {
    const app = Fastify();
    app.addHook('onRequest', requireAuth(SECRET));
    app.get('/protected', async () => ({ ok: true }));
    return app;
  }

  it('rejects a missing Authorization header', async () => {
    const app = buildTestApp();
    const response = await app.inject({ method: 'GET', url: '/protected' });
    expect(response.statusCode).toBe(401);
  });

  it('allows a request with a valid bearer token', async () => {
    const app = buildTestApp();
    const token = signToken({ clientId: 'demo-client' }, SECRET);
    const response = await app.inject({
      method: 'GET', url: '/protected', headers: { authorization: `Bearer ${token}` },
    });
    expect(response.statusCode).toBe(200);
  });
});
