import jwt, { SignOptions } from 'jsonwebtoken';

export interface TokenPayload {
  clientId: string;
}

export function signToken(payload: TokenPayload, secret: string, expiresIn: SignOptions['expiresIn'] = '15m'): string {
  return jwt.sign(payload, secret, { expiresIn });
}

export function verifyToken(token: string, secret: string): TokenPayload {
  return jwt.verify(token, secret) as TokenPayload;
}
