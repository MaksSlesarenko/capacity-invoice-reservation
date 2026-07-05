import type { Pool } from 'pg';
import { applyCapacityAdjustment, applyReconciliation, applyFxRateUpdate } from '@capacity/core';

export interface RouterLogger {
  warn: (message: string, meta?: Record<string, unknown>) => void;
  info: (message: string, meta?: Record<string, unknown>) => void;
}

export interface KafkaMessage {
  topic: string;
  value: string;
}

export interface DeadLetterEnvelope {
  originalTopic: string;
  value: string;
  reason: string;
}

export interface DeadLetterPublisher {
  publish: (envelope: DeadLetterEnvelope) => Promise<void>;
}

interface AdjustmentPayload {
  messageId: string;
  programId: string;
  deltaAmount: string;
}

interface ReconciliationPayload {
  messageId: string;
  programId: string;
  totalLimit: string;
  reserved: string;
  asOf: string;
}

interface FxRatePayload {
  messageId: string;
  base: string;
  quote: string;
  rate: string;
  timestamp: string;
}

function isNonEmptyString(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0;
}

function isValidDateString(value: unknown): value is string {
  return isNonEmptyString(value) && !Number.isNaN(new Date(value).getTime());
}

function isAdjustmentPayload(payload: unknown): payload is AdjustmentPayload {
  const p = payload as Partial<AdjustmentPayload> | null;
  return (
    !!p &&
    isNonEmptyString(p.messageId) &&
    isNonEmptyString(p.programId) &&
    isNonEmptyString(p.deltaAmount)
  );
}

function isReconciliationPayload(payload: unknown): payload is ReconciliationPayload {
  const p = payload as Partial<ReconciliationPayload> | null;
  return (
    !!p &&
    isNonEmptyString(p.messageId) &&
    isNonEmptyString(p.programId) &&
    isNonEmptyString(p.totalLimit) &&
    isNonEmptyString(p.reserved) &&
    isValidDateString(p.asOf)
  );
}

function isFxRatePayload(payload: unknown): payload is FxRatePayload {
  const p = payload as Partial<FxRatePayload> | null;
  return (
    !!p &&
    isNonEmptyString(p.messageId) &&
    isNonEmptyString(p.base) &&
    isNonEmptyString(p.quote) &&
    isNonEmptyString(p.rate) &&
    isValidDateString(p.timestamp)
  );
}

export async function routeMessage(
  pool: Pool,
  message: KafkaMessage,
  logger: RouterLogger = console,
  deadLetter?: DeadLetterPublisher
): Promise<void> {
  let payload: unknown;
  try {
    payload = JSON.parse(message.value);
  } catch {
    logger.warn(`malformed JSON payload on topic ${message.topic}`);
    await deadLetter?.publish({
      originalTopic: message.topic,
      value: message.value,
      reason: 'malformed_json',
    });
    return;
  }

  switch (message.topic) {
    case 'capacity.adjustments':
      if (!isAdjustmentPayload(payload)) {
        logger.warn(`invalid capacity.adjustments payload`, { payload });
        await deadLetter?.publish({
          originalTopic: message.topic,
          value: message.value,
          reason: 'invalid_payload',
        });
        return;
      }
      await applyCapacityAdjustment(pool, {
        messageId: payload.messageId,
        programId: payload.programId,
        deltaAmount: payload.deltaAmount,
      });
      return;

    case 'capacity.reconciliation': {
      if (!isReconciliationPayload(payload)) {
        logger.warn(`invalid capacity.reconciliation payload`, { payload });
        await deadLetter?.publish({
          originalTopic: message.topic,
          value: message.value,
          reason: 'invalid_payload',
        });
        return;
      }
      const result = await applyReconciliation(
        pool,
        {
          messageId: payload.messageId,
          programId: payload.programId,
          totalLimit: payload.totalLimit,
          reserved: payload.reserved,
          asOf: new Date(payload.asOf),
        },
        logger
      );
      if (result.reason === 'stale') {
        logger.info('skipped stale reconciliation', { programId: payload.programId });
      }
      return;
    }

    case 'fx.rates':
      if (!isFxRatePayload(payload)) {
        logger.warn(`invalid fx.rates payload`, { payload });
        await deadLetter?.publish({
          originalTopic: message.topic,
          value: message.value,
          reason: 'invalid_payload',
        });
        return;
      }
      await applyFxRateUpdate(pool, {
        messageId: payload.messageId,
        base: payload.base,
        quote: payload.quote,
        rate: payload.rate,
        timestamp: new Date(payload.timestamp),
      });
      return;

    default:
      logger.warn(`unknown topic: ${message.topic}`);
      await deadLetter?.publish({
        originalTopic: message.topic,
        value: message.value,
        reason: 'unknown_topic',
      });
  }
}
