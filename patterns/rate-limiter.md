# Token Bucket Rate Limiter — Redis Implementation

## The Problem
NetSuite enforces strict API concurrency limits. Breaching
them causes 429 errors and if workers don't back off correctly,
the job queue can enter a broken state — jobs retrying in a
tight loop, hammering the API, making things worse.

## The Solution — Token Bucket in Redis

A token bucket rate limiter built on Redis. Every worker
checks out a token before making an API call. Tokens refill
at the allowed rate. No token = wait and retry.

```typescript
async function acquireToken(
  redis: Redis,
  key: string,
  ratePerSecond: number
): Promise<boolean> {
  const now = Date.now();
  const windowKey = `rate_limit:${key}:${Math.floor(now / 1000)}`;

  const current = await redis.incr(windowKey);
  if (current === 1) {
    await redis.expire(windowKey, 2); // 2s TTL for safety
  }

  return current <= ratePerSecond;
}

// Usage in worker
async function processJob(job: Job): Promise<void> {
  const allowed = await acquireToken(redis, 'netsuite-api', 5);

  if (!allowed) {
    // Throw to trigger BullMQ retry with backoff
    throw new Error('Rate limit reached — backing off');
  }

  await netsuiteClient.sync(job.data);
}
```

## BullMQ Configuration — Exponential Backoff

Combined with BullMQ's built-in rate limiting and exponential
backoff for a fully resilient retry pipeline:

```typescript
const queue = new Queue('netsuite-sync', {
  defaultJobOptions: {
    attempts: 5,
    backoff: {
      type: 'exponential',
      delay: 2000, // 2s → 4s → 8s → 16s → 32s
    },
    removeOnComplete: 100, // Keep last 100 completed jobs
    removeOnFail: 500,     // Keep last 500 failed jobs
  },
});

// Rate limit the worker itself — max 5 jobs per second
const worker = new Worker('netsuite-sync', processJob, {
  connection: redis,
  limiter: {
    max: 5,
    duration: 1000, // per 1000ms
  },
});
```

## Monitoring — Real-Time Visibility

The Redis layer gives real-time visibility into throughput.
Alert when token bucket is consistently full — signals
that processing is being throttled:

```typescript
async function getThrottleMetrics(redis: Redis): Promise<object> {
  const now = Date.now();
  const windowKey = `rate_limit:netsuite-api:${Math.floor(now / 1000)}`;
  const current = await redis.get(windowKey);

  return {
    currentRate: parseInt(current || '0'),
    maxRate: 5,
    isThrottled: parseInt(current || '0') >= 5,
    timestamp: new Date().toISOString(),
  };
}
```

## Key Properties

| Property | Value |
|---|---|
| Algorithm | Token bucket (per-second window) |
| Storage | Redis (atomic INCR) |
| Retry strategy | Exponential backoff via BullMQ |
| Visibility | Real-time metrics via Redis |
| Max attempts | 5 (2s, 4s, 8s, 16s, 32s) |

## Why Redis for Rate Limiting?
- **Atomic operations** — INCR is atomic, no race conditions
- **TTL-based cleanup** — keys expire automatically, no cleanup needed
- **Distributed** — works across multiple worker instances
- **Fast** — sub-millisecond latency for token checks

## Related Patterns
- [Outbox Pattern](./outbox.md) — guarantees events are never lost
- [Idempotency](./idempotency.md) — safe retries without duplicates
