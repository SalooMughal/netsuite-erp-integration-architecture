# Outbox Pattern — Guaranteed Event Delivery

## The Problem
In a distributed system, writing to a database and publishing
to a message queue are two separate operations. If the app
crashes between them, the event is lost forever — silently.

In a financial system, a lost event means a missing invoice
or a reconciliation gap that auditors will find months later.

## The Solution — Transactional Outbox

Write the event to an `outbox` table **in the same database
transaction** as the business record. A separate poller then
reads the outbox and publishes to Kafka.

```sql
-- Outbox table
CREATE TABLE outbox_events (
  id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  aggregate_id TEXT NOT NULL,
  event_type   TEXT NOT NULL,  -- invoice.created | bill.created
  payload      JSONB NOT NULL,
  published    BOOLEAN NOT NULL DEFAULT FALSE,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Index for fast unpublished event polling
CREATE INDEX idx_outbox_unpublished
  ON outbox_events (created_at)
  WHERE published = FALSE;
```

```typescript
// Write business record + outbox event atomically
await db.transaction(async (trx) => {
  const invoice = await trx('invoices').insert(invoiceData).returning('*');

  await trx('outbox_events').insert({
    aggregate_id: invoice[0].id,
    event_type: 'invoice.created',
    payload: JSON.stringify(invoice[0]),
  });
});
```

## Why This Works
- If the app crashes before the transaction commits → nothing is written
- If the app crashes after commit → outbox poller picks it up on restart
- The event is **never lost** and **never duplicated** at the write stage

## Key Properties
| Property | Guarantee |
|---|---|
| At-least-once delivery | ✅ Yes |
| Exactly-once delivery | Handled downstream via idempotency |
| Zero event loss | ✅ Yes |
| Works without 2PC | ✅ Yes |

## Related Patterns
- [Idempotency](./idempotency.md) — handles duplicates on the consumer side
- [Rate Limiter](./rate-limiter.md) — controls downstream API throughput
