# Idempotency Pattern — Safe Retries Without Duplicates

## The Problem
NetSuite's API is not idempotent by default. If a job fails
halfway and retries, you get duplicate invoices in the books
— a serious accounting error that auditors will flag.

In any distributed system with retries, the same operation
can execute more than once. The system must handle this safely.

## The Solution — Deterministic External IDs

Generate a deterministic `externalId` for every record based
on its source data. Pass it to NetSuite on every write.
NetSuite deduplicates naturally — if the record exists, it
updates instead of creating a duplicate.

```typescript
// Generate a stable, deterministic ID from source data
function generateExternalId(
  recordType: string,
  internalId: string,
  companyId: string
): string {
  return `${recordType}_${companyId}_${internalId}`;
}

// Example output:
// invoice_company123_inv_456 → always the same for this record
```

## Sync Log — Audit Trail & Retry Safety

A `sync_log` table tracks every record's sync state.
Before processing, check if already synced. After success,
mark as synced. Retries are always safe.

```sql
CREATE TABLE sync_log (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  external_id   TEXT NOT NULL UNIQUE,
  record_type   TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'pending',
  attempts      INT NOT NULL DEFAULT 0,
  last_error    TEXT,
  synced_at     TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Fast lookup for pending jobs
CREATE INDEX idx_sync_log_pending
  ON sync_log (status, created_at)
  WHERE status = 'pending';
```

```typescript
async function syncToNetSuite(record: SyncRecord): Promise<void> {
  const externalId = generateExternalId(
    record.type,
    record.id,
    record.companyId
  );

  // Check if already synced — safe to retry
  const existing = await db('sync_log')
    .where({ external_id: externalId, status: 'synced' })
    .first();

  if (existing) return; // Already done — skip safely

  try {
    await netsuiteClient.upsert({
      externalId,
      ...record.data,
    });

    await db('sync_log')
      .where({ external_id: externalId })
      .update({ status: 'synced', synced_at: new Date() });

  } catch (error) {
    await db('sync_log')
      .where({ external_id: externalId })
      .update({
        status: 'failed',
        last_error: error.message,
        attempts: db.raw('attempts + 1'),
      });
    throw error; // Re-throw so BullMQ retries the job
  }
}
```

## Key Properties

| Property | Value |
|---|---|
| Duplicate prevention | ✅ Via externalId + sync_log |
| Safe retries | ✅ Check before write |
| Full audit trail | ✅ Every attempt logged |
| Performance | ✅ Partial index on pending status |

## Related Patterns
- [Outbox Pattern](./outbox.md) — guarantees events are never lost
- [Rate Limiter](./rate-limiter.md) — controls API throughput
