# NetSuite ERP Integration at Scale — Architecture Case Study

> **Note:** This case study is based on real production work at a B2B marketplace platform
> processing thousands of financial transactions daily. All company-specific details have been
> anonymised. Code examples are illustrative, not verbatim production code.

---

## 📌 Overview

This document describes the architecture and engineering decisions behind a production-grade
NetSuite ERP integration I designed and built from scratch as the lead engineer.

The system synchronises financial data in real time between an internal platform and NetSuite,
covering:

- Customer invoicing
- Supplier bills
- Intercompany transactions
- Return processing
- Automated reconciliation

**Scale:** Thousands of financial records synced daily, zero tolerance for data loss or duplicates.

---

## 🧩 The Problem

Before this system existed, the finance team was:

- Manually exporting CSVs from the internal platform and importing them into NetSuite
- Running reconciliation checks by hand at month-end — a process taking 4–5 days
- Discovering sync errors only after the fact, often when auditors flagged discrepancies
- Unable to see real-time financial position in NetSuite

The business needed a reliable, automated, auditable pipeline that kept both systems
in sync with no manual intervention.

---

## 🏗️ Architecture Overview

The integration is built on three layers that work together to guarantee reliability:
┌─────────────────────────────────────────────────────────────┐
│                     Internal Platform                        │
│                                                             │
│  ┌──────────────┐    ┌─────────────┐    ┌───────────────┐  │
│  │  PostgreSQL  │───▶│    Kafka    │───▶│    BullMQ     │  │
│  │  + Outbox    │    │   Topics    │    │   Job Queue   │  │
│  └──────────────┘    └─────────────┘    └───────┬───────┘  │
│                                                 │           │
└─────────────────────────────────────────────────┼───────────┘
│
┌───────▼───────┐
│  NetSuite API  │
│  (REST/SOAP)   │
└───────────────┘

### Layer 1 — Outbox Pattern (PostgreSQL)
Every financial event is written atomically alongside the transaction that created it.
If the application crashes mid-flight, no event is lost — it will be picked up on restart.

### Layer 2 — Kafka Event Streaming
An outbox consumer reads new events and publishes them to Kafka topics.
Each transaction type (invoice, bill, intercompany, return) has its own topic and
consumer group, keeping concerns separated and allowing independent scaling.

### Layer 3 — BullMQ Job Queue
Workers consume Kafka events and enqueue BullMQ jobs that make the actual NetSuite
API calls. This layer handles retries, rate limiting, and failure recovery.

---

## 🔥 Key Challenges & Solutions

### Challenge 1 — Idempotency (No Duplicate Records in NetSuite)

**Problem:** NetSuite's API is not idempotent by default. If a job failed halfway
and retried, we could end up with duplicate invoices in the books — a serious
accounting error.

**Solution:** Generate a deterministic `externalId` for every record:

```typescript
function generateExternalId(recordType: string, internalId: string, companyId: string): string {
  return `${recordType}_${companyId}_${internalId}`;
}
```

This ID is passed to NetSuite on every write. NetSuite deduplicates on `externalId`
naturally — if the record already exists, it updates rather than creates.

On our side, a `sync_log` table in PostgreSQL tracks every record's sync state,
making retries safe and providing a full audit trail.

```sql
CREATE TABLE sync_log (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  external_id   TEXT NOT NULL UNIQUE,
  record_type   TEXT NOT NULL,
  status        TEXT NOT NULL DEFAULT 'pending', -- pending | synced | failed
  attempts      INT NOT NULL DEFAULT 0,
  last_error    TEXT,
  synced_at     TIMESTAMPTZ,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Partial index for fast pending job queries
CREATE INDEX idx_sync_log_pending
  ON sync_log (status, created_at)
  WHERE status = 'pending';
```

---

### Challenge 2 — Database Performance at Scale

**Problem:** As transaction volume grew, queries checking for pending sync jobs
were doing full sequential scans on the `sync_log` table. Response times
degraded from milliseconds to 800ms+.

**Diagnosis:** Running `EXPLAIN ANALYZE` revealed the issue immediately:

```sql
EXPLAIN ANALYZE
SELECT * FROM sync_log
WHERE status = 'pending'
ORDER BY created_at ASC
LIMIT 100;

-- Before index:
-- Seq Scan on sync_log (cost=0.00..8420.00 rows=100)
-- Actual time: 812ms

-- After partial index:
-- Index Scan using idx_sync_log_pending
-- Actual time: 3.2ms
```

**Solution:**
- Added a **partial index** on `(status, created_at) WHERE status = 'pending'`
- Introduced **monthly table partitioning** on `sync_log` so historical data
  didn't bloat active queries
- Query time dropped from **800ms → under 5ms**

---

### Challenge 3 — NetSuite API Rate Limiting

**Problem:** NetSuite enforces strict API concurrency limits. Breaching them
causes 429 errors and, if workers don't back off correctly, can corrupt the
job queue state.

**Solution:** A **token bucket rate limiter** built on Redis, checked by every
worker before making an API call:

```typescript
async function acquireToken(redis: Redis, key: string, ratePerSecond: number): Promise<boolean> {
  const now = Date.now();
  const windowKey = `rate_limit:${key}:${Math.floor(now / 1000)}`;

  const current = await redis.incr(windowKey);
  if (current === 1) {
    await redis.expire(windowKey, 2); // 2s TTL for safety
  }

  return current <= ratePerSecond;
}
```

Combined with BullMQ's built-in rate limiting and exponential backoff:

```typescript
const queue = new Queue('netsuite-sync', {
  defaultJobOptions: {
    attempts: 5,
    backoff: {
      type: 'exponential',
      delay: 2000, // 2s, 4s, 8s, 16s, 32s
    },
  },
});
```

This allowed processing thousands of records per hour while never breaching
NetSuite's limits. The Redis layer also provided real-time visibility into
throughput — alerting if processing was being throttled.

---

## 🚀 Deployment & Rollout Strategy

Rolling out a financial integration on a live production system requires extreme care.
A bug here doesn't crash a web page — it corrupts accounting records.

**Approach: Shadow Mode → Gradual Cutover**

1. **Shadow mode (2 weeks):** The new integration ran in parallel, writing to a
   NetSuite sandbox environment. Outputs were diffed daily against the manual export.

2. **Feature flag cutover:** Once confidence reached 100%, production traffic was
   switched via feature flag — one transaction type at a time:
   - Week 1: Customer invoices only
   - Week 2: Supplier bills
   - Week 3: Intercompany transactions
   - Week 4: Returns and reconciliation

3. **Rollback procedure:** Each cutover had a documented, tested rollback — flip the
   feature flag, drain the queue, revert to manual export. Never cut over without
   a tested exit.

---

## 📊 Results

| Metric | Before | After |
|---|---|---|
| Accounting close time | 4–5 days | Less than 1 day |
| Manual reconciliation effort | Daily, full-time | Zero |
| Sync errors discovered | After the fact | Real-time alerts |
| NetSuite visibility | End of day batch | Real-time |
| Duplicate record incidents | Occasional | Zero |

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Backend | Node.js, TypeScript, NestJS |
| Database | PostgreSQL (outbox, sync_log, partitioning) |
| Event Streaming | Apache Kafka |
| Job Queue | BullMQ (Redis-backed) |
| Rate Limiting | Redis (token bucket) |
| Orchestration | Kubernetes, CronJobs |
| ERP | NetSuite REST API + SuiteScript 2.x |

---

## 💡 Lessons Learned

**1. Design for failure from day one.**
The outbox pattern wasn't an afterthought — it was the first thing I designed.
In financial systems, "what happens if this crashes mid-flight?" is not an edge
case. It is the case.

**2. EXPLAIN ANALYZE is your best friend.**
Don't guess at database performance problems. Run `EXPLAIN ANALYZE`, read the
plan, find the seq scan, add the right index. The fix is almost always simpler
than you expect — but you have to look first.

**3. Shadow mode saves careers.**
Running in parallel before cutting over is the single most important deployment
decision I made. It found three subtle data mapping bugs before they touched
production books. Two weeks of shadow mode is worth months of incident recovery.

---

## 📬 Contact

**Salman Saleem** — Senior Backend Engineer
- 📧 saloomughal789@gmail.com
- 💼 [LinkedIn](https://www.linkedin.com/in/muhammad-salman-saleem-9b79a6142)
- 🌍 Open to remote opportunities worldwide
