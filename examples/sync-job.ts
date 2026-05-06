/**
 * Example: BullMQ Sync Job
 *
 * Illustrative example of how a NetSuite sync job is structured.
 * Not verbatim production code — details anonymised.
 */

import { Job, Queue, Worker } from 'bullmq';
import { Redis } from 'ioredis';

const redis = new Redis({ host: 'localhost', port: 6379 });

// ── Types ────────────────────────────────────────────────────

interface SyncJobData {
  recordType: 'invoice' | 'bill' | 'intercompany' | 'return';
  internalId: string;
  companyId: string;
  payload: Record<string, unknown>;
}

interface SyncResult {
  externalId: string;
  netsuiteId: string;
  syncedAt: string;
}

// ── Queue Setup ──────────────────────────────────────────────

export const syncQueue = new Queue<SyncJobData>('netsuite-sync', {
  connection: redis,
  defaultJobOptions: {
    attempts: 5,
    backoff: {
      type: 'exponential',
      delay: 2000, // 2s → 4s → 8s → 16s → 32s
    },
    removeOnComplete: 100,
    removeOnFail: 500,
  },
});

// ── Helper: Generate Deterministic External ID ───────────────

function generateExternalId(
  recordType: string,
  internalId: string,
  companyId: string
): string {
  return `${recordType}_${companyId}_${internalId}`;
}

// ── Helper: Acquire Rate Limit Token ────────────────────────

async function acquireToken(
  key: string,
  ratePerSecond: number
): Promise<boolean> {
  const windowKey = `rate_limit:${key}:${Math.floor(Date.now() / 1000)}`;
  const current = await redis.incr(windowKey);
  if (current === 1) await redis.expire(windowKey, 2);
  return current <= ratePerSecond;
}

// ── Job Processor ────────────────────────────────────────────

async function processSyncJob(job: Job<SyncJobData>): Promise<SyncResult> {
  const { recordType, internalId, companyId, payload } = job.data;

  // 1. Generate idempotency key
  const externalId = generateExternalId(recordType, internalId, companyId);

  console.log(`[Job ${job.id}] Processing ${recordType} — ${externalId}`);

  // 2. Check if already synced (safe retry)
  const alreadySynced = await checkSyncLog(externalId);
  if (alreadySynced) {
    console.log(`[Job ${job.id}] Already synced — skipping`);
    return alreadySynced;
  }

  // 3. Acquire rate limit token
  const allowed = await acquireToken('netsuite-api', 5);
  if (!allowed) {
    throw new Error('Rate limit reached — BullMQ will retry with backoff');
  }

  // 4. Call NetSuite API
  const netsuiteId = await callNetSuiteApi(externalId, recordType, payload);

  // 5. Mark as synced in sync_log
  await updateSyncLog(externalId, netsuiteId);

  const result: SyncResult = {
    externalId,
    netsuiteId,
    syncedAt: new Date().toISOString(),
  };

  console.log(`[Job ${job.id}] ✅ Synced successfully — NetSuite ID: ${netsuiteId}`);

  return result;
}

// ── Stub Functions (replace with real implementations) ───────

async function checkSyncLog(externalId: string): Promise<SyncResult | null> {
  // Query sync_log table for existing synced record
  // SELECT * FROM sync_log WHERE external_id = $1 AND status = 'synced'
  return null; // stub
}

async function callNetSuiteApi(
  externalId: string,
  recordType: string,
  payload: Record<string, unknown>
): Promise<string> {
  // POST to NetSuite REST API with externalId for deduplication
  // Returns NetSuite internal ID
  return `ns_${Date.now()}`; // stub
}

async function updateSyncLog(
  externalId: string,
  netsuiteId: string
): Promise<void> {
  // UPDATE sync_log SET status = 'synced', synced_at = NOW()
  // WHERE external_id = $1
}

// ── Worker ───────────────────────────────────────────────────

const worker = new Worker<SyncJobData, SyncResult>(
  'netsuite-sync',
  processSyncJob,
  {
    connection: redis,
    concurrency: 5,
    limiter: {
      max: 5,
      duration: 1000,
    },
  }
);

worker.on('completed', (job, result) => {
  console.log(`✅ Job ${job.id} completed — ${result.externalId}`);
});

worker.on('failed', (job, error) => {
  console.error(`❌ Job ${job?.id} failed — ${error.message}`);
});

worker.on('error', (error) => {
  console.error('Worker error:', error);
});

// ── Enqueue Example ──────────────────────────────────────────

async function enqueueInvoiceSync(invoiceId: string, companyId: string) {
  await syncQueue.add(
    'sync-invoice',
    {
      recordType: 'invoice',
      internalId: invoiceId,
      companyId,
      payload: {
        // invoice fields
      },
    },
    {
      jobId: `invoice_${companyId}_${invoiceId}`, // dedup at queue level too
    }
  );
}
