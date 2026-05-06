/**
 * Example: NetSuite API Client
 *
 * Illustrative example of a NetSuite REST API client wrapper.
 * Not verbatim production code — details anonymised.
 */

import axios, { AxiosInstance } from 'axios';

// ── Types ────────────────────────────────────────────────────

interface NetSuiteConfig {
  accountId: string;
  consumerKey: string;
  consumerSecret: string;
  tokenId: string;
  tokenSecret: string;
}

interface UpsertOptions {
  externalId: string;
  recordType: RecordType;
  data: Record<string, unknown>;
}

interface UpsertResult {
  netsuiteId: string;
  externalId: string;
  action: 'created' | 'updated';
}

type RecordType = 'invoice' | 'vendorbill' | 'journalentry' | 'creditmemo';

// ── Client ───────────────────────────────────────────────────

export class NetSuiteClient {
  private client: AxiosInstance;
  private config: NetSuiteConfig;

  constructor(config: NetSuiteConfig) {
    this.config = config;
    this.client = axios.create({
      baseURL: `https://${config.accountId}.suitetalk.api.netsuite.com/services/rest/record/v1`,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Attach OAuth 1.0 signing to every request
    this.client.interceptors.request.use((req) => {
      req.headers['Authorization'] = this.buildOAuthHeader(req);
      return req;
    });

    // Centralised error handling
    this.client.interceptors.response.use(
      (res) => res,
      (error) => {
        const status = error.response?.status;
        const message = error.response?.data?.message || error.message;

        if (status === 429) {
          throw new RateLimitError(`NetSuite rate limit hit: ${message}`);
        }
        if (status === 404) {
          throw new NotFoundError(`NetSuite record not found: ${message}`);
        }
        if (status >= 500) {
          throw new NetSuiteServerError(`NetSuite server error: ${message}`);
        }

        throw new NetSuiteError(`NetSuite API error (${status}): ${message}`);
      }
    );
  }

  // ── Upsert — Core Operation ─────────────────────────────

  /**
   * Upsert a record using externalId for idempotency.
   * If the record exists → update. If not → create.
   * Safe to call multiple times with the same externalId.
   */
  async upsert(options: UpsertOptions): Promise<UpsertResult> {
    const { externalId, recordType, data } = options;

    const url = `/${recordType}/eid:${externalId}`;

    try {
      // Try update first (PATCH)
      await this.client.patch(url, data);

      return {
        netsuiteId: await this.getInternalId(recordType, externalId),
        externalId,
        action: 'updated',
      };
    } catch (error) {
      if (error instanceof NotFoundError) {
        // Record doesn't exist — create it (PUT with externalId)
        const response = await this.client.put(url, data);
        const netsuiteId = this.extractIdFromLocation(
          response.headers['location']
        );

        return { netsuiteId, externalId, action: 'created' };
      }
      throw error;
    }
  }

  // ── Get Record by External ID ────────────────────────────

  async getByExternalId(
    recordType: RecordType,
    externalId: string
  ): Promise<Record<string, unknown> | null> {
    try {
      const response = await this.client.get(
        `/${recordType}/eid:${externalId}`
      );
      return response.data;
    } catch (error) {
      if (error instanceof NotFoundError) return null;
      throw error;
    }
  }

  // ── Get Internal ID from External ID ────────────────────

  private async getInternalId(
    recordType: RecordType,
    externalId: string
  ): Promise<string> {
    const record = await this.getByExternalId(recordType, externalId);
    return record?.id as string;
  }

  // ── Extract ID from Location Header ─────────────────────

  private extractIdFromLocation(location: string): string {
    // Location: .../record/v1/invoice/123
    const parts = location.split('/');
    return parts[parts.length - 1];
  }

  // ── OAuth 1.0 Header Builder (stub) ─────────────────────

  private buildOAuthHeader(req: any): string {
    // In production: use oauth-1.0a library to sign requests
    // with consumerKey, consumerSecret, tokenId, tokenSecret
    return `OAuth realm="${this.config.accountId}", ...`;
  }
}

// ── Custom Errors ────────────────────────────────────────────

export class NetSuiteError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'NetSuiteError';
  }
}

export class RateLimitError extends NetSuiteError {
  constructor(message: string) {
    super(message);
    this.name = 'RateLimitError';
  }
}

export class NotFoundError extends NetSuiteError {
  constructor(message: string) {
    super(message);
    this.name = 'NotFoundError';
  }
}

export class NetSuiteServerError extends NetSuiteError {
  constructor(message: string) {
    super(message);
    this.name = 'NetSuiteServerError';
  }
}

// ── Usage Example ────────────────────────────────────────────

async function example() {
  const client = new NetSuiteClient({
    accountId: 'YOUR_ACCOUNT_ID',
    consumerKey: 'YOUR_CONSUMER_KEY',
    consumerSecret: 'YOUR_CONSUMER_SECRET',
    tokenId: 'YOUR_TOKEN_ID',
    tokenSecret: 'YOUR_TOKEN_SECRET',
  });

  // Upsert an invoice — safe to retry, won't create duplicates
  const result = await client.upsert({
    externalId: 'invoice_company123_inv_456',
    recordType: 'invoice',
    data: {
      entity: { id: 'customer_123' },
      tranDate: '2026-05-01',
      dueDate: '2026-06-01',
      memo: 'Order #456',
      item: [
        {
          item: { id: 'item_789' },
          quantity: 2,
          rate: 500.00,
        },
      ],
    },
  });

  console.log(`${result.action}: NetSuite ID ${result.netsuiteId}`);
}
