/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `KustoDriver` and related types declaration.
 */

import {
  getEnv,
  assertDataSource,
} from '@cubejs-backend/shared';
import {
  BaseDriver,
  DownloadQueryResultsOptions,
  DownloadQueryResultsResult,
  TableQueryResult, TableStructure,
} from '@cubejs-backend/base-driver';
import * as KustoData from 'azure-kusto-data';
import {
  AzureCliCredential,
  TokenCredential
} from '@azure/identity';

export type KustoDriverConfiguration = {
  clusterUrl?: string,
  database?: string,
  appId?: string,
  appKey?: string,
  clientId?: string, // Added for compatibility with tests
  clientSecret?: string, // Added for compatibility with tests
  authorityId?: string,
  useManagedIdentity?: boolean,
  userManagedIdentityId?: string
};

/**
 * Kusto (Azure Data Explorer) driver class.
 */
export class KustoDriver extends BaseDriver {
  protected readonly config: KustoDriverConfiguration;

  protected readonly client: KustoData.Client;

  public static dialectClass() {
    // You may want to implement a KustoQuery class for dialect-specific logic
    return null;
  }

  public static getDefaultConcurrency(): number {
    return 2;
  }

  public constructor(
    config: KustoDriverConfiguration = {}
  ) {
    super();
    // Prefer config, then Cube.js env, then Azure env, then undefined
    const clusterUrl =
      config.clusterUrl ||
      process.env.KUSTO_CLUSTER_URL;
    const database =
      config.database ||
      process.env.KUSTO_DATABASE;

    // Support both naming conventions (appId/clientId and appKey/clientSecret)
    const appId =
      config.appId ||
      config.clientId ||
      process.env.KUSTO_CLIENT_ID;

    const appKey =
      config.appKey ||
      config.clientSecret ||
      process.env.KUSTO_CLIENT_SECRET;

    const authorityId =
      config.authorityId ||
      process.env.KUSTO_TENANT_ID;

    // Safely handle boolean environment variables
    let useManagedIdentity = false;
    if (typeof config.useManagedIdentity === 'boolean') {
      useManagedIdentity = config.useManagedIdentity;
    } else if (process.env.KUSTO_USE_MANAGED_IDENTITY === 'true') {
      useManagedIdentity = true;
    }

    const userManagedIdentityId =
      config.userManagedIdentityId ||
      process.env.KUSTO_USER_MANAGED_IDENTITY_ID;

    if (!clusterUrl || !database) {
      throw new Error('Please specify Kusto connection parameters (clusterUrl, database)');
    }

    this.config = {
      clusterUrl,
      database,
      appId,
      appKey,
      authorityId,
      useManagedIdentity,
      userManagedIdentityId,
    };

    let kcsb: KustoData.KustoConnectionStringBuilder | undefined;

    if (useManagedIdentity) {
      // Managed Identity authentication (system or user-assigned)
      if (userManagedIdentityId) {
        // User-assigned managed identity
        kcsb = KustoData.KustoConnectionStringBuilder.withUserManagedIdentity(
          clusterUrl,
          userManagedIdentityId
        );
      } else {
        // System-assigned managed identity
        kcsb = KustoData.KustoConnectionStringBuilder.withSystemManagedIdentity(clusterUrl);
      }
    } else if (appId && appKey && authorityId) {
      // AAD Application credentials
      kcsb = KustoData.KustoConnectionStringBuilder.withAadApplicationKeyAuthentication(
        clusterUrl,
        appId,
        appKey,
        authorityId
      );
    } else {
      // Fallback to chained credentials (Azure CLI, Interactive Browser)
      const credential: TokenCredential = new AzureCliCredential();
      kcsb = KustoData.KustoConnectionStringBuilder.withTokenCredential(
        clusterUrl,
        credential
      );
    }

    if (!kcsb) {
      throw new Error('No valid Kusto authentication method configured.');
    }

    this.client = new KustoData.Client(kcsb);
  }

  public readOnly() {
    return true;
  }

  public async testConnection() {
    // Simple query to test connection
    await this.query('show databases');
  }

  public async query<R = unknown>(query: string, values: unknown[] = []): Promise<Array<R>> {
    // Kusto does not use parameterized queries in the same way; values are ignored
    const results = await this.client.execute(this.config.database!, query);
    let rows: R[] = [];
    if (results.primaryResults && results.primaryResults[0]) {
      rows = Array.from(results.primaryResults[0].rows()) as R[];
    }
    return rows;
  }

  public informationSchemaQuery() {
    // Kusto equivalent for listing columns
    return `
      .show tables
      | project table_name = Name
      | join kind=inner (
          .show table * schema
          | project table_name = TableName, column_name = ColumnName, data_type = ColumnType
        ) on table_name
      | project table_name, column_name, data_type
    `;
  }

  public async createSchemaIfNotExists(schemaName: string): Promise<void> {
    // Not supported in Kusto
    throw new Error('Unable to create schema, Kusto does not support it');
  }

  public async getTablesQuery(schemaName: string) {
    // Kusto does not have schemas, so ignore schemaName
    return this.query<TableQueryResult>('.show tables');
  }

  public async downloadQueryResults(query: string, values: unknown[], _options: DownloadQueryResultsOptions): Promise<DownloadQueryResultsResult> {
    const results = await this.client.execute(this.config.database!, query);
    const rows: any[] = [];
    let types: TableStructure = [];
    if (results.primaryResults && results.primaryResults[0]) {
      const table = results.primaryResults[0];
      types = table.columns.map(col => ({
        name: col.name ?? '',
        type: col.type ?? '',
      }));
      rows.push(...Array.from(table.rows()));
    }
    return {
      rows,
      types,
    };
  }

  protected normalizeQueryValues(values: unknown[]) {
    // Not used for Kusto
    return values;
  }

  protected normaliseResponse(res: any) {
    return res;
  }
}
