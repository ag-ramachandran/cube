/**
 * @copyright Cube Dev, Inc.
 * @license Apache-2.0
 * @fileoverview The `KustoDriver` and related types declaration.
 */

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
    await this.query<KustoData.KustoResultRow>('print 1');
  }

  public async query<R = unknown>(query: string, values: unknown[] = []): Promise<Array<R>> {
    // Create a new ClientRequestProperties object
    const crp = new KustoData.ClientRequestProperties();
    // If values are provided, process them based on format
    if (values && values.length > 0) {
      // Handle positional parameters (? placeholders)
      let paramIndex = 0;
      query = query.replace(/\?/g, (match) => {
        if (paramIndex < values.length) {
          const value = values[paramIndex++];
          // Handle different value types
          if (value === null || value === undefined) {
            return 'null';
          } else if (typeof value === 'string') {
            // Escape single quotes for string values
            return `'${value.replace(/'/g, '\'\'')}'`;
          } else if (typeof value === 'number') {
            return value.toString();
          } else if (typeof value === 'boolean') {
            return value ? 'true' : 'false';
          } else if (value instanceof Date) {
            // Format date as ISO string for Kusto
            return `datetime('${value.toISOString()}')`;
          } else {
            // For objects or arrays, stringify and quote
            return `'${JSON.stringify(value).replace(/'/g, '\'\'')}'`;
          }
        }
        return match; // If we run out of values, leave the ? as is
      });
    }

    // Execute the query with the parameters
    const results = query.trimStart().startsWith('.')
      ? await this.client.executeMgmt(this.config.database!, query, crp)
      : await this.client.executeQuery(this.config.database!, query, crp);

    let rows: R[] = [];
    if (results.primaryResults && results.primaryResults[0]) {
      rows = Array.from(results.primaryResults[0].rows()) as R[];
    }
    return rows;
  }

  public informationSchemaQuery() {
    // Kusto equivalent for listing columns
    return `
      .show database schema
        | where isnotempty(ColumnName)
        | project
            table_name=TableName,
            column_name=ColumnName,
            data_type = case(
                      // strings
                      ColumnType == "System.String",
                      "text",
                      ColumnType == "System.Guid",
                      "text",
                      ColumnType == "System.Object",
                      "text",
                      // dates
                      ColumnType == "System.DateTime",
                      "timestamp",
                      // decimals
                      ColumnType == "System.Decimal",
                      "decimal",
                      // floats
                      ColumnType in ("System.Double", "System.Single"),
                      "double",
                      // signed integers
                      ColumnType == "System.SByte",
                      "int",     // int8
                      ColumnType == "System.Int16",
                      "int",     // int16
                      ColumnType == "System.Int32",
                      "int",     // int32
                      ColumnType == "System.Int64",
                      "bigint",  // int64
                      // unsigned integers
                      ColumnType in ("System.UInt16", "System.UInt32"),
                      "int",
                      ColumnType == "System.UInt64",
                      "bigint",
                      // booleans
                      ColumnType == "System.Boolean",
                      "int",  // often mapped to 0/1
                      // everything else
                      "text")
                `;
  }

  public async createSchemaIfNotExists(schemaName: string): Promise<void> {
    // Not supported in Kusto
    throw new Error('Unable to create schema, Kusto does not support it');
  }

  public async getTablesQuery(schemaName: string) {
    // Kusto does not have schemas, so ignore schemaName
    return this.query<TableQueryResult>('.show tables | project table_name = TableName');
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
