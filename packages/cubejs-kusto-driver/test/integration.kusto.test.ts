import { Client, KustoConnectionStringBuilder } from 'azure-kusto-data';
import { DefaultAzureCredential } from '@azure/identity';
import { DownloadQueryResultsOptions } from '@cubejs-backend/base-driver';
import { KustoDriver } from '../src/KustoDriver';

/**
 * Class representing the test table schema
 */
class TestTableRow {
  public Id: number;

  public Name: string;

  public Value: number;

  public Date: string;

  public UValue?: string;

  public constructor(id: number, name: string, value: number, date: string, uvalue?: string) {
    this.Id = id;
    this.Name = name;
    this.Value = value;
    this.Date = date;
    this.UValue = uvalue;
  }
}

describe('KustoDriver Integration', () => {
  let driver: KustoDriver;
  let kustoClient: Client;
  const tableName = `CubeJS_INT_${Date.now()}`;
  const clusterUrl = process.env.KUSTO_CLUSTER_URL || '';
  const database = process.env.KUSTO_DATABASE || '';
  beforeAll(async () => {
    driver = new KustoDriver({
      clusterUrl,
      database
    });
    const kcsb = KustoConnectionStringBuilder.withTokenCredential(clusterUrl, new DefaultAzureCredential());
    kustoClient = new Client(kcsb);
    const createTable = `.create table ${tableName} (Id: int, Name: string, Value: real , Date: datetime)`;
    await kustoClient.executeMgmt(database, createTable);
    const ingestData = `.ingest inline into table ${tableName} <|
      1,Test,123.45,datetime(2023-01-01T00:00:00Z)
      2,Test2,678.90,datetime(2023-01-02T00:00:00Z)
    `;
    await kustoClient.execute(database, ingestData);
  });

  it('should connect and run a simple query', async () => {
    const result = await driver.query<{ Now: string }>('print Now=now()');
    expect(result.length).toBe(1);
    expect(result[0]).toHaveProperty('Now');
  });

  it('should list tables', async () => {
    const tables = await driver.getTablesQuery('');
    expect(Array.isArray(tables)).toBe(true);
  });

  it('should execute information schema query', async () => {
    const schemaQuery = driver.informationSchemaQuery();
    const result = await driver.query(schemaQuery);
    expect(Array.isArray(result)).toBe(true);
  });

  it('should download query results with correct structure', async () => {
    const query = 'print Name="Test", Value=123';
    // Set required highWaterMark option
    const options: DownloadQueryResultsOptions = {
      highWaterMark: 1048576 // 1MB buffer size
    };

    const result = await driver.downloadQueryResults(query, [], options);

    // Check if result is DownloadTableMemoryData
    if ('data' in result) {
      expect(Array.isArray(result.data)).toBe(true);
      expect(Array.isArray(result.types)).toBe(true);

      // Verify the structure of the returned types
      if (result.types.length > 0) {
        expect(result.types[0]).toHaveProperty('name');
        expect(result.types[0]).toHaveProperty('type');
      }
    }
  });

  it('should execute parametrized query correctly', async () => {
    const query = 'print Value=strcat("Hello, ", ?)';
    const params = ['World'];
    const result = await driver.query<{ Value: string }>(query, params);
    expect(result.length).toBe(1);
    expect(result[0].Value).toBe('Hello, World');
  });

  it('should handle quoted identifiers correctly', async () => {
    // This tests the normalizeQuery functionality
    const query = 'print ["Column With Spaces"]=123';
    const result = await driver.query<{ 'Column With Spaces': number }>(query);
    expect(result.length).toBe(1);
    expect(result[0]['Column With Spaces']).toBe(123);
  });

  it('should handle query with options', async () => {
    // Use a simple query with proper options
    const query = 'print Value=123 | take 1';
    const options: DownloadQueryResultsOptions = {
      highWaterMark: 1048576 // 1MB buffer size
    };

    const result = await driver.downloadQueryResults(query, [], options);

    // Check if result is DownloadTableMemoryData
    // if ('data' in result) {
    //   expect(result.data.size).toBe(1);
    // }
  });

  it('should throw error for invalid queries', async () => {
    const invalidQuery = 'invalid query syntax';
    await expect(driver.query(invalidQuery)).rejects.toThrow();
  });

  it('should test readOnly method', () => {
    expect(driver.readOnly()).toBe(true);
  });

  it('should throw error when trying to create schema', async () => {
    await expect(driver.createSchemaIfNotExists('test_schema')).rejects.toThrow(
      'Unable to create schema'
    );
  });

  it('should test connection', async () => {
    await driver.testConnection();
  });

  // Test specific error paths
  it('should handle specific error cases', async () => {
    // Testing with a non-existent table
    const nonExistentTableQuery = 'NonExistentTable | take 10';
    await expect(driver.query(nonExistentTableQuery)).rejects.toThrow();
  });

  it.each([
    { predicate: 'Id == ?', params: [1], expected: { Id: 1, Name: 'Test', Value: 123.45 } },
    { predicate: 'Name == ?', params: ['Test2'], expected: { Id: 2, Name: 'Test2', Value: 678.90 } },
    { predicate: 'Value > ?', params: [200], expected: { Id: 2, Name: 'Test2', Value: 678.90 } },
    { predicate: 'Value < ?', params: [200], expected: { Id: 1, Name: 'Test', Value: 123.45 } },
  ])('should execute parameterized query with predicate: %s', async ({ predicate, params, expected }) => {
    const query = `${tableName} | where ${predicate}`;
    const result = await driver.query<TestTableRow>(query, params);
    expect(result.length).toBe(1);
    expect(result[0].Id).toBe(expected.Id);
    expect(result[0].Name).toBe(expected.Name);
    expect(result[0].Value).toBe(expected.Value);
  });

  afterAll(async () => {
    // No explicit close needed for KustoData.Client
    await kustoClient.executeMgmt(database, `.drop table ${tableName}`);
  });
});
