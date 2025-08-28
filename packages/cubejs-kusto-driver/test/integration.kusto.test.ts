import { KustoDriver } from '../src/KustoDriver';

describe('KustoDriver Integration', () => {
  let driver: KustoDriver;

  beforeAll(() => {
    driver = new KustoDriver({
      clusterUrl: process.env.KUSTO_CLUSTER_URL,
      database: process.env.KUSTO_DATABASE
    });
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

  afterAll(async () => {
    // No explicit close needed for KustoData.Client
  });
});
