import { BaseFilter, BaseQuery } from '@cubejs-backend/schema-compiler';

// Kusto time binning functions
const GRANULARITY_TO_BIN: Record<string, (date: string) => string> = {
  day: date => `bin(${date}, 1d)`,
  week: date => `bin(${date}, 7d)`,
  hour: date => `bin(${date}, 1h)`,
  minute: date => `bin(${date}, 1m)`,
  second: date => `bin(${date}, 1s)`,
  month: date => `startofmonth(${date})`,
  quarter: date => `startofquarter(${date})`,
  year: date => `startofyear(${date})`
};

class KustoFilter extends BaseFilter {
  public likeIgnoreCase(column, not, param, type: string) {
    // Kusto uses 'contains', 'startswith', 'endswith', and 'has'
    const paramExpr = this.allocateParam(param);
    let expr = '';
    if (!type || type === 'contains') {
      expr = `${not ? 'not ' : ''}contains(tolower(${column}), tolower(${paramExpr}))`;
    } else if (type === 'starts') {
      expr = `${not ? 'not ' : ''}startswith(tolower(${column}), tolower(${paramExpr}))`;
    } else if (type === 'ends') {
      expr = `${not ? 'not ' : ''}endswith(tolower(${column}), tolower(${paramExpr}))`;
    } else {
      expr = `${not ? 'not ' : ''}has(tolower(${column}), tolower(${paramExpr}))`;
    }
    return expr;
  }
}

export class KustoQuery extends BaseQuery {
  public newFilter(filter) {
    return new KustoFilter(this, filter);
  }

  public timeGroupedColumn(granularity: string, dimension: string) {
    if (!GRANULARITY_TO_BIN[granularity]) {
      throw new Error(`Unsupported granularity: ${granularity}`);
    }
    return GRANULARITY_TO_BIN[granularity](dimension);
  }

  public convertTz(field: string) {
    // Kusto does not support arbitrary timezone conversion, but you can use datetime_add for offset
    // For UTC, just return the field
    return field;
  }

  public subtractInterval(date: string, interval: string) {
    // interval should be in ISO8601 duration format, e.g., 'P1D' for 1 day
    // Kusto: datetime_add('second', -seconds, date)
    // For simplicity, assume interval is in seconds
    return `datetime_add('second', -1 * ${interval}, ${date})`;
  }

  public addInterval(date: string, interval: string) {
    // interval should be in ISO8601 duration format, e.g., 'P1D' for 1 day
    // For simplicity, assume interval is in seconds
    return `datetime_add('second', ${interval}, ${date})`;
  }

  public timeStampCast(value: string) {
    return `todatetime(${value})`;
  }

  public timeStampParam() {
    return this.timeStampCast('?');
  }

  public nowTimestampSql(): string {
    return 'now()';
  }
}
