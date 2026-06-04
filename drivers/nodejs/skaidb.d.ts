// TypeScript declarations for the skaidb Node.js driver.

export type Consistency = 0 | 1 | 2 | "ONE" | "QUORUM" | "ALL";

export interface ClientOptions {
  host?: string;
  port?: number;
  user?: string;
  password?: string;
  consistency?: Consistency;
  connectTimeout?: number;
}

export interface Field {
  name: string;
}

export interface QueryConfig {
  text: string;
  values?: unknown[];
  consistency?: Consistency;
  rowMode?: "object" | "array";
}

export interface QueryResult<Row = any> {
  command: string;
  rowCount: number | null;
  rows: Row[];
  fields: Field[];
  columns?: string[];
}

export class Client {
  constructor(options?: ClientOptions);
  connect(): Promise<void>;
  query<Row = any>(text: string, values?: unknown[]): Promise<QueryResult<Row>>;
  query<Row = any>(config: QueryConfig, values?: unknown[]): Promise<QueryResult<Row>>;
  end(): Promise<void>;
}

export class SkaidbError extends Error {}

export const CONSISTENCY: { ONE: 0; QUORUM: 1; ALL: 2 };
