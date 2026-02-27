import { DatabaseObject, TableMetadata } from '../types/index.js';

export type DbType = 'postgres' | 'oracle';

export interface IDbConnection {
  query<T = any>(text: string, params?: any[]): Promise<T[]>;
  close(): Promise<void>;
}

export interface ISchemaInspector {
  listTables(schema: string, targetTables?: string[]): Promise<string[]>;
  getTableMetadata(schema: string, tableName: string): Promise<TableMetadata>;
  listFunctions(schema: string): Promise<DatabaseObject[]>;
  listTriggers(schema: string): Promise<DatabaseObject[]>;
}

export interface IDDLGenerator {
  generateDatabaseCreate(databaseName: string): string;
  generateSchemaCreate(schema: string): string;
  generateTableCreate(meta: TableMetadata): string;
  generateConstraints(meta: TableMetadata): string;
  generateIndexes(meta: TableMetadata): string;
  generateSequences(meta: TableMetadata): string;
  generateAddColumn(table: string, schema: string, column: any): string;
  generateAlterColumnType(table: string, schema: string, column: any): string;
  generateAlterColumnNullability(table: string, schema: string, column: any): string;
  generateAlterColumnDefault(table: string, schema: string, column: any): string;
  generateDropTrigger(table: string, schema: string, triggerName: string): string;
  generateConstraintFix(table: string, schema: string, constraint: any): string;
}

export interface IDataExtractor {
  streamTableData(meta: TableMetadata, chunkSize?: number): AsyncGenerator<string[]>;
}
