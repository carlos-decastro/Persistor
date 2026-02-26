export interface ConnectionConfig {
  host: string;
  port: number;
  database: string;
  user: string;
  password?: string;
  schema?: string;
}

export interface DatabaseConfig extends ConnectionConfig {
  tables?: string[];
  outputDir: string;
}

export interface TableColumn {
  name: string;
  dataType: string;
  isNullable: boolean;
  columnDefault: string | null;
  characterMaximumLength: number | null;
  numericPrecision: number | null;
  numericScale: number | null;
  udtName: string;
}

export interface TableConstraint {
  name: string;
  type: 'PRIMARY KEY' | 'FOREIGN KEY' | 'UNIQUE' | 'CHECK';
  columns: string[];
  foreignTable?: string;
  foreignColumns?: string[];
  definition?: string;
}

export interface TableIndex {
  name: string;
  definition: string;
}

export interface TableSequence {
  name: string;
  dataType: string;
  startValue: string;
  minValue: string;
  maxValue: string;
  incrementBy: string;
  cycle: boolean;
  cacheSize: string;
  lastValue: string;
}

export interface TableMetadata {
  name: string;
  schema: string;
  columns: TableColumn[];
  constraints: TableConstraint[];
  indexes: TableIndex[];
  sequences: TableSequence[];
}

export interface DatabaseObject {
  name: string;
  schema: string;
  definition: string;
  tableName?: string; // For triggers
}
