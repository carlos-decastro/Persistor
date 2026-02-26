export type DiffType = 'MISSING_TABLE' | 'MISSING_COLUMN' | 'TYPE_MISMATCH' | 'NULLABILITY_MISMATCH' | 'DEFAULT_MISMATCH' | 'MISSING_CONSTRAINT' | 'MISSING_INDEX' | 'MISSING_FUNCTION' | 'MISSING_TRIGGER' | 'FUNCTION_MISMATCH' | 'TRIGGER_MISMATCH';

export interface SchemaDiff {
  type: DiffType;
  table: string;
  column?: string;
  expected?: string;
  actual?: string;
  details?: string;
  fix?: string;
}

export interface ComparisonResult {
  sourceDb: string;
  targetDb: string;
  diffs: SchemaDiff[];
}
