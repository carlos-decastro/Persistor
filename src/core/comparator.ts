import { SchemaInspector } from '../inspector/inspector.js';
import { ComparisonResult, SchemaDiff } from '../types/comparison.js';
import { TableMetadata } from '../types/index.js';
import { logger } from '../utils/logger.js';

export class SchemaComparator {
  constructor(
    private sourceInspector: SchemaInspector,
    private targetInspector: SchemaInspector
  ) {}

  async compare(sourceSchema: string = 'public', targetSchema: string = 'public'): Promise<ComparisonResult> {
    const diffs: SchemaDiff[] = [];
    
    logger.info('Starting schema comparison...');

    const sourceTables = await this.sourceInspector.listTables(sourceSchema);
    const targetTables = await this.targetInspector.listTables(targetSchema);

    // 1. Check for missing tables in target
    for (const tableName of sourceTables) {
      if (!targetTables.includes(tableName)) {
        diffs.push({
          type: 'MISSING_TABLE',
          table: tableName,
          details: `Table "${tableName}" exists in source but is missing in target.`
        });
        continue;
      }

      // 2. If table exists, compare structure
      const sourceMeta = await this.sourceInspector.getTableMetadata(sourceSchema, tableName);
      const targetMeta = await this.targetInspector.getTableMetadata(targetSchema, tableName);

      this.compareColumns(sourceMeta, targetMeta, diffs);
      this.compareConstraints(sourceMeta, targetMeta, diffs);
      this.compareIndexes(sourceMeta, targetMeta, diffs);
    }

    return {
      sourceDb: sourceSchema,
      targetDb: targetSchema,
      diffs
    };
  }

  private compareColumns(source: TableMetadata, target: TableMetadata, diffs: SchemaDiff[]) {
    const targetCols = new Map(target.columns.map(c => [c.name, c]));

    for (const sCol of source.columns) {
      const tCol = targetCols.get(sCol.name);

      if (!tCol) {
        diffs.push({
          type: 'MISSING_COLUMN',
          table: source.name,
          column: sCol.name,
          details: `Column "${sCol.name}" is missing in target table.`
        });
        continue;
      }

      // Compare Data Type (using udtName for better accuracy)
      if (sCol.udtName !== tCol.udtName) {
        diffs.push({
          type: 'TYPE_MISMATCH',
          table: source.name,
          column: sCol.name,
          expected: sCol.dataType,
          actual: tCol.dataType,
          details: `Column "${sCol.name}" type mismatch. Source: ${sCol.dataType} (${sCol.udtName}), Target: ${tCol.dataType} (${tCol.udtName})`
        });
      }

      // Compare Nullability
      if (sCol.isNullable !== tCol.isNullable) {
        diffs.push({
          type: 'NULLABILITY_MISMATCH',
          table: source.name,
          column: sCol.name,
          expected: sCol.isNullable ? 'NULL' : 'NOT NULL',
          actual: tCol.isNullable ? 'NULL' : 'NOT NULL'
        });
      }

      // Compare Defaults (simple string compare)
      if (sCol.columnDefault !== tCol.columnDefault) {
        diffs.push({
          type: 'DEFAULT_MISMATCH',
          table: source.name,
          column: sCol.name,
          expected: sCol.columnDefault || 'NULL',
          actual: tCol.columnDefault || 'NULL'
        });
      }
    }
  }

  private compareConstraints(source: TableMetadata, target: TableMetadata, diffs: SchemaDiff[]) {
    const targetConstraints = new Set(target.constraints.map(c => c.name));
    
    for (const sConstraint of source.constraints) {
        // Note: constraint names might differ but definitions be equal. 
        // For simplicity and since we control the source/target, we check by name first.
        if (!targetConstraints.has(sConstraint.name)) {
            diffs.push({
                type: 'MISSING_CONSTRAINT',
                table: source.name,
                column: sConstraint.name,
                details: `Constraint "${sConstraint.name}" (${sConstraint.type}) is missing in target.`
            });
        }
    }
  }

  private compareIndexes(source: TableMetadata, target: TableMetadata, diffs: SchemaDiff[]) {
    const targetIndexes = new Set(target.indexes.map(i => i.name));

    for (const sIndex of source.indexes) {
      if (!targetIndexes.has(sIndex.name)) {
        diffs.push({
          type: 'MISSING_INDEX',
          table: source.name,
          column: sIndex.name,
          details: `Index "${sIndex.name}" is missing in target.`
        });
      }
    }
  }
}
