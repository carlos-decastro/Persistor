import { DDLGenerator } from '../generator/generator.js';
import { SchemaInspector } from '../inspector/inspector.js';
import { ComparisonResult, SchemaDiff } from '../types/comparison.js';
import { TableMetadata } from '../types/index.js';
import { logger } from '../utils/logger.js';

export class SchemaComparator {
  private generator: DDLGenerator;

  constructor(
    private sourceInspector: SchemaInspector,
    private targetInspector: SchemaInspector
  ) {
    this.generator = new DDLGenerator();
  }

  async compare(sourceSchema: string = 'public', targetSchema: string = 'public'): Promise<ComparisonResult> {
    const diffs: SchemaDiff[] = [];
    
    logger.info('Starting schema comparison...');

    const sourceTables = await this.sourceInspector.listTables(sourceSchema);
    const targetTables = await this.targetInspector.listTables(targetSchema);

    // 1. Check for missing tables in target
    for (const tableName of sourceTables) {
      if (!targetTables.includes(tableName)) {
        const sourceMeta = await this.sourceInspector.getTableMetadata(sourceSchema, tableName);
        diffs.push({
          type: 'MISSING_TABLE',
          table: tableName,
          details: `Table "${tableName}" exists in source but is missing in target.`,
          fix: this.generator.generateTableCreate(sourceMeta)
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

    // 3. Compare Functions
    await this.compareFunctions(sourceSchema, targetSchema, diffs);

    // 4. Compare Triggers
    await this.compareTriggers(sourceSchema, targetSchema, diffs);

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
        let fix = `ALTER TABLE "${source.schema}"."${source.name}" ADD COLUMN "${sCol.name}" ${sCol.dataType.toUpperCase()}`;
        if (sCol.characterMaximumLength) {
          fix += `(${sCol.characterMaximumLength})`;
        } else if (sCol.numericPrecision !== null && sCol.numericScale !== null) {
          fix += `(${sCol.numericPrecision}, ${sCol.numericScale})`;
        }
        if (!sCol.isNullable) fix += ' NOT NULL';
        if (sCol.columnDefault) fix += ` DEFAULT ${sCol.columnDefault}`;
        fix += ';';

        diffs.push({
          type: 'MISSING_COLUMN',
          table: source.name,
          column: sCol.name,
          details: `Column "${sCol.name}" is missing in target table.`,
          fix
        });
        continue;
      }

      // Compare Data Type (using udtName for better accuracy)
      if (sCol.udtName !== tCol.udtName) {
        let typeStr = sCol.dataType.toUpperCase();
        if (sCol.characterMaximumLength) typeStr += `(${sCol.characterMaximumLength})`;
        else if (sCol.numericPrecision !== null && sCol.numericScale !== null) typeStr += `(${sCol.numericPrecision}, ${sCol.numericScale})`;

        diffs.push({
          type: 'TYPE_MISMATCH',
          table: source.name,
          column: sCol.name,
          expected: sCol.dataType,
          actual: tCol.dataType,
          details: `Column "${sCol.name}" type mismatch. Source: ${sCol.dataType} (${sCol.udtName}), Target: ${tCol.dataType} (${tCol.udtName})`,
          fix: `ALTER TABLE "${source.schema}"."${source.name}" ALTER COLUMN "${sCol.name}" TYPE ${typeStr};`
        });
      }

      // Compare Nullability
      if (sCol.isNullable !== tCol.isNullable) {
        diffs.push({
          type: 'NULLABILITY_MISMATCH',
          table: source.name,
          column: sCol.name,
          expected: sCol.isNullable ? 'NULL' : 'NOT NULL',
          actual: tCol.isNullable ? 'NULL' : 'NOT NULL',
          fix: `ALTER TABLE "${source.schema}"."${source.name}" ALTER COLUMN "${sCol.name}" ${sCol.isNullable ? 'DROP NOT NULL' : 'SET NOT NULL'};`
        });
      }

      // Compare Defaults (simple string compare)
      if (sCol.columnDefault !== tCol.columnDefault) {
        diffs.push({
          type: 'DEFAULT_MISMATCH',
          table: source.name,
          column: sCol.name,
          expected: sCol.columnDefault || 'NULL',
          actual: tCol.columnDefault || 'NULL',
          fix: sCol.columnDefault 
            ? `ALTER TABLE "${source.schema}"."${source.name}" ALTER COLUMN "${sCol.name}" SET DEFAULT ${sCol.columnDefault};`
            : `ALTER TABLE "${source.schema}"."${source.name}" ALTER COLUMN "${sCol.name}" DROP DEFAULT;`
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
            let fix = '';
            if (sConstraint.type === 'PRIMARY KEY') {
                fix = `ALTER TABLE "${source.schema}"."${source.name}" ADD CONSTRAINT "${sConstraint.name}" PRIMARY KEY (${sConstraint.columns.map(c => `"${c}"`).join(', ')});`;
            } else if (sConstraint.type === 'FOREIGN KEY') {
                fix = `ALTER TABLE "${source.schema}"."${source.name}" ADD CONSTRAINT "${sConstraint.name}" FOREIGN KEY (${sConstraint.columns.map(c => `"${c}"`).join(', ')}) REFERENCES "${source.schema}"."${sConstraint.foreignTable}" (${sConstraint.foreignColumns?.map(c => `"${c}"`).join(', ')});`;
            } else if (sConstraint.type === 'UNIQUE') {
                fix = `ALTER TABLE "${source.schema}"."${source.name}" ADD CONSTRAINT "${sConstraint.name}" UNIQUE (${sConstraint.columns.map(c => `"${c}"`).join(', ')});`;
            }

            diffs.push({
                type: 'MISSING_CONSTRAINT',
                table: source.name,
                column: sConstraint.name,
                details: `Constraint "${sConstraint.name}" (${sConstraint.type}) is missing in target.`,
                fix
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
          details: `Index "${sIndex.name}" is missing in target.`,
          fix: `${sIndex.definition};`
        });
      }
    }
  }

  private async compareFunctions(sourceSchema: string, targetSchema: string, diffs: SchemaDiff[]) {
    const sourceFunctions = await this.sourceInspector.listFunctions(sourceSchema);
    const targetFunctions = await this.targetInspector.listFunctions(targetSchema);
    const targetFuncMap = new Map(targetFunctions.map(f => [f.name, f]));

    for (const sFunc of sourceFunctions) {
      const tFunc = targetFuncMap.get(sFunc.name);

      if (!tFunc) {
        diffs.push({
          type: 'MISSING_FUNCTION',
          table: '-',
          column: sFunc.name,
          details: `Function "${sFunc.name}" is missing in target.`,
          fix: `${sFunc.definition};`
        });
        continue;
      }

      if (sFunc.definition !== tFunc.definition) {
        diffs.push({
          type: 'FUNCTION_MISMATCH',
          table: '-',
          column: sFunc.name,
          details: `Function "${sFunc.name}" definition mismatch.`,
          fix: `${sFunc.definition};`
        });
      }
    }
  }

  private async compareTriggers(sourceSchema: string, targetSchema: string, diffs: SchemaDiff[]) {
    const sourceTriggers = await this.sourceInspector.listTriggers(sourceSchema);
    const targetTriggers = await this.targetInspector.listTriggers(targetSchema);
    const targetTrigMap = new Map(targetTriggers.map(t => [`${t.tableName}.${t.name}`, t]));

    for (const sTrig of sourceTriggers) {
      const tTrig = targetTrigMap.get(`${sTrig.tableName}.${sTrig.name}`);

      if (!tTrig) {
        diffs.push({
          type: 'MISSING_TRIGGER',
          table: sTrig.tableName!,
          column: sTrig.name,
          details: `Trigger "${sTrig.name}" on table "${sTrig.tableName}" is missing in target.`,
          fix: `${sTrig.definition};`
        });
        continue;
      }

      if (sTrig.definition !== tTrig.definition) {
        diffs.push({
          type: 'TRIGGER_MISMATCH',
          table: sTrig.tableName!,
          column: sTrig.name,
          details: `Trigger "${sTrig.name}" on table "${sTrig.tableName}" definition mismatch.`,
          fix: `DROP TRIGGER IF EXISTS "${sTrig.name}" ON "${sTrig.schema}"."${sTrig.tableName}";\n${sTrig.definition};`
        });
      }
    }
  }
}
