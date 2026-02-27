import { EngineFactory } from '../engines/factory.js';
import { IDDLGenerator, ISchemaInspector } from '../engines/interfaces.js';
import { ComparisonResult, SchemaDiff } from '../types/comparison.js';
import { DbType, TableMetadata } from '../types/index.js';
import { logger } from '../utils/logger.js';

export class SchemaComparator {
  private generator: IDDLGenerator;

  constructor(
    private sourceInspector: ISchemaInspector,
    private targetInspector: ISchemaInspector,
    private dbType: DbType = 'postgres'
  ) {
    this.generator = EngineFactory.createGenerator(this.dbType);
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
        diffs.push({
          type: 'MISSING_COLUMN',
          table: source.name,
          column: sCol.name,
          details: `Column "${sCol.name}" is missing in target table.`,
          fix: this.generator.generateAddColumn(source.name, source.schema, sCol)
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
          details: `Column "${sCol.name}" type mismatch. Source: ${sCol.dataType} (${sCol.udtName}), Target: ${tCol.dataType} (${tCol.udtName})`,
          fix: this.generator.generateAlterColumnType(source.name, source.schema, sCol)
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
          fix: this.generator.generateAlterColumnNullability(source.name, source.schema, sCol)
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
          fix: this.generator.generateAlterColumnDefault(source.name, source.schema, sCol)
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
          details: `Constraint "${sConstraint.name}" (${sConstraint.type}) is missing in target.`,
          fix: this.generator.generateConstraintFix(source.name, source.schema, sConstraint)
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
        const dropTrigger = this.generator.generateDropTrigger(sTrig.tableName!, sTrig.schema!, sTrig.name);
        diffs.push({
          type: 'TRIGGER_MISMATCH',
          table: sTrig.tableName!,
          column: sTrig.name,
          details: `Trigger "${sTrig.name}" on table "${sTrig.tableName}" definition mismatch.`,
          fix: `${dropTrigger}\n${sTrig.definition};`
        });
      }
    }
  }
}
