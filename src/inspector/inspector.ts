import { DbConnection } from '../db/connection.js';
import { TableColumn, TableConstraint, TableIndex, TableMetadata, TableSequence } from '../types/index.js';
import { logger } from '../utils/logger.js';

export class SchemaInspector {
  constructor(private db: DbConnection) {}

  async listTables(schema: string = 'public', targetTables?: string[]): Promise<string[]> {
    let query = `
      SELECT table_name 
      FROM information_schema.tables 
      WHERE table_schema = $1 
      AND table_type = 'BASE TABLE'
    `;
    const params: any[] = [schema];

    if (targetTables && targetTables.length > 0) {
      query += ` AND table_name = ANY($2)`;
      params.push(targetTables);
    }

    const rows = await this.db.query(query, params);
    return rows.map(r => r.table_name);
  }

  async getTableMetadata(schema: string, tableName: string): Promise<TableMetadata> {
    logger.info(`Inspecting metadata for table: ${schema}.${tableName}`);
    
    const [columns, constraints, indexes, sequences] = await Promise.all([
      this.getColumns(schema, tableName),
      this.getConstraints(schema, tableName),
      this.getIndexes(schema, tableName),
      this.getSequences(schema, tableName)
    ]);

    return {
      name: tableName,
      schema,
      columns,
      constraints,
      indexes,
      sequences
    };
  }

  private async getColumns(schema: string, tableName: string): Promise<TableColumn[]> {
    const rows = await this.db.query(`
      SELECT 
        column_name as "name", 
        data_type as "dataType", 
        is_nullable = 'YES' as "isNullable", 
        column_default as "columnDefault",
        character_maximum_length as "characterMaximumLength",
        numeric_precision as "numericPrecision",
        numeric_scale as "numericScale",
        udt_name as "udtName"
      FROM information_schema.columns 
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position
    `, [schema, tableName]);
    
    return rows;
  }

  private async getConstraints(schema: string, tableName: string): Promise<TableConstraint[]> {
    // This is simplified, for real FKs we need more details
    const rows = await this.db.query(`
      SELECT
        tc.constraint_name as "name",
        tc.constraint_type as "type",
        kcu.column_name,
        ccu.table_name AS foreign_table,
        ccu.column_name AS foreign_column
      FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_name = kcu.constraint_name
          AND tc.table_schema = kcu.table_schema
        LEFT JOIN information_schema.constraint_column_usage AS ccu
          ON ccu.constraint_name = tc.constraint_name
          AND ccu.table_schema = tc.table_schema
      WHERE tc.table_schema = $1 AND tc.table_name = $2
    `, [schema, tableName]);

    const constraintsMap = new Map<string, TableConstraint>();

    for (const row of rows) {
      if (!constraintsMap.has(row.name)) {
        constraintsMap.set(row.name, {
          name: row.name,
          type: row.type,
          columns: [],
          foreignTable: row.foreign_table || undefined,
          foreignColumns: []
        });
      }
      const c = constraintsMap.get(row.name)!;
      if (!c.columns.includes(row.column_name)) c.columns.push(row.column_name);
      if (row.foreign_column && !c.foreignColumns?.includes(row.foreign_column)) {
        c.foreignColumns?.push(row.foreign_column);
      }
    }

    return Array.from(constraintsMap.values());
  }

  private async getIndexes(schema: string, tableName: string): Promise<TableIndex[]> {
    // Exclude PK indexes since they are usually created with the table or via ALTER TABLE
    const rows = await this.db.query(`
      SELECT
        indexname as name,
        indexdef as definition
      FROM pg_indexes
      WHERE schemaname = $1 AND tablename = $2
      AND indexname NOT IN (
        SELECT constraint_name 
        FROM information_schema.table_constraints 
        WHERE table_schema = $1 AND table_name = $2 AND constraint_type = 'PRIMARY KEY'
      )
    `, [schema, tableName]);
    
    return rows;
  }

  private async getSequences(schema: string, tableName: string): Promise<TableSequence[]> {
    // Find sequences associated with columns (SERIAL/IDENTITY)
    const rows = await this.db.query(`
      SELECT 
        s.relname as name,
        n.nspname as schema
      FROM pg_class s
      JOIN pg_namespace n ON n.oid = s.relnamespace
      JOIN pg_depend d ON d.objid = s.oid
      JOIN pg_class t ON t.oid = d.refobjid
      WHERE s.relkind = 'S' 
        AND n.nspname = $1 
        AND t.relname = $2
    `, [schema, tableName]);

    const result: TableSequence[] = [];
    for (const row of rows) {
      const seqDetails = await this.db.query(`
        SELECT * FROM pg_sequences WHERE schemaname = $1 AND sequencename = $2
      `, [row.schema, row.name]);
      
      if (seqDetails.length > 0) {
        const sd = seqDetails[0];
        result.push({
          name: sd.sequencename,
          dataType: sd.data_type,
          startValue: sd.start_value,
          minValue: sd.min_value,
          maxValue: sd.max_value,
          incrementBy: sd.increment_by,
          cycle: sd.cycle,
          cacheSize: sd.cache_size,
          lastValue: sd.last_value || sd.start_value
        });
      }
    }
    return result;
  }
}
