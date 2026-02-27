import { DatabaseObject, TableColumn, TableConstraint, TableIndex, TableMetadata } from '../../types/index.js';
import { logger } from '../../utils/logger.js';
import { ISchemaInspector } from '../interfaces.js';
import { OracleConnection } from './OracleConnection.js';

export class OracleInspector implements ISchemaInspector {
  constructor(private db: OracleConnection) {}

  async listTables(schema: string = '', targetTables?: string[]): Promise<string[]> {
    // Oracle usually uses OWNER/SCHEMA as the user name if not specified
    let query = `SELECT table_name FROM user_tables`;
    const params: any[] = [];

    if (targetTables && targetTables.length > 0) {
      const placeholders = targetTables.map((_, i) => `:${i + 1}`).join(',');
      query += ` WHERE table_name IN (${placeholders})`;
      params.push(...targetTables);
    }

    const rows = await this.db.query(query, params);
    return rows.map(r => r.TABLE_NAME);
  }

  async getTableMetadata(schema: string, tableName: string): Promise<TableMetadata> {
    logger.info(`Inspecting metadata for Oracle table: ${tableName}`);
    
    const [columns, constraints, indexes] = await Promise.all([
      this.getColumns(tableName),
      this.getConstraints(tableName),
      this.getIndexes(tableName)
    ]);

    return {
      name: tableName,
      schema: schema || 'USER', 
      columns,
      constraints,
      indexes,
      sequences: [] // Oracle sequences are usually independent objects
    };
  }

  private async getColumns(tableName: string): Promise<TableColumn[]> {
    const rows = await this.db.query(`
      SELECT 
        column_name as "name", 
        data_type as "dataType", 
        nullable as "isNullable", 
        data_default as "columnDefault",
        data_length as "characterMaximumLength",
        data_precision as "numericPrecision",
        data_scale as "numericScale"
      FROM user_tab_columns 
      WHERE table_name = :1
      ORDER BY column_id
    `, [tableName]);
    
    return rows.map(r => ({
      name: r.name,
      dataType: r.dataType,
      isNullable: r.isNullable === 'Y',
      columnDefault: r.columnDefault || null,
      characterMaximumLength: r.characterMaximumLength,
      numericPrecision: r.numericPrecision,
      numericScale: r.numericScale,
      udtName: r.dataType // Oracle doesn't have UDT name in the same way
    }));
  }

  private async getConstraints(tableName: string): Promise<TableConstraint[]> {
    const rows = await this.db.query(`
      SELECT
        c.constraint_name as "name",
        c.constraint_type as "type",
        cc.column_name
      FROM 
        user_constraints c
        JOIN user_cons_columns cc ON c.constraint_name = cc.constraint_name
      WHERE c.table_name = :1
    `, [tableName]);

    const constraintsMap = new Map<string, TableConstraint>();

    for (const row of rows) {
      if (!constraintsMap.has(row.name)) {
        let type: any = 'CHECK';
        if (row.type === 'P') type = 'PRIMARY KEY';
        else if (row.type === 'R') type = 'FOREIGN KEY';
        else if (row.type === 'U') type = 'UNIQUE';

        constraintsMap.set(row.name, {
          name: row.name,
          type: type,
          columns: [],
          foreignColumns: []
        });
      }
      const c = constraintsMap.get(row.name)!;
      if (!c.columns.includes(row.column_name)) c.columns.push(row.column_name);
    }

    return Array.from(constraintsMap.values());
  }

  private async getIndexes(tableName: string): Promise<TableIndex[]> {
    const rows = await this.db.query(`
      SELECT
        index_name as name
      FROM user_indexes
      WHERE table_name = :1
      AND generated = 'N'
    `, [tableName]);
    
    return rows.map(r => ({
      name: r.NAME,
      definition: `-- Index ${r.NAME} on ${tableName}` // Oracle index definition is more complex to reconstruct from views easily
    }));
  }

  async listFunctions(schema: string = ''): Promise<DatabaseObject[]> {
    const rows = await this.db.query(`
      SELECT 
        name,
        text as definition
      FROM user_source
      WHERE type = 'FUNCTION'
      ORDER BY name, line
    `);
    
    // user_source has one row per line, need to aggregate
    const functionsMap = new Map<string, string>();
    for (const row of rows) {
      const current = functionsMap.get(row.NAME) || '';
      functionsMap.set(row.NAME, current + row.DEFINITION);
    }

    return Array.from(functionsMap.entries()).map(([name, definition]) => ({
      name,
      schema: schema || 'USER',
      definition
    }));
  }

  async listTriggers(schema: string = ''): Promise<DatabaseObject[]> {
    const rows = await this.db.query(`
      SELECT 
        trigger_name as name,
        table_name as "tableName",
        trigger_body as definition
      FROM user_triggers
    `);
    
    return rows.map(r => ({
      name: r.NAME,
      schema: schema || 'USER',
      tableName: r.tableName,
      definition: r.DEFINITION
    }));
  }
}
