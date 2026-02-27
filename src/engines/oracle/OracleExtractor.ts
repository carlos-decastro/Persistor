import oracledb from 'oracledb';
import { TableMetadata } from '../../types/index.js';
import { IDataExtractor } from '../interfaces.js';
import { OracleConnection } from './OracleConnection.js';

export class OracleExtractor implements IDataExtractor {
  constructor(private db: OracleConnection) {}

  async *streamTableData(meta: TableMetadata, chunkSize: number = 1000): AsyncGenerator<string[]> {
    const conn = await this.db.getConnection();
    const query = `SELECT * FROM ${meta.name}`;
    
    const result = await conn.execute(query, [], {
      resultSet: true,
      outFormat: oracledb.OUT_FORMAT_OBJECT
    });

    const resultSet = result.resultSet!;
    
    try {
      let rows: any[];
      do {
        rows = await resultSet.getRows(chunkSize);

        if (rows.length > 0) {
          const insertStatements = rows.map(row => this.formatInsert(meta, row));
          yield insertStatements;
        }
      } while (rows.length > 0);
    } finally {
      await resultSet.close();
    }
  }

  private formatInsert(meta: TableMetadata, row: any): string {
    const columns = meta.columns.map(c => `${c.name}`).join(', ');
    const values = meta.columns.map(c => this.formatValue(row[c.name], c.dataType)).join(', ');
    
    return `INSERT INTO ${meta.name} (${columns}) VALUES (${values});`;
  }

  private formatValue(val: any, type: string): string {
    if (val === null || val === undefined) return 'NULL';

    const t = type.toLowerCase();

    if (t === 'number' && typeof val === 'number') {
      return val.toString();
    }

    if (t === 'date' || t.includes('timestamp')) {
        // Simple Oracle date format
        return `TO_TIMESTAMP('${new Date(val).toISOString().slice(0, 19).replace('T', ' ')}', 'YYYY-MM-DD HH24:MI:SS')`;
    }

    // Default: string escape
    return `'${val.toString().replace(/'/g, "''")}'`;
  }
}
