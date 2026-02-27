import Cursor from 'pg-cursor';
import { TableMetadata } from '../../types/index.js';
import { IDataExtractor } from '../interfaces.js';
import { PostgresConnection } from './PostgresConnection.js';

export class PostgresExtractor implements IDataExtractor {
  constructor(private db: PostgresConnection) {}

  async *streamTableData(meta: TableMetadata, chunkSize: number = 1000): AsyncGenerator<string[]> {
    const client = await this.db.getClient();
    const query = `SELECT * FROM "${meta.schema}"."${meta.name}"`;
    const cursor = client.query(new Cursor(query));

    try {
      let rows: any[];
      do {
        rows = await new Promise((resolve, reject) => {
          cursor.read(chunkSize, (err, rows) => {
            if (err) reject(err);
            else resolve(rows);
          });
        });

        if (rows.length > 0) {
          const insertStatements = rows.map(row => this.formatInsert(meta, row));
          yield insertStatements;
        }
      } while (rows.length > 0);
    } finally {
      cursor.close(() => {
        client.release();
      });
    }
  }

  private formatInsert(meta: TableMetadata, row: any): string {
    const columns = meta.columns.map(c => `"${c.name}"`).join(', ');
    const values = meta.columns.map(c => this.formatValue(row[c.name], c.dataType)).join(', ');
    
    return `INSERT INTO ${meta.schema}.${meta.name} (${columns}) VALUES (${values});`;
  }

  private formatValue(val: any, type: string): string {
    if (val === null || val === undefined) return 'NULL';

    const t = type.toLowerCase();

    if (t === 'boolean') {
      return val ? 'TRUE' : 'FALSE';
    }

    if (t.includes('int') || t.includes('decimal') || t.includes('numeric') || t.includes('real') || t.includes('double')) {
      return val.toString();
    }

    if (t.includes('json')) {
      return `'${JSON.stringify(val).replace(/'/g, "''")}'::${t}`;
    }

    if (t === 'bytea') {
      if (Buffer.isBuffer(val)) {
        return `'\\x${val.toString('hex')}'`;
      }
      return `'${val.toString().replace(/'/g, "''")}'`;
    }
    
    if (Array.isArray(val)) {
        const elements = val.map(v => this.formatValue(v, 'text')).join(',');
        return `ARRAY[${elements}]`;
    }

    return `'${val.toString().replace(/'/g, "''")}'`;
  }
}
