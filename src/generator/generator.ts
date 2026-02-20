import { TableColumn, TableMetadata } from '../types/index.js';

export class DDLGenerator {
  generateDatabaseCreate(databaseName: string): string {
    return `-- Database Creation\nCREATE DATABASE "${databaseName}";\n\n`;
  }

  generateSchemaCreate(schema: string): string {
    if (schema === 'public') return '';
    return `CREATE SCHEMA IF NOT EXISTS "${schema}";\n\n`;
  }

  generateTableCreate(meta: TableMetadata): string {
    const columns = meta.columns.map(col => this.formatColumn(col)).join(',\n  ');
    
    let sql = `CREATE TABLE "${meta.schema}"."${meta.name}" (\n  ${columns}\n);\n\n`;

    // Add Primary Keys (can be inline but separating for clarity)
    const pks = meta.constraints.filter(c => c.type === 'PRIMARY KEY');
    for (const pk of pks) {
      sql += `ALTER TABLE "${meta.schema}"."${meta.name}" ADD CONSTRAINT "${pk.name}" PRIMARY KEY (${pk.columns.map(c => `"${c}"`).join(', ')});\n`;
    }

    return sql;
  }

  generateConstraints(meta: TableMetadata): string {
    let sql = '';
    const fks = meta.constraints.filter(c => c.type === 'FOREIGN KEY');
    
    for (const fk of fks) {
      sql += `ALTER TABLE "${meta.schema}"."${meta.name}" ADD CONSTRAINT "${fk.name}" FOREIGN KEY (${fk.columns.map(c => `"${c}"`).join(', ')}) REFERENCES "${meta.schema}"."${fk.foreignTable}" (${fk.foreignColumns?.map(c => `"${c}"`).join(', ')});\n`;
    }

    const uniques = meta.constraints.filter(c => c.type === 'UNIQUE');
    for (const u of uniques) {
      sql += `ALTER TABLE "${meta.schema}"."${meta.name}" ADD CONSTRAINT "${u.name}" UNIQUE (${u.columns.map(c => `"${c}"`).join(', ')});\n`;
    }

    return sql ? sql + '\n' : '';
  }

  generateIndexes(meta: TableMetadata): string {
    let sql = '';
    for (const idx of meta.indexes) {
      sql += `${idx.definition};\n`;
    }
    return sql ? sql + '\n' : '';
  }

  generateSequences(meta: TableMetadata): string {
    let sql = '';
    for (const seq of meta.sequences) {
      sql += `CREATE SEQUENCE IF NOT EXISTS "${meta.schema}"."${seq.name}"
  START WITH ${seq.startValue}
  INCREMENT BY ${seq.incrementBy}
  NO MINVALUE
  NO MAXVALUE
  CACHE ${seq.cacheSize};\n\n`;
      
      // Sync sequence value if needed
      if (seq.lastValue) {
        sql += `SELECT setval('"${meta.schema}"."${seq.name}"', ${seq.lastValue}, true);\n\n`;
      }
    }
    return sql;
  }

  private formatColumn(col: TableColumn): string {
    let parts = [`"${col.name}"`, col.dataType.toUpperCase()];

    if (col.characterMaximumLength) {
      parts[1] += `(${col.characterMaximumLength})`;
    } else if (col.numericPrecision !== null && col.numericScale !== null) {
      parts[1] += `(${col.numericPrecision}, ${col.numericScale})`;
    }

    if (!col.isNullable) {
      parts.push('NOT NULL');
    }

    if (col.columnDefault) {
      parts.push(`DEFAULT ${col.columnDefault}`);
    }

    return parts.join(' ');
  }
}
