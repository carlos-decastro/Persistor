import { TableColumn, TableMetadata } from '../../types/index.js';
import { IDDLGenerator } from '../interfaces.js';

export class OracleDDLGenerator implements IDDLGenerator {
  generateDatabaseCreate(databaseName: string): string {
    return `-- Oracle Database Creation (Handled externally or via Tablespaces)\n-- CREATE TABLESPACE ${databaseName}_DATA ... ;\n\n`;
  }

  generateSchemaCreate(schema: string): string {
    return `-- Oracle User/Schema Creation\n-- CREATE USER ${schema} IDENTIFIED BY password;\n-- GRANT CONNECT, RESOURCE TO ${schema};\n\n`;
  }

  generateTableCreate(meta: TableMetadata): string {
    const columns = meta.columns.map(col => this.formatColumn(col)).join(',\n  ');
    let sql = `CREATE TABLE ${meta.name} (\n  ${columns}\n);\n\n`;

    // Add Primary Keys
    const pks = meta.constraints.filter(c => c.type === 'PRIMARY KEY');
    for (const pk of pks) {
      sql += `ALTER TABLE ${meta.name} ADD CONSTRAINT ${pk.name} PRIMARY KEY (${pk.columns.join(', ')});\n`;
    }

    return sql;
  }

  generateConstraints(meta: TableMetadata): string {
    let sql = '';
    const fks = meta.constraints.filter(c => c.type === 'FOREIGN KEY');
    
    for (const fk of fks) {
      sql += `ALTER TABLE ${meta.name} ADD CONSTRAINT ${fk.name} FOREIGN KEY (${fk.columns.join(', ')}) REFERENCES ${fk.foreignTable} (${fk.foreignColumns?.join(', ')});\n`;
    }

    const uniques = meta.constraints.filter(c => c.type === 'UNIQUE');
    for (const u of uniques) {
      sql += `ALTER TABLE ${meta.name} ADD CONSTRAINT ${u.name} UNIQUE (${u.columns.join(', ')});\n`;
    }

    return sql ? sql + '\n' : '';
  }

  generateIndexes(meta: TableMetadata): string {
    let sql = '';
    for (const idx of meta.indexes) {
      sql += `-- ${idx.definition};\n`; // Index defs in Oracle are usually simple CREATE INDEX ...
    }
    return sql ? sql + '\n' : '';
  }

  generateSequences(meta: TableMetadata): string {
    return '-- Oracle Sequences are handled as independent objects.\n';
  }

  generateAddColumn(table: string, schema: string, column: TableColumn): string {
    const formattedCol = this.formatColumn(column);
    return `ALTER TABLE ${table} ADD (${formattedCol});`;
  }

  generateAlterColumnType(table: string, schema: string, column: TableColumn): string {
    const formattedCol = this.formatColumn(column);
    return `ALTER TABLE ${table} MODIFY (${formattedCol});`;
  }

  generateAlterColumnNullability(table: string, schema: string, column: TableColumn): string {
    return `ALTER TABLE ${table} MODIFY (${column.name} ${column.isNullable ? 'NULL' : 'NOT NULL'});`;
  }

  generateAlterColumnDefault(table: string, schema: string, column: TableColumn): string {
    return `ALTER TABLE ${table} MODIFY (${column.name} DEFAULT ${column.columnDefault || 'NULL'});`;
  }

  generateDropTrigger(table: string, schema: string, triggerName: string): string {
    return `DROP TRIGGER ${triggerName};`;
  }

  generateConstraintFix(table: string, schema: string, constraint: any): string {
    if (constraint.type === 'PRIMARY KEY') {
      return `ALTER TABLE ${table} ADD CONSTRAINT ${constraint.name} PRIMARY KEY (${constraint.columns.join(', ')});`;
    } else if (constraint.type === 'FOREIGN KEY') {
      return `ALTER TABLE ${table} ADD CONSTRAINT ${constraint.name} FOREIGN KEY (${constraint.columns.join(', ')}) REFERENCES ${constraint.foreignTable} (${constraint.foreignColumns?.join(', ')});`;
    } else if (constraint.type === 'UNIQUE') {
      return `ALTER TABLE ${table} ADD CONSTRAINT ${constraint.name} UNIQUE (${constraint.columns.join(', ')});`;
    }
    return '';
  }

  private formatColumn(col: TableColumn): string {
    let dataType = col.dataType.toUpperCase();
    
    // Type mapping Postgres -> Oracle (rough mapping)
    if (dataType.includes('VARCHAR') || dataType.includes('TEXT')) {
      dataType = 'VARCHAR2';
    } else if (dataType.includes('INT') || dataType.includes('NUMERIC') || dataType.includes('DECIMAL')) {
      dataType = 'NUMBER';
    } else if (dataType.includes('TIMESTAMP')) {
      dataType = 'TIMESTAMP';
    } else if (dataType.includes('BOOLEAN')) {
      dataType = 'NUMBER(1)'; // Boolean is usually NUMBER(1) in Oracle
    }

    let parts = [`${col.name}`, dataType];

    if (col.characterMaximumLength && dataType === 'VARCHAR2') {
      parts[1] += `(${col.characterMaximumLength})`;
    } else if (col.numericPrecision !== null && col.numericScale !== null && dataType === 'NUMBER') {
      parts[1] += `(${col.numericPrecision}, ${col.numericScale})`;
    }

    if (!col.isNullable) {
      parts.push('NOT NULL');
    }

    // Defaults in Oracle don't need parentheses usually but depend on the expression
    if (col.columnDefault) {
      parts.push(`DEFAULT ${col.columnDefault}`);
    }

    return parts.join(' ');
  }
}
