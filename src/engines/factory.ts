import { ConnectionConfig, DbType } from '../types/index.js';
import { IDataExtractor, IDbConnection, IDDLGenerator, ISchemaInspector } from './interfaces.js';
import { OracleConnection } from './oracle/OracleConnection.js';
import { OracleDDLGenerator } from './oracle/OracleDDLGenerator.js';
import { OracleExtractor } from './oracle/OracleExtractor.js';
import { OracleInspector } from './oracle/OracleInspector.js';
import { PostgresConnection } from './postgres/PostgresConnection.js';
import { PostgresDDLGenerator } from './postgres/PostgresDDLGenerator.js';
import { PostgresExtractor } from './postgres/PostgresExtractor.js';
import { PostgresInspector } from './postgres/PostgresInspector.js';

export class EngineFactory {
  static createConnection(type: DbType, config: ConnectionConfig): IDbConnection {
    switch (type) {
      case 'postgres':
        return new PostgresConnection(config);
      case 'oracle':
        return new OracleConnection(config);
      default:
        throw new Error(`Unsupported database type: ${type}`);
    }
  }

  static createInspector(type: DbType, connection: IDbConnection): ISchemaInspector {
    switch (type) {
      case 'postgres':
        return new PostgresInspector(connection as PostgresConnection);
      case 'oracle':
        return new OracleInspector(connection as OracleConnection);
      default:
        throw new Error(`Unsupported database type: ${type}`);
    }
  }

  static createGenerator(type: DbType): IDDLGenerator {
    switch (type) {
      case 'postgres':
        return new PostgresDDLGenerator();
      case 'oracle':
        return new OracleDDLGenerator();
      default:
        throw new Error(`Unsupported database type: ${type}`);
    }
  }

  static createExtractor(type: DbType, connection: IDbConnection): IDataExtractor {
    switch (type) {
      case 'postgres':
        return new PostgresExtractor(connection as PostgresConnection);
      case 'oracle':
        return new OracleExtractor(connection as OracleConnection);
      default:
        throw new Error(`Unsupported database type: ${type}`);
    }
  }
}
