import oracledb from 'oracledb';
import { ConnectionConfig } from '../../types/index.js';
import { logger } from '../../utils/logger.js';
import { IDbConnection } from '../interfaces.js';

export class OracleConnection implements IDbConnection {
  private connection: oracledb.Connection | null = null;
  private config: ConnectionConfig;

  constructor(config: ConnectionConfig) {
    this.config = config;
    // Enable thin mode if not already (default in newer versions)
  }

  private async connect(): Promise<oracledb.Connection> {
    if (this.connection) return this.connection;

    const connectString = `${this.config.host}:${this.config.port}/${this.config.database}`;
    
    try {
      this.connection = await oracledb.getConnection({
        user: this.config.user,
        password: this.config.password,
        connectString: connectString,
      });
      logger.info(`Connected to Oracle: ${connectString}`);
      return this.connection;
    } catch (error) {
      logger.error({ connectString, error }, 'Oracle connection failed');
      throw error;
    }
  }

  async query<T = any>(text: string, params?: any[]): Promise<T[]> {
    const conn = await this.connect();
    const start = Date.now();
    try {
      const result = await conn.execute(text, params || [], {
        outFormat: oracledb.OUT_FORMAT_OBJECT,
        autoCommit: true
      });
      
      const duration = Date.now() - start;
      logger.debug({ query: text, duration, rows: result.rows?.length }, 'Executed Oracle query');
      
      return (result.rows as T[]) || [];
    } catch (error) {
      logger.error({ query: text, error }, 'Oracle query execution failed');
      throw error;
    }
  }

  async close() {
    if (this.connection) {
      await this.connection.close();
      this.connection = null;
      logger.info('Oracle connection closed');
    }
  }

  async getConnection(): Promise<oracledb.Connection> {
    return await this.connect();
  }
}
