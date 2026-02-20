import { Pool, PoolClient } from 'pg';
import { ConnectionConfig } from '../types/index.js';
import { logger } from '../utils/logger.js';

export class DbConnection {
  private pool: Pool;

  constructor(config: ConnectionConfig) {
    this.pool = new Pool({
      host: config.host,
      port: config.port,
      database: config.database,
      user: config.user,
      password: config.password,
      max: 10,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 2000,
    });

    this.pool.on('error', (err) => {
      logger.error(err, 'Unexpected error on idle client');
    });
  }

  async getClient(): Promise<PoolClient> {
    return await this.pool.connect();
  }

  async query<T = any>(text: string, params?: any[]): Promise<T[]> {
    const start = Date.now();
    try {
      const res = await this.pool.query(text, params);
      const duration = Date.now() - start;
      logger.debug({ query: text, duration, rows: res.rowCount }, 'Executed query');
      return res.rows;
    } catch (error) {
      logger.error({ query: text, error }, 'Query execution failed');
      throw error;
    }
  }

  async close() {
    await this.pool.end();
    logger.info('Database connection pool closed');
  }
}
