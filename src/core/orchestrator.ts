import { EngineFactory } from '../engines/factory.js';
import { IDDLGenerator, IDataExtractor, IDbConnection, ISchemaInspector } from '../engines/interfaces.js';
import { DatabaseConfig, TableMetadata } from '../types/index.js';
import { logger } from '../utils/logger.js';
import { SQLWriter } from '../writer/writer.js';

export class Orchestrator {
  private db: IDbConnection;
  private inspector: ISchemaInspector;
  private ddlGen: IDDLGenerator;
  private extractor: IDataExtractor;
  private writer: SQLWriter;

  constructor(private config: DatabaseConfig) {
    const type = config.dbType || 'postgres';
    this.db = EngineFactory.createConnection(type, config);
    this.inspector = EngineFactory.createInspector(type, this.db);
    this.ddlGen = EngineFactory.createGenerator(type);
    this.extractor = EngineFactory.createExtractor(type, this.db);
    this.writer = new SQLWriter(config.outputDir, config.database);
  }

  async run() {
    try {
      logger.info('Starting backup process...');
      await this.writer.open();

      const schema = this.config.schema || 'public';

      // 0. Database and Schema
      await this.writer.write(this.ddlGen.generateDatabaseCreate(this.config.database));
      await this.writer.write(this.ddlGen.generateSchemaCreate(schema));

      const tableNames = await this.inspector.listTables(schema, this.config.tables);
      
      const tablesMetadata: TableMetadata[] = [];

      // 1. Inspect all tables
      logger.info(`Found ${tableNames.length} tables to process.`);
      for (const name of tableNames) {
        const meta = await this.inspector.getTableMetadata(schema, name);
        tablesMetadata.push(meta);
      }

      // 2. Generate Sequences
      logger.info('Generating sequences...');
      for (const meta of tablesMetadata) {
        const seqSql = this.ddlGen.generateSequences(meta);
        if (seqSql) await this.writer.write(seqSql);
      }

      // 3. Generate Table Structure (CREATE TABLE)
      logger.info('Generating table structures...');
      for (const meta of tablesMetadata) {
        const tableSql = this.ddlGen.generateTableCreate(meta);
        await this.writer.write(tableSql);
      }

      // 4. Extract and Write Data (INSERTs)
      logger.info('Extracting data...');
      for (const meta of tablesMetadata) {
        logger.info(`Streaming data from ${meta.name}...`);
        for await (const chunk of this.extractor.streamTableData(meta)) {
          await this.writer.write(chunk.join('\n') + '\n');
        }
        await this.writer.write('\n');
      }

      // 5. Generate Indexes
      logger.info('Generating indexes...');
      for (const meta of tablesMetadata) {
        const indexSql = this.ddlGen.generateIndexes(meta);
        if (indexSql) await this.writer.write(indexSql);
      }

      // 6. Generate Foreign Keys and Constraints
      logger.info('Generating foreign keys and constraints...');
      for (const meta of tablesMetadata) {
        const constraintSql = this.ddlGen.generateConstraints(meta);
        if (constraintSql) await this.writer.write(constraintSql);
      }

      await this.writer.close();
      await this.db.close();
      
      logger.info(`Backup completed successfully! Location: ${this.writer.getFilePath()}`);
    } catch (error) {
      logger.error(error, 'Backup failed');
      throw error;
    }
  }
}
