import dayjs from 'dayjs';
import fs from 'fs-extra';
import path from 'path';
import { logger } from '../utils/logger.js';

export class SQLWriter {
  private filePath: string;
  private writeStream: fs.WriteStream | null = null;

  constructor(outputDir: string, databaseName: string) {
    const timestamp = dayjs().format('YYYY_MM_DD_HH_mm');
    const fileName = `backup_${databaseName}_${timestamp}.sql`;
    
    fs.ensureDirSync(outputDir);
    this.filePath = path.join(outputDir, fileName);
  }

  async open() {
    this.writeStream = fs.createWriteStream(this.filePath, { flags: 'w' });
    logger.info(`Backup file created: ${this.filePath}`);
    
    // Write Header
    await this.write(`-- PERSISTOR PostgreSQL Backup\n`);
    await this.write(`-- Date: ${dayjs().format('YYYY-MM-DD HH:mm:ss')}\n`);
    await this.write(`-- --------------------------------------------------\n\n`);
    
    // Disable constraints globally if possible or use replica mode
    await this.write(`SET session_replication_role = replica;\n\n`);
  }

  async write(content: string): Promise<void> {
    if (!this.writeStream) throw new Error('Writer not opened');
    
    return new Promise((resolve, reject) => {
      const success = this.writeStream!.write(content, (err) => {
        if (err) reject(err);
        else resolve();
      });
      
      if (!success) {
        this.writeStream!.once('drain', resolve);
      }
    });
  }

  async close() {
    if (this.writeStream) {
      await this.write(`\nSET session_replication_role = DEFAULT;\n`);
      await this.write(`-- Backup Completed.\n`);
      
      return new Promise<void>((resolve) => {
        this.writeStream!.end(() => {
          logger.info('Backup file closed');
          resolve();
        });
      });
    }
  }

  getFilePath() {
    return this.filePath;
  }
}
