import Table from 'cli-table3';
import { Command } from 'commander';
import path from 'path';
import { fileURLToPath } from 'url';
import { z } from 'zod';
import { SchemaComparator } from '../core/comparator.js';
import { Orchestrator } from '../core/orchestrator.js';
import { DbConnection } from '../db/connection.js';
import { SchemaInspector } from '../inspector/inspector.js';
import { SchemaExporter } from '../utils/exporter.js';
import { logger } from '../utils/logger.js';

const dbConfigSchema = z.object({
  host: z.string().default('localhost'),
  port: z.string().default('5432').transform(Number),
  database: z.string(),
  user: z.string(),
  password: z.string().optional(),
  schema: z.string().default('public'),
});

const backupConfigSchema = dbConfigSchema.extend({
  tables: z.string().optional().transform(t => t ? t.split(',') : undefined),
  outputDir: z.string().default(path.join(process.cwd(), 'files', 'dumps')),
});

export async function runCli() {
  const program = new Command();

  program
    .name('persistor')
    .description('Database utility for PostgreSQL')
    .version('1.0.0');

  program
    .command('backup')
    .description('Full PostgreSQL backup tool using only SELECT permissions')
    .requiredOption('-d, --database <string>', 'Database name')
    .requiredOption('-u, --user <string>', 'Database user')
    .option('-h, --host <string>', 'Database host', 'localhost')
    .option('-p, --port <number>', 'Database port', '5432')
    .option('-P, --password <string>', 'Database password')
    .option('-s, --schema <string>', 'Database schema', 'public')
    .option('-t, --tables <string>', 'Comma separated list of tables to backup')
    .option('-o, --output-dir <string>', 'Output directory for the backup file', path.join(process.cwd(), 'files', 'dumps'))
    .action(async (options: any) => {
      try {
        const validatedConfig = backupConfigSchema.parse(options);
        const orchestrator = new Orchestrator(validatedConfig);
        await orchestrator.run();
      } catch (error) {
        if (error instanceof z.ZodError) {
          logger.error({ errors: error.issues }, 'Invalid configuration');
        } else {
          logger.error(error, 'Error during execution');
        }
        process.exit(1);
      }
    });

  program
    .command('compare')
    .description('Compare two database schemas and list differences')
    // Source DB Options
    .requiredOption('--s-db <string>', 'Source Database name')
    .requiredOption('--s-user <string>', 'Source Database user')
    .option('--s-host <string>', 'Source Database host', 'localhost')
    .option('--s-port <number>', 'Source Database port', '5432')
    .option('--s-pass <string>', 'Source Database password')
    .option('--s-schema <string>', 'Source Database schema', 'public')
    // Target DB Options
    .requiredOption('--t-db <string>', 'Target Database name')
    .requiredOption('--t-user <string>', 'Target Database user')
    .option('--t-host <string>', 'Target Database host', 'localhost')
    .option('--t-port <number>', 'Target Database port', '5432')
    .option('--t-pass <string>', 'Target Database password')
    .option('--t-schema <string>', 'Target Database schema', 'public')
    .option('-o, --output <string>', 'Output file path (e.g. results.xlsx or results.csv)')
    .action(async (options: any) => {
      try {
        const sourceConfig = dbConfigSchema.parse({
          host: options.sHost,
          port: options.sPort,
          database: options.sDb,
          user: options.sUser,
          password: options.sPass,
          schema: options.sSchema,
        });

        const targetConfig = dbConfigSchema.parse({
          host: options.tHost,
          port: options.tPort,
          database: options.tDb,
          user: options.tUser,
          password: options.tPass,
          schema: options.tSchema,
        });

        const sourceDb = new DbConnection(sourceConfig);
        const targetDb = new DbConnection(targetConfig);

        const comparator = new SchemaComparator(
          new SchemaInspector(sourceDb),
          new SchemaInspector(targetDb)
        );

        const result = await comparator.compare(sourceConfig.schema, targetConfig.schema);

        if (result.diffs.length === 0) {
          console.log('\n✅ No differences found. Schemas are identical.');
        } else {
          console.log(`\n❌ Found ${result.diffs.length} differences:\n`);
          
          const table = new Table({
            head: ['Table', 'Type', 'Column/Constraint', 'Source (Expected)', 'Target (Actual)'],
            colWidths: [40, 30, 50, 60, 30],
            wordWrap: true
          });

          result.diffs.forEach(diff => {
            table.push([
              diff.table,
              diff.type.replace('_', ' '),
              diff.column || '-',
              diff.expected || '-',
              diff.actual || '-'
            ]);
          });

          console.log(table.toString());

          if (options.output === true) {
            // Handle case where flag is present but no value is given
            const timestamp = path.join(process.cwd(), 'files', 'comparisons', `comparison_${result.sourceDb}_vs_${result.targetDb}.xlsx`);
            await SchemaExporter.exportToSheet(result, timestamp);
          } else if (options.output) {
            // If it's a relative path, put it in comparisons dir if it doesn't look like an absolute path
            let outputPath = options.output;
            if (!path.isAbsolute(outputPath)) {
              outputPath = path.join(process.cwd(), 'files', 'comparisons', outputPath);
            }
            await SchemaExporter.exportToSheet(result, outputPath);
          }
        }

        await sourceDb.close();
        await targetDb.close();
      } catch (error) {
        if (error instanceof z.ZodError) {
          logger.error({ errors: error.issues }, 'Invalid configuration');
        } else {
          logger.error(error, 'Error during execution');
        }
        process.exit(1);
      }
    });

  program.parse();
}

const isMain = process.argv[1] && fileURLToPath(import.meta.url).endsWith(process.argv[1]);

if (isMain) {
  runCli();
}
