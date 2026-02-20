import ExcelJS from 'exceljs';
import fs from 'fs-extra';
import path from 'path';
import { ComparisonResult } from '../types/comparison.js';
import { logger } from './logger.js';

export class SchemaExporter {
  static async exportToSheet(result: ComparisonResult, outputPath: string) {
    fs.ensureDirSync(path.dirname(outputPath));
    const ext = path.extname(outputPath).toLowerCase();

    if (ext === '.csv') {
      await this.exportToCSV(result, outputPath);
    } else {
      await this.exportToExcel(result, outputPath);
    }
  }

  private static async exportToExcel(result: ComparisonResult, outputPath: string) {
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Schema Comparison');

    sheet.columns = [
      { header: 'Table', key: 'table', width: 30 },
      { header: 'Type', key: 'type', width: 25 },
      { header: 'Column/Constraint', key: 'column', width: 35 },
      { header: 'Source (Expected)', key: 'expected', width: 50 },
      { header: 'Target (Actual)', key: 'actual', width: 50 },
    ];

    // Styling header
    sheet.getRow(1).font = { bold: true };
    sheet.getRow(1).fill = {
      type: 'pattern',
      pattern: 'solid',
      fgColor: { argb: 'FFE0E0E0' }
    };

    result.diffs.forEach(diff => {
      sheet.addRow({
        table: diff.table,
        type: diff.type.replace('_', ' '),
        column: diff.column || '-',
        expected: diff.expected || '-',
        actual: diff.actual || '-',
      });
    });

    await workbook.xlsx.writeFile(outputPath);
    logger.info(`Comparison results exported to Excel: ${outputPath}`);
  }

  private static async exportToCSV(result: ComparisonResult, outputPath: string) {
    const workbook = new ExcelJS.Workbook();
    const sheet = workbook.addWorksheet('Schema Comparison');

    sheet.columns = [
      { header: 'Table', key: 'table' },
      { header: 'Type', key: 'type' },
      { header: 'Column/Constraint', key: 'column' },
      { header: 'Source (Expected)', key: 'expected' },
      { header: 'Target (Actual)', key: 'actual' },
    ];

    result.diffs.forEach(diff => {
      sheet.addRow({
        table: diff.table,
        type: diff.type.replace('_', ' '),
        column: diff.column || '-',
        expected: diff.expected || '-',
        actual: diff.actual || '-',
      });
    });

    await workbook.csv.writeFile(outputPath);
    logger.info(`Comparison results exported to CSV: ${outputPath}`);
  }
}
