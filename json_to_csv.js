const fs = require('fs');
const path = require('path');
const { Parser } = require('json2csv');

function exportJsonToCsv(jsonPath, optionalName = '') {
  if (!fs.existsSync(jsonPath)) {
    console.error(`❌ File not found: ${jsonPath}`);
    process.exit(1);
  }

  const jsonData = JSON.parse(fs.readFileSync(jsonPath, 'utf-8'));
  const dataArray = Object.entries(jsonData).map(([email, data]) => ({ email, ...data }));

  if (!dataArray.length) {
    console.warn('⚠️ No data to export.');
    return;
  }

  const fields = Object.keys(dataArray[0]);
  const parser = new Parser({ fields });
  const csv = parser.parse(dataArray);

  const baseName = path.basename(jsonPath, '.json');
  const dateStr = new Date().toISOString().split('T')[0];
  const outputName = `${baseName}_${dateStr}${optionalName ? `_${optionalName}` : ''}.csv`;
  fs.writeFileSync(outputName, csv);

  console.log(`✅ CSV exported: ${outputName}`);
}

// --- Run script ---
const args = process.argv.slice(2);
if (args.length < 1) {
  console.error('❌ Usage: node export.js <json_path> [optional_name]');
  process.exit(1);
}

const [jsonPath, optionalName] = args;
exportJsonToCsv(jsonPath, optionalName);
