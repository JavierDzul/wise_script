const fs = require('fs');
const csv = require('csv-parser');

const dbPath = './lead_database.json';

// Load the database
function loadDb() {
  if (!fs.existsSync(dbPath)) {
    console.error('❌ Database file not found:', dbPath);
    process.exit(1);
  }
  return JSON.parse(fs.readFileSync(dbPath));
}

// Load a CSV file and return email → track_id mapping
function loadCsv(csvFilePath) {
  return new Promise((resolve, reject) => {
    const records = {};
    fs.createReadStream(csvFilePath)
      .pipe(csv())
      .on('data', row => {
        if (row.EMAIL && row.track_id) {
          records[row.EMAIL] = row.track_id;
        }
      })
      .on('end', () => resolve(records))
      .on('error', reject);
  });
}

async function compareWithDatabase(csvPath) {
  const db = loadDb();
  const csvData = await loadCsv(csvPath);

  let mismatches = 0;
  let missingInDb = 0;

  for (const email in csvData) {
    const csvTrackId = csvData[email];
    const dbRecord = db[email];

    if (!dbRecord) {
      console.log(`⚠️  Email not found in DB: ${email}`);
      missingInDb++;
    } else if (dbRecord.track_id !== csvTrackId) {
      console.log(`❌ Mismatch for ${email}: CSV(${csvTrackId}) ≠ DB(${dbRecord.track_id})`);
      mismatches++;
    }
  }

  if (mismatches === 0 && missingInDb === 0) {
    console.log('✅ All track_ids match between CSV and database.');
  } else {
    console.log(`\nSummary:\n- Mismatches: ${mismatches}\n- Missing in DB: ${missingInDb}`);
  }
}

// --- Run script ---
const inputCsv = process.argv[2];
if (!inputCsv) {
  console.error('❌ Usage: node compareWithDb.js <file.csv>');
  process.exit(1);
}

compareWithDatabase(inputCsv);


// comapare to the database 