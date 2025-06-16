const fs = require('fs');
const path = require('path');
const { v4: uuidv4 } = require('uuid');
const csv = require('csv-parser');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

const dbFilePath = './lead_database.json';

// Load or initialize JSON database
function loadDatabase() {
  if (!fs.existsSync(dbFilePath)) {
    fs.writeFileSync(dbFilePath, JSON.stringify({}));
  }
  return JSON.parse(fs.readFileSync(dbFilePath));
}

function saveDatabase(db) {
  fs.writeFileSync(dbFilePath, JSON.stringify(db, null, 2));
}

function getOrCreateTrackId(db, email, firstName, lastName, country, sid, incomingTrackId = null) {
  if (db[email]) {
    if (incomingTrackId && incomingTrackId !== db[email].track_id) {
      console.warn(`⚠️  Warning: Mismatched track_id for ${email}. Overriding CSV value with database value.`);
    }
    return db[email].track_id;
  } else {
    const newId = uuidv4();
    db[email] = {
      first_name: firstName,
      last_name: lastName,
      country: country,
      sid: sid,
      track_id: newId,
    };
    return newId;
  }
}

async function processCSV(inputPath) {
  const outputPath = inputPath.replace(/\.csv$/, '_tracked.csv');
  const results = [];

  const readStream = fs.createReadStream(inputPath);
  const parser = csv();

  readStream.pipe(parser)
    .on('data', row => results.push(row))
    .on('end', async () => {
      const outputRecords = [];
      const db = loadDatabase();

      for (const row of results) {
        const email = row.EMAIL || row.email;
        const first = row.FIRST_NAME || row.first_name || '';
        const last = row.LAST_NAME || row.last_name || '';
        const country = row.COUNTRY || row.country || '';
        const sid = row.SID || row.sid || '';
        const incomingTrackId = row.track_id || null;

        const trackId = getOrCreateTrackId(db, email, first, last, country, sid, incomingTrackId);
        row.track_id = trackId;
        outputRecords.push(row);
      }

      saveDatabase(db);

      const headers = Object.keys(outputRecords[0]).map(h => ({ id: h, title: h }));
      const csvWriter = createCsvWriter({
        path: outputPath,
        header: headers,
      });

      await csvWriter.writeRecords(outputRecords);
      console.log(`✅ Output written to ${outputPath}`);
    });
}

// --- Run Script ---
const inputFile = process.argv[2];
if (!inputFile) {
  console.error('❌ Usage: node script.js <file.csv>');
  process.exit(1);
}

processCSV(inputFile);
