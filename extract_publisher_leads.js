const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');
const { v4: uuidv4 } = require('uuid');

const leadDBPath = './lead_database.json';
const outputDBPath = './new_leads.json';

function loadLeadDatabase() {
  if (!fs.existsSync(leadDBPath)) return {};
  return JSON.parse(fs.readFileSync(leadDBPath));
}

function extractSnippet(url) {
  try {
    const parsedUrl = new URL(url);
    return parsedUrl.searchParams.get('snippet') || 'WISE';
  } catch (e) {
    return 'wise';
  }
}

function generateUniqueTrackId(existingIds) {
  let newId;
  do {
    newId = uuidv4();
  } while (existingIds.has(newId));
  return newId;
}

async function processCSV(inputPath) {
  const leads = loadLeadDatabase();
  const newLeads = {};

  // Precompute the set of all existing track_ids for faster lookup
  const allTrackIds = new Set();
  for (const key in leads) {
    if (leads[key].track_id) {
      allTrackIds.add(leads[key].track_id);
    }
  }

  fs.createReadStream(inputPath)
    .pipe(csv())
    .on('data', (row) => {
      const normalized = Object.fromEntries(
        Object.entries(row).map(([k, v]) => [k.trim().toLowerCase(), v])
      );

      const email = normalized['email'];
      if (!email) {
        console.warn('⚠️ Skipping row without email:', row);
        return;
      }

      const firstName = normalized['first name'] || '';
      const firstPageSeen = normalized['first page seen'] || '';
      const publisherId = extractSnippet(firstPageSeen);

      let trackId;
      if (leads[email]) {
        trackId = leads[email].track_id;
      } else {
        trackId = generateUniqueTrackId(allTrackIds);
        allTrackIds.add(trackId);
        leads[email] = { track_id: trackId }; // Update base lead DB in memory
      }

      newLeads[email] = {
        first_name: firstName,
        publisher_id: publisherId,
        track_id: trackId
      };
    })
    .on('end', () => {
      fs.writeFileSync(outputDBPath, JSON.stringify(newLeads, null, 2));
      console.log(`✅ Extracted data with track IDs written to ${outputDBPath}`);
    })
    .on('error', (err) => {
      console.error('❌ Error while processing CSV:', err);
    });
}

// --- Run Script ---
const inputFile = process.argv[2];
if (!inputFile) {
  console.error('❌ Usage: node extract.js <file.csv>');
  process.exit(1);
}

processCSV(inputFile);
