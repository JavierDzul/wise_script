const fs = require('fs');
const csv = require('csv-parser');

function loadCSV(filePath) {
  return new Promise((resolve, reject) => {
    const data = {};
    fs.createReadStream(filePath)
      .pipe(csv())
      .on('data', row => {
        if (row.EMAIL && row.track_id) {
          data[row.EMAIL] = row.track_id;
        }
      })
      .on('end', () => resolve(data))
      .on('error', reject);
  });
}

async function compareFiles(file1, file2) {
  const data1 = await loadCSV(file1);
  const data2 = await loadCSV(file2);

  let differences = 0;

  for (const email in data1) {
    if (data2[email] && data1[email] !== data2[email]) {
      console.log(`❌ Mismatch for ${email}: ${data1[email]} ≠ ${data2[email]}`);
      differences++;
    }
  }

  if (differences === 0) {
    console.log('✅ All track_ids match.');
  } else {
    console.log(`⚠️ ${differences} mismatches found.`);
  }
}

// --- Run the comparison ---
const [file1, file2] = process.argv.slice(2);
if (!file1 || !file2) {
  console.error('❌ Usage: node compareTrackIds.js <first_file.csv> <second_file.csv>');
  process.exit(1);
}

compareFiles(file1, file2);
