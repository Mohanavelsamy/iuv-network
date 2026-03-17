const fs = require("fs");
const path = require("path");

function cleanFolder(folderPath) {

  if (!fs.existsSync(folderPath)) return;

  const files = fs.readdirSync(folderPath);

  for (const file of files) {

    const filePath = path.join(folderPath, file);

    try {

      const stat = fs.statSync(filePath);

      const age = Date.now() - stat.mtimeMs;

      if (age > 3600000) { // older than 1 hour

        fs.unlinkSync(filePath);
        console.log("Deleted old temp file:", file);

      }

    } catch (err) {

      console.log("Cleanup error:", err.message);

    }

  }

}

function runCleanup() {

  const tempPath = path.join(__dirname, "../temp");
  const processedPath = path.join(__dirname, "../processed");

  cleanFolder(tempPath);
  cleanFolder(processedPath);

}

module.exports = runCleanup;