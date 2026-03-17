const axios = require("axios");
const fs = require("fs");
const path = require("path");

function extractFileId(driveUrl) {
  try {
    const url = new URL(driveUrl);

    if (url.searchParams.get("id")) {
      return url.searchParams.get("id");
    }

    const match = driveUrl.match(/\/d\/([a-zA-Z0-9_-]+)/);
    if (match) {
      return match[1];
    }

    return null;
  } catch (err) {
    return null;
  }
}

async function downloadFromDrive(driveUrl) {
  const fileId = extractFileId(driveUrl);

  if (!fileId) {
    throw new Error("Invalid Google Drive URL");
  }

  const downloadUrl = `https://drive.google.com/uc?export=download&id=${fileId}`;

  const tempDir = path.join(__dirname, "../temp");

  if (!fs.existsSync(tempDir)) {
    fs.mkdirSync(tempDir);
  }

  const filePath = path.join(tempDir, `${fileId}.tmp`);

  const response = await axios({
    url: downloadUrl,
    method: "GET",
    responseType: "stream"
  });

  const writer = fs.createWriteStream(filePath);

  response.data.pipe(writer);

  return new Promise((resolve, reject) => {
    writer.on("finish", () => resolve(filePath));
    writer.on("error", reject);
  });
}

module.exports = downloadFromDrive;