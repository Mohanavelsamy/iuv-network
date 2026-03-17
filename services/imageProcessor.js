const sharp = require("sharp");
const path = require("path");
const fs = require("fs");

async function processImage(inputPath) {
  try {

    const outputDir = path.join(__dirname, "../processed");

    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir);
    }

    const fileName = path.basename(inputPath).split(".")[0];

    const outputPath = path.join(outputDir, `${fileName}.webp`);

    await sharp(inputPath)
      .rotate() // auto-rotate based on EXIF
      .resize(1080, 1080, {
        fit: "cover"
      })
      .webp({
        quality: 82
      })
      .toFile(outputPath);

    return outputPath;

  } catch (err) {
    console.error("Image processing error:", err);
    throw err;
  }
}

module.exports = processImage;