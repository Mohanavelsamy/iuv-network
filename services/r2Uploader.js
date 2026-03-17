const AWS = require("aws-sdk");
const fs = require("fs");
const path = require("path");

require("dotenv").config();

const s3 = new AWS.S3({
  endpoint: `https://${process.env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com`,
  accessKeyId: process.env.R2_ACCESS_KEY,
  secretAccessKey: process.env.R2_SECRET_KEY,
  signatureVersion: "v4"
});

async function uploadToR2(localFilePath, userId, version) {

  try {

    if (!fs.existsSync(localFilePath)) {
      throw new Error(`File not found: ${localFilePath}`);
    }

    const fileBuffer = fs.readFileSync(localFilePath);

    const objectKey = `content/${userId}/v${version}_${Date.now()}.webp`;

    const params = {
      Bucket: process.env.R2_BUCKET,
      Key: objectKey,
      Body: fileBuffer,
      ContentType: "image/webp",

      // IMPORTANT FOR CDN PERFORMANCE
      CacheControl: "public, max-age=31536000, immutable",

      ContentLength: fileBuffer.length
    };

    await s3.putObject(params).promise();

    const publicUrl = `${process.env.R2_PUBLIC_URL}/${objectKey}`;

    return publicUrl;

  } catch (err) {

    console.error("R2 upload error:", err.message);

    throw err;

  }

}

module.exports = uploadToR2;