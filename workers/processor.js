const supabase = require("../config/supabase");
const downloadDriveFile = require("../services/driveDownloader");
const processImage = require("../services/imageProcessor");
const uploadToR2 = require("../services/r2Uploader");
const fs = require("fs");

async function processJob(row) {

  try {

    const userId = row.user_id;

    console.log(`Processing user: ${userId}`);

    // Lock job
    const { error: lockError } = await supabase
      .from("pairing_database")
      .update({
        content_processing_status: "processing",
        processing_started_at: new Date()
      })
      .eq("user_id", userId);

    if (lockError) {
      console.error("Lock error:", lockError);
      return;
    }

    console.log(`Job locked for ${userId}`);

    // STEP 1 — Download file from Google Drive
    const downloadedPath = await downloadDriveFile(row.file_link);

    console.log("File downloaded:", downloadedPath);

    // STEP 2 — Process image
    const processedPath = await processImage(downloadedPath);

    console.log("Image processed:", processedPath);

    // STEP 3 — Upload to R2
    const version = (row.content_version || 0) + 1;

    const creativeUrl = await uploadToR2(
      processedPath,
      userId,
      version
    );

    console.log("Uploaded to R2:", creativeUrl);

    // STEP 4 — Update Supabase
    const { error: updateError } = await supabase
      .from("pairing_database")
      .update({
        creative_url: creativeUrl,
        processed_hash: row.file_hash,
        content_version: version,
        content_processing_status: "completed",
        processing_started_at: null,
        updated_at: new Date()
      })
      .eq("user_id", userId);

    if (updateError) {
      console.error("Update error:", updateError);
      return;
    }

    // STEP 5 — Cleanup temp files
    try {
      fs.unlinkSync(downloadedPath);
      fs.unlinkSync(processedPath);
    } catch (cleanupErr) {
      console.log("Temp cleanup skipped:", cleanupErr.message);
    }

    console.log(`Processing complete for ${userId}`);

  } catch (err) {

    console.error("Processing failed:", err);

    await supabase
      .from("pairing_database")
      .update({
        content_processing_status: "failed",
        processing_started_at: null
      })
      .eq("user_id", row.user_id);

  }

}

module.exports = processJob;