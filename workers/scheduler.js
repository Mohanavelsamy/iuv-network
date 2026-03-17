const supabase = require("../config/supabase");
const processJob = require("./processor");

async function scanForJobs() {

  try {

    console.log("Scanning for content jobs...");

    const { data, error } = await supabase
      .from("pairing_database")
      .select("*")
      .not("file_link", "is", null)
      .not("file_hash", "is", null)
      .neq("file_hash", "processed_hash")
      .eq("content_processing_status", "ready")
      .limit(5);

    if (error) {
      console.error("Scheduler error:", error);
      return;
    }

    if (!data || data.length === 0) {
      console.log("No jobs found");
      return;
    }

    console.log(`Found ${data.length} job(s)`);

    for (const row of data) {

      console.log(`Dispatching job for user: ${row.user_id}`);

      await processJob(row);

    }

  } catch (err) {

    console.error("Unexpected scheduler error:", err);

  }

}

function startScheduler() {

  console.log("Scheduler started");

  // run once immediately
  scanForJobs();

  // run every 10 seconds
  setInterval(scanForJobs, 10000);

}

module.exports = startScheduler;