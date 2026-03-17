const supabase = require("../config/supabase");

async function recoverStuckJobs() {

  try {

    console.log("Checking for stuck jobs...");

    const fiveMinutesAgo = new Date(Date.now() - 5 * 60 * 1000);

    const { data, error } = await supabase
      .from("pairing_database")
      .select("*")
      .eq("content_processing_status", "processing")
      .lt("processing_started_at", fiveMinutesAgo);

    if (error) {
      console.error("Recovery query error:", error);
      return;
    }

    if (!data || data.length === 0) {
      console.log("No stuck jobs");
      return;
    }

    console.log(`Recovering ${data.length} stuck job(s)`);

    for (const row of data) {

      await supabase
        .from("pairing_database")
        .update({
          content_processing_status: "ready",
          processing_started_at: null
        })
        .eq("user_id", row.user_id);

      console.log(`Recovered job for ${row.user_id}`);

    }

  } catch (err) {

    console.error("Recovery worker error:", err);

  }

}

module.exports = recoverStuckJobs;