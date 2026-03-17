require("dotenv").config();

const startScheduler = require("./workers/scheduler");
const startRecoveryWorker = require("./workers/recovery");
const startCleanupWorker = require("./workers/cleanup");
const startMatchmaker = require("./workers/matchmaker");

console.log("Content pipeline started...");

// Start scheduler
startScheduler();

// ✅ ADD THIS
startMatchmaker();

// Recovery worker
setInterval(startRecoveryWorker, 2 * 60 * 1000);

// Cleanup worker
setInterval(startCleanupWorker, 60 * 60 * 1000);