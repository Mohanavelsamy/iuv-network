require("dotenv").config();

const startScheduler = require("./workers/scheduler");
const startRecoveryWorker = require("./workers/recovery");
const startCleanupWorker = require("./workers/cleanup");

// Start scheduler
startScheduler();

// Recovery worker (every 2 minutes)
setInterval(startRecoveryWorker, 2 * 60 * 1000);

// Cleanup worker (every 1 hour)
setInterval(startCleanupWorker, 60 * 60 * 1000);

console.log("Content pipeline started...");