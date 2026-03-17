const supabase = require("../config/supabase");

const CYCLE_MS = 30_000;
const IDLE_CYCLES_THRESHOLD = 10; // cycles without pairing before user is strictly prioritized
const VALID_PAIRS_REFRESH_MS = 5 * 60_000; // 5 minutes

// Per-taluk in-memory state (kept simple/serializable so it can later move to Redis/Redis-like store)
// taluk -> {
//   validPairs: Set<"A|B">,
//   usedPairs: Set<"A|B">,
//   lastPairedAt: Map<userId, ms>,
//   lastPairedCycle: Map<userId, cycle>,
//   lastUserSnapshot: string[],   // sorted user_id list for change detection
//   lastValidPairsRefresh: number // timestamp ms
// }
const talukState = new Map();

function normalize(value) {
  return String(value || "").trim().toLowerCase();
}

// Redis-ready abstraction layer
function getState(talukId) {
  let state = talukState.get(talukId);
  if (!state) {
    state = {
      validPairs: new Set(),
      usedPairs: new Set(),
      lastPairedAt: new Map(),
      lastPairedCycle: new Map(),
      lastUserSnapshot: [],
      lastValidPairsRefresh: 0
    };
    talukState.set(talukId, state);
  }
  return state;
}

function setState(talukId, state) {
  talukState.set(talukId, state);
}

function parsePreferenceSet(row) {
  const raw = row.preferences;
  if (!raw) return new Set();

  if (Array.isArray(raw)) {
    return new Set(raw.map((v) => String(v).trim()).filter((s) => s.length > 0));
  }

  if (typeof raw === "string") {
    try {
      const parsed = JSON.parse(raw);
      if (Array.isArray(parsed)) {
        return new Set(parsed.map((v) => String(v).trim()).filter((s) => s.length > 0));
      }
    } catch {
      // fall through to CSV-style parsing
    }
    return new Set(
      raw
        .split(",")
        .map((s) => String(s).trim())
        .filter((s) => s.length > 0)
    );
  }

  return new Set();
}

function getBusinessCategory(row) {
  return row.business_category ?? row.category_id ?? null;
}

function getCompetitorCategory(row) {
  return row.competitor_business_category ?? null;
}

async function runMatchmakerOnce() {
  try {
    const nowMs = Date.now();
    const cycle = Math.floor(nowMs / CYCLE_MS);
    const nextBoundaryMs = nowMs - (nowMs % CYCLE_MS) + CYCLE_MS;
    const displayAt = new Date(nextBoundaryMs).toISOString();

    console.log("[Matchmaker] Scanning users from pairing_database...");
    console.log(`[Matchmaker] Cycle: ${cycle}`);
    console.log(`[Matchmaker] display_at: ${displayAt}`);

    const { data, error } = await supabase.from("pairing_database").select("*");

    if (error) {
      console.error("[Matchmaker] Supabase query error:", error);
      return;
    }

    const allRows = data || [];
    console.log(`[Matchmaker] Total users: ${allRows.length}`);

    // Log each user's raw status values
    for (const row of allRows) {
      console.log(
        `[Matchmaker] user_id=${row.user_id} subscription_status="${row.subscription_status}" device_status="${row.device_status}"`
      );
    }

    // Eligibility rules:
    // - subscription_status === "active"
    // - device_status === "online"
    // - file_link not null
    // - last_seen within last 60 seconds
    const eligible = allRows.filter((row) => {
      const subscription = normalize(row.subscription_status);
      const device = normalize(row.device_status);
      const hasFile = row.file_link != null;
      const lastSeenMs = row.last_seen ? new Date(row.last_seen).getTime() : NaN;
      const freshHeartbeat = Number.isFinite(lastSeenMs) && lastSeenMs > nowMs - 60_000;
      return subscription === "active" && device === "online" && hasFile && freshHeartbeat;
    });

    console.log(`[Matchmaker] Eligible users: ${eligible.length}`);

    // Group by taluk
    const byTaluk = new Map();
    for (const row of eligible) {
      const talukId = row.taluk ?? "unknown";
      if (!byTaluk.has(talukId)) byTaluk.set(talukId, []);
      byTaluk.get(talukId).push(row);
    }

    console.log(`[Matchmaker] Taluk groups: ${byTaluk.size}`);

    // For each taluk, precompute valid pairs (when needed), apply fairness memory, then sort and build rotational pairs
    for (const [talukId, users] of byTaluk.entries()) {
      const state = getState(talukId);
      const { validPairs, usedPairs, lastPairedAt, lastPairedCycle, lastUserSnapshot, lastValidPairsRefresh } = state;

      const sorted = users.slice().sort((a, b) => {
        const idA = a.user_id;
        const idB = b.user_id;

        const cycleA = lastPairedCycle.get(idA) ?? -Infinity;
        const cycleB = lastPairedCycle.get(idB) ?? -Infinity;
        const idleCyclesA = cycle - cycleA;
        const idleCyclesB = cycle - cycleB;

        // 1) idleCycles DESC (higher first when above threshold)
        const idleA = idleCyclesA > IDLE_CYCLES_THRESHOLD;
        const idleB = idleCyclesB > IDLE_CYCLES_THRESHOLD;
        if (idleA !== idleB) {
          return idleA ? -1 : 1;
        }

        // If both in same idle band, use raw idleCycles as tiebreaker (DESC)
        if (idleCyclesA !== idleCyclesB) {
          return idleCyclesB - idleCyclesA;
        }

        const lastA = lastPairedAt.get(idA) ?? 0;
        const lastB = lastPairedAt.get(idB) ?? 0;

        // 2) lastPairedAt ASC
        if (lastA !== lastB) {
          return lastA - lastB;
        }

        const firstBootA = a.first_boot_timestamp ? new Date(a.first_boot_timestamp).getTime() : 0;
        const firstBootB = b.first_boot_timestamp ? new Date(b.first_boot_timestamp).getTime() : 0;

        // 3) first_boot_timestamp ASC
        return firstBootA - firstBootB;
      });

      const n = sorted.length;
      console.log(`[Matchmaker] taluk=${talukId} users=${n}`);

      if (n < 2) {
        // 0 or 1 user → no pairing possible
        continue;
      }

      // --- VALID PAIR PRECOMPUTATION (per taluk) ---
      // Build maps for quick lookups
      const categoryById = new Map();
      const competitorById = new Map();
      const prefsById = new Map();

      for (const u of users) {
        const uid = u.user_id;
        categoryById.set(uid, getBusinessCategory(u));
        competitorById.set(uid, getCompetitorCategory(u));
        prefsById.set(uid, parsePreferenceSet(u));
      }

      // Detect if user set changed vs last snapshot (by sorted user_id list as strings)
      const currentIds = sorted.map((u) => String(u.user_id));
      let usersChanged = currentIds.length !== lastUserSnapshot.length;
      if (!usersChanged) {
        for (let i = 0; i < currentIds.length; i++) {
          if (currentIds[i] !== lastUserSnapshot[i]) {
            usersChanged = true;
            break;
          }
        }
      }

      const needsTimeRefresh = nowMs - lastValidPairsRefresh > VALID_PAIRS_REFRESH_MS;

      if (usersChanged || needsTimeRefresh) {
        validPairs.clear();

        for (let i = 0; i < n; i++) {
          for (let j = i + 1; j < n; j++) {
            const a = sorted[i];
            const b = sorted[j];
            const idA = a.user_id;
            const idB = b.user_id;

            const categoryA = categoryById.get(idA);
            const categoryB = categoryById.get(idB);
            const competitorA = competitorById.get(idA);
            const competitorB = competitorById.get(idB);
            const prefsA = prefsById.get(idA);
            const prefsB = prefsById.get(idB);

            const keyA = String(idA);
            const keyB = String(idB);
            const pairKey = keyA < keyB ? `${keyA}|${keyB}` : `${keyB}|${keyA}`;

            // Category / competitor conflicts
            if (categoryA != null && categoryB != null && categoryA === categoryB) {
              console.log(
                `[Matchmaker] Skip precompute pair ${idA} ↔ ${idB}: category conflict (same business_category)`
              );
              continue;
            }
            if (competitorA != null && categoryB != null && competitorA === categoryB) {
              console.log(
                `[Matchmaker] Skip precompute pair ${idA} ↔ ${idB}: category conflict (A.competitor_business_category)`
              );
              continue;
            }
            if (competitorB != null && categoryA != null && competitorB === categoryA) {
              console.log(
                `[Matchmaker] Skip precompute pair ${idA} ↔ ${idB}: category conflict (B.competitor_business_category)`
              );
              continue;
            }

            // Preferences conflicts (string user IDs)
            if (prefsA.has(String(idB)) || prefsB.has(String(idA))) {
              console.log(
                `[Matchmaker] Skip precompute pair ${idA} ↔ ${idB}: preference conflict`
              );
              continue;
            }

            validPairs.add(pairKey);
          }
        }

        state.lastUserSnapshot = currentIds;
        state.lastValidPairsRefresh = nowMs;
      }

      // --- FAIRNESS RESET based on valid pairs ---
      if (validPairs.size === 0) {
        console.log(`[Matchmaker] No valid pairs for taluk=${talukId}, skipping`);
        continue;
      }

      if (usedPairs.size >= validPairs.size) {
        console.log(
          `[Matchmaker] Fairness reset (all valid pairs exhausted) for taluk=${talukId} (usedPairs.size=${usedPairs.size}, validPairs.size=${validPairs.size})`
        );
        usedPairs.clear();
      }

      // Rotation offset k (Option B):
      // k = cycle % (n - 1) + 1  → 1..(n-1), avoids self pairing
      const k = (cycle % (n - 1)) + 1;
      const pairs = []; // store [userA, userB] for payload
      let remainingUsers = sorted.slice();

      // Consume users: each user can appear at most once in this cycle
      while (remainingUsers.length >= 2) {
        const a = remainingUsers[0];
        const idA = a.user_id;
        const categoryA = categoryById.get(idA);
        const competitorA = competitorById.get(idA);
        const prefsA = prefsById.get(idA);

        let pairedIndex = -1;

        const m = remainingUsers.length;
        for (let offset = 0; offset < m; offset++) {
          // Skip offset=0 because that's A itself
          const j = (0 + k + offset) % m;
          const b = remainingUsers[j];

          if (!b) continue;

          const idB = b.user_id;
          const categoryB = categoryById.get(idB);
          const competitorB = competitorById.get(idB);
          const prefsB = prefsById.get(idB);

          // Self pairing
          if (idA === idB) {
            console.log(`[Matchmaker] Reject pair ${idA} ↔ ${idB}: self`);
            continue;
          }

          const keyA = String(idA);
          const keyB = String(idB);
          const pairKey = keyA < keyB ? `${keyA}|${keyB}` : `${keyB}|${keyA}`;

          // Only allow pairs that were precomputed as valid
          if (!validPairs.has(pairKey)) {
            console.log(
              `[Matchmaker] Reject pair ${idA} ↔ ${idB}: not in validPairs (category/preference conflict)`
            );
            continue;
          }

          // Fairness memory: skip pairs already used in previous cycles
          if (usedPairs.has(pairKey)) {
            console.log(
              `[Matchmaker] Reject pair ${idA} ↔ ${idB}: already used in cycle history`
            );
            continue;
          }

          // Accept this pair
          usedPairs.add(pairKey);
          lastPairedAt.set(idA, nowMs);
          lastPairedAt.set(idB, nowMs);
          lastPairedCycle.set(idA, cycle);
          lastPairedCycle.set(idB, cycle);

          pairs.push([idA, idB]);
          console.log(
            `[Matchmaker] Pair: ${idA} ↔ ${idB} (taluk=${talukId}, k=${k}, offset=${offset})`
          );

          pairedIndex = j;
          break;
        }

        if (pairedIndex === -1) {
          console.log(
            `[Matchmaker] No valid pair found for user ${idA} in taluk=${talukId} after ${remainingUsers.length} attempts`
          );
          // Remove A only
          remainingUsers = remainingUsers.slice(1);
        } else {
          // Remove both A (index 0) and B (pairedIndex)
          const indicesToRemove = new Set([0, pairedIndex]);
          remainingUsers = remainingUsers.filter((_, idx) => !indicesToRemove.has(idx));
        }
      }

      // Handle odd/unpaired user: push to high priority next cycle
      if (remainingUsers.length === 1) {
        const lone = remainingUsers[0];
        const loneId = lone.user_id;
        console.log(
          `[Matchmaker] User ${loneId} unpaired → priority next cycle (taluk=${talukId})`
        );
        // Force high idleCycles next cycle
        lastPairedCycle.set(loneId, cycle - IDLE_CYCLES_THRESHOLD - 1);
      }

      const payload = {
        cycle,
        display_at: displayAt,
        pairs
      };

      publishPairing(talukId, payload);

      // persist updated state (keeps Redis migration trivial later)
      setState(talukId, state);
    }
  } catch (err) {
    console.error("[Matchmaker] Unexpected error:", err);
  }
}

function publishPairing(talukId, payload) {
  const topic = `iuv/taluk/${talukId}/pairing`;

  // Placeholder for MQTT integration; keeps logging for now
  console.log(
    "[Matchmaker][MQTT]",
    "topic=",
    topic,
    "payload=",
    JSON.stringify({ taluk: talukId, ...payload }, null, 2)
  );
}

function startMatchmaker() {
  console.log("[Matchmaker] Matchmaker started");

  // Run once immediately
  runMatchmakerOnce();

  // Then every 30 seconds
  setInterval(runMatchmakerOnce, CYCLE_MS);
}

module.exports = startMatchmaker;

