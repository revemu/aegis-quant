/**
 * CLI helper to create or update an admin user password directly in PostgreSQL (`bot_users`).
 * Usage: npm run orch:set-admin -- <username> <password>
 */

import dotenv from "dotenv";
import { OrchestratorStore } from "./orchestrator-store.js";

dotenv.config({ override: true });

async function main(): Promise<void> {
  const username = (process.argv[2] || process.env.ORCH_ADMIN_USER || "admin").trim().toLowerCase();
  const password = (process.argv[3] || process.env.ORCH_ADMIN_PASSWORD || "").trim();
  const databaseUrl = process.env.DATABASE_URL?.trim();

  if (!databaseUrl) {
    console.error("❌ DATABASE_URL is missing in .env");
    process.exit(1);
  }
  if (!password || password.length < 4) {
    console.error("Usage: npm run orch:set-admin -- <username> <password (min 4 chars)>");
    process.exit(1);
  }

  const store = new OrchestratorStore(databaseUrl);
  await store.init();
  await store.upsertAdminUser(username, password, username);
  console.log(`✅ Admin user "${username}" saved to PostgreSQL bot_users.`);
  await store.close();
}

main().catch((err) => {
  console.error("❌ Failed:", (err as Error).message);
  process.exit(1);
});
