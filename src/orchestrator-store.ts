/**
 * PostgreSQL-backed Registry, Multi-Owner User Auth & Session Store
 * for the Multi-Instance Bot Orchestrator.
 */

import crypto from "node:crypto";
import postgres, { type Sql } from "postgres";

export type UserRole = "admin" | "owner";

export interface OrchUser {
  username: string;
  displayName: string;
  role: UserRole;
  createdAt: number;
  updatedAt: number;
}

export interface OrchInstance {
  instanceKey: string;
  displayName: string;
  ownerUsername: string;
  enabled: boolean;
  internalPort: number;
  createdAt: number;
  updatedAt: number;
}

const SESSION_TTL_MS = 30 * 24 * 3_600_000;
const BASE_INTERNAL_PORT = 13333;

function hashPassword(password: string): string {
  const salt = crypto.randomBytes(16).toString("hex");
  const hash = crypto.scryptSync(password, salt, 64).toString("hex");
  return `${salt}:${hash}`;
}

function verifyPassword(password: string, stored: string): boolean {
  const parts = String(stored || "").split(":");
  if (parts.length !== 2) return false;
  const [salt, hex] = parts;
  if (!salt || !hex) return false;
  try {
    const derived = crypto.scryptSync(password, salt, 64);
    const expected = Buffer.from(hex, "hex");
    if (derived.length !== expected.length) return false;
    return crypto.timingSafeEqual(derived, expected);
  } catch {
    return false;
  }
}

export function isValidInstanceKey(key: string): boolean {
  return /^[a-z0-9][a-z0-9_-]{1,31}$/.test(key);
}

export function isValidUsername(username: string): boolean {
  return /^[a-z0-9][a-z0-9_.-]{1,31}$/.test(username);
}

export class OrchestratorStore {
  private readonly sql: Sql;
  private readonly log: (msg: string) => void;
  public adminUsername = "admin";
  public seededDefaultAdmin = false;

  constructor(databaseUrl: string, log?: (msg: string) => void) {
    this.log = log ?? ((m) => console.log(`[orch-store] ${m}`));
    this.sql = postgres(databaseUrl, {
      max: 6,
      idle_timeout: 20,
      connect_timeout: 10,
      onnotice: () => {},
    });
  }

  public async init(): Promise<void> {
    await this.sql`
      CREATE TABLE IF NOT EXISTS bot_users (
        username TEXT PRIMARY KEY,
        display_name TEXT NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL DEFAULT 'owner',
        created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL
      )
    `;

    await this.sql`
      CREATE TABLE IF NOT EXISTS bot_sessions (
        token TEXT PRIMARY KEY,
        username TEXT NOT NULL,
        expires_at BIGINT NOT NULL,
        created_at BIGINT NOT NULL
      )
    `;

    await this.sql`
      CREATE INDEX IF NOT EXISTS idx_bot_sessions_exp
      ON bot_sessions (expires_at)
    `;

    await this.sql`
      CREATE TABLE IF NOT EXISTS bot_instances (
        instance_key TEXT PRIMARY KEY,
        display_name TEXT NOT NULL,
        owner_username TEXT NOT NULL,
        enabled BOOLEAN NOT NULL DEFAULT TRUE,
        internal_port INT NOT NULL,
        created_at BIGINT NOT NULL,
        updated_at BIGINT NOT NULL
      )
    `;

    // Cleanup expired sessions on boot
    await this.sql`DELETE FROM bot_sessions WHERE expires_at < ${Date.now()}`;

    await this.seedDefaultAdminIfNeeded();
    await this.syncDiscoveredInstances();
  }

  private async seedDefaultAdminIfNeeded(): Promise<void> {
    const [{ cnt }] = await this.sql<{ cnt: string }[]>`SELECT COUNT(*)::text AS cnt FROM bot_users`;
    const envAdminUser = (process.env.ORCH_ADMIN_USER || "admin").trim().toLowerCase();
    const envAdminPass = process.env.ORCH_ADMIN_PASSWORD?.trim();

    if (Number(cnt) === 0) {
      const initialPass = envAdminPass || "admin1234";
      const now = Date.now();
      await this.sql`
        INSERT INTO bot_users (username, display_name, password_hash, role, created_at, updated_at)
        VALUES (${envAdminUser}, ${"Administrator"}, ${hashPassword(initialPass)}, 'admin', ${now}, ${now})
        ON CONFLICT (username) DO NOTHING
      `;
      this.adminUsername = envAdminUser;
      if (!envAdminPass) {
        this.seededDefaultAdmin = true;
        this.log(`⚠️ Seeded initial admin user "${envAdminUser}" with default password "admin1234" — please change it in UI or set ORCH_ADMIN_PASSWORD!`);
      } else {
        this.log(`✅ Seeded initial admin user "${envAdminUser}" from ORCH_ADMIN_PASSWORD.`);
      }
      return;
    }

    const [firstAdmin] = await this.sql<{ username: string }[]>`
      SELECT username FROM bot_users WHERE role = 'admin' ORDER BY created_at ASC LIMIT 1
    `;
    this.adminUsername = firstAdmin?.username || envAdminUser;
  }

  /**
   * Auto-discovers pre-existing instances (e.g., `binance_dry`, `alpaca_kyne`) from
   * `bot_settings` / `bot_strategy_states` and registers them into `bot_instances`.
   */
  public async syncDiscoveredInstances(): Promise<number> {
    const hasSettings = await this.tableExists("bot_settings");
    const hasStates = await this.tableExists("bot_strategy_states");
    const discovered = new Set<string>();

    if (hasSettings) {
      const rows = await this.sql<{ instance_key: string }[]>`
        SELECT DISTINCT instance_key FROM bot_settings WHERE instance_key IS NOT NULL AND instance_key <> ''
      `;
      rows.forEach((r) => discovered.add(String(r.instance_key).trim()));
    }
    if (hasStates) {
      const rows = await this.sql<{ instance_key: string }[]>`
        SELECT DISTINCT instance_key FROM bot_strategy_states WHERE instance_key IS NOT NULL AND instance_key <> ''
      `;
      rows.forEach((r) => discovered.add(String(r.instance_key).trim()));
    }

    if (discovered.size === 0) return 0;

    const existing = await this.listInstances();
    const existingKeys = new Set(existing.map((i) => i.instanceKey));
    const usedPorts = new Set(existing.map((i) => i.internalPort));

    let added = 0;
    const now = Date.now();
    for (const key of Array.from(discovered).sort()) {
      if (existingKeys.has(key)) continue;
      const port = this.allocateNextPort(usedPorts);
      usedPorts.add(port);
      await this.sql`
        INSERT INTO bot_instances (instance_key, display_name, owner_username, enabled, internal_port, created_at, updated_at)
        VALUES (${key}, ${key}, ${this.adminUsername}, TRUE, ${port}, ${now}, ${now})
        ON CONFLICT (instance_key) DO NOTHING
      `;
      added++;
      this.log(`🔄 Auto-migrated existing instance "${key}" -> owner=${this.adminUsername}, port=${port}`);
    }
    return added;
  }

  private async tableExists(tableName: string): Promise<boolean> {
    const [row] = await this.sql<{ exists: boolean }[]>`
      SELECT to_regclass(${`public.${tableName}`}) IS NOT NULL AS exists
    `;
    return Boolean(row?.exists);
  }

  private allocateNextPort(usedPorts: Set<number>): number {
    let port = BASE_INTERNAL_PORT;
    while (usedPorts.has(port)) port++;
    return port;
  }

  // ── Users & Sessions ────────────────────────────────────────────────────

  public async authenticate(username: string, password: string): Promise<{ user: OrchUser; token: string } | null> {
    const cleanUser = String(username || "").trim().toLowerCase();
    const rows = await this.sql<any[]>`
      SELECT * FROM bot_users WHERE username = ${cleanUser} LIMIT 1
    `;
    const row = rows[0];
    if (!row || !verifyPassword(String(password || ""), String(row.password_hash))) {
      return null;
    }

    const token = crypto.randomBytes(32).toString("hex");
    const now = Date.now();
    await this.sql`
      INSERT INTO bot_sessions (token, username, expires_at, created_at)
      VALUES (${token}, ${cleanUser}, ${now + SESSION_TTL_MS}, ${now})
    `;

    return {
      user: {
        username: String(row.username),
        displayName: String(row.display_name || row.username),
        role: row.role === "admin" ? "admin" : "owner",
        createdAt: Number(row.created_at),
        updatedAt: Number(row.updated_at),
      },
      token,
    };
  }

  public async getUserByToken(token: string): Promise<OrchUser | null> {
    if (!token || token.length < 16) return null;
    const now = Date.now();
    const rows = await this.sql<any[]>`
      SELECT u.username, u.display_name, u.role, u.created_at, u.updated_at
      FROM bot_sessions s
      JOIN bot_users u ON u.username = s.username
      WHERE s.token = ${token} AND s.expires_at > ${now}
      LIMIT 1
    `;
    const r = rows[0];
    if (!r) return null;
    return {
      username: String(r.username),
      displayName: String(r.display_name || r.username),
      role: r.role === "admin" ? "admin" : "owner",
      createdAt: Number(r.created_at),
      updatedAt: Number(r.updated_at),
    };
  }

  public async logout(token: string): Promise<void> {
    if (!token) return;
    await this.sql`DELETE FROM bot_sessions WHERE token = ${token}`;
  }

  public async listUsers(): Promise<OrchUser[]> {
    const rows = await this.sql<any[]>`
      SELECT username, display_name, role, created_at, updated_at
      FROM bot_users
      ORDER BY role ASC, created_at ASC
    `;
    return rows.map((r) => ({
      username: String(r.username),
      displayName: String(r.display_name || r.username),
      role: r.role === "admin" ? "admin" : "owner",
      createdAt: Number(r.created_at),
      updatedAt: Number(r.updated_at),
    }));
  }

  public async createUser(params: {
    username: string;
    password: string;
    displayName?: string;
    role?: UserRole;
  }): Promise<OrchUser> {
    const username = String(params.username || "").trim().toLowerCase();
    if (!isValidUsername(username)) {
      throw new Error("Invalid username (use 2-32 chars: a-z, 0-9, _, ., -)");
    }
    if (!params.password || params.password.length < 4) {
      throw new Error("Password must be at least 4 characters");
    }
    const role: UserRole = params.role === "admin" ? "admin" : "owner";
    const displayName = (params.displayName || username).trim();
    const now = Date.now();

    await this.sql`
      INSERT INTO bot_users (username, display_name, password_hash, role, created_at, updated_at)
      VALUES (${username}, ${displayName}, ${hashPassword(params.password)}, ${role}, ${now}, ${now})
    `;

    return { username, displayName, role, createdAt: now, updatedAt: now };
  }

  public async updatePassword(username: string, newPassword: string): Promise<void> {
    const clean = String(username || "").trim().toLowerCase();
    if (!newPassword || newPassword.length < 4) {
      throw new Error("New password must be at least 4 characters");
    }
    const res = await this.sql`
      UPDATE bot_users
      SET password_hash = ${hashPassword(newPassword)}, updated_at = ${Date.now()}
      WHERE username = ${clean}
    `;
    if (res.count === 0) throw new Error(`User "${clean}" not found`);
  }

  public async verifyUserPassword(username: string, password: string): Promise<boolean> {
    const clean = String(username || "").trim().toLowerCase();
    const rows = await this.sql<{ password_hash: string }[]>`
      SELECT password_hash FROM bot_users WHERE username = ${clean} LIMIT 1
    `;
    return Boolean(rows[0] && verifyPassword(password, String(rows[0].password_hash)));
  }

  public async deleteUser(username: string): Promise<void> {
    const clean = String(username || "").trim().toLowerCase();
    const users = await this.listUsers();
    const target = users.find((u) => u.username === clean);
    if (!target) throw new Error(`User "${clean}" not found`);
    if (target.role === "admin") {
      const adminCount = users.filter((u) => u.role === "admin").length;
      if (adminCount <= 1) {
        throw new Error("Cannot delete the last admin account");
      }
    }
    await this.sql`DELETE FROM bot_sessions WHERE username = ${clean}`;
    await this.sql`DELETE FROM bot_users WHERE username = ${clean}`;
  }

  // ── Instance Registry ───────────────────────────────────────────────────

  public async listInstances(forUser?: Pick<OrchUser, "username" | "role">): Promise<OrchInstance[]> {
    const rows =
      forUser && forUser.role !== "admin"
        ? await this.sql<any[]>`
            SELECT * FROM bot_instances
            WHERE owner_username = ${forUser.username}
            ORDER BY created_at ASC, instance_key ASC
          `
        : await this.sql<any[]>`
            SELECT * FROM bot_instances
            ORDER BY created_at ASC, instance_key ASC
          `;

    return rows.map((r) => ({
      instanceKey: String(r.instance_key),
      displayName: String(r.display_name || r.instance_key),
      ownerUsername: String(r.owner_username),
      enabled: Boolean(r.enabled),
      internalPort: Number(r.internal_port),
      createdAt: Number(r.created_at),
      updatedAt: Number(r.updated_at),
    }));
  }

  public async getInstance(instanceKey: string): Promise<OrchInstance | null> {
    const rows = await this.sql<any[]>`
      SELECT * FROM bot_instances WHERE instance_key = ${instanceKey} LIMIT 1
    `;
    const r = rows[0];
    if (!r) return null;
    return {
      instanceKey: String(r.instance_key),
      displayName: String(r.display_name || r.instance_key),
      ownerUsername: String(r.owner_username),
      enabled: Boolean(r.enabled),
      internalPort: Number(r.internal_port),
      createdAt: Number(r.created_at),
      updatedAt: Number(r.updated_at),
    };
  }

  public async createInstance(params: {
    instanceKey: string;
    displayName?: string;
    ownerUsername: string;
    enabled?: boolean;
    initialSettings?: Record<string, any>;
  }): Promise<OrchInstance> {
    const key = String(params.instanceKey || "").trim().toLowerCase();
    if (!isValidInstanceKey(key)) {
      throw new Error("Invalid instanceKey (use 2-32 chars: a-z, 0-9, _, -)");
    }

    const existing = await this.getInstance(key);
    if (existing) {
      throw new Error(`Instance "${key}" already exists`);
    }

    const all = await this.listInstances();
    const usedPorts = new Set(all.map((i) => i.internalPort));
    const internalPort = this.allocateNextPort(usedPorts);
    const displayName = (params.displayName || key).trim();
    const ownerUsername = String(params.ownerUsername || this.adminUsername).trim().toLowerCase();
    const enabled = params.enabled !== false;
    const now = Date.now();

    await this.sql.begin(async (tx) => {
      const q = tx as unknown as Sql;
      await q`
        INSERT INTO bot_instances (instance_key, display_name, owner_username, enabled, internal_port, created_at, updated_at)
        VALUES (${key}, ${displayName}, ${ownerUsername}, ${enabled}, ${internalPort}, ${now}, ${now})
      `;

      if (params.initialSettings && Object.keys(params.initialSettings).length > 0) {
        await q`
          CREATE TABLE IF NOT EXISTS bot_settings (
            instance_key TEXT NOT NULL,
            key TEXT NOT NULL,
            value_json JSONB NOT NULL,
            updated_at BIGINT NOT NULL,
            PRIMARY KEY (instance_key, key)
          )
        `;
        for (const [sKey, sVal] of Object.entries(params.initialSettings)) {
          if (sVal === undefined) continue;
          const jsonStr = JSON.stringify(sVal);
          await q`
            INSERT INTO bot_settings (instance_key, key, value_json, updated_at)
            VALUES (${key}, ${sKey}, ${jsonStr}::jsonb, ${now})
            ON CONFLICT (instance_key, key) DO UPDATE SET
              value_json = EXCLUDED.value_json,
              updated_at = EXCLUDED.updated_at
          `;
        }
      }
    });

    return {
      instanceKey: key,
      displayName,
      ownerUsername,
      enabled,
      internalPort,
      createdAt: now,
      updatedAt: now,
    };
  }

  public async updateInstance(
    instanceKey: string,
    patch: { displayName?: string; ownerUsername?: string; enabled?: boolean },
  ): Promise<OrchInstance | null> {
    const existing = await this.getInstance(instanceKey);
    if (!existing) return null;

    const nextDisplay = patch.displayName !== undefined ? String(patch.displayName).trim() || existing.displayName : existing.displayName;
    const nextOwner = patch.ownerUsername !== undefined ? String(patch.ownerUsername).trim().toLowerCase() : existing.ownerUsername;
    const nextEnabled = patch.enabled !== undefined ? Boolean(patch.enabled) : existing.enabled;
    const now = Date.now();

    await this.sql`
      UPDATE bot_instances
      SET display_name = ${nextDisplay},
          owner_username = ${nextOwner},
          enabled = ${nextEnabled},
          updated_at = ${now}
      WHERE instance_key = ${instanceKey}
    `;

    return {
      ...existing,
      displayName: nextDisplay,
      ownerUsername: nextOwner,
      enabled: nextEnabled,
      updatedAt: now,
    };
  }

  public async deleteInstance(instanceKey: string, purgeData = false): Promise<boolean> {
    const existing = await this.getInstance(instanceKey);
    if (!existing) return false;

    await this.sql.begin(async (tx) => {
      const q = tx as unknown as Sql;
      await q`DELETE FROM bot_instances WHERE instance_key = ${instanceKey}`;
      if (purgeData) {
        const tables = [
          "bot_settings",
          "bot_strategy_states",
          "bot_orders",
          "trade_learning_journal",
          "symbol_learning_state",
          "learning_adjustments",
        ];
        for (const tbl of tables) {
          const [row] = await q<{ exists: boolean }[]>`
            SELECT to_regclass(${`public.${tbl}`}) IS NOT NULL AS exists
          `;
          if (row?.exists) {
            await q.unsafe(`DELETE FROM ${tbl} WHERE instance_key = $1`, [instanceKey]);
          }
        }
      }
    });
    return true;
  }

  public async close(): Promise<void> {
    await this.sql.end({ timeout: 3 }).catch(() => {});
  }
}
