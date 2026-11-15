/**
 * Multi-Instance Bot Orchestrator
 *
 * Runs a single HTTP server (default port 3333) that:
 *  1. Manages isolated child processes (`src/index.ts`) per `instanceKey` (Zero single-point crash).
 *  2. Auto-discovers and migrates pre-existing PostgreSQL instances (`binance_dry`, `alpaca_kyne`, etc.).
 *  3. Enforces multi-owner authentication (`admin` vs `owner`) via HttpOnly session cookies.
 *  4. Reverse-proxies REST API + SSE streams (`/i/:instanceKey/api/...`) to each localhost worker.
 */

import "dotenv/config";
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { APP_VERSION_INFO } from "./version.js";
import { OrchestratorStore, type OrchInstance, type OrchUser } from "./orchestrator-store.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");
const PUBLIC_DIR = path.resolve(ROOT_DIR, "public");
const TSX_CLI_PATH = path.resolve(ROOT_DIR, "node_modules/tsx/dist/cli.mjs");
const WORKER_ENTRY_PATH = path.resolve(ROOT_DIR, "src/index.ts");

const ORCH_PORT = parseInt(process.env.ORCHESTRATOR_PORT || process.env.PORT || "3333", 10) || 3333;
const MAX_LOG_LINES = 120;

interface WorkerTelemetry {
  exchange?: string;
  strategyType?: string;
  symbol?: string;
  dryRun?: boolean;
  isPaused?: boolean;
  netPnl?: number;
  updatedAt?: number;
}

interface WorkerRuntime {
  instanceKey: string;
  child: ChildProcess | null;
  status: "stopped" | "starting" | "running" | "crashed";
  pid: number | null;
  startedAt: number | null;
  restarts: number;
  consecutiveCrashes: number;
  lastExitCode: number | null;
  manualStop: boolean;
  restartTimer: NodeJS.Timeout | null;
  logs: string[];
  telemetry: WorkerTelemetry;
}

const MIME_TYPES: Record<string, string> = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
  ".png": "image/png",
  ".svg": "image/svg+xml",
  ".ico": "image/x-icon",
};

function orchLog(msg: string): void {
  const ts = new Date().toISOString().replace("T", " ").slice(0, 23);
  console.log(`[orchestrator ${ts}] ${msg}`);
}

function sendJson(res: http.ServerResponse, status: number, payload: unknown, extraHeaders?: Record<string, string>): void {
  if (res.headersSent) return;
  res.writeHead(status, {
    "Content-Type": "application/json; charset=utf-8",
    "Cache-Control": "no-store",
    ...(extraHeaders || {}),
  });
  res.end(JSON.stringify(payload));
}

function readJsonBody(req: http.IncomingMessage, maxBytes = 512 * 1024): Promise<Record<string, any>> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = [];
    let total = 0;
    req.on("data", (chunk: Buffer) => {
      total += chunk.length;
      if (total > maxBytes) {
        req.destroy();
        reject(new Error("Request body too large"));
        return;
      }
      chunks.push(chunk);
    });
    req.on("end", () => {
      if (chunks.length === 0) return resolve({});
      try {
        const parsed = JSON.parse(Buffer.concat(chunks).toString("utf8"));
        resolve(parsed && typeof parsed === "object" ? parsed : {});
      } catch {
        reject(new Error("Invalid JSON payload"));
      }
    });
    req.on("error", reject);
  });
}

function parseCookies(req: http.IncomingMessage): Record<string, string> {
  const raw = req.headers.cookie || "";
  const out: Record<string, string> = {};
  for (const part of raw.split(";")) {
    const idx = part.indexOf("=");
    if (idx <= 0) continue;
    const k = part.slice(0, idx).trim();
    const v = part.slice(idx + 1).trim();
    try {
      out[k] = decodeURIComponent(v);
    } catch {
      out[k] = v;
    }
  }
  return out;
}

class BotOrchestrator {
  private readonly store: OrchestratorStore;
  private readonly workers = new Map<string, WorkerRuntime>();
  private server?: http.Server;
  private pollTimer: NodeJS.Timeout | null = null;
  private shuttingDown = false;

  constructor(store: OrchestratorStore) {
    this.store = store;
  }

  public async start(): Promise<void> {
    const instances = await this.store.listInstances();
    orchLog(`Loaded ${instances.length} registered instance(s) from PostgreSQL.`);

    this.startHttpServer();

    for (const inst of instances) {
      if (inst.enabled) {
        await this.spawnWorker(inst);
      } else {
        this.ensureRuntime(inst.instanceKey);
      }
    }

    this.pollTimer = setInterval(() => {
      this.refreshWorkersTelemetry().catch(() => {});
    }, 5000);
  }

  private ensureRuntime(instanceKey: string): WorkerRuntime {
    let rt = this.workers.get(instanceKey);
    if (!rt) {
      rt = {
        instanceKey,
        child: null,
        status: "stopped",
        pid: null,
        startedAt: null,
        restarts: 0,
        consecutiveCrashes: 0,
        lastExitCode: null,
        manualStop: false,
        restartTimer: null,
        logs: [],
        telemetry: {},
      };
      this.workers.set(instanceKey, rt);
    }
    return rt;
  }

  private pushWorkerLog(rt: WorkerRuntime, line: string): void {
    if (!line.trim()) return;
    rt.logs.push(line);
    if (rt.logs.length > MAX_LOG_LINES) {
      rt.logs.splice(0, rt.logs.length - MAX_LOG_LINES);
    }
    console.log(`[${rt.instanceKey}] ${line}`);
  }

  public async spawnWorker(inst: OrchInstance): Promise<void> {
    if (this.shuttingDown) return;
    const rt = this.ensureRuntime(inst.instanceKey);
    if (rt.restartTimer) {
      clearTimeout(rt.restartTimer);
      rt.restartTimer = null;
    }
    if (rt.child && !rt.child.killed) {
      return;
    }

    rt.manualStop = false;
    rt.status = "starting";

    const useTsxCli = fs.existsSync(TSX_CLI_PATH);
    const args = useTsxCli ? [TSX_CLI_PATH, WORKER_ENTRY_PATH] : [WORKER_ENTRY_PATH];

    const childEnv: NodeJS.ProcessEnv = {
      ...process.env,
      BOT_INSTANCE: inst.instanceKey,
      FORCE_DASHBOARD_PORT: String(inst.internalPort),
      DASHBOARD_HOST: "127.0.0.1",
      BOT_DB_PATH: `data/grid-bot.${inst.instanceKey}.db.json`,
      SETTINGS_DB_PATH: `data/settings.${inst.instanceKey}.db.json`,
    };

    const child = spawn(process.execPath, args, {
      cwd: ROOT_DIR,
      env: childEnv,
      stdio: ["ignore", "pipe", "pipe"],
    });

    rt.child = child;
    rt.pid = child.pid ?? null;
    rt.startedAt = Date.now();
    rt.status = "running";

    orchLog(`Started worker "${inst.instanceKey}" (pid=${rt.pid}, internalPort=${inst.internalPort})`);

    let stdoutBuf = "";
    child.stdout?.on("data", (chunk: Buffer) => {
      stdoutBuf += chunk.toString("utf8");
      const lines = stdoutBuf.split(/\r?\n/);
      stdoutBuf = lines.pop() ?? "";
      for (const l of lines) this.pushWorkerLog(rt, l);
    });

    let stderrBuf = "";
    child.stderr?.on("data", (chunk: Buffer) => {
      stderrBuf += chunk.toString("utf8");
      const lines = stderrBuf.split(/\r?\n/);
      stderrBuf = lines.pop() ?? "";
      for (const l of lines) this.pushWorkerLog(rt, `[ERR] ${l}`);
    });

    child.on("exit", async (code, signal) => {
      if (stdoutBuf.trim()) this.pushWorkerLog(rt, stdoutBuf.trim());
      if (stderrBuf.trim()) this.pushWorkerLog(rt, `[ERR] ${stderrBuf.trim()}`);

      const ranMs = rt.startedAt ? Date.now() - rt.startedAt : 0;
      rt.child = null;
      rt.pid = null;
      rt.lastExitCode = code;

      if (this.shuttingDown || rt.manualStop) {
        rt.status = "stopped";
        rt.consecutiveCrashes = 0;
        orchLog(`Worker "${inst.instanceKey}" stopped cleanly (code=${code}, signal=${signal}).`);
        return;
      }

      rt.status = "crashed";
      rt.consecutiveCrashes = ranMs > 60_000 ? 1 : rt.consecutiveCrashes + 1;
      const delayMs = Math.min(30_000, 2_000 * Math.pow(2, Math.min(4, rt.consecutiveCrashes - 1)));
      orchLog(`⚠️ Worker "${inst.instanceKey}" exited unexpectedly (code=${code}, signal=${signal}). Restarting in ${(delayMs / 1000).toFixed(1)}s...`);

      rt.restartTimer = setTimeout(async () => {
        rt.restartTimer = null;
        if (this.shuttingDown || rt.manualStop) return;
        const latest = await this.store.getInstance(inst.instanceKey).catch(() => null);
        if (!latest || !latest.enabled) return;
        rt.restarts++;
        await this.spawnWorker(latest);
      }, delayMs);
    });
  }

  public async stopWorker(instanceKey: string): Promise<void> {
    const rt = this.ensureRuntime(instanceKey);
    rt.manualStop = true;
    if (rt.restartTimer) {
      clearTimeout(rt.restartTimer);
      rt.restartTimer = null;
    }
    const child = rt.child;
    if (!child) {
      rt.status = "stopped";
      rt.pid = null;
      return;
    }

    await new Promise<void>((resolve) => {
      const killTimer = setTimeout(() => {
        try {
          child.kill("SIGKILL");
        } catch {
          // ignore
        }
        resolve();
      }, 4000);

      child.once("exit", () => {
        clearTimeout(killTimer);
        resolve();
      });

      try {
        child.kill("SIGTERM");
      } catch {
        clearTimeout(killTimer);
        resolve();
      }
    });
    rt.status = "stopped";
    rt.child = null;
    rt.pid = null;
  }

  private async refreshWorkersTelemetry(): Promise<void> {
    const instances = await this.store.listInstances().catch(() => [] as OrchInstance[]);
    await Promise.all(
      instances.map(async (inst) => {
        const rt = this.workers.get(inst.instanceKey);
        if (!rt || rt.status !== "running") return;
        try {
          const controller = new AbortController();
          const timer = setTimeout(() => controller.abort(), 1500);
          const res = await fetch(`http://127.0.0.1:${inst.internalPort}/api/state`, {
            signal: controller.signal,
          });
          clearTimeout(timer);
          if (!res.ok) return;
          const data: any = await res.json();
          rt.telemetry = {
            exchange: data.exchange,
            strategyType: data.strategyType,
            symbol: data.symbol,
            dryRun: data.dryRun,
            isPaused: data.isPaused,
            netPnl: data.portfolio?.netPnl ?? data.netPnl ?? data.realizedPnl,
            updatedAt: Date.now(),
          };
        } catch {
          // worker still booting
        }
      }),
    );
  }

  // ── Auth Helpers ────────────────────────────────────────────────────────

  private extractToken(req: http.IncomingMessage, url: URL): string {
    const cookies = parseCookies(req);
    if (cookies.orch_session) return cookies.orch_session;
    const auth = req.headers.authorization || "";
    if (auth.toLowerCase().startsWith("bearer ")) return auth.slice(7).trim();
    const qTok = url.searchParams.get("token");
    return qTok ? qTok.trim() : "";
  }

  private async getRequestUser(req: http.IncomingMessage, url: URL): Promise<OrchUser | null> {
    const token = this.extractToken(req, url);
    if (!token) return null;
    return this.store.getUserByToken(token);
  }

  private canAccessInstance(user: OrchUser, inst: OrchInstance): boolean {
    return user.role === "admin" || inst.ownerUsername === user.username;
  }

  // ── HTTP Server & Proxy ─────────────────────────────────────────────────

  private startHttpServer(): void {
    this.server = http.createServer((req, res) => {
      this.handleRequest(req, res).catch((err) => {
        orchLog(`HTTP error: ${(err as Error).message}`);
        sendJson(res, 500, { error: "Internal orchestrator error" });
      });
    });

    this.server.listen(ORCH_PORT, () => {
      orchLog(`🚀 Orchestrator Hub live at http://localhost:${ORCH_PORT} (v${APP_VERSION_INFO.version}, ${APP_VERSION_INFO.commit})`);
    });

    this.server.on("error", (err) => {
      orchLog(`HTTP server error: ${err.message}`);
    });
  }

  private async handleRequest(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const method = req.method || "GET";
    const fullUrl = req.url || "/";
    const url = new URL(fullUrl, `http://127.0.0.1:${ORCH_PORT}`);
    const pathname = url.pathname;

    res.setHeader("Access-Control-Allow-Origin", req.headers.origin || "*");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, PATCH, DELETE, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
    if (method === "OPTIONS") {
      res.writeHead(204);
      res.end();
      return;
    }

    // Version & Health
    if (method === "GET" && pathname === "/api/version") {
      return sendJson(res, 200, { ...APP_VERSION_INFO, orchestrator: true });
    }

    // Auth & Session Info
    if (method === "GET" && pathname === "/api/orch/me") {
      const user = await this.getRequestUser(req, url);
      return sendJson(res, 200, {
        orchestrator: true,
        authenticated: Boolean(user),
        user: user ? { username: user.username, displayName: user.displayName, role: user.role } : null,
        seededDefaultAdmin: this.store.seededDefaultAdmin,
      });
    }

    if (method === "POST" && pathname === "/api/orch/auth/login") {
      const body = await readJsonBody(req);
      const auth = await this.store.authenticate(String(body.username || ""), String(body.password || ""));
      if (!auth) {
        return sendJson(res, 401, { error: "ชื่อผู้ใช้หรือรหัสผ่านไม่ถูกต้อง" });
      }
      const cookie = `orch_session=${encodeURIComponent(auth.token)}; HttpOnly; Path=/; SameSite=Lax; Max-Age=${30 * 86400}`;
      return sendJson(
        res,
        200,
        {
          success: true,
          user: {
            username: auth.user.username,
            displayName: auth.user.displayName,
            role: auth.user.role,
          },
        },
        { "Set-Cookie": cookie },
      );
    }

    if (method === "POST" && pathname === "/api/orch/auth/logout") {
      const token = this.extractToken(req, url);
      await this.store.logout(token).catch(() => {});
      return sendJson(
        res,
        200,
        { success: true },
        { "Set-Cookie": "orch_session=; HttpOnly; Path=/; SameSite=Lax; Max-Age=0" },
      );
    }

    // Reverse Proxy `/i/:key/...` -> local worker
    if (pathname.startsWith("/i/")) {
      const rest = pathname.slice(3);
      const slashIdx = rest.indexOf("/");
      const instanceKey = decodeURIComponent(slashIdx >= 0 ? rest.slice(0, slashIdx) : rest);
      const targetPath = (slashIdx >= 0 ? rest.slice(slashIdx) : "/") + (url.search || "");

      const user = await this.getRequestUser(req, url);
      if (!user) return sendJson(res, 401, { error: "Authentication required" });

      const inst = await this.store.getInstance(instanceKey);
      if (!inst) return sendJson(res, 404, { error: `Instance "${instanceKey}" not found` });
      if (!this.canAccessInstance(user, inst)) {
        return sendJson(res, 403, { error: "Forbidden: You do not own this instance" });
      }

      const rt = this.workers.get(instanceKey);
      if (!rt || rt.status !== "running") {
        return sendJson(res, 503, { error: `Instance "${instanceKey}" is not running`, status: rt?.status || "stopped" });
      }

      return this.proxyToWorker(req, res, inst.internalPort, targetPath);
    }

    // Protected `/api/orch/...` routes
    if (pathname.startsWith("/api/orch/")) {
      const user = await this.getRequestUser(req, url);
      if (!user) return sendJson(res, 401, { error: "Authentication required" });
      return this.handleOrchApi(req, res, method, pathname, user);
    }

    // Static Dashboard files (served for `/`, `/index.html`, `/app.js`, `/style.css`, etc.)
    if (method === "GET") {
      return this.serveStatic(res, pathname);
    }

    return sendJson(res, 404, { error: "Not found" });
  }

  private async handleOrchApi(
    req: http.IncomingMessage,
    res: http.ServerResponse,
    method: string,
    pathname: string,
    user: OrchUser,
  ): Promise<void> {
    if (method === "POST" && pathname === "/api/orch/auth/change-password") {
      const body = await readJsonBody(req);
      const ok = await this.store.verifyUserPassword(user.username, String(body.currentPassword || ""));
      if (!ok) return sendJson(res, 400, { error: "รหัสผ่านเดิมไม่ถูกต้อง" });
      await this.store.updatePassword(user.username, String(body.newPassword || ""));
      this.store.seededDefaultAdmin = false;
      return sendJson(res, 200, { success: true });
    }

    // ── Instance List & Create ──
    if (method === "GET" && pathname === "/api/orch/instances") {
      const list = await this.store.listInstances(user);
      const enriched = list.map((inst) => {
        const rt = this.workers.get(inst.instanceKey);
        return {
          ...inst,
          status: rt?.status || "stopped",
          pid: rt?.pid ?? null,
          uptimeSec: rt?.startedAt && rt.status === "running" ? Math.floor((Date.now() - rt.startedAt) / 1000) : 0,
          restarts: rt?.restarts ?? 0,
          telemetry: rt?.telemetry ?? {},
        };
      });
      return sendJson(res, 200, { instances: enriched });
    }

    if (method === "POST" && pathname === "/api/orch/instances") {
      const body = await readJsonBody(req);
      const key = String(body.instanceKey || "").trim().toLowerCase();
      const ownerUsername =
        user.role === "admin" && body.ownerUsername
          ? String(body.ownerUsername).trim().toLowerCase()
          : user.username;

      const exchange = String(body.exchange || "binance").toLowerCase();
      const strategyType = String(body.strategyType || "ma_fan_portfolio");
      const initialSettings: Record<string, any> = {
        exchange,
        strategyType,
        dryRun: body.dryRun !== false,
        startPaused: true,
        symbol: exchange === "alpaca" ? "AAPL" : (exchange === "dreamdex" ? "SOMI:USDSO" : "BTCFDUSD"),
      };
      if (body.portfolioMaxBudgetUsd !== undefined) {
        initialSettings.portfolioMaxBudgetUsd = Math.max(0, Number(body.portfolioMaxBudgetUsd) || 0);
      }

      try {
        const created = await this.store.createInstance({
          instanceKey: key,
          displayName: body.displayName || key,
          ownerUsername,
          enabled: body.autoStart !== false,
          initialSettings,
        });
        if (created.enabled) {
          await this.spawnWorker(created);
        }
        return sendJson(res, 201, { success: true, instance: created });
      } catch (err: any) {
        return sendJson(res, 400, { error: err.message });
      }
    }

    // ── Per-Instance Lifecycle & Logs ──
    const instMatch = pathname.match(/^\/api\/orch\/instances\/([^/]+)(?:\/(start|stop|restart|logs))?$/);
    if (instMatch) {
      const instanceKey = decodeURIComponent(instMatch[1]!);
      const action = instMatch[2] || "";
      const inst = await this.store.getInstance(instanceKey);
      if (!inst) return sendJson(res, 404, { error: `Instance "${instanceKey}" not found` });
      if (!this.canAccessInstance(user, inst)) {
        return sendJson(res, 403, { error: "Forbidden: You do not own this instance" });
      }

      if (method === "GET" && action === "logs") {
        const rt = this.workers.get(instanceKey);
        return sendJson(res, 200, { instanceKey, status: rt?.status || "stopped", logs: rt?.logs || [] });
      }

      if (method === "POST" && action === "start") {
        const updated = (await this.store.updateInstance(instanceKey, { enabled: true })) || inst;
        await this.spawnWorker(updated);
        return sendJson(res, 200, { success: true, instanceKey, status: "running" });
      }

      if (method === "POST" && action === "stop") {
        await this.store.updateInstance(instanceKey, { enabled: false });
        await this.stopWorker(instanceKey);
        return sendJson(res, 200, { success: true, instanceKey, status: "stopped" });
      }

      if (method === "POST" && action === "restart") {
        await this.stopWorker(instanceKey);
        const updated = (await this.store.updateInstance(instanceKey, { enabled: true })) || inst;
        await this.spawnWorker(updated);
        return sendJson(res, 200, { success: true, instanceKey, status: "running" });
      }

      if (method === "PATCH" && !action) {
        const body = await readJsonBody(req);
        const patch: { displayName?: string; ownerUsername?: string; enabled?: boolean } = {};
        if (body.displayName !== undefined) patch.displayName = String(body.displayName);
        if (body.enabled !== undefined) patch.enabled = Boolean(body.enabled);
        if (body.ownerUsername !== undefined) {
          if (user.role !== "admin") return sendJson(res, 403, { error: "Only admin can reassign ownership" });
          patch.ownerUsername = String(body.ownerUsername);
        }
        const updated = await this.store.updateInstance(instanceKey, patch);
        return sendJson(res, 200, { success: true, instance: updated });
      }

      if (method === "DELETE" && !action) {
        const purgeData = req.url?.includes("purge=true") && user.role === "admin";
        await this.stopWorker(instanceKey);
        await this.store.deleteInstance(instanceKey, Boolean(purgeData));
        this.workers.delete(instanceKey);
        return sendJson(res, 200, { success: true, instanceKey, purgeData: Boolean(purgeData) });
      }
    }

    // ── Admin User Management ──
    if (pathname.startsWith("/api/orch/users")) {
      if (user.role !== "admin") {
        return sendJson(res, 403, { error: "Admin access required" });
      }
      if (method === "GET" && pathname === "/api/orch/users") {
        const users = await this.store.listUsers();
        return sendJson(res, 200, { users });
      }
      if (method === "POST" && pathname === "/api/orch/users") {
        const body = await readJsonBody(req);
        try {
          const created = await this.store.createUser({
            username: String(body.username || ""),
            password: String(body.password || ""),
            displayName: body.displayName,
            role: body.role === "admin" ? "admin" : "owner",
          });
          return sendJson(res, 201, { success: true, user: created });
        } catch (err: any) {
          return sendJson(res, 400, { error: err.message });
        }
      }
      const userMatch = pathname.match(/^\/api\/orch\/users\/([^/]+)(?:\/(reset-password))?$/);
      if (userMatch) {
        const targetUsername = decodeURIComponent(userMatch[1]!);
        const subAction = userMatch[2] || "";
        if (method === "POST" && subAction === "reset-password") {
          const body = await readJsonBody(req);
          try {
            await this.store.updatePassword(targetUsername, String(body.newPassword || ""));
            return sendJson(res, 200, { success: true });
          } catch (err: any) {
            return sendJson(res, 400, { error: err.message });
          }
        }
        if (method === "DELETE" && !subAction) {
          if (targetUsername === user.username) {
            return sendJson(res, 400, { error: "Cannot delete your own active admin account" });
          }
          try {
            await this.store.deleteUser(targetUsername);
            return sendJson(res, 200, { success: true });
          } catch (err: any) {
            return sendJson(res, 400, { error: err.message });
          }
        }
      }
    }

    return sendJson(res, 404, { error: "Orchestrator route not found" });
  }

  private proxyToWorker(
    clientReq: http.IncomingMessage,
    clientRes: http.ServerResponse,
    targetPort: number,
    targetPath: string,
  ): void {
    const isSse = targetPath.startsWith("/api/events");
    const headers: http.OutgoingHttpHeaders = {
      ...clientReq.headers,
      host: `127.0.0.1:${targetPort}`,
    };
    delete headers["cookie"];

    const proxyReq = http.request(
      {
        hostname: "127.0.0.1",
        port: targetPort,
        path: targetPath,
        method: clientReq.method || "GET",
        headers,
      },
      (proxyRes) => {
        const resHeaders: http.OutgoingHttpHeaders = { ...proxyRes.headers };
        if (isSse) {
          resHeaders["cache-control"] = "no-cache";
          resHeaders["x-accel-buffering"] = "no";
        }
        clientRes.writeHead(proxyRes.statusCode || 200, resHeaders);
        proxyRes.pipe(clientRes);
      },
    );

    proxyReq.on("error", (err) => {
      sendJson(clientRes, 502, { error: `Worker unavailable: ${err.message}` });
    });

    if (isSse) {
      proxyReq.setTimeout(0);
    } else {
      proxyReq.setTimeout(30_000, () => {
        proxyReq.destroy(new Error("Worker request timeout"));
      });
    }

    clientRes.on("close", () => {
      if (!clientRes.writableFinished) {
        proxyReq.destroy();
      }
    });

    clientReq.pipe(proxyReq);
  }

  private serveStatic(res: http.ServerResponse, pathname: string): void {
    const rel = pathname === "/" ? "index.html" : pathname.replace(/^\/+/, "");
    const resolved = path.resolve(PUBLIC_DIR, rel);
    if (!resolved.startsWith(PUBLIC_DIR) || !fs.existsSync(resolved) || fs.statSync(resolved).isDirectory()) {
      res.writeHead(404, { "Content-Type": "text/plain" });
      res.end("Not found");
      return;
    }
    const ext = path.extname(resolved).toLowerCase();
    res.writeHead(200, {
      "Content-Type": MIME_TYPES[ext] || "application/octet-stream",
      "Cache-Control": "no-store",
    });
    fs.createReadStream(resolved).pipe(res);
  }

  public async shutdown(): Promise<void> {
    if (this.shuttingDown) return;
    this.shuttingDown = true;
    orchLog("Shutting down Orchestrator and stopping all workers...");
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.server) {
      await new Promise<void>((resolve) => this.server!.close(() => resolve())).catch(() => {});
    }
    await Promise.all(Array.from(this.workers.keys()).map((k) => this.stopWorker(k)));
    await this.store.close();
    orchLog("All workers stopped. Goodbye.");
  }
}

async function bootstrap(): Promise<void> {
  const databaseUrl = process.env.DATABASE_URL?.trim();
  if (!databaseUrl) {
    console.error("❌ DATABASE_URL is required for Multi-Instance Orchestrator (PostgreSQL).");
    process.exit(1);
  }

  const store = new OrchestratorStore(databaseUrl, orchLog);
  await store.init();

  const orch = new BotOrchestrator(store);
  await orch.start();

  const onSignal = async () => {
    await orch.shutdown();
    process.exit(0);
  };
  process.on("SIGINT", onSignal);
  process.on("SIGTERM", onSignal);
}

bootstrap().catch((err) => {
  console.error("[orchestrator] fatal:", err);
  process.exit(1);
});
