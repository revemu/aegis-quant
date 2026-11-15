/**
 * Multi-Instance & Multi-Node Cluster Bot Orchestrator
 *
 * Runs a single HTTP server (default port 3333) that:
 *  1. Registers this server as a Cluster Node (`bot_nodes`) in shared PostgreSQL.
 *  2. Acquires atomic Distributed Lease Locks (`bot_instances.lease_node_id`) with
 *     least-loaded balancing & automatic failover across nodes.
 *  3. Manages isolated child processes (`src/index.ts`) per locally leased `instanceKey`.
 *  4. Enforces multi-owner authentication (`admin` vs `owner`) via shared PostgreSQL sessions.
 *  5. Reverse-proxies REST API + SSE streams (`/i/:key/api/...`) to either the local worker
 *     or transparently across nodes to whichever peer node currently holds the instance lease.
 */

import dotenv from "dotenv";
dotenv.config({ override: true });

import crypto from "node:crypto";
import os from "node:os";
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { fileURLToPath } from "node:url";
import { APP_VERSION_INFO } from "./version.js";
import { OrchestratorStore, type OrchInstance, type OrchNode, type OrchUser } from "./orchestrator-store.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT_DIR = path.resolve(__dirname, "..");
const PUBLIC_DIR = path.resolve(ROOT_DIR, "public");
const TSX_CLI_PATH = path.resolve(ROOT_DIR, "node_modules/tsx/dist/cli.mjs");
const WORKER_ENTRY_PATH = path.resolve(ROOT_DIR, "src/index.ts");

const ORCH_PORT = parseInt(process.env.ORCHESTRATOR_PORT || process.env.PORT || "3333", 10) || 3333;
const NODE_ID = (process.env.ORCH_NODE_ID || `${os.hostname()}-${ORCH_PORT}`).trim();
const ADVERTISE_URL = (process.env.ORCH_ADVERTISE_URL || `http://127.0.0.1:${ORCH_PORT}`).trim().replace(/\/+$/, "");
const MAX_WORKERS = Math.max(1, parseInt(process.env.ORCH_MAX_WORKERS || "20", 10) || 20);
const LEASE_TTL_MS = 20_000;
const RECONCILE_INTERVAL_MS = 4_000;
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
  console.log(`[orch:${NODE_ID} ${ts}] ${msg}`);
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
  private readonly clusterSecret: string;
  private readonly nodeStartedAt = Date.now();
  private readonly workers = new Map<string, WorkerRuntime>();
  private server?: http.Server;
  private reconcileTimer: NodeJS.Timeout | null = null;
  private pollTimer: NodeJS.Timeout | null = null;
  private reconcileInFlight = false;
  private shuttingDown = false;

  constructor(store: OrchestratorStore, databaseUrl: string) {
    this.store = store;
    this.clusterSecret =
      process.env.ORCH_CLUSTER_SECRET?.trim() ||
      crypto.createHash("sha256").update(databaseUrl).digest("hex").slice(0, 32);
  }

  public async start(): Promise<void> {
    this.startHttpServer();

    await this.store.upsertNodeHeartbeat({
      nodeId: NODE_ID,
      advertiseUrl: ADVERTISE_URL,
      runningCount: 0,
      maxWorkers: MAX_WORKERS,
      lastHeartbeatMs: Date.now(),
      startedAt: this.nodeStartedAt,
    });

    await this.reconcileClusterLeases();

    this.reconcileTimer = setInterval(() => {
      this.reconcileClusterLeases().catch((err) => {
        orchLog(`⚠️ Lease reconcile error: ${(err as Error).message}`);
      });
    }, RECONCILE_INTERVAL_MS);

    this.pollTimer = setInterval(() => {
      this.refreshWorkersTelemetry().catch(() => {});
    }, 5000);
  }

  private getLocalRunningCount(): number {
    let cnt = 0;
    for (const rt of this.workers.values()) {
      if (rt.status === "running" || rt.status === "starting") cnt++;
    }
    return cnt;
  }

  /**
   * Distributed Cluster Reconciler:
   *  - Refreshes this node's heartbeat in `bot_nodes`
   *  - Renews leases for workers running locally (or stops them if disabled/reassigned)
   *  - Balances unassigned/expired instances onto the least-loaded active node
   */
  private async reconcileClusterLeases(): Promise<void> {
    if (this.shuttingDown || this.reconcileInFlight) return;
    this.reconcileInFlight = true;
    try {
      const now = Date.now();
      await this.store.upsertNodeHeartbeat({
        nodeId: NODE_ID,
        advertiseUrl: ADVERTISE_URL,
        runningCount: this.getLocalRunningCount(),
        maxWorkers: MAX_WORKERS,
        lastHeartbeatMs: now,
        startedAt: this.nodeStartedAt,
      });

      const [instances, activeNodes] = await Promise.all([
        this.store.listInstances(),
        this.store.listActiveNodes(LEASE_TTL_MS),
      ]);
      const activeNodeIds = new Set(activeNodes.map((n) => n.nodeId));
      const byKey = new Map(instances.map((i) => [i.instanceKey, i]));

      // 1. Reconcile workers currently running on THIS node
      for (const [key, rt] of Array.from(this.workers.entries())) {
        if (rt.status !== "running" && rt.status !== "starting") continue;
        const inst = byKey.get(key);
        const preferredOnline = inst?.preferredNodeId && activeNodeIds.has(inst.preferredNodeId);
        const shouldYieldToPreferred = Boolean(preferredOnline && inst!.preferredNodeId !== NODE_ID);

        if (!inst || !inst.enabled || shouldYieldToPreferred) {
          orchLog(
            `Releasing "${key}" on this node (${!inst ? "deleted" : !inst.enabled ? "disabled" : `yielding to preferred=${inst.preferredNodeId}`}).`,
          );
          await this.stopWorker(key);
          await this.store.releaseLease(key, NODE_ID).catch(() => {});
          continue;
        }

        // Renew lease; if another node legitimately stole expired lease, yield cleanly
        const renewed = await this.store.tryAcquireOrRenewLease(key, NODE_ID, LEASE_TTL_MS, !preferredOnline);
        if (!renewed) {
          orchLog(`⚠️ Lost lease for "${key}" to another node; stopping local worker.`);
          await this.stopWorker(key);
        }
      }

      // 2. Claim unclaimed / expired enabled instances based on least-loaded balancing
      let localCount = this.getLocalRunningCount();
      const eligibleNodes = activeNodes.filter((n) => n.runningCount < n.maxWorkers);
      const leastLoaded = eligibleNodes[0] || null;

      for (const inst of instances) {
        if (!inst.enabled) continue;
        const localRt = this.workers.get(inst.instanceKey);
        if (localRt && (localRt.status === "running" || localRt.status === "starting")) continue;
        if (localCount >= MAX_WORKERS) break;

        const leaseActive = Boolean(inst.leaseNodeId && inst.leaseExpiresMs > now && activeNodeIds.has(inst.leaseNodeId));
        if (leaseActive && inst.leaseNodeId !== NODE_ID) continue;

        const preferredOnline = Boolean(inst.preferredNodeId && activeNodeIds.has(inst.preferredNodeId));
        if (preferredOnline && inst.preferredNodeId !== NODE_ID) continue;

        // Fair load balancing: if preferred is NOT forcing this node, wait for least-loaded node
        // unless lease expired > 8s ago (emergency failover if least-loaded node failed to claim)
        const leaseExpiredOver8s = !inst.leaseNodeId || inst.leaseExpiresMs < now - 8_000;
        const isMyTurnByLoad = !leastLoaded || leastLoaded.nodeId === NODE_ID || inst.leaseNodeId === NODE_ID;
        if (!preferredOnline && !isMyTurnByLoad && !leaseExpiredOver8s) continue;

        const claimed = await this.store.tryAcquireOrRenewLease(
          inst.instanceKey,
          NODE_ID,
          LEASE_TTL_MS,
          !preferredOnline,
        );
        if (claimed) {
          localCount++;
          await this.spawnWorker(claimed);
        }
      }
    } finally {
      this.reconcileInFlight = false;
    }
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
        const claimed = await this.store.tryAcquireOrRenewLease(inst.instanceKey, NODE_ID, LEASE_TTL_MS).catch(() => null);
        if (!claimed || !claimed.enabled) return;
        rt.restarts++;
        await this.spawnWorker(claimed);
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

  private isValidClusterRequest(req: http.IncomingMessage): boolean {
    const header = String(req.headers["x-orch-cluster-key"] || "");
    if (!header || header.length !== this.clusterSecret.length) return false;
    try {
      return crypto.timingSafeEqual(Buffer.from(header), Buffer.from(this.clusterSecret));
    } catch {
      return false;
    }
  }

  private async fetchPeerJson(node: OrchNode, peerPath: string, method = "GET"): Promise<any> {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), 3000);
    try {
      const res = await fetch(`${node.advertiseUrl}${peerPath}`, {
        method,
        headers: { "x-orch-cluster-key": this.clusterSecret },
        signal: controller.signal,
      });
      if (!res.ok) return null;
      return await res.json();
    } catch {
      return null;
    } finally {
      clearTimeout(timer);
    }
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
      orchLog(`🚀 Orchestrator Hub live at http://localhost:${ORCH_PORT} (node=${NODE_ID}, advertise=${ADVERTISE_URL}, v${APP_VERSION_INFO.version})`);
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

    // Internal Node-to-Node Cluster Endpoints
    if (pathname.startsWith("/internal/")) {
      if (!this.isValidClusterRequest(req)) {
        return sendJson(res, 403, { error: "Invalid cluster signature" });
      }
      return this.handleInternalClusterRequest(req, res, method, pathname, url);
    }

    // Version & Health
    if (method === "GET" && pathname === "/api/version") {
      return sendJson(res, 200, { ...APP_VERSION_INFO, orchestrator: true, nodeId: NODE_ID });
    }

    // Auth & Session Info
    if (method === "GET" && pathname === "/api/orch/me") {
      const user = await this.getRequestUser(req, url);
      return sendJson(res, 200, {
        orchestrator: true,
        nodeId: NODE_ID,
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

    // Reverse Proxy `/i/:key/...` -> local worker OR peer cluster node
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

      const localRt = this.workers.get(instanceKey);
      if (localRt && localRt.status === "running") {
        return this.proxyToUrl(req, res, `http://127.0.0.1:${inst.internalPort}${targetPath}`);
      }

      if (inst.leaseNodeId && inst.leaseNodeId !== NODE_ID && inst.leaseExpiresMs > Date.now()) {
        const peer = await this.store.getNodeById(inst.leaseNodeId);
        if (peer) {
          return this.proxyToUrl(
            req,
            res,
            `${peer.advertiseUrl}/internal/i/${encodeURIComponent(instanceKey)}${targetPath}`,
            { "x-orch-cluster-key": this.clusterSecret },
          );
        }
      }

      return sendJson(res, 503, {
        error: `Instance "${instanceKey}" is not running`,
        status: localRt?.status || (inst.enabled ? "starting" : "stopped"),
      });
    }

    // Protected `/api/orch/...` routes
    if (pathname.startsWith("/api/orch/")) {
      const user = await this.getRequestUser(req, url);
      if (!user) return sendJson(res, 401, { error: "Authentication required" });
      return this.handleOrchApi(req, res, method, pathname, user);
    }

    if (method === "GET") {
      return this.serveStatic(res, pathname);
    }

    return sendJson(res, 404, { error: "Not found" });
  }

  private async handleInternalClusterRequest(
    req: http.IncomingMessage,
    res: http.ServerResponse,
    method: string,
    pathname: string,
    url: URL,
  ): Promise<void> {
    if (pathname.startsWith("/internal/i/")) {
      const rest = pathname.slice("/internal/i/".length);
      const slashIdx = rest.indexOf("/");
      const instanceKey = decodeURIComponent(slashIdx >= 0 ? rest.slice(0, slashIdx) : rest);
      const targetPath = (slashIdx >= 0 ? rest.slice(slashIdx) : "/") + (url.search || "");
      const inst = await this.store.getInstance(instanceKey);
      const rt = this.workers.get(instanceKey);
      if (!inst || !rt || rt.status !== "running") {
        return sendJson(res, 503, { error: `Worker "${instanceKey}" not active on node ${NODE_ID}` });
      }
      return this.proxyToUrl(req, res, `http://127.0.0.1:${inst.internalPort}${targetPath}`);
    }

    if (method === "GET" && pathname === "/internal/telemetry") {
      const telemetryMap: Record<string, any> = {};
      for (const [k, rt] of this.workers.entries()) {
        telemetryMap[k] = {
          status: rt.status,
          pid: rt.pid,
          uptimeSec: rt.startedAt && rt.status === "running" ? Math.floor((Date.now() - rt.startedAt) / 1000) : 0,
          restarts: rt.restarts,
          telemetry: rt.telemetry,
        };
      }
      return sendJson(res, 200, { nodeId: NODE_ID, workers: telemetryMap });
    }

    const peerMatch = pathname.match(/^\/internal\/instances\/([^/]+)\/(stop|restart|logs)$/);
    if (peerMatch) {
      const key = decodeURIComponent(peerMatch[1]!);
      const action = peerMatch[2]!;
      const rt = this.workers.get(key);

      if (method === "GET" && action === "logs") {
        return sendJson(res, 200, { instanceKey: key, nodeId: NODE_ID, status: rt?.status || "stopped", logs: rt?.logs || [] });
      }
      if (method === "POST" && action === "stop") {
        await this.stopWorker(key);
        await this.store.releaseLease(key, NODE_ID).catch(() => {});
        return sendJson(res, 200, { success: true, instanceKey: key, nodeId: NODE_ID, status: "stopped" });
      }
      if (method === "POST" && action === "restart") {
        await this.stopWorker(key);
        const inst = await this.store.tryAcquireOrRenewLease(key, NODE_ID, LEASE_TTL_MS);
        if (inst) await this.spawnWorker(inst);
        return sendJson(res, 200, { success: true, instanceKey: key, nodeId: NODE_ID, status: inst ? "running" : "stopped" });
      }
    }

    return sendJson(res, 404, { error: "Internal route not found" });
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

    if (method === "GET" && pathname === "/api/orch/nodes") {
      const nodes = await this.store.listActiveNodes(LEASE_TTL_MS);
      return sendJson(res, 200, { currentNodeId: NODE_ID, nodes });
    }

    // ── Instance List & Create ──
    if (method === "GET" && pathname === "/api/orch/instances") {
      const [list, activeNodes] = await Promise.all([
        this.store.listInstances(user),
        this.store.listActiveNodes(LEASE_TTL_MS),
      ]);
      const peerTelemetries = new Map<string, Record<string, any>>();
      await Promise.all(
        activeNodes
          .filter((n) => n.nodeId !== NODE_ID)
          .map(async (n) => {
            const resp = await this.fetchPeerJson(n, "/internal/telemetry");
            if (resp?.workers) peerTelemetries.set(n.nodeId, resp.workers);
          }),
      );

      const now = Date.now();
      const enriched = list.map((inst) => {
        const localRt = this.workers.get(inst.instanceKey);
        if (localRt && (localRt.status === "running" || localRt.status === "starting")) {
          return {
            ...inst,
            status: localRt.status,
            pid: localRt.pid,
            uptimeSec: localRt.startedAt ? Math.floor((now - localRt.startedAt) / 1000) : 0,
            restarts: localRt.restarts,
            telemetry: localRt.telemetry,
            runningOnNode: NODE_ID,
          };
        }
        const peerWorkers = inst.leaseNodeId ? peerTelemetries.get(inst.leaseNodeId) : undefined;
        const peerRt = peerWorkers?.[inst.instanceKey];
        if (peerRt) {
          return {
            ...inst,
            status: peerRt.status || "running",
            pid: peerRt.pid ?? null,
            uptimeSec: peerRt.uptimeSec ?? 0,
            restarts: peerRt.restarts ?? 0,
            telemetry: peerRt.telemetry ?? {},
            runningOnNode: inst.leaseNodeId,
          };
        }
        const remoteActive = Boolean(inst.leaseNodeId && inst.leaseExpiresMs > now);
        return {
          ...inst,
          status: inst.enabled ? (remoteActive ? "running" : "starting") : "stopped",
          pid: null,
          uptimeSec: 0,
          restarts: 0,
          telemetry: {},
          runningOnNode: remoteActive ? inst.leaseNodeId : null,
        };
      });

      return sendJson(res, 200, {
        currentNodeId: NODE_ID,
        nodes: activeNodes,
        instances: enriched,
      });
    }

    if (method === "POST" && pathname === "/api/orch/instances") {
      const body = await readJsonBody(req);
      const key = String(body.instanceKey || "").trim().toLowerCase();
      const ownerUsername =
        user.role === "admin" && body.ownerUsername
          ? String(body.ownerUsername).trim().toLowerCase()
          : user.username;

      const preferredNodeId =
        user.role === "admin" && body.preferredNodeId ? String(body.preferredNodeId).trim() : null;

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
          preferredNodeId,
          initialSettings,
        });
        if (created.enabled) {
          await this.reconcileClusterLeases();
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

      const peerNode =
        inst.leaseNodeId && inst.leaseNodeId !== NODE_ID && inst.leaseExpiresMs > Date.now()
          ? await this.store.getNodeById(inst.leaseNodeId)
          : null;

      if (method === "GET" && action === "logs") {
        if (peerNode) {
          const peerLogs = await this.fetchPeerJson(peerNode, `/internal/instances/${encodeURIComponent(instanceKey)}/logs`);
          if (peerLogs) return sendJson(res, 200, peerLogs);
        }
        const rt = this.workers.get(instanceKey);
        return sendJson(res, 200, { instanceKey, nodeId: NODE_ID, status: rt?.status || "stopped", logs: rt?.logs || [] });
      }

      if (method === "POST" && action === "start") {
        await this.store.updateInstance(instanceKey, { enabled: true });
        await this.reconcileClusterLeases();
        return sendJson(res, 200, { success: true, instanceKey, status: "running" });
      }

      if (method === "POST" && action === "stop") {
        await this.store.updateInstance(instanceKey, { enabled: false });
        if (peerNode) {
          await this.fetchPeerJson(peerNode, `/internal/instances/${encodeURIComponent(instanceKey)}/stop`, "POST");
        }
        await this.stopWorker(instanceKey);
        await this.store.releaseLease(instanceKey, NODE_ID).catch(() => {});
        return sendJson(res, 200, { success: true, instanceKey, status: "stopped" });
      }

      if (method === "POST" && action === "restart") {
        await this.store.updateInstance(instanceKey, { enabled: true });
        if (peerNode) {
          const resp = await this.fetchPeerJson(peerNode, `/internal/instances/${encodeURIComponent(instanceKey)}/restart`, "POST");
          if (resp) return sendJson(res, 200, resp);
        }
        await this.stopWorker(instanceKey);
        await this.reconcileClusterLeases();
        return sendJson(res, 200, { success: true, instanceKey, status: "running" });
      }

      if (method === "PATCH" && !action) {
        const body = await readJsonBody(req);
        const patch: { displayName?: string; ownerUsername?: string; enabled?: boolean; preferredNodeId?: string | null } = {};
        if (body.displayName !== undefined) patch.displayName = String(body.displayName);
        if (body.enabled !== undefined) patch.enabled = Boolean(body.enabled);
        if (body.ownerUsername !== undefined) {
          if (user.role !== "admin") return sendJson(res, 403, { error: "Only admin can reassign ownership" });
          patch.ownerUsername = String(body.ownerUsername);
        }
        if (body.preferredNodeId !== undefined) {
          if (user.role !== "admin") return sendJson(res, 403, { error: "Only admin can pin cluster node" });
          patch.preferredNodeId = body.preferredNodeId ? String(body.preferredNodeId).trim() : null;
        }
        const updated = await this.store.updateInstance(instanceKey, patch);
        await this.reconcileClusterLeases();
        return sendJson(res, 200, { success: true, instance: updated });
      }

      if (method === "DELETE" && !action) {
        const purgeData = req.url?.includes("purge=true") && user.role === "admin";
        if (peerNode) {
          await this.fetchPeerJson(peerNode, `/internal/instances/${encodeURIComponent(instanceKey)}/stop`, "POST");
        }
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

  private proxyToUrl(
    clientReq: http.IncomingMessage,
    clientRes: http.ServerResponse,
    targetFullUrl: string,
    extraHeaders?: Record<string, string>,
  ): void {
    const parsed = new URL(targetFullUrl);
    const isSse = parsed.pathname.endsWith("/api/events");
    const headers: http.OutgoingHttpHeaders = {
      ...clientReq.headers,
      host: parsed.host,
      ...(extraHeaders || {}),
    };
    delete headers["cookie"];

    const proxyReq = http.request(
      {
        hostname: parsed.hostname,
        port: Number(parsed.port || 80),
        path: `${parsed.pathname}${parsed.search}`,
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
    orchLog("Shutting down Orchestrator, stopping workers & releasing cluster leases...");
    if (this.reconcileTimer) {
      clearInterval(this.reconcileTimer);
      this.reconcileTimer = null;
    }
    if (this.pollTimer) {
      clearInterval(this.pollTimer);
      this.pollTimer = null;
    }
    if (this.server) {
      await new Promise<void>((resolve) => this.server!.close(() => resolve())).catch(() => {});
    }
    await Promise.all(Array.from(this.workers.keys()).map((k) => this.stopWorker(k)));
    await this.store.releaseAllLeasesForNode(NODE_ID).catch(() => {});
    await this.store.close();
    orchLog("All workers stopped and leases released. Goodbye.");
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

  const orch = new BotOrchestrator(store, databaseUrl);
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
