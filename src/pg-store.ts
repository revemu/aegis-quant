/**
 * PostgreSQL Persistence Store for Dynamic Grid Bot ("pg-store.ts")
 *
 * Activated automatically whenever `DATABASE_URL` is set in environment/.env.
 * Stores settings, strategy states (including multi-slot portfolio states),
 * unified orders/trades, and provides the shared `market_candles` table schema.
 *
 * Isolates concurrent bot instances (e.g. `:3333` DreamDEX vs `:3334` Alpaca)
 * via `instance_key` (`BOT_INSTANCE` or `port_${PORT}`).
 */

import postgres, { type Sql } from "postgres";
import type { BotDatabaseSchema, StrategyStateRecord, UnifiedOrderRecord } from "./db.js";
import type { Candle } from "./market-structure.js";

/**
 * Merge historical & freshly fetched candles by timestamp (ascending), keeping at most `maxKeep` latest bars.
 */
export function mergeCandles(existing: Candle[], incoming: Candle[], maxKeep?: number): Candle[] {
  const map = new Map<number, Candle>();
  for (const c of existing) {
    const t = Math.floor(Number(c?.time));
    if (Number.isFinite(t) && Number.isFinite(c?.close) && c.close > 0) {
      map.set(t, { ...c, time: t });
    }
  }
  for (const c of incoming) {
    const t = Math.floor(Number(c?.time));
    if (Number.isFinite(t) && Number.isFinite(c?.close) && c.close > 0) {
      map.set(t, { ...c, time: t });
    }
  }
  const sorted = Array.from(map.values()).sort((a, b) => a.time - b.time);
  return maxKeep && maxKeep > 0 && sorted.length > maxKeep ? sorted.slice(-maxKeep) : sorted;
}

/**
 * Return only bars from `incoming` that are new or have updated OHLCV/closed status compared to `existing`.
 */
export function filterChangedCandles(existing: Candle[], incoming: Candle[]): Candle[] {
  if (incoming.length === 0) return [];
  const prevByTime = new Map<number, Candle>();
  for (const c of existing) {
    if (c && Number.isFinite(c.time)) prevByTime.set(Math.floor(c.time), c);
  }
  const changed: Candle[] = [];
  for (const c of incoming) {
    const t = Math.floor(Number(c?.time));
    if (!Number.isFinite(t) || !Number.isFinite(c?.close) || c.close <= 0) continue;
    const prev = prevByTime.get(t);
    if (
      !prev ||
      prev.open !== c.open ||
      prev.high !== c.high ||
      prev.low !== c.low ||
      prev.close !== c.close ||
      (prev.volume ?? 0) !== (c.volume ?? 0) ||
      (prev.isClosed !== false) !== (c.isClosed !== false)
    ) {
      changed.push({ ...c, time: t });
    }
  }
  return changed;
}

export interface LearningJournalRecord {
  journalUid: string;
  recordType: "TRADE" | "BLOCKED_SIGNAL";
  exchange: string;
  symbol: string;
  status: "OPEN" | "CLOSED" | "PENDING_EVAL" | "EVALUATED";
  entryTimeMs: number;
  exitTimeMs?: number | null;
  entryPrice: number;
  exitPrice?: number | null;
  qty?: number | null;
  allocatedQuote?: number | null;
  pnlQuote?: number | null;
  pnlPct?: number | null;
  maePct?: number | null;
  mfePct?: number | null;
  holdMinutes?: number | null;
  exitReason?: string | null;
  blockReason?: string | null;
  features: Record<string, any>;
  counterfactualPnlPct?: number | null;
  counterfactualVerdict?: "WOULD_WIN" | "WOULD_LOSE" | "NEUTRAL" | null;
  updatedAt: number;
}

export interface SymbolLearningStateRecord {
  symbol: string;
  consecutiveSl: number;
  wins: number;
  losses: number;
  totalPnlQuote: number;
  scorePenalty: number;
  cooldownUntilMs: number;
  lastReason: string;
  updatedAt: number;
}

export interface LearningAdjustmentRecord {
  paramName: string;
  oldValue: number;
  newValue: number;
  sampleSize: number;
  rationale: string;
  applied: boolean;
  createdAt: number;
}

function sanitizeJson<T>(val: T): T {
  return JSON.parse(JSON.stringify(val ?? null)) as T;
}

export function resolvePgInstanceKey(): string {
  const explicit = process.env.BOT_INSTANCE?.trim();
  if (explicit) return explicit;
  const port = process.env.PORT?.trim() || process.env.DASHBOARD_PORT?.trim();
  if (port) return `port_${port}`;
  return "default";
}

function buildOrderUid(o: UnifiedOrderRecord, fallbackIndex: number): string {
  const base = o.id || o.orderId || o.txHash || o.fillTxHash || o.createTxHash;
  if (base && String(base).trim()) return String(base).trim();
  const ts = o.time || o.fillTime || o.placedTime || 0;
  return `legacy_${ts}_${o.side || "BUY"}_${fallbackIndex}`;
}

function inferOrderExchange(o: UnifiedOrderRecord): "dreamdex" | "binance" | "alpaca" {
  const explicit = String((o as any)?.exchange || "").toLowerCase().trim();
  if (explicit === "dreamdex" || explicit === "binance" || explicit === "alpaca") {
    return explicit;
  }
  const sym = String(o.symbol || "").toUpperCase();
  const quote = String(o.quoteAsset || "").toUpperCase();
  const reason = String(o.reason || "");
  const feeAsset = String(o.feeAsset || "").toUpperCase();
  const isDex =
    sym.includes(":") ||
    quote === "USDSO" ||
    Boolean(o.explorerUrl?.includes("somnia")) ||
    Boolean(o.txHash?.startsWith("0x"));
  if (isDex) return "dreamdex";
  const isBinance =
    quote === "FDUSD" ||
    quote === "USDT" ||
    quote === "USDC" ||
    sym.endsWith("FDUSD") ||
    sym.endsWith("USDT") ||
    sym.endsWith("USDC") ||
    feeAsset === "BNB" ||
    reason.includes("Binance");
  if (isBinance) return "binance";
  return "alpaca";
}

export class PgStore {
  private readonly sql: Sql;
  public readonly instanceKey: string;
  private readonly log: (msg: string) => void;

  private writeChain: Promise<void> = Promise.resolve();
  private pendingSettings: Record<string, any> | null = null;
  private pendingData: BotDatabaseSchema | null = null;
  private debounceTimer: NodeJS.Timeout | null = null;

  private lastSettingsHash = "";
  private lastStateHashes = new Map<string, string>();
  private lastOrderHashes = new Map<string, string>();
  private closed = false;

  constructor(databaseUrl: string, instanceKey?: string, log?: (msg: string) => void) {
    this.instanceKey = instanceKey || resolvePgInstanceKey();
    this.log = log ?? ((msg) => console.log(`[postgres] ${msg}`));
    this.sql = postgres(databaseUrl, {
      max: 8,
      idle_timeout: 20,
      connect_timeout: 10,
      onnotice: () => {},
    });
  }

  public getSql(): Sql {
    return this.sql;
  }

  public async initSchema(): Promise<void> {
    await this.sql`
      CREATE TABLE IF NOT EXISTS bot_settings (
        instance_key TEXT NOT NULL,
        key TEXT NOT NULL,
        value_json JSONB NOT NULL,
        updated_at BIGINT NOT NULL,
        PRIMARY KEY (instance_key, key)
      )
    `;

    await this.sql`
      CREATE TABLE IF NOT EXISTS bot_strategy_states (
        instance_key TEXT NOT NULL,
        state_key TEXT NOT NULL,
        state_json JSONB NOT NULL,
        updated_at BIGINT NOT NULL,
        PRIMARY KEY (instance_key, state_key)
      )
    `;

    await this.sql`
      CREATE TABLE IF NOT EXISTS bot_orders (
        instance_key TEXT NOT NULL,
        order_uid TEXT NOT NULL,
        exchange TEXT,
        symbol TEXT,
        side TEXT,
        status TEXT,
        action TEXT,
        price DOUBLE PRECISION,
        qty DOUBLE PRECISION,
        pnl_quote DOUBLE PRECISION,
        timestamp_ms BIGINT NOT NULL,
        raw_json JSONB NOT NULL,
        PRIMARY KEY (instance_key, order_uid)
      )
    `;

    await this.sql`
      CREATE INDEX IF NOT EXISTS idx_bot_orders_inst_ts
      ON bot_orders (instance_key, timestamp_ms DESC)
    `;

    await this.sql`
      CREATE TABLE IF NOT EXISTS market_candles (
        exchange TEXT NOT NULL,
        symbol TEXT NOT NULL,
        timeframe TEXT NOT NULL,
        time_sec BIGINT NOT NULL,
        open DOUBLE PRECISION NOT NULL,
        high DOUBLE PRECISION NOT NULL,
        low DOUBLE PRECISION NOT NULL,
        close DOUBLE PRECISION NOT NULL,
        volume DOUBLE PRECISION NOT NULL DEFAULT 0,
        is_closed BOOLEAN NOT NULL DEFAULT TRUE,
        updated_at BIGINT NOT NULL,
        PRIMARY KEY (exchange, symbol, timeframe, time_sec)
      )
    `;

    await this.sql`
      CREATE INDEX IF NOT EXISTS idx_market_candles_lookup
      ON market_candles (exchange, symbol, timeframe, time_sec DESC)
    `;

    await this.sql`
      CREATE TABLE IF NOT EXISTS trade_learning_journal (
        instance_key TEXT NOT NULL,
        journal_uid TEXT NOT NULL,
        record_type TEXT NOT NULL,
        exchange TEXT NOT NULL,
        symbol TEXT NOT NULL,
        status TEXT NOT NULL,
        entry_time_ms BIGINT NOT NULL,
        exit_time_ms BIGINT,
        entry_price DOUBLE PRECISION NOT NULL,
        exit_price DOUBLE PRECISION,
        qty DOUBLE PRECISION,
        allocated_quote DOUBLE PRECISION,
        pnl_quote DOUBLE PRECISION,
        pnl_pct DOUBLE PRECISION,
        mae_pct DOUBLE PRECISION,
        mfe_pct DOUBLE PRECISION,
        hold_minutes DOUBLE PRECISION,
        exit_reason TEXT,
        block_reason TEXT,
        features_json JSONB NOT NULL,
        counterfactual_pnl_pct DOUBLE PRECISION,
        counterfactual_verdict TEXT,
        updated_at BIGINT NOT NULL,
        PRIMARY KEY (instance_key, journal_uid)
      )
    `;

    await this.sql`
      CREATE INDEX IF NOT EXISTS idx_learning_journal_type_ts
      ON trade_learning_journal (instance_key, record_type, status, entry_time_ms DESC)
    `;

    await this.sql`
      CREATE TABLE IF NOT EXISTS symbol_learning_state (
        instance_key TEXT NOT NULL,
        symbol TEXT NOT NULL,
        consecutive_sl INT NOT NULL DEFAULT 0,
        wins INT NOT NULL DEFAULT 0,
        losses INT NOT NULL DEFAULT 0,
        total_pnl_quote DOUBLE PRECISION NOT NULL DEFAULT 0,
        score_penalty DOUBLE PRECISION NOT NULL DEFAULT 0,
        cooldown_until_ms BIGINT NOT NULL DEFAULT 0,
        last_reason TEXT NOT NULL DEFAULT '',
        updated_at BIGINT NOT NULL,
        PRIMARY KEY (instance_key, symbol)
      )
    `;

    await this.sql`
      CREATE TABLE IF NOT EXISTS learning_adjustments (
        id SERIAL PRIMARY KEY,
        instance_key TEXT NOT NULL,
        param_name TEXT NOT NULL,
        old_value DOUBLE PRECISION NOT NULL,
        new_value DOUBLE PRECISION NOT NULL,
        sample_size INT NOT NULL,
        rationale TEXT NOT NULL,
        applied BOOLEAN NOT NULL DEFAULT TRUE,
        created_at BIGINT NOT NULL
      )
    `;

    await this.sql`
      CREATE INDEX IF NOT EXISTS idx_learning_adjustments_inst_ts
      ON learning_adjustments (instance_key, created_at DESC)
    `;
  }

  public async hasInstanceData(): Promise<boolean> {
    const [row] = await this.sql<{ has_data: boolean }[]>`
      SELECT (
        EXISTS (SELECT 1 FROM bot_settings WHERE instance_key = ${this.instanceKey})
        OR EXISTS (SELECT 1 FROM bot_strategy_states WHERE instance_key = ${this.instanceKey})
        OR EXISTS (SELECT 1 FROM bot_orders WHERE instance_key = ${this.instanceKey})
      ) AS has_data
    `;
    return Boolean(row?.has_data);
  }

  public async loadInstanceData(defaultState: StrategyStateRecord): Promise<BotDatabaseSchema> {
    const [settingRows, stateRows, orderRows] = await Promise.all([
      this.sql<{ key: string; value_json: any; updated_at: string | number }[]>`
        SELECT key, value_json, updated_at
        FROM bot_settings
        WHERE instance_key = ${this.instanceKey}
      `,
      this.sql<{ state_key: string; state_json: any; updated_at: string | number }[]>`
        SELECT state_key, state_json, updated_at
        FROM bot_strategy_states
        WHERE instance_key = ${this.instanceKey}
      `,
      this.sql<{ order_uid: string; raw_json: any }[]>`
        SELECT order_uid, raw_json
        FROM bot_orders
        WHERE instance_key = ${this.instanceKey}
        ORDER BY timestamp_ms DESC, order_uid ASC
        LIMIT 5000
      `,
    ]);

    const settings: Record<string, any> = {};
    for (const r of settingRows) {
      settings[r.key] = r.value_json;
    }

    let mainState: StrategyStateRecord = { ...defaultState };
    let lockedChannels: Record<string, any> = {};
    const statesBySymbol: Record<string, Partial<StrategyStateRecord>> = {};
    let latestUpdatedAt = 0;

    this.lastStateHashes.clear();
    for (const r of stateRows) {
      const ts = Number(r.updated_at || 0);
      if (ts > latestUpdatedAt) latestUpdatedAt = ts;
      const hashStr = JSON.stringify(r.state_json);
      this.lastStateHashes.set(r.state_key, hashStr);

      if (r.state_key === "__MAIN__") {
        mainState = { ...defaultState, ...(r.state_json || {}) };
      } else if (r.state_key === "__LOCKED_CHANNELS__") {
        lockedChannels = r.state_json && typeof r.state_json === "object" ? r.state_json : {};
      } else if (r.state_key.startsWith("sym:")) {
        const symKey = r.state_key.slice(4);
        statesBySymbol[symKey] = r.state_json || {};
      }
    }

    const activeExchange = String(settings.exchange || "binance").toLowerCase();
    const orders: UnifiedOrderRecord[] = [];
    this.lastOrderHashes.clear();
    for (const r of orderRows) {
      if (r.raw_json && typeof r.raw_json === "object") {
        const ord = r.raw_json as UnifiedOrderRecord;
        if (inferOrderExchange(ord) !== activeExchange) continue;
        orders.push(ord);
        this.lastOrderHashes.set(r.order_uid, JSON.stringify(r.raw_json));
      }
    }

    this.lastSettingsHash = JSON.stringify(settings);

    return {
      version: 2,
      createdAt: latestUpdatedAt || Date.now(),
      updatedAt: latestUpdatedAt || Date.now(),
      state: mainState,
      lockedChannels,
      statesBySymbol,
      orders,
      settings,
    };
  }

  public scheduleSettingsSync(settings: Record<string, any>): void {
    if (this.closed) return;
    this.pendingSettings = sanitizeJson({ ...settings });
    this.scheduleFlush();
  }

  public scheduleDataSync(data: BotDatabaseSchema): void {
    if (this.closed) return;
    this.pendingData = sanitizeJson({
      ...data,
      orders: Array.isArray(data.orders) ? data.orders.slice(0, 5000) : [],
    });
    this.scheduleFlush();
  }

  private scheduleFlush(): void {
    if (this.debounceTimer) return;
    this.debounceTimer = setTimeout(() => {
      this.debounceTimer = null;
      this.enqueuePendingSync();
    }, 200);
  }

  private enqueuePendingSync(): void {
    const nextSettings = this.pendingSettings;
    const nextData = this.pendingData;
    this.pendingSettings = null;
    this.pendingData = null;

    if (!nextSettings && !nextData) return;

    this.writeChain = this.writeChain
      .then(async () => {
        if (nextSettings) {
          await this.applySettingsSync(nextSettings);
        }
        if (nextData) {
          await this.applyDataSync(nextData);
        }
      })
      .catch((err) => {
        this.log(`⚠️ Postgres background sync error: ${(err as Error).message}`);
      });
  }

  public async flushImmediate(settings?: Record<string, any>, data?: BotDatabaseSchema): Promise<void> {
    if (this.debounceTimer) {
      clearTimeout(this.debounceTimer);
      this.debounceTimer = null;
    }
    if (settings) this.pendingSettings = sanitizeJson({ ...settings });
    if (data) {
      this.pendingData = sanitizeJson({
        ...data,
        orders: Array.isArray(data.orders) ? data.orders.slice(0, 5000) : [],
      });
    }
    this.enqueuePendingSync();
    await this.writeChain;
  }

  private async applySettingsSync(settings: Record<string, any>): Promise<void> {
    const settingsHash = JSON.stringify(settings);
    if (settingsHash === this.lastSettingsHash) return;

    const now = Date.now();
    const entries = Object.entries(settings);
    const keepKeys = entries.map(([k]) => k);

    await this.sql.begin(async (tx) => {
      const q = tx as unknown as Sql;
      if (keepKeys.length > 0) {
        await q`
          DELETE FROM bot_settings
          WHERE instance_key = ${this.instanceKey}
            AND key NOT IN ${q(keepKeys)}
        `;
      } else {
        await q`
          DELETE FROM bot_settings
          WHERE instance_key = ${this.instanceKey}
        `;
      }

      if (entries.length > 0) {
        const rows = entries.map(([key, val]) => ({
          instance_key: this.instanceKey,
          key,
          value_json: this.sql.json(sanitizeJson(val)),
          updated_at: now,
        }));
        await q`
          INSERT INTO bot_settings ${q(rows, "instance_key", "key", "value_json", "updated_at")}
          ON CONFLICT (instance_key, key) DO UPDATE SET
            value_json = EXCLUDED.value_json,
            updated_at = EXCLUDED.updated_at
        `;
      }
    });

    this.lastSettingsHash = settingsHash;
  }

  private async applyDataSync(data: BotDatabaseSchema): Promise<void> {
    const now = Date.now();

    // 1. Collect changed state entries
    const stateMap = new Map<string, any>();
    stateMap.set("__MAIN__", data.state || {});
    stateMap.set("__LOCKED_CHANNELS__", data.lockedChannels || {});
    if (data.statesBySymbol) {
      for (const [sym, st] of Object.entries(data.statesBySymbol)) {
        stateMap.set(`sym:${sym}`, st || {});
      }
    }

    const changedStateRows: Array<{ instance_key: string; state_key: string; state_json: any; updated_at: number }> = [];
    const deletedStateKeys: string[] = [];
    for (const [stateKey, payload] of stateMap.entries()) {
      const serialized = JSON.stringify(payload);
      if (this.lastStateHashes.get(stateKey) !== serialized) {
        changedStateRows.push({
          instance_key: this.instanceKey,
          state_key: stateKey,
          state_json: this.sql.json(payload),
          updated_at: now,
        });
      }
    }
    for (const prevKey of this.lastStateHashes.keys()) {
      if (!stateMap.has(prevKey)) deletedStateKeys.push(prevKey);
    }

    // 2. Collect changed / deleted orders
    const orders = Array.isArray(data.orders) ? data.orders : [];
    const currentOrderMap = new Map<string, { order: UnifiedOrderRecord; serialized: string }>();
    const seenCounts = new Map<string, number>();

    for (let i = 0; i < orders.length; i++) {
      const ord = orders[i]!;
      let uid = buildOrderUid(ord, i);
      const count = seenCounts.get(uid) || 0;
      seenCounts.set(uid, count + 1);
      if (count > 0) uid = `${uid}__${count}`;

      currentOrderMap.set(uid, {
        order: ord,
        serialized: JSON.stringify(ord),
      });
    }

    const changedOrderRows: Array<{
      instance_key: string;
      order_uid: string;
      exchange: string | null;
      symbol: string | null;
      side: string | null;
      status: string | null;
      action: string | null;
      price: number | null;
      qty: number | null;
      pnl_quote: number | null;
      timestamp_ms: number;
      raw_json: any;
    }> = [];

    for (const [uid, entry] of currentOrderMap.entries()) {
      if (this.lastOrderHashes.get(uid) === entry.serialized) continue;
      const o = entry.order;
      const rawTs = Number(o.time || o.fillTime || o.placedTime || now);
      const tsMs = rawTs > 0 && rawTs < 1e12 ? Math.floor(rawTs * 1000) : Math.floor(rawTs || now);
      const pnlVal = o.pnlQuote ?? o.pnl ?? o.pnlUsdso;
      changedOrderRows.push({
        instance_key: this.instanceKey,
        order_uid: uid,
        exchange: (o as any).exchange ? String((o as any).exchange).toLowerCase() : inferOrderExchange(o),
        symbol: o.symbol ? String(o.symbol) : null,
        side: o.side ? String(o.side) : null,
        status: o.status ? String(o.status) : null,
        action: o.action ? String(o.action) : null,
        price: Number.isFinite(Number(o.fillPrice ?? o.price)) ? Number(o.fillPrice ?? o.price) : null,
        qty: Number.isFinite(Number(o.executedQty ?? o.qty)) ? Number(o.executedQty ?? o.qty) : null,
        pnl_quote: pnlVal !== undefined && Number.isFinite(Number(pnlVal)) ? Number(pnlVal) : null,
        timestamp_ms: tsMs,
        raw_json: this.sql.json(o),
      });
    }

    const deletedOrderUids: string[] = [];
    for (const prevUid of this.lastOrderHashes.keys()) {
      if (!currentOrderMap.has(prevUid)) {
        deletedOrderUids.push(prevUid);
      }
    }

    if (
      changedStateRows.length === 0 &&
      deletedStateKeys.length === 0 &&
      changedOrderRows.length === 0 &&
      deletedOrderUids.length === 0
    ) {
      return;
    }

    await this.sql.begin(async (tx) => {
      const q = tx as unknown as Sql;

      if (deletedStateKeys.length > 0) {
        await q`
          DELETE FROM bot_strategy_states
          WHERE instance_key = ${this.instanceKey}
            AND state_key IN ${q(deletedStateKeys)}
        `;
      }

      if (changedStateRows.length > 0) {
        await q`
          INSERT INTO bot_strategy_states ${q(changedStateRows, "instance_key", "state_key", "state_json", "updated_at")}
          ON CONFLICT (instance_key, state_key) DO UPDATE SET
            state_json = EXCLUDED.state_json,
            updated_at = EXCLUDED.updated_at
        `;
      }

      if (deletedOrderUids.length > 0) {
        await q`
          DELETE FROM bot_orders
          WHERE instance_key = ${this.instanceKey}
            AND order_uid IN ${q(deletedOrderUids)}
        `;
      }

      // Batch upsert orders in chunks of 200 to stay safely within Postgres parameter limits (65,535)
      for (let i = 0; i < changedOrderRows.length; i += 200) {
        const chunk = changedOrderRows.slice(i, i + 200);
        await q`
          INSERT INTO bot_orders ${q(
            chunk,
            "instance_key",
            "order_uid",
            "exchange",
            "symbol",
            "side",
            "status",
            "action",
            "price",
            "qty",
            "pnl_quote",
            "timestamp_ms",
            "raw_json",
          )}
          ON CONFLICT (instance_key, order_uid) DO UPDATE SET
            exchange = EXCLUDED.exchange,
            symbol = EXCLUDED.symbol,
            side = EXCLUDED.side,
            status = EXCLUDED.status,
            action = EXCLUDED.action,
            price = EXCLUDED.price,
            qty = EXCLUDED.qty,
            pnl_quote = EXCLUDED.pnl_quote,
            timestamp_ms = EXCLUDED.timestamp_ms,
            raw_json = EXCLUDED.raw_json
        `;
      }
    });

    // Update diff caches only after transaction commits cleanly
    for (const [stateKey, payload] of stateMap.entries()) {
      this.lastStateHashes.set(stateKey, JSON.stringify(payload));
    }
    for (const delKey of deletedStateKeys) {
      this.lastStateHashes.delete(delKey);
    }
    for (const [uid, entry] of currentOrderMap.entries()) {
      this.lastOrderHashes.set(uid, entry.serialized);
    }
    for (const delUid of deletedOrderUids) {
      this.lastOrderHashes.delete(delUid);
    }
  }

  // ── Shared Market Candles Cache (`market_candles`) ─────────────────────────

  public async loadMarketCandles(
    exchange: string,
    symbol: string,
    timeframe: string,
    limit: number,
  ): Promise<Candle[]> {
    const cleanSym = symbol.toUpperCase().trim();
    if (!cleanSym) return [];
    const batch = await this.loadMarketCandlesBatch(exchange, [cleanSym], timeframe, limit);
    return batch.get(cleanSym) ?? [];
  }

  public async loadMarketCandlesBatch(
    exchange: string,
    symbols: string[],
    timeframe: string,
    limit: number,
  ): Promise<Map<string, Candle[]>> {
    const result = new Map<string, Candle[]>();
    const cleanSyms = Array.from(new Set(symbols.map((s) => s.toUpperCase().trim()).filter(Boolean)));
    if (cleanSyms.length === 0 || limit <= 0) return result;

    const ex = exchange.toLowerCase().trim();
    const tf = timeframe.toLowerCase().trim();
    const safeLimit = Math.max(1, Math.min(Math.floor(limit), 100_000));

    type CandleRow = {
      symbol: string;
      time_sec: string | number;
      open: number;
      high: number;
      low: number;
      close: number;
      volume: number;
      is_closed: boolean;
    };

    const rows = await this.sql<CandleRow[]>`
      WITH ranked AS (
        SELECT
          symbol,
          time_sec,
          open,
          high,
          low,
          close,
          volume,
          is_closed,
          ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY time_sec DESC) AS rn
        FROM market_candles
        WHERE exchange = ${ex}
          AND timeframe = ${tf}
          AND symbol IN ${this.sql(cleanSyms)}
      )
      SELECT symbol, time_sec, open, high, low, close, volume, is_closed
      FROM ranked
      WHERE rn <= ${safeLimit}
      ORDER BY symbol ASC, time_sec ASC
    `;

    for (const r of rows) {
      const sym = String(r.symbol).toUpperCase().trim();
      const time = Number(r.time_sec);
      if (!Number.isFinite(time) || !Number.isFinite(r.close) || r.close <= 0) continue;
      let list = result.get(sym);
      if (!list) {
        list = [];
        result.set(sym, list);
      }
      list.push({
        time,
        open: Number(r.open),
        high: Number(r.high),
        low: Number(r.low),
        close: Number(r.close),
        volume: Number(r.volume ?? 0),
        isClosed: Boolean(r.is_closed),
      });
    }

    return result;
  }

  public async upsertMarketCandles(
    exchange: string,
    symbol: string,
    timeframe: string,
    candles: Candle[],
  ): Promise<number> {
    return this.upsertMarketCandlesBatch([{ exchange, symbol, timeframe, candles }]);
  }

  public async upsertMarketCandlesBatch(
    items: Array<{ exchange: string; symbol: string; timeframe: string; candles: Candle[] }>,
  ): Promise<number> {
    if (this.closed || items.length === 0) return 0;

    const nowMs = Date.now();
    const dedup = new Map<
      string,
      {
        exchange: string;
        symbol: string;
        timeframe: string;
        time_sec: number;
        open: number;
        high: number;
        low: number;
        close: number;
        volume: number;
        is_closed: boolean;
        updated_at: number;
      }
    >();

    for (const item of items) {
      const ex = item.exchange.toLowerCase().trim();
      const sym = item.symbol.toUpperCase().trim();
      const tf = item.timeframe.toLowerCase().trim();
      if (!ex || !sym || !tf || !Array.isArray(item.candles) || item.candles.length === 0) continue;

      for (const c of item.candles) {
        const timeSec = Math.floor(Number(c?.time));
        const open = Number(c?.open);
        const high = Number(c?.high);
        const low = Number(c?.low);
        const close = Number(c?.close);
        const volume = Number.isFinite(Number(c?.volume)) ? Number(c?.volume) : 0;
        if (
          !Number.isFinite(timeSec) ||
          !Number.isFinite(open) ||
          !Number.isFinite(high) ||
          !Number.isFinite(low) ||
          !Number.isFinite(close) ||
          close <= 0
        ) {
          continue;
        }
        const key = `${ex}|${sym}|${tf}|${timeSec}`;
        dedup.set(key, {
          exchange: ex,
          symbol: sym,
          timeframe: tf,
          time_sec: timeSec,
          open,
          high,
          low,
          close,
          volume,
          is_closed: c.isClosed !== false,
          updated_at: nowMs,
        });
      }
    }

    const rows = Array.from(dedup.values());
    if (rows.length === 0) return 0;

    await this.sql.begin(async (tx) => {
      const q = tx as unknown as Sql;
      // 11 columns/row * 500 rows = 5,500 params, safely under Postgres's 65,535 param cap
      for (let i = 0; i < rows.length; i += 500) {
        const chunk = rows.slice(i, i + 500);
        await q`
          INSERT INTO market_candles ${q(
            chunk,
            "exchange",
            "symbol",
            "timeframe",
            "time_sec",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "is_closed",
            "updated_at",
          )}
          ON CONFLICT (exchange, symbol, timeframe, time_sec) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            is_closed = EXCLUDED.is_closed,
            updated_at = EXCLUDED.updated_at
        `;
      }
    });

    return rows.length;
  }

  // ── Learning Engine Journal, Cooldowns & Adjustment Audit ────────────────

  public async upsertLearningJournalBatch(entries: LearningJournalRecord[]): Promise<void> {
    if (this.closed || entries.length === 0) return;

    const dedup = new Map<string, LearningJournalRecord>();
    for (const e of entries) {
      if (!e.journalUid) continue;
      dedup.set(e.journalUid, e);
    }
    const list = Array.from(dedup.values());
    if (list.length === 0) return;

    const rows = list.map((e) => ({
      instance_key: this.instanceKey,
      journal_uid: e.journalUid,
      record_type: e.recordType,
      exchange: e.exchange.toLowerCase(),
      symbol: e.symbol.toUpperCase(),
      status: e.status,
      entry_time_ms: Math.floor(e.entryTimeMs),
      exit_time_ms: e.exitTimeMs !== undefined && e.exitTimeMs !== null ? Math.floor(e.exitTimeMs) : null,
      entry_price: Number(e.entryPrice) || 0,
      exit_price: e.exitPrice !== undefined && e.exitPrice !== null ? Number(e.exitPrice) : null,
      qty: e.qty !== undefined && e.qty !== null ? Number(e.qty) : null,
      allocated_quote: e.allocatedQuote !== undefined && e.allocatedQuote !== null ? Number(e.allocatedQuote) : null,
      pnl_quote: e.pnlQuote !== undefined && e.pnlQuote !== null ? Number(e.pnlQuote) : null,
      pnl_pct: e.pnlPct !== undefined && e.pnlPct !== null ? Number(e.pnlPct) : null,
      mae_pct: e.maePct !== undefined && e.maePct !== null ? Number(e.maePct) : null,
      mfe_pct: e.mfePct !== undefined && e.mfePct !== null ? Number(e.mfePct) : null,
      hold_minutes: e.holdMinutes !== undefined && e.holdMinutes !== null ? Number(e.holdMinutes) : null,
      exit_reason: e.exitReason ?? null,
      block_reason: e.blockReason ?? null,
      features_json: this.sql.json(sanitizeJson(e.features || {}) as any),
      counterfactual_pnl_pct:
        e.counterfactualPnlPct !== undefined && e.counterfactualPnlPct !== null ? Number(e.counterfactualPnlPct) : null,
      counterfactual_verdict: e.counterfactualVerdict ?? null,
      updated_at: Math.floor(e.updatedAt || Date.now()),
    }));

    for (let i = 0; i < rows.length; i += 200) {
      const chunk = rows.slice(i, i + 200);
      await this.sql`
        INSERT INTO trade_learning_journal ${this.sql(
          chunk,
          "instance_key",
          "journal_uid",
          "record_type",
          "exchange",
          "symbol",
          "status",
          "entry_time_ms",
          "exit_time_ms",
          "entry_price",
          "exit_price",
          "qty",
          "allocated_quote",
          "pnl_quote",
          "pnl_pct",
          "mae_pct",
          "mfe_pct",
          "hold_minutes",
          "exit_reason",
          "block_reason",
          "features_json",
          "counterfactual_pnl_pct",
          "counterfactual_verdict",
          "updated_at",
        )}
        ON CONFLICT (instance_key, journal_uid) DO UPDATE SET
          status = EXCLUDED.status,
          exit_time_ms = COALESCE(EXCLUDED.exit_time_ms, trade_learning_journal.exit_time_ms),
          exit_price = COALESCE(EXCLUDED.exit_price, trade_learning_journal.exit_price),
          pnl_quote = COALESCE(EXCLUDED.pnl_quote, trade_learning_journal.pnl_quote),
          pnl_pct = COALESCE(EXCLUDED.pnl_pct, trade_learning_journal.pnl_pct),
          mae_pct = COALESCE(EXCLUDED.mae_pct, trade_learning_journal.mae_pct),
          mfe_pct = COALESCE(EXCLUDED.mfe_pct, trade_learning_journal.mfe_pct),
          hold_minutes = COALESCE(EXCLUDED.hold_minutes, trade_learning_journal.hold_minutes),
          exit_reason = COALESCE(EXCLUDED.exit_reason, trade_learning_journal.exit_reason),
          counterfactual_pnl_pct = COALESCE(EXCLUDED.counterfactual_pnl_pct, trade_learning_journal.counterfactual_pnl_pct),
          counterfactual_verdict = COALESCE(EXCLUDED.counterfactual_verdict, trade_learning_journal.counterfactual_verdict),
          updated_at = EXCLUDED.updated_at
      `;
    }
  }

  public async closeLearningTrade(params: {
    journalUid: string;
    exitTimeMs: number;
    exitPrice: number;
    pnlQuote: number;
    pnlPct: number;
    maePct: number;
    mfePct: number;
    holdMinutes: number;
    exitReason: string;
  }): Promise<void> {
    if (this.closed || !params.journalUid) return;
    await this.sql`
      UPDATE trade_learning_journal
      SET
        status = 'CLOSED',
        exit_time_ms = ${Math.floor(params.exitTimeMs)},
        exit_price = ${Number(params.exitPrice)},
        pnl_quote = ${Number(params.pnlQuote)},
        pnl_pct = ${Number(params.pnlPct)},
        mae_pct = ${Number(params.maePct)},
        mfe_pct = ${Number(params.mfePct)},
        hold_minutes = ${Number(params.holdMinutes)},
        exit_reason = ${params.exitReason},
        updated_at = ${Date.now()}
      WHERE instance_key = ${this.instanceKey}
        AND journal_uid = ${params.journalUid}
    `;
  }

  public async loadLearningJournal(
    recordType: "TRADE" | "BLOCKED_SIGNAL",
    status?: string,
    limit = 300,
  ): Promise<LearningJournalRecord[]> {
    const safeLimit = Math.max(1, Math.min(limit, 2000));
    const rows = status
      ? await this.sql<any[]>`
          SELECT * FROM trade_learning_journal
          WHERE instance_key = ${this.instanceKey}
            AND record_type = ${recordType}
            AND status = ${status}
          ORDER BY entry_time_ms DESC
          LIMIT ${safeLimit}
        `
      : await this.sql<any[]>`
          SELECT * FROM trade_learning_journal
          WHERE instance_key = ${this.instanceKey}
            AND record_type = ${recordType}
          ORDER BY entry_time_ms DESC
          LIMIT ${safeLimit}
        `;

    return rows.map((r) => ({
      journalUid: String(r.journal_uid),
      recordType: r.record_type,
      exchange: String(r.exchange),
      symbol: String(r.symbol),
      status: r.status,
      entryTimeMs: Number(r.entry_time_ms),
      exitTimeMs: r.exit_time_ms !== null ? Number(r.exit_time_ms) : null,
      entryPrice: Number(r.entry_price),
      exitPrice: r.exit_price !== null ? Number(r.exit_price) : null,
      qty: r.qty !== null ? Number(r.qty) : null,
      allocatedQuote: r.allocated_quote !== null ? Number(r.allocated_quote) : null,
      pnlQuote: r.pnl_quote !== null ? Number(r.pnl_quote) : null,
      pnlPct: r.pnl_pct !== null ? Number(r.pnl_pct) : null,
      maePct: r.mae_pct !== null ? Number(r.mae_pct) : null,
      mfePct: r.mfe_pct !== null ? Number(r.mfe_pct) : null,
      holdMinutes: r.hold_minutes !== null ? Number(r.hold_minutes) : null,
      exitReason: r.exit_reason ?? null,
      blockReason: r.block_reason ?? null,
      features: typeof r.features_json === "string" ? JSON.parse(r.features_json) : (r.features_json || {}),
      counterfactualPnlPct: r.counterfactual_pnl_pct !== null ? Number(r.counterfactual_pnl_pct) : null,
      counterfactualVerdict: r.counterfactual_verdict ?? null,
      updatedAt: Number(r.updated_at),
    }));
  }

  public async loadPendingBlockedSignals(beforeEntryMs: number, limit = 150): Promise<LearningJournalRecord[]> {
    const safeLimit = Math.max(1, Math.min(limit, 500));
    const rows = await this.sql<any[]>`
      SELECT * FROM trade_learning_journal
      WHERE instance_key = ${this.instanceKey}
        AND record_type = 'BLOCKED_SIGNAL'
        AND status = 'PENDING_EVAL'
        AND entry_time_ms <= ${Math.floor(beforeEntryMs)}
      ORDER BY entry_time_ms ASC
      LIMIT ${safeLimit}
    `;
    return rows.map((r) => ({
      journalUid: String(r.journal_uid),
      recordType: "BLOCKED_SIGNAL",
      exchange: String(r.exchange),
      symbol: String(r.symbol),
      status: "PENDING_EVAL",
      entryTimeMs: Number(r.entry_time_ms),
      entryPrice: Number(r.entry_price),
      blockReason: r.block_reason ?? null,
      features: typeof r.features_json === "string" ? JSON.parse(r.features_json) : (r.features_json || {}),
      updatedAt: Number(r.updated_at),
    }));
  }

  public async updateBlockedEvaluations(
    updates: Array<{
      journalUid: string;
      counterfactualPnlPct: number;
      counterfactualVerdict: "WOULD_WIN" | "WOULD_LOSE" | "NEUTRAL";
      maePct: number;
      mfePct: number;
    }>,
  ): Promise<void> {
    if (this.closed || updates.length === 0) return;
    const nowMs = Date.now();
    await this.sql.begin(async (tx) => {
      const q = tx as unknown as Sql;
      for (const u of updates) {
        await q`
          UPDATE trade_learning_journal
          SET
            status = 'EVALUATED',
            counterfactual_pnl_pct = ${u.counterfactualPnlPct},
            counterfactual_verdict = ${u.counterfactualVerdict},
            mae_pct = ${u.maePct},
            mfe_pct = ${u.mfePct},
            updated_at = ${nowMs}
          WHERE instance_key = ${this.instanceKey}
            AND journal_uid = ${u.journalUid}
        `;
      }
    });
  }

  public async loadSymbolLearningStates(): Promise<SymbolLearningStateRecord[]> {
    const rows = await this.sql<any[]>`
      SELECT * FROM symbol_learning_state
      WHERE instance_key = ${this.instanceKey}
    `;
    return rows.map((r) => ({
      symbol: String(r.symbol).toUpperCase(),
      consecutiveSl: Number(r.consecutive_sl || 0),
      wins: Number(r.wins || 0),
      losses: Number(r.losses || 0),
      totalPnlQuote: Number(r.total_pnl_quote || 0),
      scorePenalty: Number(r.score_penalty || 0),
      cooldownUntilMs: Number(r.cooldown_until_ms || 0),
      lastReason: String(r.last_reason || ""),
      updatedAt: Number(r.updated_at || 0),
    }));
  }

  public async upsertSymbolLearningState(rec: SymbolLearningStateRecord): Promise<void> {
    if (this.closed) return;
    const sym = rec.symbol.toUpperCase().trim();
    if (!sym) return;
    await this.sql`
      INSERT INTO symbol_learning_state (
        instance_key, symbol, consecutive_sl, wins, losses,
        total_pnl_quote, score_penalty, cooldown_until_ms, last_reason, updated_at
      ) VALUES (
        ${this.instanceKey}, ${sym}, ${rec.consecutiveSl}, ${rec.wins}, ${rec.losses},
        ${rec.totalPnlQuote}, ${rec.scorePenalty}, ${Math.floor(rec.cooldownUntilMs)}, ${rec.lastReason}, ${Math.floor(rec.updatedAt)}
      )
      ON CONFLICT (instance_key, symbol) DO UPDATE SET
        consecutive_sl = EXCLUDED.consecutive_sl,
        wins = EXCLUDED.wins,
        losses = EXCLUDED.losses,
        total_pnl_quote = EXCLUDED.total_pnl_quote,
        score_penalty = EXCLUDED.score_penalty,
        cooldown_until_ms = EXCLUDED.cooldown_until_ms,
        last_reason = EXCLUDED.last_reason,
        updated_at = EXCLUDED.updated_at
    `;
  }

  public async recordLearningAdjustment(rec: LearningAdjustmentRecord): Promise<void> {
    if (this.closed) return;
    await this.sql`
      INSERT INTO learning_adjustments (
        instance_key, param_name, old_value, new_value, sample_size, rationale, applied, created_at
      ) VALUES (
        ${this.instanceKey}, ${rec.paramName}, ${rec.oldValue}, ${rec.newValue},
        ${rec.sampleSize}, ${rec.rationale}, ${rec.applied}, ${Math.floor(rec.createdAt)}
      )
    `;
  }

  public async loadLearningAdjustments(limit = 50): Promise<LearningAdjustmentRecord[]> {
    const safeLimit = Math.max(1, Math.min(limit, 200));
    const rows = await this.sql<any[]>`
      SELECT * FROM learning_adjustments
      WHERE instance_key = ${this.instanceKey}
      ORDER BY created_at DESC
      LIMIT ${safeLimit}
    `;
    return rows.map((r) => ({
      paramName: String(r.param_name),
      oldValue: Number(r.old_value),
      newValue: Number(r.new_value),
      sampleSize: Number(r.sample_size),
      rationale: String(r.rationale),
      applied: Boolean(r.applied),
      createdAt: Number(r.created_at),
    }));
  }

  public async close(): Promise<void> {
    this.closed = true;
    await this.flushImmediate().catch(() => {});
    await this.sql.end({ timeout: 5 }).catch(() => {});
  }
}
