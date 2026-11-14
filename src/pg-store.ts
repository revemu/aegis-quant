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

    const orders: UnifiedOrderRecord[] = [];
    this.lastOrderHashes.clear();
    for (const r of orderRows) {
      if (r.raw_json && typeof r.raw_json === "object") {
        orders.push(r.raw_json as UnifiedOrderRecord);
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
        exchange: (o as any).exchange ? String((o as any).exchange) : null,
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

  public async close(): Promise<void> {
    this.closed = true;
    await this.flushImmediate().catch(() => {});
    await this.sql.end({ timeout: 5 }).catch(() => {});
  }
}
