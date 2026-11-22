import type { Candle } from "./market-structure.js";

export interface MacroEventContext {
  eventName: "FOMC" | "CPI" | "NFP" | null;
  inWindow: boolean;
  isEventDay: boolean;
  tag: string | null;
}

const FOMC_DATES = new Set<string>([
  "2025-01-29",
  "2025-03-19",
  "2025-05-07",
  "2025-06-18",
  "2025-07-30",
  "2025-09-17",
  "2025-10-29",
  "2025-12-10",
  "2026-01-28",
  "2026-03-18",
  "2026-04-29",
  "2026-06-17",
  "2026-07-29",
  "2026-09-16",
  "2026-10-28",
  "2026-12-09",
  "2027-01-27",
  "2027-03-17",
  "2027-04-28",
  "2027-06-09",
  "2027-07-28",
  "2027-09-15",
  "2027-10-27",
  "2027-12-08",
]);

const CPI_DATES = new Set<string>([
  "2025-01-15",
  "2025-02-12",
  "2025-03-12",
  "2025-04-10",
  "2025-05-13",
  "2025-06-11",
  "2025-07-15",
  "2025-08-12",
  "2025-09-11",
  "2025-10-15",
  "2025-11-13",
  "2025-12-10",
  "2026-01-13",
  "2026-02-13",
  "2026-03-11",
  "2026-04-10",
  "2026-05-12",
  "2026-06-10",
  "2026-07-14",
  "2026-08-12",
  "2026-09-11",
  "2026-10-14",
  "2026-11-10",
  "2026-12-10",
  "2027-01-13",
  "2027-02-10",
  "2027-03-10",
  "2027-04-13",
  "2027-05-12",
  "2027-06-10",
  "2027-07-14",
  "2027-08-11",
  "2027-09-14",
  "2027-10-13",
  "2027-11-10",
  "2027-12-10",
]);

const nyCalFmt = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  year: "numeric",
  month: "2-digit",
  day: "2-digit",
  weekday: "short",
  hour: "2-digit",
  minute: "2-digit",
  hour12: false,
});

function parseNyCalendar(tsSec: number): { dateStr: string; weekday: string; day: number; mins: number } {
  const parts = nyCalFmt.formatToParts(new Date(tsSec * 1000));
  let year = "1970";
  let month = "01";
  let dayStr = "01";
  let weekday = "";
  let hr = 0;
  let mn = 0;
  for (const p of parts) {
    if (p.type === "year") year = p.value;
    else if (p.type === "month") month = p.value;
    else if (p.type === "day") dayStr = p.value;
    else if (p.type === "weekday") weekday = p.value;
    else if (p.type === "hour") hr = Number(p.value) % 24;
    else if (p.type === "minute") mn = Number(p.value) || 0;
  }
  return {
    dateStr: `${year}-${month}-${dayStr}`,
    weekday,
    day: Number(dayStr) || 1,
    mins: hr * 60 + mn,
  };
}

/**
 * Deterministic zero-lookahead detector for scheduled US macro releases (FOMC, CPI, NFP).
 */
export function getMacroEventContext(barTimeSec: number): MacroEventContext {
  if (!Number.isFinite(barTimeSec) || barTimeSec <= 0) {
    return { eventName: null, inWindow: false, isEventDay: false, tag: null };
  }
  const ny = parseNyCalendar(barTimeSec);
  const isNfp = ny.weekday === "Fri" && ny.day >= 1 && ny.day <= 7;
  const isFomc = FOMC_DATES.has(ny.dateStr);
  const isCpi = CPI_DATES.has(ny.dateStr);

  if (isFomc) {
    const inWindow = Math.abs(ny.mins - 840) <= 60; // 14:00 ET +- 60m
    return {
      eventName: "FOMC",
      inWindow,
      isEventDay: true,
      tag: inWindow ? "MACRO_WINDOW:FOMC" : "MACRO_DAY:FOMC",
    };
  }
  if (isCpi) {
    const inWindow = Math.abs(ny.mins - 510) <= 60; // 08:30 ET +- 60m
    return {
      eventName: "CPI",
      inWindow,
      isEventDay: true,
      tag: inWindow ? "MACRO_WINDOW:CPI" : "MACRO_DAY:CPI",
    };
  }
  if (isNfp) {
    const inWindow = Math.abs(ny.mins - 510) <= 60; // 08:30 ET +- 60m
    return {
      eventName: "NFP",
      inWindow,
      isEventDay: true,
      tag: inWindow ? "MACRO_WINDOW:NFP" : "MACRO_DAY:NFP",
    };
  }
  return { eventName: null, inWindow: false, isEventDay: false, tag: null };
}

/**
 * Strictly causal 24h volatility/news shock detector over closed 15m bars [endIdx - 95 .. endIdx].
 */
export function detectRecentShock24h(c15: Candle[], endIdx: number, curAtrPct: number): boolean {
  if (!c15 || endIdx < 0 || endIdx >= c15.length) return false;
  const start = Math.max(0, endIdx - 95);
  const atr = Number.isFinite(curAtrPct) && curAtrPct > 0 ? curAtrPct : 0.4;
  const rangeThreshPct = Math.max(2.2, atr * 3.5);
  const bodyThreshPct = Math.max(1.6, atr * 3.0);

  for (let i = start; i <= endIdx; i++) {
    const b = c15[i];
    if (!b || !(b.open > 0) || !(b.close > 0)) continue;
    const rangePct = ((b.high - b.low) / b.close) * 100;
    const bodyPct = (Math.abs(b.close - b.open) / b.open) * 100;
    if (rangePct >= rangeThreshPct || bodyPct >= bodyThreshPct) {
      return true;
    }
  }
  return false;
}

const EARNINGS_TTL_MS = 12 * 3_600_000;

/**
 * Non-blocking cached US equities earnings calendar backed by Nasdaq public calendar API.
 * Strictly fails silently on network errors so it never interrupts live trading ticks.
 */
export class EarningsCalendarCache {
  private cache = new Map<string, { fetchedAt: number; symbols: Set<string> }>();
  private inFlight = new Set<string>();

  public async refreshForAroundDate(nowMs = Date.now()): Promise<void> {
    const todayDate = parseNyCalendar(Math.floor(nowMs / 1000)).dateStr;
    const tomorrowDate = parseNyCalendar(Math.floor((nowMs + 86_400_000) / 1000)).dateStr;
    await Promise.all([this.fetchDateIfNeeded(todayDate, nowMs), this.fetchDateIfNeeded(tomorrowDate, nowMs)]);
  }

  private async fetchDateIfNeeded(dateStr: string, nowMs: number): Promise<void> {
    const existing = this.cache.get(dateStr);
    if (existing && nowMs - existing.fetchedAt < EARNINGS_TTL_MS) return;
    if (this.inFlight.has(dateStr)) return;

    this.inFlight.add(dateStr);
    try {
      const res = await fetch(`https://api.nasdaq.com/api/calendar/earnings?date=${encodeURIComponent(dateStr)}`, {
        headers: {
          "User-Agent": "Mozilla/5.0 (compatible; DynamicGridBot/1.0)",
          Accept: "application/json",
        },
        signal: AbortSignal.timeout(5000),
      });
      if (!res.ok) return;
      const json: any = await res.json().catch(() => null);
      const rows = Array.isArray(json?.data?.rows) ? json.data.rows : [];
      const syms = new Set<string>();
      for (const r of rows) {
        const sym = String(r?.symbol || "")
          .toUpperCase()
          .trim();
        if (sym) syms.add(sym);
      }
      this.cache.set(dateStr, { fetchedAt: Date.now(), symbols: syms });
    } catch {
      // Non-blocking: ignore timeouts or transient HTTP blocks
    } finally {
      this.inFlight.delete(dateStr);
    }
  }

  public getEarningsTag(symbol: string, nowMs = Date.now()): "EARNINGS_TODAY" | "EARNINGS_TOMORROW" | null {
    const sym = String(symbol || "")
      .toUpperCase()
      .trim();
    if (!sym) return null;
    const todayDate = parseNyCalendar(Math.floor(nowMs / 1000)).dateStr;
    const tomorrowDate = parseNyCalendar(Math.floor((nowMs + 86_400_000) / 1000)).dateStr;
    if (this.cache.get(todayDate)?.symbols.has(sym)) return "EARNINGS_TODAY";
    if (this.cache.get(tomorrowDate)?.symbols.has(sym)) return "EARNINGS_TOMORROW";
    return null;
  }
}

export const earningsCalendar = new EarningsCalendarCache();
