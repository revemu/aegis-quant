import type { Candle } from "../../market-structure.js";
import { calcCandleAtrPct14, isGapInducedBar, isOpeningSession45mBar } from "../../learning-engine.js";
import type { PortfolioChartSignalMarker, SymbolIndicatorSnapshot } from "./types.js";

export type PullbackModeName = "EMA4_TOUCH" | "EMA8_HOLD" | "EMA8_CONFIRMED" | "EMA16_CONFIRMED";

export function normalizePullbackMode(m?: string): PullbackModeName {
  if (m === "EMA4_TOUCH") return "EMA4_TOUCH";
  if (m === "EMA8_CONFIRMED") return "EMA8_CONFIRMED";
  if (m === "EMA16_CONFIRMED") return "EMA16_CONFIRMED";
  return "EMA8_HOLD";
}

export function getPullbackModeLabel(mode: PullbackModeName): string {
  switch (mode) {
    case "EMA16_CONFIRMED":
      return "EMA16 Confirmed";
    case "EMA8_CONFIRMED":
      return "EMA8 Confirmed";
    case "EMA4_TOUCH":
      return "EMA4 Touch";
    default:
      return "EMA8 Hold";
  }
}

export interface PullbackConfirmResult {
  triggered: boolean;
  touchedZone: boolean;
  reason: string;
  supportPrice: number;
}

/**
 * Evaluates whether a closed 15m candle confirms a pullback entry under the active mode.
 * Pure & strictly causal: relies only on closed bar `bar` and EMAs at that bar, with optional
 * live chase/support guardrails when `livePrice` is passed during live order evaluation.
 */
export function evaluatePullbackCandleConfirm(opts: {
  bar: Candle;
  m4: number;
  m8: number;
  m16: number;
  pullbackMode: PullbackModeName;
  livePrice?: number;
}): PullbackConfirmResult {
  const { bar, m4, m8, m16, pullbackMode, livePrice } = opts;

  const range = Math.max(1e-9, bar.high - bar.low);
  const bodyLow = Math.min(bar.open, bar.close);
  const bodyHigh = Math.max(bar.open, bar.close);
  const lowerWickPct = ((bodyLow - bar.low) / range) * 100;
  const upperWickPct = ((bar.high - bodyHigh) / range) * 100;
  const closePosPct = ((bar.close - bar.low) / range) * 100;
  const isGreen = bar.close > bar.open;
  const hasBuyerDefense = isGreen || (lowerWickPct >= 35 && closePosPct >= 55);
  const cleanTop = upperWickPct < 45;

  if (pullbackMode === "EMA4_TOUCH") {
    const touched = bar.low <= m4 * 1.001 || (livePrice !== undefined && livePrice <= m4 * 1.001);
    const liveOk = livePrice === undefined || livePrice <= m4 * 1.0015;
    return {
      triggered: touched && liveOk,
      touchedZone: touched,
      reason: touched && liveOk ? "EMA4 Touch" : "รอย่อแตะ EMA4",
      supportPrice: m4,
    };
  }

  if (pullbackMode === "EMA16_CONFIRMED") {
    const touched = bar.low <= m16 * 1.002;
    const held = bar.close >= m16 * 0.998;
    const liveOk =
      livePrice === undefined || (livePrice >= m16 * 0.998 && livePrice <= bar.close * 1.004);
    const ok = touched && held && hasBuyerDefense && cleanTop && liveOk;
    return {
      triggered: ok,
      touchedZone: touched,
      reason: ok
        ? "EMA16 Confirmed (แท่งปิดคอนเฟิร์มฐาน)"
        : touched && !held
          ? "หลุดฐาน EMA16 (รอสร้างฐาน)"
          : touched && (!hasBuyerDefense || !cleanTop)
            ? "แตะ EMA16 แล้ว รอแท่งเขียว/ไส้ล่างรับ"
            : "รอย่อถึงฐาน EMA16 + ปิดคอนเฟิร์ม",
      supportPrice: m16,
    };
  }

  if (pullbackMode === "EMA8_CONFIRMED") {
    const touched = bar.low <= m8 * 1.0015;
    const held = bar.close >= m8 * 0.999;
    const liveOk =
      livePrice === undefined || (livePrice >= m8 * 0.9985 && livePrice <= bar.close * 1.004);
    const ok = touched && held && hasBuyerDefense && cleanTop && liveOk;
    return {
      triggered: ok,
      touchedZone: touched,
      reason: ok
        ? "EMA8 Confirmed (แท่งปิดคอนเฟิร์มฐาน)"
        : touched && !held
          ? "หลุดฐาน EMA8 (รอสร้างฐาน)"
          : touched && (!hasBuyerDefense || !cleanTop)
            ? "แตะ EMA8 แล้ว รอแท่งเขียว/ไส้ล่างรับ"
            : "รอย่อถึงฐาน EMA8 + ปิดคอนเฟิร์ม",
      supportPrice: m8,
    };
  }

  // Default: EMA8_HOLD (strict closed-bar touch & hold, no sticky multi-bar chase)
  const touched = bar.low <= m8 * 1.0015;
  const held = bar.close >= m8 * 0.999 && bar.close <= m4 * 1.002;
  const liveOk =
    livePrice === undefined ||
    (livePrice >= m8 * 0.9985 && livePrice <= Math.max(m4 * 1.002, bar.close * 1.004));
  const ok = touched && held && liveOk;
  return {
    triggered: ok,
    touchedZone: touched,
    reason: ok
      ? "EMA8 Hold (ปิดยืนเหนือ EMA8)"
      : touched && bar.close < m8 * 0.999
        ? "หลุดฐาน EMA8 (รอปิดยืนเหนือ EMA8)"
        : "รอย่อหาฐาน EMA8 + ปิดยืนแนวรับ",
    supportPrice: m8,
  };
}

/**
 * Computes causal MA-Fan Portfolio chart signal markers over the trailing closed 15m bars
 * of the inspected ticker, showing 8x16 crosses, ready entry triggers, filter blocks, and live slot entry.
 */
export function computePortfolioChartSignals(opts: {
  ind: SymbolIndicatorSnapshot;
  benchmark15m: Candle[];
  exchangeName: string;
  pullbackMode: PullbackModeName;
  minVolRatio: number;
  minRelStrength24h: number;
  minRsi1h: number;
  minRoomToEma200Pct: number;
  disasterSlPct: number;
  useSessionGuard: boolean;
  useVolTargetedRisk: boolean;
  activeSlot?: { slotIndex: number; entryTime: number; entryPrice: number } | null;
}): PortfolioChartSignalMarker[] {
  const {
    ind,
    benchmark15m,
    exchangeName,
    pullbackMode,
    minVolRatio,
    minRelStrength24h,
    minRsi1h,
    minRoomToEma200Pct,
    disasterSlPct,
    useSessionGuard,
    useVolTargetedRisk,
    activeSlot,
  } = opts;

  const markers: PortfolioChartSignalMarker[] = [];
  const end = ind.lastClosedIdx;
  if (!ind.c15 || end < 200) return markers;

  const start = Math.max(200, end - 220);
  const benchIndexByTime = new Map<number, number>();
  for (let i = 0; i < benchmark15m.length; i++) {
    benchIndexByTime.set(benchmark15m[i]!.time, i);
  }

  const isAlpaca = exchangeName === "alpaca";
  const minSnrAtr = isAlpaca ? 1.25 : 0.65;

  let hadBottomFan = false;
  let fanIdx = -999;
  let waitingPullback = false;
  let waitFromIdx = -999;
  let crossWasGap = false;
  let blockedEmittedForWave = false;
  let p1 = 0;

  for (let idx = start; idx <= end; idx++) {
    const c = ind.c15[idx]!;
    const prevC = ind.c15[idx - 1]!;
    const prevPrevC = idx >= 2 ? ind.c15[idx - 2] : undefined;

    while (
      p1 + 1 < ind.c1h.length &&
      ind.c1h[p1 + 1]!.isClosed !== false &&
      ind.c1h[p1 + 1]!.time + 3600 <= c.time + 900
    ) {
      p1++;
    }

    const m4 = ind.m4[idx]!;
    const m8 = ind.m8[idx]!;
    const m16 = ind.m16[idx]!;
    const m25 = ind.m25[idx]!;
    const m75 = ind.m75[idx]!;
    const m200 = ind.m200[idx]!;
    const prevM8 = ind.m8[idx - 1]!;
    const prevM16 = ind.m16[idx - 1]!;

    const isBottomFan = m4 < m8 && m8 < m16 && m16 < m25 && m25 < m75 && m75 < m200;
    if (isBottomFan) {
      hadBottomFan = true;
      fanIdx = idx;
    }
    if (hadBottomFan && idx - fanIdx > 20 && !waitingPullback) {
      hadBottomFan = false;
    }
    if (waitingPullback && (idx - waitFromIdx >= 12 || m8 < m16)) {
      waitingPullback = false;
      crossWasGap = false;
      blockedEmittedForWave = false;
    }

    const is8Cross16Up = prevM8 <= prevM16 && m8 > m16;
    if (!waitingPullback && hadBottomFan && is8Cross16Up) {
      const rsiVal = ind.rsi15[idx] ?? 0;
      const rsiOk = rsiVal >= 35 && rsiVal > (ind.rsi15[idx - 1] ?? 0);
      const macdOk = (ind.macdHist15[idx] ?? 0) > (ind.macdHist15[idx - 1] ?? 0);
      if (rsiOk && macdOk) {
        const postOpenBarGap = Boolean(
          prevC &&
            prevPrevC &&
            prevC.time - prevPrevC.time > 3600 &&
            isGapInducedBar(prevPrevC, prevC, exchangeName),
        );
        waitingPullback = true;
        waitFromIdx = idx;
        crossWasGap = isGapInducedBar(prevC, c, exchangeName) || postOpenBarGap;
        blockedEmittedForWave = false;

        markers.push({
          time: c.time,
          kind: "CROSS_8X16",
          text: "8>16",
          price: c.low,
          detail: `EMA8 ตัดขึ้น EMA16 (RSI15 ${rsiVal.toFixed(0)} • MACD↑) เปิดหน้ารอย่อ ${getPullbackModeLabel(pullbackMode)}`,
        });
      }
      continue;
    }

    if (waitingPullback && idx > waitFromIdx && m8 >= m16) {
      const curBarGap = isGapInducedBar(prevC, c, exchangeName);
      const isGapBlocked = crossWasGap || curBarGap;
      const confirm = evaluatePullbackCandleConfirm({
        bar: c,
        m4,
        m8,
        m16,
        pullbackMode,
      });

      if (confirm.triggered || (isGapBlocked && confirm.touchedZone)) {
        const rsi1h = p1 >= 14 && ind.rsi1h.length > p1 ? (ind.rsi1h[p1] ?? 50) : 50;
        let upV = 0;
        let dnV = 0;
        for (let k = Math.max(0, idx - 11); k <= idx; k++) {
          const b = ind.c15[k]!;
          const qv = (b.volume || 1) * b.close;
          if (b.close >= b.open) upV += qv;
          else dnV += qv;
        }
        const buyVolRatio12 = dnV > 0 ? upV / dnV : 2.0;

        const ref24Idx = Math.max(0, idx - 96);
        const coinRet24h = ind.c15[ref24Idx]?.open
          ? ((c.close - ind.c15[ref24Idx]!.open) / ind.c15[ref24Idx]!.open) * 100
          : 0;
        const bIdx = benchIndexByTime.get(c.time);
        const benchRet24h =
          bIdx !== undefined && bIdx >= 96 && benchmark15m[bIdx - 96]?.open
            ? ((benchmark15m[bIdx]!.close - benchmark15m[bIdx - 96]!.open) / benchmark15m[bIdx - 96]!.open) * 100
            : 0;
        const relStrength24h = coinRet24h - benchRet24h;

        const dist200 = c.close > 0 ? ((m200 - c.close) / c.close) * 100 : 0;
        const atrPct = calcCandleAtrPct14(ind.c15, idx);
        const snrAtr = (isAlpaca ? dist200 : Math.max(0.8, dist200)) / Math.max(0.12, atrPct);
        const openBlocked =
          useSessionGuard &&
          isOpeningSession45mBar(ind.c15, idx, exchangeName) &&
          (buyVolRatio12 < 1.85 || relStrength24h < 0.4);

        const blocks: string[] = [];
        if (isGapBlocked) blocks.push("Gap Trap");
        if (openBlocked) blocks.push("Open 45m Guard");
        if (useVolTargetedRisk && snrAtr < minSnrAtr) blocks.push(`Low SNR (${snrAtr.toFixed(2)}x<${minSnrAtr}x)`);
        if (minRoomToEma200Pct > 0 && dist200 < minRoomToEma200Pct)
          blocks.push(`Room200 ${dist200.toFixed(1)}%<${minRoomToEma200Pct}%`);
        if (rsi1h < minRsi1h) blocks.push(`1hRSI ${rsi1h.toFixed(0)}<${minRsi1h}`);
        if (buyVolRatio12 < minVolRatio) blocks.push(`VolR ${buyVolRatio12.toFixed(2)}<${minVolRatio}`);
        if (relStrength24h < minRelStrength24h) blocks.push(`RS ${relStrength24h.toFixed(1)}%<${minRelStrength24h}%`);

        if (blocks.length === 0 && confirm.triggered) {
          const slRef = disasterSlPct > 0 ? disasterSlPct : 2.0;
          const rrPlan = dist200 > 0 ? dist200 / slRef : 0;
          markers.push({
            time: c.time,
            kind: "SIGNAL_READY",
            text: "⚡ เข้าได้",
            price: c.close,
            detail: `⚡ สัญญาณเข้าได้ (${getPullbackModeLabel(pullbackMode)}) • VolR ${buyVolRatio12.toFixed(2)}x • RS ${relStrength24h >= 0 ? "+" : ""}${relStrength24h.toFixed(1)}% • 1hRSI ${rsi1h.toFixed(0)} • Room200 ${dist200.toFixed(2)}% (R:R ${rrPlan.toFixed(2)})`,
          });
          waitingPullback = false;
          hadBottomFan = false;
          blockedEmittedForWave = false;
        } else if (!blockedEmittedForWave && idx >= end - 80) {
          markers.push({
            time: c.time,
            kind: "SIGNAL_BLOCKED",
            text: "⛔",
            price: c.high,
            detail: `⛔ ติดกรอง (${getPullbackModeLabel(pullbackMode)}): ${blocks.join(", ")}`,
          });
          blockedEmittedForWave = true;
        }
      }
    }
  }

  if (activeSlot && activeSlot.entryTime > 0 && ind.c15.length > 0) {
    const entrySec = activeSlot.entryTime > 1e12 ? Math.floor(activeSlot.entryTime / 1000) : Math.floor(activeSlot.entryTime);
    const bucketSec = Math.floor(entrySec / 900) * 900;
    let matchedBar = ind.c15.find((b) => b.time === bucketSec);
    if (!matchedBar) {
      for (let i = ind.c15.length - 1; i >= 0; i--) {
        if (ind.c15[i]!.time <= entrySec) {
          matchedBar = ind.c15[i]!;
          break;
        }
      }
    }
    if (matchedBar) {
      markers.push({
        time: matchedBar.time,
        kind: "SLOT_ENTRY",
        text: `🟢 S${activeSlot.slotIndex + 1} เข้า`,
        price: activeSlot.entryPrice,
        detail: `Slot ${activeSlot.slotIndex + 1} เข้าซื้อจริง @ $${activeSlot.entryPrice.toFixed(2)}`,
      });
    }
  }

  return markers;
}
