import type { Candle } from "../../market-structure.js";

export type PortfolioUniversePreset = "us_top49" | "us_top25" | "custom";
export type PortfolioTp1Mode = "EMA25" | "EMA75" | "EMA200" | "none";
export type PortfolioSitOutMode = "off" | "btc1d" | "btc4h" | "dual";
export type PortfolioRankBy = "COMPOSITE_QUALITY" | "RSI15";

export interface SymbolIndicatorSnapshot {
  symbol: string;
  c15: Candle[];
  c1h: Candle[];
  m4: number[];
  m8: number[];
  m16: number[];
  m25: number[];
  m75: number[];
  m200: number[];
  rsi15: number[];
  macdHist15: number[];
  rsi1h: number[];
  lastClosedIdx: number;
  lastClosedTime: number;
  livePrice: number;
}

export interface CoinSetupState {
  hadBottomFan: boolean;
  fanExpiryBarTime: number;
  waitingPullback: boolean;
  pullbackExpiryBarTime: number;
  waitFromBarTime: number;
  bottomLow: number;
}

export type RadarStage =
  | "HOLDING"
  | "PULLBACK_READY"
  | "WAITING_PULLBACK"
  | "CROSS_8X16_FAIL_FILTERS"
  | "BOTTOM_FAN"
  | "SCANNING";

export interface PortfolioRadarCandidate {
  symbol: string;
  price: number;
  stage: RadarStage;
  score: number;
  rsi15: number;
  rsi1h: number;
  buyVolRatio12: number;
  relStrength24h: number;
  upperSpreadPct: number;
  ema4: number;
  ema8: number;
  ema16: number;
  ema25: number;
  ema75: number;
  ema200: number;
  distanceToEma4Pct: number;
  distanceToEma200Pct: number;
  filterNote?: string;
}

export interface PortfolioSlotTelemetry {
  slotIndex: number;
  status: "HOLDING" | "EMPTY";
  symbol?: string;
  entryPrice?: number;
  currentPrice?: number;
  qty?: number;
  initialQty?: number;
  allocatedCapital?: number;
  marketValue?: number;
  unrealizedPnl?: number;
  unrealizedPnlPct?: number;
  stopLossPrice?: number;
  tp1TargetPrice?: number;
  tp1Done?: boolean;
  beLocked?: boolean;
  isBullishFan?: boolean;
  holdingBars?: number;
  entryTime?: number;
  phaseText?: string;
  progressPct?: number;
  emas?: {
    ema4: number;
    ema8: number;
    ema16: number;
    ema25: number;
    ema75: number;
    ema200: number;
  };
  nextCandidate?: PortfolioRadarCandidate;
  readyBudget?: number;
}

export interface PortfolioMacroState {
  benchmarkSymbol: string;
  benchmarkPrice: number;
  benchmarkRet24h: number;
  isSitOut: boolean;
  sitOutMode: PortfolioSitOutMode;
  reason: string;
  breadthAboveEma25Pct: number;
  universeCount: number;
  pass4h: boolean;
  pass1d: boolean;
  lastScanTime: number;
}
