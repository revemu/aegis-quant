/**
 * @license
 * Copyright DreamDEX S.A.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://github.com/somnia-chain/dreamdex-bot-kit/blob/main/LICENSE
 */

import dotenv from "dotenv";
import { getDatabase } from "./db.js";
import { normalizeStrategyType, type StrategyType } from "./strategy/types.js";

dotenv.config();

const envSymbol = process.env.SYMBOL || "BTCUSDT";
const cleanBinanceSymbol = envSymbol.toLowerCase().replace(/[\/\-_:]/g, "");

/**
 * Baseline fallback configuration defaults.
 * All runtime strategy parameters are loaded from and stored directly in the
 * ACID database (`grid-bot.db.json` -> `db.settings`) as the Single Source of Truth.
 */
export const defaultStrategyConfig = {
  // ── Strategy Selection ──────────────────────────────────────────────────
  /** Active Trading Strategy: "dynamic_grid" (default), "swing_trade", or "ma_ribbon". */
  strategyType: "dynamic_grid" as StrategyType,
  get strategyMode(): StrategyType { return this.strategyType; },
  set strategyMode(v: StrategyType) { this.strategyType = normalizeStrategyType(v); },

  // ── MA Fan Strategy Parameters ──────────────────────────────────────────
  maFanExecutionTimeframe: "15m" as "5m" | "15m",
  maFanSupervisorTimeframe: "15m" as "15m" | "1h",
  maFanAutoCalibrateSpreadOnSwitch: true,
  maFanManualSpreadPct: 0.40,
  maFanUseAtrAdaptiveSpread: true,
  maFanAtrMultiplier: 1.25,
  maFanTp1Ratio: 0.50,
  maFanTrailingPullbackPct: 0.45,
  maFanBreakevenFeeBufferPct: 0.05,
  maFanSlBufferPct: 0.25,
  maFanUseRatchetSl: true,
  maFanBullCushion: false,
  maFanWaterfallGuard: true,
  maFanMacroGate: "noDeepBear" as "off" | "noDeepBear" | "bullOnly",
  maFanTradeMode: "dynamic_grid" as "dip_hunter" | "trend_rider" | "dual" | "dynamic_grid" | "dual_hybrid",
  maFanTradeDirection: "long" as "long" | "short" | "both",
  maFanUpsideFloor: true,
  maFanCircuitBreaker: true,
  maFanCircuitBreakerUntil: 0,
  maFanMaxBudgetUsd: 200,
  maFanEntryOnClosedOnly: true,

  // ── Core Parameters ────────────────────────────────────────────────────
  symbol: envSymbol,
  /** Stop opening new longs once total base inventory exceeds this (Quote/USDT terms). */
  maxInventoryQuote: 40,
  /** Backward compatibility alias for maxInventoryQuote */
  get maxInventoryUsdso(): number { return this.maxInventoryQuote; },
  set maxInventoryUsdso(v: number) { this.maxInventoryQuote = v; },
  /** Skip a cycle if the book spread is wider than this. */
  maxSpreadBps: 60,
  /** Halt buying (offload-only) once session PnL drops below −this. */
  maxSessionLossQuote: 25,
  /** Backward compatibility alias for maxSessionLossQuote */
  get maxSessionLossUsdso(): number { return this.maxSessionLossQuote; },
  set maxSessionLossUsdso(v: number) { this.maxSessionLossQuote = v; },
  /** Maximum allowable Bid discount (%) compared to reference price before allowing Cut Loss sell. */
  cutLossMaxBidDiscountPct: 0.75,
  /** If a lot can't hit its sell trigger within this long, cut it and re-anchor (ms). 0 = off / disabled. Default 0. */
  stuckTimeoutMs: 0,
  /** Poll interval / Tick speed in ms (default 500ms). */
  intervalMs: 500,
  /** Expiration duration for resting Maker limit orders in hours (default 24h). */
  orderExpireHours: 24,
  /** Timezone for log timestamps and formatting. Default "Asia/Bangkok". */
  timezone: process.env.TZ || "Asia/Bangkok",
  dryRun: process.env.DRY_RUN === "true" || false,

  /** Active Exchange Adapter: "binance" (default), "dreamdex", or "alpaca". */
  exchange: (process.env.EXCHANGE || "binance").toLowerCase(),

  // ── Binance API Credentials ─────────────────────────────────────────────
  binanceApiKey: process.env.BINANCE_API_KEY || "",
  binanceApiSecret: process.env.BINANCE_API_SECRET || "",
  binanceBaseUrl: process.env.BINANCE_BASE_URL || "https://api.binance.com",

  // ── Alpaca US Equities Credentials & Endpoints ──────────────────────────
  alpacaApiKey: process.env.ALPACA_API_KEY || "",
  alpacaApiSecret: process.env.ALPACA_API_SECRET || "",
  alpacaBaseUrl: process.env.ALPACA_BASE_URL || "https://paper-api.alpaca.markets",
  alpacaDataUrl: process.env.ALPACA_DATA_URL || "https://data.alpaca.markets",
  /** Ignore US market hours clock check for testing / forced execution (default false). */
  alpacaIgnoreMarketHours: false,

  // ── DreamDEX On-Chain Credentials & Network ────────────────────────────
  dreamdexPrivateKey: process.env.DREAMDEX_PRIVATE_KEY || process.env.PRIVATE_KEY || "",
  dreamdexRpcUrl: process.env.RPC_URL || "https://api.infra.mainnet.somnia.network",
  dreamdexNetwork: "mainnet" as "mainnet" | "testnet",
  /** Native SOMI gas reserve on DreamDEX (Somnia Network) to prevent wallet running out of gas for txs (default 2.0 SOMI). */
  minGasReserveSomi: 2.0,

  // ── Dynamic-grid specific: volatility-driven step sizing ────────────────
  /** "binance" (external WS feed) or "dreamdex" (self-sampled from pool.topOfBook()). */
  atrSource: "binance",
  /** Number of bars ATR is averaged over (classic ATR uses 14). Used by both sources. */
  atrLookback: 14,
  /** step_bps = atrPct * 10_000 * atrMultiplier, before clamping. */
  atrMultiplier: 0.8,
  /** Never let the computed step go below this many bps. */
  minStepBps: 15,
  /** Never let the computed step go above this many bps. */
  maxStepBps: 150,
  /** Recompute the step from ATR every N ticks (avoids re-pricing every second). */
  recalcEveryTicks: 5,
  /** Fixed step used before ATR has enough bars to be meaningful, or while the feed is stale. */
  warmupStepBps: 30,

  // ── DGRID_ATR_SOURCE=dreamdex only ───────────────────────────────────────
  /** Duration of each synthetic OHLC bar built from sampled mid price, ms. */
  barMs: 60_000,

  // ── DGRID_ATR_SOURCE=binance only ────────────────────────────────────────
  // ── Timeframes ────────────────────────────────────────────────────────────
  /** Unified timeframe used for Dow Theory macro bounds, candlestick chart, and channel levels (default "15m"). */
  dowTimeframe: "15m",
  /** Faster micro timeframe used for localized execution timing, drop momentum & knife guard (default "5m"). */
  tradingTimeframe: "5m",
  /** Higher timeframe (HTF) used for major structural anchor support/resistance floors and ceilings (default "4h"). */
  htfTimeframe: "4h",
  get trendTimeframe(): string { return this.dowTimeframe; },
  set trendTimeframe(v: string) { this.dowTimeframe = v; },
  /** Lowercase Binance symbol to stream klines for. */
  binanceSymbol: cleanBinanceSymbol,
  /** wss host. */
  binanceWsBase: process.env.BINANCE_WS_URL || "wss://stream.binance.com:9443",
  /** Feed is considered stale after this long with no message, ms. */
  binanceStaleMs: 30_000,

  // ── Candlestick & Data History ──────────────────────────────────────────
  /** Initial number of historical candles fetched from Binance upon startup (default 300, max 1000). */
  initialCandleCount: 300,
  /** Maximum number of candles maintained in memory and on chart buffer (default 600, max 2000). */
  maxCandleCount: 600,

  // ── Dow Theory & Market Structure Bounds ──────────────────────────────────
  /** Number of bars on left & right to confirm a pivot swing high/low (default 3). */
  dowSwingLookback: 3,
  /** When true, pauses opening new buy orders during a confirmed Dow downtrend (LH+LL). Default true. */
  dowTrendFilter: true,
  /** When true, pauses opening new buy orders while price is under a descending Downtrend Line until breakout. Default false. */
  trendlineFilter: false,
  /** When true, allows resuming buy orders if price stands/closes above Buy Level 2 (30% Zone) for at least 1 candle after a floor breakdown. */
  enableBuyLevel2Recovery: true,
  /** When true, only places buy orders strictly above ascending Trend Support line and pauses buying if support breaks. */
  enableBuyAboveTrendSupport: false,
  /** When true, pauses opening new buy orders (waits for Higher Low reversal or reclaim) if price breaks below ascending Trend Support line. Default true. */
  enableExitOnTrendSupportBreak: true,
  /** When true, immediately liquidates 100% inventory to cash on trend support break. Default false (retains inventory and waits for reversal or 0% Floor cut). */
  liquidateOnTrendSupportBreak: false,
  /** When true, immediately liquidates inventory on Dow Theory confirmed DOWNTREND (LH+LL) to avoid taking full floor cut loss. Default false. */
  enableExitOnDowntrendRegime: false,
  /** When true, pauses buying when price breaks below previous swing low (BOS) and waits for a Higher Low (HL) base formation before resuming. When false, acts as pure aggressive grid like Backtest. Default true. */
  enableBaseConfirmationFilter: true,
  /** When true, only places sell orders strictly below descending Trendline Resistance. */
  enableSellBelowTrendResistance: false,
  /** When true, enables selling at the real frame (Real S/R Ceil) when it adjusts down, choosing the lower price between trendline and real ceiling. Default true. */
  enableSellAtRealCeiling: true,
  /** When true, only maintains/places buy orders when price is at or below Sell Level 1 (<= 52%). Default true. */
  enableBuyBelowSellLevel1: true,
  /** When true, only maintains/places sell orders when price is at or above Buy Level 1 (>= 42%). Default true. */
  enableSellAboveBuyLevel1: true,
  /** Active toggles for Buy Levels 1..5. If a level is disabled, it is skipped and budget is redistributed among active levels. */
  enabledBuyLevels: [true, true, false, false, false] as boolean[],
  /** Active toggles for Sell Targets 1..5. If a target is disabled, it is skipped and inventory is redistributed among active targets. */
  enabledSellTargets: [true, true, false, false, false] as boolean[],
  /** Grid zone percentage allocations for Buy Levels 1..5 (default [20, 12, 8, 4, 2]%). */
  buyLevelPcts: [20, 12, 8, 4, 2] as number[],
  /** Grid zone percentage allocations for Sell Targets 1..5 (default [52, 72, 82, 90, 95]%). */
  sellLevelPcts: [52, 72, 82, 90, 95] as number[],
  /** Relative tranche weights for Sell Targets 1..5 (default [50, 50, 0, 0, 0]). */
  sellLevelWeights: [50, 50, 0, 0, 0] as number[],
  /**
   * Buy Tranche Sizing Mode:
   * 1. "PYRAMID" (Default): Weighted sizing [55%, 45%, 0%, 0%, 0%] from Buy L1..L2 to pull average entry lower toward the floor.
   * 2. "EQUAL": Equal allocation per active buy level.
   */
  buySizingMode: "PYRAMID" as "EQUAL" | "PYRAMID",
  /** Relative tranche weights for Buy Levels 1..5 when buySizingMode === "PYRAMID" (default [55, 45, 0, 0, 0]). */
  buyLevelWeights: [55, 45, 0, 0, 0] as number[],
  /** Minimum net profit spread (%) above cost basis required for sell targets to prevent fee-eroded micro-exits in narrow channels (default 0.25%). */
  minNetProfitPct: 0.25,
  /** Breakeven/micro-profit percentage required for LH Dynamic Escape during confirmed downtrend (LH+LL) to escape safely (default 0.06%). */
  breakevenEscapeProfitPct: 0.06,
  /** Maximum number of active resting Maker buy limit orders placed simultaneously on book (default 5 for full grid coverage on CEX). */
  maxActiveMakerBuys: 5,
  /** When true, dynamically lowers sell target to a confirmed Lower High (LH) formed below Sell Target 1 while holding inventory (if profitable >= minNetProfitPct). */
  enableLhDynamicEscape: true,
  /**
   * Order Execution Mode:
   * 1. "IOC_BRACKET" (Default): Zero resting orders on book. Dynamically snipes buys/sells via IOC when price enters level brackets. Aggregates tranches on fast moves to save gas.
   * 2. "MAKER_LIMIT": Maintains resting limit orders on DreamDEX CLOB book.
   */
  orderExecutionMode: "IOC_BRACKET" as "IOC_BRACKET" | "MAKER_LIMIT",
  /** Slippage tolerance (%) for IOC orders in IOC_BRACKET mode (default 0.35%). */
  iocSlippagePct: 0.35,
  /**
   * Profit taking / Sell mode:
   * 1. "GRID_CASHFLOW" (Default): Sell at all grid targets regardless of avgEntry to maximize cash flow.
   * 2. "PORTFOLIO_AVG_PROFIT": Only sell at prices higher than portfolio average entry (+minNetProfitPct).
   * 3. "LOT_BASED_PROFIT": Lot-based matching; only sell inventory from lots bought lower than the sell target price.
   */
  sellProfitMode: "PORTFOLIO_AVG_PROFIT" as "GRID_CASHFLOW" | "PORTFOLIO_AVG_PROFIT" | "LOT_BASED_PROFIT",
  /** Computed alias for sellProfitMode === "PORTFOLIO_AVG_PROFIT". */
  get requireProfitAboveAvgEntry(): boolean { return this.sellProfitMode === "PORTFOLIO_AVG_PROFIT"; },
  set requireProfitAboveAvgEntry(v: boolean) { if (v) this.sellProfitMode = "PORTFOLIO_AVG_PROFIT"; },
  /** Automatically cut loss / sell all inventory if price breaks below Lower Bound (0% Floor). */
  cutLossAtLowerBound: true,
  /** Number of confirmed candle periods below floor buffer required before executing cut loss (default 1). */
  cutLossConfirmCandles: 1,
  /** When true, cut loss triggers strictly when breaking below the buffer line (cutLossBound), not on floor breakdown. */
  cutLossOnBufferOnly: true,
  /** Required continuous elapsed time (seconds) below cutLossBound before executing fast cut (default 60s, 0 = instant real-time). */
  cutLossBufferDelaySeconds: 60,
  /** When true, executes Cut Loss in 2 stages (Stage 1: 50% de-risk on buffer break, Stage 2: remaining 50% if price drops below 2x buffer or next candle closes lower). */
  enableTwoStageCutLoss: true,
  /** Buffer multiplier for Stage 2 full cut loss (default 2.0x buffer distance below Floor). */
  cutLossStage2Multiplier: 2.0,
  /** Automatically sell 100% of all inventory when price reaches Upper Bound (100% Ceiling). */
  takeProfitAtUpperBound: true,
  /** When true, if market price exceeds a grid sell level's target price, executes an immediate IOC sell. */
  enableIocSellWhenExceeded: true,

  // ── Triangle Squeeze / Trendline Convergence ────────────────────────────
  /** When true, pauses accumulation and enters standby when support and resistance trendlines converge. */
  enableTriangleSqueezeExit: false,
  /** When true, forcibly liquidates open position to cash upon entering squeeze (default false: keep inventory & sell targets active). */
  liquidatePositionOnSqueeze: false,
  /** Minimum spread (%) between resistance and support trendlines before triggering squeeze exit (default 1.5%). */
  triangleSqueezeSpreadPct: 1.5,
  /** Minimum channel width (%) required to resume trading after a squeeze exit (default 2.0%). */
  minTradeableChannelWidthPct: 2.0,

  // ── Capital Staging & Stabilized Channel DCA ────────────────────────────
  /** Number of capital zones to divide budget for staged DCA when channel moves down and stabilizes (default 2, e.g. 2 = 2 zones of 50%, 4 = 4 zones of 25%). */
  capitalZones: 2,
  /** Minimum drop % between new lower channel and previous zone's lowest buy price before unlocking next DCA zone (default 2.5%). */
  zoneMinDropPct: 2.5,
  /** Number of closed candles a lower channel must remain stable (not making new lows) before unlocking next DCA zone (default 3 bars). */
  zoneStabilizeCandles: 3,
  /** Require confirmed point of reversal (Higher Low, Valley, Breakout, Rebound) before placing buy orders in Zone 2+ (default true). */
  zoneRequireReversal: true,
  /** Guard against opening new grid positions when the coin is in an AVOID/DOWNTREND market regime (default false). */
  enableReadinessGuard: false,
  /** Zone exit strategy: "PORTFOLIO_AVG" (default), "ORIGINAL_TARGETS", "PER_ZONE", "HYBRID_ZONE_RECOVERY", or "AVG_PROFIT_ALL". */
  zoneExitMode: "PORTFOLIO_AVG" as "PORTFOLIO_AVG" | "ORIGINAL_TARGETS" | "PER_ZONE" | "HYBRID_ZONE_RECOVERY" | "AVG_PROFIT_ALL",
  /** Cut loss behavior mode for dynamic grid in backtest: "none" (default for DCA hold), "entry_floor" (strict entry floor SL), or "final_zone_floor" (SL only after all zones exhausted). */
  gridCutLossMode: "entry_floor" as "none" | "entry_floor" | "final_zone_floor",

  // ── Grid Trailing Stop & Ratchet Profit Lock ───────────────────────────
  /** When true, activates dynamic Ratchet Step Trailing Stop for held grid lots to lock profits and rotate cashflow faster. Default true. */
  enableGridTrailingStop: true,
  /** Channel span ratio threshold to trigger Break-even lock (moving Stop Loss to entry + fee). Default 0.44 (44% of channel). */
  gridTrailingBreakevenRatio: 0.44,
  /** Channel span ratio threshold to trigger Profit Lock Step 2 (moving Stop Loss to 30% of channel). Default 0.52. */
  gridTrailingStep2Ratio: 0.52,
  /** Channel span ratio threshold to trigger Profit Lock (moving Stop Loss to 50% of channel). Default 0.65. */
  gridTrailingProfitLockRatio: 0.65,
  /** Channel span ratio where Stop Loss is locked when profit lock triggers. Default 0.50. */
  gridTrailingProfitLockTargetRatio: 0.50,
  /** Time-decay breakeven escape: holding bars on dowTimeframe before lowering limit sell to entry+profit (default 18 bars = 4.5h on 15m). */
  gridTimeDecayBars: 18,

  // ── Support & Resistance Channel Calculation Mode ────────────────────────
  /**
   * Channel boundary calculation mode:
   * 1. "DOW_SWINGS": Pure Dow Swings with progressive nearest-swing expansion outward from current price.
   * 2. "DOW_ATR_CLAMP": Dow Theory Swings with dynamic ATR min/max width clamping & true wick support.
   * 3. "FIXED_PCT_CLAMP": Dow Theory Swings clamped to fixed min/max percentage width.
   * 4. "DONCHIAN_ATR" (Default): Donchian 48b Channel (Highest High & Lowest Low of lookback bars) + Dow ChoCh Gate.
   * 5. "VOLUME_PROFILE_HTF": Hybrid Volume Profile (POC/VAH/VAL) + 4H HTF Structural Anchor.
   */
  channelMode: "DONCHIAN_ATR" as "DOW_SWINGS" | "DOW_ATR_CLAMP" | "FIXED_PCT_CLAMP" | "DONCHIAN_ATR" | "VOLUME_PROFILE_HTF",
  /** Lookback bars for Volume Profile calculation on macro candles (default 192 = 48 hours on 15m). */
  vpLookbackBars: 192,
  /** Min channel width for DOW_SWINGS & FIXED_PCT_CLAMP & DONCHIAN_ATR (in %, default 6.0%). */
  minChannelWidthPct: 6.0,
  /** Max channel width for DOW_SWINGS & FIXED_PCT_CLAMP (in %, default 20.0%). */
  maxChannelWidthPct: 20.0,
  /** When true, freezes/locks channel bounds while holding inventory position to avoid shifting sell targets. */
  lockChannelInPosition: true,
  /** When true, recalculates and shifts channel bounds only when a candle closes (confirmed candle), preventing jitter during live ticks. */
  adjustChannelOnClosedOnly: true,
  /** Min channel width multiplier of ATR for DOW_ATR_CLAMP (default 2.5x ATR). */
  atrMinMultiplier: 2.5,
  /** Max channel width multiplier of ATR for DOW_ATR_CLAMP (default 5.0x ATR). */
  atrMaxMultiplier: 5.0,
  /** Donchian lookback bars for DONCHIAN_ATR mode (default 48 bars = ~12 hours on 15m). */
  donchianLookback: 48,
  /** Shared lookback (bars) for Peak/Valley detection, trendlines and market structure analysis (default 300 = initial candle load). */
  srLookbackCandles: 300,
  /** Safety buffer below floor (in % of price, default 1.5%). */
  floorBufferPct: 1.5,
  /** When true, dynamically scales Floor Cut-Loss buffer according to rolling ATR volatility to avoid wick stop-outs. Default true. */
  enableDynamicAtrBuffer: true,
  /** Multiplier of rolling ATR percentage to apply to dynamic Floor Buffer (default 0.75x ATR). */
  atrBufferMultiplier: 0.75,
  /** When true, activates micro-timeframe momentum analysis to pause buying during consecutive heavy red dump bars (waterfall knife). Default true. */
  enableWaterfallKnifeGuard: true,
  /** When true, enforces Trend-Gated Structural Channel (disables channel creation in downtrends or breaking lows). Default true. */
  enableTrendChannelGating: true,
  /** Minimum safe % distance from current price to floor before a channel can be opened (default 1.5%). Prevents dangerously close floors. */
  minFloorProximityPct: 1.5,
  /** Use true wick (candle high/low) for swing pivots instead of body clipping (default false). */
  useTrueWick: false,
  /** Rejection wick threshold (% of candle range) to clip to candle body when useTrueWick is false (default 50.0%). */
  wickThresholdPct: 50.0,

  // ── Narrow Channel Mode & Edge Bracketing ───────────────────────────────
  /** When true, collapses stepped grid levels to edge brackets (Floor + buffer / Ceil - buffer) if channel width < narrowChannelThresholdPct. */
  enableNarrowChannelCollapse: true,
  /** Channel width (%) threshold below which narrow channel mode is active (default 1.2%). */
  narrowChannelThresholdPct: 1.2,
  /** Buffer (%) above floor for Buy Edge Floor limit price in narrow channels (default 0.05%). */
  narrowChannelBuyBufferPct: 0.05,
  /** Buffer (%) below ceiling for Sell Edge Ceil limit price in narrow channels (default 0.05%). */
  narrowChannelSellBufferPct: 0.05,

  // ── Order Stability & Fee Optimization (Hysteresis) ─────────────────────
  /** Minimum % change in channel bounds before shifting grid levels (default 1.0%). Prevents order churn and fee waste on minor fluctuations. */
  minChannelShiftPct: 1.0,
  /** Price tolerance (%) between desired grid level and existing open order to keep resting order without canceling (default 1.0%). */
  orderPriceTolerancePct: 1.0,
  /** Quantity difference ratio (%) before rebalancing an existing open order (default 10%). */
  orderQtyTolerancePct: 10.0,

  // ── Legacy Internal Flags (Disabled) ─────────────────────────────────────
  laggardThresholdPct: 0.4,
  enableLaggardSnipe: false,
  enableLaggardGuard: false,

  // ── State Persistence & Offline Database (Lots & PnL recovery on restart) ──
  /** Persist open lots, realized PnL, order history, and gas tracking to offline database. Default true. */
  persistState: true,
  /** File path for offline ACID database. Isolated per BOT_INSTANCE when set. */
  dbPath: process.env.BOT_DB_PATH || (process.env.BOT_INSTANCE ? `data/grid-bot.${process.env.BOT_INSTANCE}.db.json` : "data/grid-bot.db.json"),
  /** Legacy JSON state file path to auto-migrate from. Default ".grid-state.json". */
  stateFile: ".grid-state.json",

  // ── Bot Control & Initial State ──────────────────────────────────────────
  /** Start bot in PAUSED mode on boot (chart and bounds calculate normally, no orders placed/closed). Default true. */
  startPaused: true,

  // ── Web Dashboard ────────────────────────────────────────────────────────
  /** Port to serve the live TradingView dashboard on (0 to disable). Default 3333 or FORCE_DASHBOARD_PORT/PORT. */
  dashboardPort: process.env.FORCE_DASHBOARD_PORT
    ? parseInt(process.env.FORCE_DASHBOARD_PORT, 10)
    : (process.env.PORT ? parseInt(process.env.PORT, 10) : 3333),

  // ── Swing Trade Strategy Specific Parameters ───────────────────────────
  /** Minimum Risk:Reward ratio required to execute a swing entry (default 2.0). */
  swingMinRiskReward: 2.0,
  /** Stop Loss buffer percentage below the structural swing low / HL base (default 0.5%). */
  swingStopLossBufferPct: 0.5,
  /** Maximum portfolio percentage allocated to a single swing trade position (default 60%). */
  swingMaxPositionPct: 60.0,
  /** Take Profit 1 partial exit ratio (0.5 = sell 50% at TP1). */
  swingTp1CloseRatio: 0.5,
  /** Take Profit 1 target type: "CENTER_POC" or "FIXED_PCT" (default "CENTER_POC"). */
  swingTp1TargetType: "CENTER_POC" as "CENTER_POC" | "FIXED_PCT",
  /** Take Profit 2 target type: "SWING_HIGH_HH" or "TRAILING_STOP" (default "SWING_HIGH_HH"). */
  swingTp2TargetType: "SWING_HIGH_HH" as "SWING_HIGH_HH" | "TRAILING_STOP",
  /** Trailing Stop activation gain % after TP1 reached (default 1.5%). */
  swingTrailingActivationPct: 1.5,
  /** Trailing Stop pullback tolerance % (default 0.8%). */
  swingTrailingPullbackPct: 0.8,
  /** Allowed entry triggers for swing trades (default: HL base confirmation and support retest; trendline breakout false by default to prevent FOMO buy). */
  swingEnableHlBaseEntry: true,
  swingEnableTrendlineBreakoutEntry: false,
  swingEnableSupportRetestEntry: true,
  /** Enable Bearish Candlestick Reversal Exit at resistance (Shooting Star / Engulfing) to lock profit early (default true). */
  swingEnableBearishReversalExit: true,
  /** Minimum profit % required before allowing early bearish reversal exit (default 0.35%). */
  swingBearishReversalMinProfitPct: 0.35,
  /** Tolerance % from resistance structure (Trendline/TP/Ceiling) to evaluate bearish reversal (default 0.8%). */
  swingBearishReversalResistanceTolerancePct: 0.8,

  // ── Autonomous Market Regime & Pair Rotation ───────────────────────────
  /** When true, automatically evaluates market regime on cycle exit or idle cash and rotates to the best performing pair. Default false. */
  enableAutoRotation: false,
  /** Quote asset to scan and trade: "USDT" or "FDUSD". Default "USDT". */
  rotationQuoteAsset: "USDT" as "USDT" | "FDUSD",
  /** Minimum interval (cooldown) in minutes between pair rotations (default 180 = 3h). */
  rotationIntervalMinutes: 180,
  /** If flat cash (0 lots held) for longer than this (minutes), trigger rotation evaluation (default 120 = 2h). */
  rotationIdleCashMinutes: 120,
  /** Percentage margin by which a candidate's composite score must exceed current symbol to trigger rotation (default 20%). */
  rotationScoreMarginPct: 20,
  /** Minimum 24h quote volume (USD) for rotation candidates (default 3,000,000). */
  rotationMinVolumeUsd: 3_000_000,
  /** Optional whitelist of symbols allowed for rotation (empty = all liquid pairs in selected quote asset). */
  rotationWhitelist: [] as string[],
  /** Blacklisted symbols excluded from rotation. */
  rotationBlacklist: [
    "USDCUSDT", "FDUSDUSDT", "TUSDUSDT", "USDPUSDT", "DAIUSDT", "BUSDUSDT", "EURIUSDT", "EURUSDT",
    "USDCFDUSD", "TUSDFDUSD", "USDPFDUSD", "EURFDUSD"
  ] as string[],
  /** When true, pauses altcoin rotation and holds 100% Cash when BTC is in macro breakdown (> 1.2% below 24h SMA or dumping > 2% in 4h). Default true. */
  rotationEnableBtcMacroGate: true,

  // ── MA-Fan Multi-Slot Portfolio Strategy ("ma_fan_portfolio") ──────────
  /** Maximum concurrent held portfolio slots (default 3 from walk-forward backtest). */
  portfolioMaxSlots: 3,
  /** Universe preset for portfolio scanning: "us_top49" (full 49 blue-chips/ETFs), "us_top25" (core 25), or "custom". */
  portfolioUniversePreset: "us_top49" as "us_top49" | "us_top25" | "custom",
  /** Maximum number of liquid pairs fetched into the live portfolio scanner. */
  portfolioMaxUniversePairs: 49,
  /** Optional custom ticker whitelist (used when preset === "custom", or overrides auto-universe if non-empty). */
  portfolioCustomWhitelist: [] as string[],
  /** Tickers excluded from portfolio entry scanning. */
  portfolioBlacklist: [] as string[],
  /** Portfolio TP1 target mode: "EMA200" (backtest winner), "EMA75", "EMA25", or "none". */
  portfolioTp1TargetMode: "EMA200" as "EMA25" | "EMA75" | "EMA200" | "none",
  /** Minimum TP1 target gain percentage above entry (default 1.2%). */
  portfolioTp1MinGainPct: 1.2,
  /** Partial 50% take-profit when reaching TP1 target in profit (>= portfolioTp1MinGainPct). */
  portfolioPartialTpAtEma200: true,
  /** Lock stop-loss at breakeven (+0.25%) once TP1 target is reached (when portfolioRunnerTrailPct === 0). */
  portfolioLockBreakevenAtEma200: true,
  /** Optional post-TP1 runner trailing stop % from peak (0 = use +0.25% BE lock; >0 e.g. 1.2 = trail from peak with +0.1% floor). */
  portfolioRunnerTrailPct: 0,
  /** Hard calibrated disaster stop-loss percentage below entry (default 2.0%). */
  portfolioDisasterSlPct: 2.0,
  /** Stale Time-Stop: cut stagnant open slot before TP1 after N 15m trading bars if gain < portfolioStaleExitMaxGainPct (0 = disabled). */
  portfolioStaleExitBars: 16,
  /** Maximum gain % threshold for Stale Time-Stop (default 0.3%). */
  portfolioStaleExitMaxGainPct: 0.3,
  /** Minimum required distance % to EMA200 before opening a new slot (0 = disabled). */
  portfolioMinRoomToEma200Pct: 0,
  /** Macro sit-out benchmark guard: "dual", "breadth_35" (Bear & Breadth < 35%), "btc1d", "btc4h", or "off". */
  portfolioSitOutMode: "dual" as "off" | "btc1d" | "btc4h" | "dual" | "breadth_35",
  /** Candidate ranking priority when multiple setups trigger on the same 15m bar. */
  portfolioRankBy: "COMPOSITE_QUALITY" as "COMPOSITE_QUALITY" | "RSI15",
  /** Optional max hold hours per slot (0 = disabled). */
  portfolioMaxHoldHours: 0,
  /** Max total capital allocated across all portfolio slots in USD/Quote (0 = use full free wallet cash). */
  portfolioMaxBudgetUsd: 0,
  /** Percentage of total portfolio capital allowed to be deployed across all slots (MM cap, default 50%). */
  portfolioCapitalUsagePct: 50,
  /** Pullback entry timing mode: "EMA16_CONFIRMED" | "EMA8_CONFIRMED" | "EMA8_HOLD" | "EMA4_TOUCH". */
  portfolioPullbackMode: "EMA8_HOLD" as "EMA8_HOLD" | "EMA4_TOUCH" | "EMA8_CONFIRMED" | "EMA16_CONFIRMED",
  /** Quant Q1: Cross-asset rolling return correlation guard (blocks rho > 0.75 vs held slots). */
  portfolioQuantCorrelationGuard: true,
  /** Quant Q2: Regime-conditional TP1 & filter auto-switching by market breadth. */
  portfolioQuantRegimeConditional: true,
  /** Quant Q3: Opening-session 45m trap filter requiring stronger momentum/volume confirmation. */
  portfolioQuantSessionGuard: true,
  /** Quant Q4: Volatility-normalized ATR SNR gate (>=1.25x) & inverse-vol outlier sizing. */
  portfolioQuantVolTargetedRisk: true,
  /** Quant Q5: Empirical Bayes Expected Value ranking from Shadow Walk-Forward buckets. */
  portfolioQuantEmpiricalEv: true,
  /** Quant Q6: Closed-loop OOS Context Gate enforcement ("off" | "observe" | "auto_half_size" | "auto_enforce"). */
  portfolioQuantContextGateMode: "auto_half_size" as "off" | "observe" | "auto_half_size" | "auto_enforce",
  /** Minimum 12-bar buy volume ratio required before entering a pullback (default 1.3). */
  portfolioMinBuyVolRatio: 1.3,
  /** Minimum 24h relative strength vs benchmark in % required before entering (default -1.0). */
  portfolioMinRelStrength24h: -1.0,
  /** Minimum 1h RSI required before entering a pullback (default 40). */
  portfolioMinRsi1h: 40,
  /** Active symbol currently inspected on the main chart in portfolio mode. */
  portfolioInspectedSymbol: "",
  /** Bounded Learning Engine Auto-Tuning mode: "all" (autonomous zero-config default), "dry_run_only", or "off". */
  learningAutoTuneMode: "all" as "dry_run_only" | "all" | "off",
  /** Enforce Survival Guard slot/size/entry blocks (false = observe & log only to evaluate accuracy while verifying execution). */
  portfolioSurvivalGuardEnforced: false,
};

/**
 * Loads the active strategy configuration exclusively from Database settings (`db.settings`),
 * falling back to defaultStrategyConfig for any uninitialized fields.
 */
export function loadConfigFromDatabase(): typeof defaultStrategyConfig {
  try {
    const db = getDatabase({ dbPath: defaultStrategyConfig.dbPath });
    const saved = db.getAllSettings();
    if (saved && Object.keys(saved).length > 0) {
      const merged = {
        ...defaultStrategyConfig,
        ...saved,
      };
      if (saved.strategyType !== undefined || saved.strategyMode !== undefined) {
        merged.strategyType = normalizeStrategyType(saved.strategyType ?? saved.strategyMode);
      }
      if (saved.symbol) {
        merged.symbol = String(saved.symbol).toUpperCase().replace(/[\/\-_:]/g, "");
        merged.binanceSymbol = String(saved.symbol).toLowerCase().replace(/[\/\-_:]/g, "");
      } else if (saved.exchange === "dreamdex" || process.env.EXCHANGE === "dreamdex") {
        merged.symbol = "SOMI:USDSO";
        merged.binanceSymbol = "somiusdt";
      }
      if (saved.maxInventoryQuote !== undefined) {
        merged.maxInventoryQuote = Number(saved.maxInventoryQuote);
      } else if (saved.maxInventoryUsdso !== undefined) {
        merged.maxInventoryQuote = Number(saved.maxInventoryUsdso);
      }
      if (saved.maxSessionLossQuote !== undefined) {
        merged.maxSessionLossQuote = Number(saved.maxSessionLossQuote);
      } else if (saved.maxSessionLossUsdso !== undefined) {
        merged.maxSessionLossQuote = Number(saved.maxSessionLossUsdso);
      }
      if (saved.binanceApiKey !== undefined && saved.binanceApiKey !== "******" && String(saved.binanceApiKey).trim() !== "") {
        merged.binanceApiKey = String(saved.binanceApiKey).trim();
      }
      if (saved.binanceApiSecret !== undefined && saved.binanceApiSecret !== "******" && String(saved.binanceApiSecret).trim() !== "") {
        merged.binanceApiSecret = String(saved.binanceApiSecret).trim();
      }
      if (saved.dreamdexPrivateKey !== undefined && saved.dreamdexPrivateKey !== "******" && String(saved.dreamdexPrivateKey).trim() !== "") {
        merged.dreamdexPrivateKey = String(saved.dreamdexPrivateKey).trim();
      }
      if (saved.alpacaApiKey !== undefined && saved.alpacaApiKey !== "******" && String(saved.alpacaApiKey).trim() !== "") {
        merged.alpacaApiKey = String(saved.alpacaApiKey).trim();
      }
      if (saved.alpacaApiSecret !== undefined && saved.alpacaApiSecret !== "******" && String(saved.alpacaApiSecret).trim() !== "") {
        merged.alpacaApiSecret = String(saved.alpacaApiSecret).trim();
      }
      if (saved.alpacaBaseUrl !== undefined && String(saved.alpacaBaseUrl).trim() !== "") {
        merged.alpacaBaseUrl = String(saved.alpacaBaseUrl).trim();
      }
      if (saved.alpacaDataUrl !== undefined && String(saved.alpacaDataUrl).trim() !== "") {
        merged.alpacaDataUrl = String(saved.alpacaDataUrl).trim();
      }
      if (saved.alpacaIgnoreMarketHours !== undefined) {
        merged.alpacaIgnoreMarketHours = Boolean(saved.alpacaIgnoreMarketHours);
      }
      if (saved.binanceBaseUrl !== undefined) merged.binanceBaseUrl = String(saved.binanceBaseUrl);
      if (saved.binanceWsBase !== undefined) merged.binanceWsBase = String(saved.binanceWsBase);
      if (saved.dryRun !== undefined) merged.dryRun = Boolean(saved.dryRun);
      if (saved.timezone !== undefined) merged.timezone = String(saved.timezone);
      if (process.env.FORCE_DASHBOARD_PORT) {
        merged.dashboardPort = parseInt(process.env.FORCE_DASHBOARD_PORT, 10);
      } else if (saved.dashboardPort !== undefined) {
        merged.dashboardPort = Number(saved.dashboardPort);
      }
      if (saved.maFanUseRatchetSl !== undefined) merged.maFanUseRatchetSl = Boolean(saved.maFanUseRatchetSl);
      if (saved.maFanBullCushion !== undefined) merged.maFanBullCushion = Boolean(saved.maFanBullCushion);
      if (saved.maFanWaterfallGuard !== undefined) merged.maFanWaterfallGuard = Boolean(saved.maFanWaterfallGuard);
      if (saved.maFanMacroGate !== undefined && ["off", "noDeepBear", "bullOnly"].includes(saved.maFanMacroGate)) {
        merged.maFanMacroGate = saved.maFanMacroGate;
      }
      if (saved.maFanTradeMode !== undefined && ["dip_hunter", "trend_rider", "dual", "dynamic_grid", "dual_hybrid"].includes(saved.maFanTradeMode)) {
        merged.maFanTradeMode = saved.maFanTradeMode;
      }
      if (saved.maFanTradeDirection !== undefined && ["long", "short", "both"].includes(saved.maFanTradeDirection)) {
        merged.maFanTradeDirection = saved.maFanTradeDirection;
      }
      if (saved.maFanUpsideFloor !== undefined) merged.maFanUpsideFloor = Boolean(saved.maFanUpsideFloor);
      if (saved.maFanCircuitBreaker !== undefined) merged.maFanCircuitBreaker = Boolean(saved.maFanCircuitBreaker);
      if (saved.maFanCircuitBreakerUntil !== undefined) merged.maFanCircuitBreakerUntil = Number(saved.maFanCircuitBreakerUntil);
      if (saved.htfTimeframe !== undefined) merged.htfTimeframe = String(saved.htfTimeframe);
      if (saved.vpLookbackBars !== undefined) merged.vpLookbackBars = Number(saved.vpLookbackBars);
      if (saved.enableTrendChannelGating !== undefined) merged.enableTrendChannelGating = Boolean(saved.enableTrendChannelGating);
      if (saved.minFloorProximityPct !== undefined) merged.minFloorProximityPct = Number(saved.minFloorProximityPct);
      if (saved.enableNarrowChannelCollapse !== undefined) merged.enableNarrowChannelCollapse = Boolean(saved.enableNarrowChannelCollapse);
      if (saved.adjustChannelOnClosedOnly !== undefined) merged.adjustChannelOnClosedOnly = Boolean(saved.adjustChannelOnClosedOnly);
      if (saved.cutLossOnBufferOnly !== undefined) merged.cutLossOnBufferOnly = Boolean(saved.cutLossOnBufferOnly);
      if (saved.cutLossBufferDelaySeconds !== undefined) merged.cutLossBufferDelaySeconds = Number(saved.cutLossBufferDelaySeconds);
      if (saved.enableTwoStageCutLoss !== undefined) merged.enableTwoStageCutLoss = Boolean(saved.enableTwoStageCutLoss);
      if (saved.cutLossStage2Multiplier !== undefined) merged.cutLossStage2Multiplier = Number(saved.cutLossStage2Multiplier);
      if (saved.enableLhDynamicEscape !== undefined) merged.enableLhDynamicEscape = Boolean(saved.enableLhDynamicEscape);
      if (saved.minNetProfitPct !== undefined) merged.minNetProfitPct = Number(saved.minNetProfitPct);
      if (saved.capitalZones !== undefined) merged.capitalZones = Math.max(1, Math.min(5, Number(saved.capitalZones) || 1));
      if (saved.zoneMinDropPct !== undefined) merged.zoneMinDropPct = Math.max(0.5, Number(saved.zoneMinDropPct) || 2.5);
      if (saved.zoneStabilizeCandles !== undefined) merged.zoneStabilizeCandles = Math.max(1, Math.min(20, Number(saved.zoneStabilizeCandles) || 3));
      if (saved.zoneRequireReversal !== undefined) merged.zoneRequireReversal = Boolean(saved.zoneRequireReversal);
      if (saved.enableReadinessGuard !== undefined) merged.enableReadinessGuard = Boolean(saved.enableReadinessGuard);
      if (saved.zoneExitMode) {
        merged.zoneExitMode = saved.zoneExitMode as any;
      }
      if (saved.gridCutLossMode === "none" || saved.gridCutLossMode === "entry_floor" || saved.gridCutLossMode === "final_zone_floor") {
        merged.gridCutLossMode = saved.gridCutLossMode;
      }
      if (saved.enableGridTrailingStop !== undefined) merged.enableGridTrailingStop = Boolean(saved.enableGridTrailingStop);
      if (saved.gridTrailingBreakevenRatio !== undefined) merged.gridTrailingBreakevenRatio = Number(saved.gridTrailingBreakevenRatio);
      if (saved.gridTrailingProfitLockRatio !== undefined) merged.gridTrailingProfitLockRatio = Number(saved.gridTrailingProfitLockRatio);
      if (saved.gridTrailingProfitLockTargetRatio !== undefined) merged.gridTrailingProfitLockTargetRatio = Number(saved.gridTrailingProfitLockTargetRatio);
      if (saved.breakevenEscapeProfitPct !== undefined) merged.breakevenEscapeProfitPct = Number(saved.breakevenEscapeProfitPct);
      if (saved.maxActiveMakerBuys !== undefined) merged.maxActiveMakerBuys = Number(saved.maxActiveMakerBuys);
      if (saved.enableBuyAboveTrendSupport !== undefined) merged.enableBuyAboveTrendSupport = Boolean(saved.enableBuyAboveTrendSupport);
      if (saved.enableExitOnTrendSupportBreak !== undefined) merged.enableExitOnTrendSupportBreak = Boolean(saved.enableExitOnTrendSupportBreak);
      if (saved.liquidateOnTrendSupportBreak !== undefined) merged.liquidateOnTrendSupportBreak = Boolean(saved.liquidateOnTrendSupportBreak);
      if (saved.enableExitOnDowntrendRegime !== undefined) merged.enableExitOnDowntrendRegime = Boolean(saved.enableExitOnDowntrendRegime);
      if (saved.enableBaseConfirmationFilter !== undefined) merged.enableBaseConfirmationFilter = Boolean(saved.enableBaseConfirmationFilter);
      if (saved.enableWaterfallKnifeGuard !== undefined) merged.enableWaterfallKnifeGuard = Boolean(saved.enableWaterfallKnifeGuard);
      if (saved.buySizingMode === "EQUAL" || saved.buySizingMode === "PYRAMID") {
        merged.buySizingMode = saved.buySizingMode;
      }
      if (Array.isArray(saved.buyLevelWeights)) {
        merged.buyLevelWeights = saved.buyLevelWeights.map((v: any) => Math.max(1, Number(v) || 10));
        const defaultWeights = [10, 15, 20, 25, 30];
        while (merged.buyLevelWeights.length < 5) {
          merged.buyLevelWeights.push(defaultWeights[merged.buyLevelWeights.length] ?? 20);
        }
      }
      if (saved.narrowChannelThresholdPct !== undefined) merged.narrowChannelThresholdPct = Number(saved.narrowChannelThresholdPct);
      if (saved.narrowChannelBuyBufferPct !== undefined) merged.narrowChannelBuyBufferPct = Number(saved.narrowChannelBuyBufferPct);
      if (saved.narrowChannelSellBufferPct !== undefined) merged.narrowChannelSellBufferPct = Number(saved.narrowChannelSellBufferPct);
      if (Array.isArray(saved.enabledBuyLevels)) {
        merged.enabledBuyLevels = saved.enabledBuyLevels.map((v: any) => Boolean(v));
        while (merged.enabledBuyLevels.length < 5) merged.enabledBuyLevels.push(true);
      }
      if (Array.isArray(saved.enabledSellTargets)) {
        merged.enabledSellTargets = saved.enabledSellTargets.map((v: any) => Boolean(v));
        while (merged.enabledSellTargets.length < 5) merged.enabledSellTargets.push(true);
      }
      if (Array.isArray(saved.buyLevelPcts)) {
        merged.buyLevelPcts = saved.buyLevelPcts.map((v: any) => Number(v));
        const defaultBuys = [42, 32, 22, 12, 2];
        while (merged.buyLevelPcts.length < 5) {
          merged.buyLevelPcts.push(defaultBuys[merged.buyLevelPcts.length] ?? 2);
        }
      }
      if (Array.isArray(saved.sellLevelPcts)) {
        merged.sellLevelPcts = saved.sellLevelPcts.map((v: any) => Number(v));
        const defaultSells = [52, 62, 72, 82, 92];
        while (merged.sellLevelPcts.length < 5) {
          merged.sellLevelPcts.push(defaultSells[merged.sellLevelPcts.length] ?? 92);
        }
      }
      if (Array.isArray(saved.sellLevelWeights)) {
        merged.sellLevelWeights = saved.sellLevelWeights.map((v: any) => Math.max(0, Number(v) || 0));
        const defaultSellWeights = [50, 50, 0, 0, 0];
        while (merged.sellLevelWeights.length < 5) {
          merged.sellLevelWeights.push(defaultSellWeights[merged.sellLevelWeights.length] ?? 0);
        }
      }
      if (saved.enableAutoRotation !== undefined) merged.enableAutoRotation = Boolean(saved.enableAutoRotation);
      if (saved.rotationQuoteAsset === "USDT" || saved.rotationQuoteAsset === "FDUSD") {
        merged.rotationQuoteAsset = saved.rotationQuoteAsset;
      }
      if (saved.rotationIntervalMinutes !== undefined) {
        merged.rotationIntervalMinutes = Math.max(15, Number(saved.rotationIntervalMinutes) || 180);
      }
      if (saved.rotationIdleCashMinutes !== undefined) {
        merged.rotationIdleCashMinutes = Math.max(10, Number(saved.rotationIdleCashMinutes) || 120);
      }
      if (saved.rotationScoreMarginPct !== undefined) {
        merged.rotationScoreMarginPct = Math.max(5, Number(saved.rotationScoreMarginPct) || 20);
      }
      if (saved.rotationMinVolumeUsd !== undefined) {
        merged.rotationMinVolumeUsd = Math.max(100_000, Number(saved.rotationMinVolumeUsd) || 3_000_000);
      }
      if (Array.isArray(saved.rotationWhitelist)) {
        merged.rotationWhitelist = saved.rotationWhitelist.map((s: any) => String(s).toUpperCase().trim()).filter(Boolean);
      }
      if (Array.isArray(saved.rotationBlacklist)) {
        merged.rotationBlacklist = saved.rotationBlacklist.map((s: any) => String(s).toUpperCase().trim()).filter(Boolean);
      }
      if (saved.rotationEnableBtcMacroGate !== undefined) {
        merged.rotationEnableBtcMacroGate = Boolean(saved.rotationEnableBtcMacroGate);
      }
      if (saved.portfolioMaxSlots !== undefined) {
        merged.portfolioMaxSlots = Math.max(1, Math.min(5, Number(saved.portfolioMaxSlots) || 3));
      }
      if (["us_top49", "us_top25", "custom"].includes(saved.portfolioUniversePreset)) {
        merged.portfolioUniversePreset = saved.portfolioUniversePreset;
      }
      if (saved.portfolioMaxUniversePairs !== undefined) {
        merged.portfolioMaxUniversePairs = Math.max(5, Math.min(60, Number(saved.portfolioMaxUniversePairs) || 49));
      }
      if (Array.isArray(saved.portfolioCustomWhitelist)) {
        merged.portfolioCustomWhitelist = saved.portfolioCustomWhitelist.map((s: any) => String(s).toUpperCase().trim()).filter(Boolean);
      }
      if (Array.isArray(saved.portfolioBlacklist)) {
        merged.portfolioBlacklist = saved.portfolioBlacklist.map((s: any) => String(s).toUpperCase().trim()).filter(Boolean);
      }
      if (["EMA25", "EMA75", "EMA200", "none"].includes(saved.portfolioTp1TargetMode)) {
        merged.portfolioTp1TargetMode = saved.portfolioTp1TargetMode;
      }
      if (saved.portfolioTp1MinGainPct !== undefined && Number.isFinite(Number(saved.portfolioTp1MinGainPct))) {
        merged.portfolioTp1MinGainPct = Math.min(5.0, Math.max(0.5, Number(saved.portfolioTp1MinGainPct)));
      }
      if (saved.portfolioPartialTpAtEma200 !== undefined) merged.portfolioPartialTpAtEma200 = Boolean(saved.portfolioPartialTpAtEma200);
      if (saved.portfolioLockBreakevenAtEma200 !== undefined) merged.portfolioLockBreakevenAtEma200 = Boolean(saved.portfolioLockBreakevenAtEma200);
      if (saved.portfolioRunnerTrailPct !== undefined && Number.isFinite(Number(saved.portfolioRunnerTrailPct))) {
        merged.portfolioRunnerTrailPct = Math.min(5.0, Math.max(0, Number(saved.portfolioRunnerTrailPct)));
      }
      if (saved.portfolioDisasterSlPct !== undefined) merged.portfolioDisasterSlPct = Math.max(0, Number(saved.portfolioDisasterSlPct));
      if (saved.portfolioStaleExitBars !== undefined && Number.isFinite(Number(saved.portfolioStaleExitBars))) {
        merged.portfolioStaleExitBars = Math.min(96, Math.max(0, Math.round(Number(saved.portfolioStaleExitBars))));
      }
      if (saved.portfolioStaleExitMaxGainPct !== undefined && Number.isFinite(Number(saved.portfolioStaleExitMaxGainPct))) {
        merged.portfolioStaleExitMaxGainPct = Math.min(2.0, Math.max(-2.0, Number(saved.portfolioStaleExitMaxGainPct)));
      }
      if (saved.portfolioMinRoomToEma200Pct !== undefined && Number.isFinite(Number(saved.portfolioMinRoomToEma200Pct))) {
        merged.portfolioMinRoomToEma200Pct = Math.min(10.0, Math.max(0, Number(saved.portfolioMinRoomToEma200Pct)));
      }
      if (["off", "btc1d", "btc4h", "dual", "breadth_35"].includes(saved.portfolioSitOutMode)) {
        merged.portfolioSitOutMode = saved.portfolioSitOutMode;
      }
      if (saved.portfolioRankBy === "RSI15" || saved.portfolioRankBy === "COMPOSITE_QUALITY") {
        merged.portfolioRankBy = saved.portfolioRankBy;
      }
      if (saved.portfolioMaxHoldHours !== undefined) merged.portfolioMaxHoldHours = Math.max(0, Number(saved.portfolioMaxHoldHours) || 0);
      if (saved.portfolioMaxBudgetUsd !== undefined) merged.portfolioMaxBudgetUsd = Math.max(0, Number(saved.portfolioMaxBudgetUsd) || 0);
      if (saved.portfolioCapitalUsagePct !== undefined && Number.isFinite(Number(saved.portfolioCapitalUsagePct))) {
        merged.portfolioCapitalUsagePct = Math.min(100, Math.max(10, Number(saved.portfolioCapitalUsagePct)));
      }
      if (
        saved.portfolioPullbackMode === "EMA8_HOLD" ||
        saved.portfolioPullbackMode === "EMA4_TOUCH" ||
        saved.portfolioPullbackMode === "EMA8_CONFIRMED" ||
        saved.portfolioPullbackMode === "EMA16_CONFIRMED"
      ) {
        merged.portfolioPullbackMode = saved.portfolioPullbackMode;
      }
      if (typeof saved.portfolioQuantCorrelationGuard === "boolean") {
        merged.portfolioQuantCorrelationGuard = saved.portfolioQuantCorrelationGuard;
      }
      if (typeof saved.portfolioQuantRegimeConditional === "boolean") {
        merged.portfolioQuantRegimeConditional = saved.portfolioQuantRegimeConditional;
      }
      if (typeof saved.portfolioQuantSessionGuard === "boolean") {
        merged.portfolioQuantSessionGuard = saved.portfolioQuantSessionGuard;
      }
      if (typeof saved.portfolioQuantVolTargetedRisk === "boolean") {
        merged.portfolioQuantVolTargetedRisk = saved.portfolioQuantVolTargetedRisk;
      }
      if (typeof saved.portfolioQuantEmpiricalEv === "boolean") {
        merged.portfolioQuantEmpiricalEv = saved.portfolioQuantEmpiricalEv;
      }
      if (
        saved.portfolioQuantContextGateMode === "off" ||
        saved.portfolioQuantContextGateMode === "observe" ||
        saved.portfolioQuantContextGateMode === "auto_half_size" ||
        saved.portfolioQuantContextGateMode === "auto_enforce"
      ) {
        merged.portfolioQuantContextGateMode = saved.portfolioQuantContextGateMode;
      }
      if (saved.portfolioMinBuyVolRatio !== undefined && Number.isFinite(Number(saved.portfolioMinBuyVolRatio))) {
        merged.portfolioMinBuyVolRatio = Math.min(5.0, Math.max(0.5, Number(saved.portfolioMinBuyVolRatio)));
      }
      if (saved.portfolioMinRelStrength24h !== undefined && Number.isFinite(Number(saved.portfolioMinRelStrength24h))) {
        merged.portfolioMinRelStrength24h = Math.min(10, Math.max(-10, Number(saved.portfolioMinRelStrength24h)));
      }
      if (saved.portfolioMinRsi1h !== undefined && Number.isFinite(Number(saved.portfolioMinRsi1h))) {
        merged.portfolioMinRsi1h = Math.min(70, Math.max(15, Number(saved.portfolioMinRsi1h)));
      }
      if (typeof saved.portfolioInspectedSymbol === "string") merged.portfolioInspectedSymbol = saved.portfolioInspectedSymbol.toUpperCase().trim();
      if (saved.learningAutoTuneMode === "dry_run_only" || saved.learningAutoTuneMode === "all" || saved.learningAutoTuneMode === "off") {
        merged.learningAutoTuneMode = saved.learningAutoTuneMode;
      }
      if (typeof saved.portfolioSurvivalGuardEnforced === "boolean") {
        merged.portfolioSurvivalGuardEnforced = saved.portfolioSurvivalGuardEnforced;
      }
      return merged;
    }
  } catch {
    // If DB is not yet available, use default baseline
  }
  return { ...defaultStrategyConfig };
}

export const config = loadConfigFromDatabase();
export type Config = typeof defaultStrategyConfig;
