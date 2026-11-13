/**
 * @license
 * Copyright DreamDEX S.A.
 *
 * Use of this source code is governed by an MIT-style license that can be
 * found in the LICENSE file at https://github.com/somnia-chain/dreamdex-bot-kit/blob/main/LICENSE
 */

import { normalizeStrategyType, type IStrategy, type StrategyType, type StrategyContext } from "./types.js";
import { DynamicGrid } from "./index.js";
import { SwingTraderStrategy } from "./swing/index.js";
import { MaFanTradingStrategy } from "./ma-fan/index.js";

/**
 * Strategy Factory: Instantiates and initializes the configured strategy.
 * Enables seamless switching between Dynamic Grid, Swing Trade, and MA-Ribbon strategies.
 */
export function createStrategy(
  type: StrategyType = "dynamic_grid",
  context: StrategyContext,
): IStrategy {
  switch (normalizeStrategyType(type)) {
    case "swing_trade":
      return new SwingTraderStrategy(context);
    case "ma_ribbon":
      return new MaFanTradingStrategy(context);
    case "dynamic_grid":
    default:
      return new DynamicGrid(
        context.exchange,
        context.symbolInfo,
        context.config,
        context.atrSource,
        context.log,
        context.macroDowEngine,
        context.localTrendEngine,
        context.htfDowEngine,
      );
  }
}
