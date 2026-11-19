/**
 * Alpaca US Equities Adapter (Paper & Live Trading)
 * Implements IExchangeClient for US Stocks and ETFs via Alpaca API v2.
 */

import {
  IExchangeClient,
  ExchangeSymbolInfo,
  ExchangeAccountBalances,
  ExchangeOpenOrder,
  ExchangeOrderResult,
  ExchangeTopOfBook,
  PlaceOrderParams,
  ExchangeTrade,
} from "../types.js";
import { normalizeStockSymbol, parseAlpacaSnapshotToBook } from "./feed.js";

export interface AlpacaClientOptions {
  apiKey?: string;
  apiSecret?: string;
  baseUrl?: string;
  dataUrl?: string;
}

export interface AlpacaMarketClock {
  timestamp: string;
  isOpen: boolean;
  nextOpen: string;
  nextClose: string;
}

function mapAlpacaOrderStatus(rawStatus?: string): ExchangeOrderResult["status"] {
  const s = (rawStatus || "").toLowerCase();
  if (s === "filled") return "FILLED";
  if (s === "partially_filled") return "PARTIALLY_FILLED";
  if (s === "new" || s === "accepted" || s === "pending_new") return "NEW";
  if (s === "canceled" || s === "pending_cancel") return "CANCELED";
  if (s === "expired") return "EXPIRED";
  if (s === "rejected") return "REJECTED";
  return "NEW";
}

function formatAlpacaQty(qty: number): string {
  const rounded = Math.round(qty * 10_000) / 10_000;
  return String(rounded > 0 ? rounded : qty);
}

function formatAlpacaPrice(price: number): string {
  const decimals = price >= 1 ? 2 : 4;
  const factor = 10 ** decimals;
  return (Math.round(price * factor) / factor).toFixed(decimals);
}

export class AlpacaClient implements IExchangeClient {
  public readonly exchangeName = "alpaca";
  public readonly isDex = false;
  public readonly hasGasFees = false;
  public readonly nativeGasAsset = "USD";
  public readonly supportsOrderExpiry = false;

  private apiKey: string;
  private apiSecret: string;
  private baseUrl: string;
  private dataUrl: string;
  private cachedClock?: { clock: AlpacaMarketClock; fetchedAt: number };

  constructor(opts: AlpacaClientOptions = {}) {
    this.apiKey = (opts.apiKey || process.env.ALPACA_API_KEY || "").trim();
    this.apiSecret = (opts.apiSecret || process.env.ALPACA_API_SECRET || "").trim();
    this.baseUrl = (opts.baseUrl || process.env.ALPACA_BASE_URL || "https://paper-api.alpaca.markets").replace(/\/+$/, "");
    this.dataUrl = (opts.dataUrl || process.env.ALPACA_DATA_URL || "https://data.alpaca.markets").replace(/\/+$/, "");
  }

  public hasCredentials(): boolean {
    return Boolean(this.apiKey && this.apiSecret);
  }

  public updateCredentials(apiKey: string, apiSecret: string, baseUrl?: string, dataUrl?: string): void {
    this.apiKey = apiKey.trim();
    this.apiSecret = apiSecret.trim();
    if (baseUrl) this.baseUrl = baseUrl.replace(/\/+$/, "");
    if (dataUrl) this.dataUrl = dataUrl.replace(/\/+$/, "");
    this.cachedClock = undefined;
  }

  public getCredentials(): { apiKey: string; apiSecret: string; baseUrl: string; dataUrl: string } {
    return {
      apiKey: this.apiKey,
      apiSecret: this.apiSecret,
      baseUrl: this.baseUrl,
      dataUrl: this.dataUrl,
    };
  }

  public async syncTime(): Promise<number> {
    return Date.now();
  }

  public async getMarketClock(forceRefresh = false): Promise<AlpacaMarketClock | undefined> {
    if (!this.hasCredentials()) return undefined;
    if (!forceRefresh && this.cachedClock && Date.now() - this.cachedClock.fetchedAt < 15_000) {
      return this.cachedClock.clock;
    }
    try {
      const res = await fetch(`${this.baseUrl}/v2/clock`, { headers: this.getHeaders() });
      if (!res.ok) return undefined;
      const data = (await res.json()) as {
        timestamp: string;
        is_open: boolean;
        next_open: string;
        next_close: string;
      };
      const clock: AlpacaMarketClock = {
        timestamp: data.timestamp,
        isOpen: Boolean(data.is_open),
        nextOpen: data.next_open,
        nextClose: data.next_close,
      };
      this.cachedClock = { clock, fetchedAt: Date.now() };
      return clock;
    } catch {
      return undefined;
    }
  }

  private getHeaders(): Record<string, string> {
    return {
      "APCA-API-KEY-ID": this.apiKey,
      "APCA-API-SECRET-KEY": this.apiSecret,
      "Content-Type": "application/json",
    };
  }

  public async getExchangeInfo(symbol: string): Promise<ExchangeSymbolInfo> {
    const cleanSym = normalizeStockSymbol(symbol);
    try {
      const res = await fetch(`${this.baseUrl}/v2/assets/${cleanSym}`, {
        headers: this.getHeaders(),
      });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const asset = await res.json();
      const isTradable = asset.status === "active" && asset.tradable !== false;
      const fractionable = Boolean(asset.fractionable);
      return {
        symbol: cleanSym,
        status: isTradable ? "TRADING" : "HALTED",
        baseAsset: cleanSym,
        quoteAsset: "USD",
        baseAssetPrecision: 4,
        quotePrecision: 2,
        minQty: fractionable ? 0.0001 : 1,
        maxQty: 1000000,
        stepSize: fractionable ? 0.0001 : 1,
        qtyPrecision: fractionable ? 4 : 0,
        minPrice: 0.01,
        maxPrice: 1000000,
        tickSize: 0.01,
        pricePrecision: 2,
        minNotional: 1.0,
      };
    } catch {
      return {
        symbol: cleanSym,
        status: "TRADING",
        baseAsset: cleanSym,
        quoteAsset: "USD",
        baseAssetPrecision: 4,
        quotePrecision: 2,
        minQty: 0.0001,
        maxQty: 1000000,
        stepSize: 0.0001,
        qtyPrecision: 4,
        minPrice: 0.01,
        maxPrice: 1000000,
        tickSize: 0.01,
        pricePrecision: 2,
        minNotional: 1.0,
      };
    }
  }

  public async getAccountBalances(symbol: string): Promise<ExchangeAccountBalances> {
    const cleanSym = normalizeStockSymbol(symbol);

    // 1. Fetch cash & equity from Account
    const accRes = await fetch(`${this.baseUrl}/v2/account`, { headers: this.getHeaders() });
    if (!accRes.ok) throw new Error(`Failed to fetch Alpaca account: HTTP ${accRes.status}`);
    const acc = await accRes.json();
    const quoteCash = parseFloat(acc.cash) || 0;
    const quoteEquity = parseFloat(acc.equity) || 0;

    // 2. Fetch position for base asset
    let baseQty = 0;
    const allBalances: Record<string, { free: number; locked: number }> = {
      USD: { free: quoteCash, locked: Math.max(0, quoteEquity - quoteCash) },
    };

    try {
      const posRes = await fetch(`${this.baseUrl}/v2/positions/${cleanSym}`, { headers: this.getHeaders() });
      if (posRes.ok) {
        const pos = await posRes.json();
        baseQty = parseFloat(pos.qty) || 0;
        const availQty = pos.qty_available !== undefined ? parseFloat(pos.qty_available) || 0 : baseQty;
        allBalances[cleanSym] = { free: availQty, locked: Math.max(0, baseQty - availQty) };
      }
    } catch {}

    const baseFree = allBalances[cleanSym]?.free ?? baseQty;
    const baseLocked = allBalances[cleanSym]?.locked ?? 0;

    return {
      baseAsset: cleanSym,
      quoteAsset: "USD",
      baseFree,
      baseLocked,
      baseTotal: baseQty,
      quoteFree: quoteCash,
      quoteLocked: Math.max(0, quoteEquity - quoteCash),
      quoteTotal: quoteEquity,
      allBalances,
    };
  }

  public async getAllPositions(): Promise<Array<{
    symbol: string;
    qty: number;
    qtyAvailable: number;
    avgEntryPrice: number;
    currentPrice: number;
    marketValue: number;
    unrealizedPnl: number;
    unrealizedPnlPct: number;
  }>> {
    const res = await fetch(`${this.baseUrl}/v2/positions`, { headers: this.getHeaders() });
    if (!res.ok) throw new Error(`Failed to fetch Alpaca positions: HTTP ${res.status}`);
    const list = (await res.json()) as any[];
    if (!Array.isArray(list)) return [];
    return list
      .map((p) => {
        const qty = parseFloat(p.qty) || 0;
        const qtyAvailable = p.qty_available !== undefined ? parseFloat(p.qty_available) || 0 : qty;
        return {
          symbol: normalizeStockSymbol(String(p.symbol || "")),
          qty,
          qtyAvailable,
          avgEntryPrice: parseFloat(p.avg_entry_price) || 0,
          currentPrice: parseFloat(p.current_price) || 0,
          marketValue: parseFloat(p.market_value) || 0,
          unrealizedPnl: parseFloat(p.unrealized_pl) || 0,
          unrealizedPnlPct: (parseFloat(p.unrealized_plpc) || 0) * 100,
        };
      })
      .filter((p) => p.symbol && p.qty > 0);
  }

  public async getOpenOrders(symbol?: string): Promise<ExchangeOpenOrder[]> {
    let url = `${this.baseUrl}/v2/orders?status=open&limit=100`;
    if (symbol) {
      const cleanSym = normalizeStockSymbol(symbol);
      url += `&symbols=${cleanSym}`;
    }

    const res = await fetch(url, { headers: this.getHeaders() });
    if (!res.ok) throw new Error(`Failed to fetch open orders: HTTP ${res.status}`);
    const orders = await res.json();

    return orders.map((o: any) => ({
      symbol: o.symbol,
      orderId: o.id,
      clientOrderId: o.client_order_id,
      price: parseFloat(o.limit_price) || 0,
      origQty: parseFloat(o.qty) || 0,
      executedQty: parseFloat(o.filled_qty) || 0,
      cummulativeQuoteQty: parseFloat(o.filled_avg_price || 0) * parseFloat(o.filled_qty || 0),
      status: mapAlpacaOrderStatus(o.status),
      timeInForce: o.time_in_force?.toUpperCase() || "DAY",
      type: o.type?.toUpperCase() || "LIMIT",
      side: o.side?.toUpperCase() === "BUY" ? "BUY" : "SELL",
      time: Math.floor(new Date(o.submitted_at).getTime() / 1000),
      updateTime: Math.floor(new Date(o.updated_at || o.submitted_at).getTime() / 1000),
      isWorking: true,
    }));
  }

  public async getOrder(symbol: string, orderId: string | number): Promise<ExchangeOpenOrder | ExchangeOrderResult> {
    const res = await fetch(`${this.baseUrl}/v2/orders/${orderId}`, { headers: this.getHeaders() });
    if (!res.ok) throw new Error(`Failed to fetch order ${orderId}: HTTP ${res.status}`);
    const o = await res.json();
    const status = mapAlpacaOrderStatus(o.status);
    const submittedSec = Math.floor(new Date(o.submitted_at).getTime() / 1000);
    const updatedSec = Math.floor(new Date(o.updated_at || o.submitted_at).getTime() / 1000);

    return {
      symbol: o.symbol,
      orderId: o.id,
      clientOrderId: o.client_order_id,
      price: parseFloat(o.limit_price || o.filled_avg_price) || 0,
      origQty: parseFloat(o.qty) || 0,
      executedQty: parseFloat(o.filled_qty) || 0,
      cummulativeQuoteQty: parseFloat(o.filled_avg_price || 0) * parseFloat(o.filled_qty || 0),
      status,
      timeInForce: o.time_in_force?.toUpperCase() || "DAY",
      type: o.type?.toUpperCase() || "LIMIT",
      side: o.side?.toUpperCase() === "BUY" ? "BUY" : "SELL",
      time: submittedSec,
      updateTime: updatedSec,
      isWorking: status === "NEW" || status === "PARTIALLY_FILLED",
    };
  }

  public async getTopOfBook(symbol: string): Promise<ExchangeTopOfBook | undefined> {
    const cleanSym = normalizeStockSymbol(symbol);
    try {
      const res = await fetch(`${this.dataUrl}/v2/stocks/${cleanSym}/snapshot?feed=iex`, {
        headers: this.getHeaders(),
      });
      if (!res.ok) return undefined;
      const snap = await res.json();
      return parseAlpacaSnapshotToBook(snap);
    } catch {
      return undefined;
    }
  }

  public async placeOrder(params: PlaceOrderParams): Promise<ExchangeOrderResult> {
    const cleanSym = normalizeStockSymbol(params.symbol);
    const side = params.side.toLowerCase();
    const type = params.type.toLowerCase().includes("limit") ? "limit" : "market";

    // Alpaca supports fractional shares for BOTH market & limit only with time_in_force: "day"
    const body: Record<string, any> = {
      symbol: cleanSym,
      qty: formatAlpacaQty(params.qty),
      side,
      type,
      time_in_force: "day",
    };

    if (type === "limit" && params.price) {
      body.limit_price = formatAlpacaPrice(params.price);
    }
    if (params.clientOrderId) {
      body.client_order_id = params.clientOrderId;
    }

    const res = await fetch(`${this.baseUrl}/v2/orders`, {
      method: "POST",
      headers: this.getHeaders(),
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const errTxt = await res.text();
      throw new Error(`Alpaca placeOrder failed: HTTP ${res.status} - ${errTxt}`);
    }

    let o = await res.json();
    if (type === "market" && o?.id) {
      const delays = [250, 350, 500, 700, 1000, 1200];
      for (const waitMs of delays) {
        const curStatus = mapAlpacaOrderStatus(o.status);
        const curFilledQty = parseFloat(o.filled_qty || 0);
        const curFilledAvg = parseFloat(o.filled_avg_price || 0);
        if (curStatus === "FILLED" && curFilledQty > 0 && curFilledAvg > 0) break;
        if (curStatus === "CANCELED" || curStatus === "REJECTED" || curStatus === "EXPIRED") break;
        await new Promise((r) => setTimeout(r, waitMs));
        try {
          const pollRes = await fetch(`${this.baseUrl}/v2/orders/${o.id}`, { headers: this.getHeaders() });
          if (pollRes.ok) {
            o = await pollRes.json();
          }
        } catch {
          // ignore transient poll failure and retry
        }
      }
      const finalStatus = mapAlpacaOrderStatus(o.status);
      const finalFilledQty = parseFloat(o.filled_qty || 0);
      if ((finalStatus === "CANCELED" || finalStatus === "REJECTED" || finalStatus === "EXPIRED") && finalFilledQty <= 0) {
        throw new Error(`Alpaca market order ${o.id} ${o.status} without fill`);
      }
    }
    return {
      symbol: o.symbol,
      orderId: o.id,
      clientOrderId: o.client_order_id,
      transactTime: Math.floor(new Date(o.submitted_at).getTime() / 1000),
      price: parseFloat(o.limit_price || o.filled_avg_price) || params.price || 0,
      origQty: parseFloat(o.qty) || params.qty,
      executedQty: parseFloat(o.filled_qty) || 0,
      cummulativeQuoteQty: parseFloat(o.filled_avg_price || 0) * parseFloat(o.filled_qty || 0),
      status: mapAlpacaOrderStatus(o.status),
      timeInForce: o.time_in_force?.toUpperCase() || "DAY",
      type: o.type?.toUpperCase() || "LIMIT",
      side: params.side,
    };
  }

  public async cancelOrder(_symbol: string, orderId: string | number): Promise<any> {
    const res = await fetch(`${this.baseUrl}/v2/orders/${orderId}`, {
      method: "DELETE",
      headers: this.getHeaders(),
    });
    if (!res.ok && res.status !== 404) {
      throw new Error(`Alpaca cancelOrder failed: HTTP ${res.status}`);
    }
    return { success: true };
  }

  public async cancelAllOpenOrders(symbol?: string): Promise<any> {
    if (symbol) {
      const open = await this.getOpenOrders(symbol);
      if (open.length === 0) return { success: true, canceledCount: 0 };
      const results = await Promise.allSettled(
        open.map((o) => this.cancelOrder(symbol, o.orderId)),
      );
      const canceledCount = results.filter((r) => r.status === "fulfilled").length;
      return { success: canceledCount === open.length, canceledCount };
    }
    const res = await fetch(`${this.baseUrl}/v2/orders`, {
      method: "DELETE",
      headers: this.getHeaders(),
    });
    return { success: res.ok };
  }

  public async getMyTrades(symbol?: string, limit = 50): Promise<ExchangeTrade[]> {
    const cleanFilter = symbol ? normalizeStockSymbol(symbol) : undefined;
    const url = `${this.baseUrl}/v2/account/activities/FILL?direction=desc&page_size=${limit}`;
    const res = await fetch(url, { headers: this.getHeaders() });
    if (!res.ok) return [];
    const activities = await res.json();

    return activities
      .filter((a: any) => !cleanFilter || a.symbol === cleanFilter)
      .map((a: any, idx: number) => ({
        symbol: a.symbol,
        id: a.id || idx,
        orderId: a.order_id,
        price: parseFloat(a.price) || 0,
        qty: parseFloat(a.qty) || 0,
        quoteQty: (parseFloat(a.price) || 0) * (parseFloat(a.qty) || 0),
        commission: 0,
        commissionAsset: "USD",
        time: Math.floor(new Date(a.transaction_time).getTime() / 1000),
        isBuyer: a.side === "buy",
      }));
  }
}
