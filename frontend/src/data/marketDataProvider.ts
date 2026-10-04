import axios from 'axios';
import { MARKET_CONFIG } from '../config/market';
import type { MarketItem, ApiMarketItem } from '../types';
import { calcDipScore, isPennyStockPrice } from '../utils/finance';

// Interface for market data provider (future extensibility)
export interface IMarketDataProvider {
  getMarketItems(): Promise<MarketItem[]>;
}

// Real API Market Data Provider
// Backend (FastAPI + SQLite) liczy wszystkie metryki Quant Engine:
// dynamiczny Target Edge, Max Buy, realEdge, spread, liquidity, delta24h, min/max24h.
// Frontend dokłada jedynie pochodne wskaźniki prezentacyjne (Dip Score, sanity-check penny).
export class RealMarketDataProvider implements IMarketDataProvider {
  private baseUrl: string;

  constructor() {
    this.baseUrl = MARKET_CONFIG.api.baseUrl;
  }

  async getMarketItems(): Promise<MarketItem[]> {
    try {
      const response = await axios.get<ApiMarketItem[]>(
        `${this.baseUrl}${MARKET_CONFIG.api.endpoints.marketData}`
      );

      return response.data
        .filter(item => item.lowestAsk > 0)
        .map((item) => ({
          id: item.id,
          name: item.name,
          category: item.category,
          lowestAsk: item.lowestAsk,
          highestBid: item.highestBid,
          spreadPercent: item.spreadPercent,
          volume24h: item.volume24h,
          liquidityScore: item.liquidityScore,
          maxBuyPrice: item.maxBuyPrice,
          expectedExit: item.expectedExit,
          estimatedNetExit: item.estimatedNetExit,
          estimatedEdgePercent: item.estimatedEdgePercent,
          delta24h: item.delta24h,
          min24h: item.min24h,
          max24h: item.max24h,
          isPennyStock: item.isPennyStock || isPennyStockPrice(item.lowestAsk),
          dipScore: calcDipScore(item.lowestAsk, item.sparkline ?? []),
          sparkline: item.sparkline ?? [],
          updatedAt: item.updatedAt || new Date().toISOString(),
        }));
    } catch (error) {
      console.error('Failed to fetch market data:', error);
      throw new Error(
        error instanceof Error
          ? `API Error: ${error.message}`
          : 'Failed to fetch market data from backend'
      );
    }
  }
}

// Export singleton instance
export const marketDataProvider = new RealMarketDataProvider();
