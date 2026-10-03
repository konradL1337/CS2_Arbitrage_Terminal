import axios from 'axios';
import { MARKET_CONFIG } from '../config/market';
import type { MarketItem, BackendMarketItem } from '../types';
import { calcNetPrice, calcEdgePercent } from '../utils/finance';

// Interface for market data provider (future extensibility)
export interface IMarketDataProvider {
  getMarketItems(): Promise<MarketItem[]>;
}

// Determine item category based on name
const categorizeItem = (name: string): 'case' | 'sticker' | 'skin' => {
  const lowerName = name.toLowerCase();
  if (lowerName.includes('case')) return 'case';
  if (lowerName.includes('sticker') || lowerName.includes('capsule')) return 'sticker';
  return 'skin';
};

// Calculate liquidity score based on volume and spread
const calculateLiquidityScore = (volume: number, spreadPercent: number): number => {
  // High volume + low spread = high liquidity
  let score = 0;
  
  // Volume component (0-60 points)
  if (volume >= 100) score += 60;
  else if (volume >= 50) score += 45;
  else if (volume >= 20) score += 30;
  else if (volume >= 10) score += 15;
  else if (volume >= 5) score += 5;
  
  // Spread component (0-40 points, lower spread = better)
  if (spreadPercent <= 5) score += 40;
  else if (spreadPercent <= 10) score += 30;
  else if (spreadPercent <= 20) score += 20;
  else if (spreadPercent <= 30) score += 10;
  
  return Math.min(100, score);
};

// Real API Market Data Provider
export class RealMarketDataProvider implements IMarketDataProvider {
  private baseUrl: string;

  constructor() {
    this.baseUrl = MARKET_CONFIG.api.baseUrl;
  }

  async getMarketItems(): Promise<MarketItem[]> {
    try {
      const response = await axios.get<BackendMarketItem[]>(
        `${this.baseUrl}${MARKET_CONFIG.api.endpoints.marketData}`
      );

      // Transform backend data to domain model
      return response.data
        .filter(item => item.steam_price !== null && item.csfloat_price !== null)
        .map((item, index) => {
          const steamPrice = item.steam_price ?? 0;
          const csfloatPrice = item.csfloat_price ?? 0;
          const volume = item.steam_volume ?? 0;

          // Map backend fields to domain model
          const lowestAsk = steamPrice;
          const highestBid = csfloatPrice;
          
          // Calculate spread percentage
          const spreadPercent = lowestAsk > 0 
            ? ((lowestAsk - highestBid) / lowestAsk) * 100 
            : 0;

          // Calculate max buy price (with slippage and fees)
          const maxBuyPrice = (steamPrice * 0.88) / MARKET_CONFIG.steam.feeMultiplier;
          
          // Expected exit is current steam price
          const expectedExit = steamPrice;
          const estimatedNetExit = calcNetPrice(expectedExit);
          
          // Calculate edge percentage
          const estimatedEdgePercent = calcEdgePercent(maxBuyPrice, expectedExit);

          // Calculate liquidity score
          const liquidityScore = calculateLiquidityScore(volume, spreadPercent);

          // Estimate buy depth based on volume
          const buyDepth = Math.floor(volume / 10);

          return {
            id: `item-${index}-${Date.now()}`,
            name: item.item_name,
            category: categorizeItem(item.item_name),
            lowestAsk,
            highestBid,
            spreadPercent,
            volume24h: volume,
            buyDepth,
            liquidityScore,
            maxBuyPrice,
            expectedExit,
            estimatedNetExit,
            estimatedEdgePercent,
            delta24h: item.delta24h ?? 0,
            delta7d: item.delta7d ?? 0,
            sparkline: item.sparkline ?? [],
            updatedAt: item.timestamp ?? new Date().toISOString(),
          };
        });
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
