import { MARKET_CONFIG } from '../config/market';
import type { PortfolioState } from '../types';

// Kwota otrzymana po prowizji Valve: Cena / 1.15
export const calcNetPrice = (grossPrice: number): number => {
  return grossPrice / MARKET_CONFIG.steam.feeMultiplier;
};

// Dynamiczny Target Edge zależny od płynności (volume24h)
export const calcTargetEdge = (volume24h: number): number => {
  if (volume24h >= 10000) return 0.09; // hiperpłynne skrzynki
  if (volume24h >= 1000) return 0.13;
  if (volume24h >= 100) return 0.17;
  return 0.22; // bufor ryzyka dla rzadkich przedmiotów
};

// Max Buy (Bid Limit) = (steamPrice / 1.15) / (1 + targetEdge)
export const calcMaxBuyPrice = (steamPrice: number, volume24h: number): number => {
  const netExit = calcNetPrice(steamPrice);
  const targetEdge = calcTargetEdge(volume24h);
  return Number((netExit / (1 + targetEdge)).toFixed(2));
};

// Realny Edge % po zaokrągleniu Max Buy do 2 miejsc
export const calcRealEdgePercent = (maxBuyPrice: number, steamPrice: number): number => {
  if (maxBuyPrice <= 0) return 0;
  const netExit = calcNetPrice(steamPrice);
  return Number((((netExit - maxBuyPrice) / maxBuyPrice) * 100).toFixed(1));
};

// Syzyf Dip Score: 0% = cena na absolutnym dołku 24h, 100% = na szczycie
export const calcDipScore = (steamPrice: number, sparkline: number[]): number => {
  if (!sparkline || sparkline.length === 0) return 50;
  const min24h = Math.min(...sparkline);
  const max24h = Math.max(...sparkline);
  const range = max24h - min24h || 1;
  return Number((((steamPrice - min24h) / range) * 100).toFixed(1));
};

// Ochrona przed penny-stocks: stała opłata Valve zabija zysk na mikro-transakcjach
export const isPennyStockPrice = (steamPrice: number): boolean => {
  return steamPrice < MARKET_CONFIG.steam.pennyStockThresholdPLN;
};

// Edge % = ((Net Exit - Max Buy) / Max Buy) * 100
export const calcEdgePercent = (maxBuy: number, expectedExitGross: number): number => {
  if (maxBuy <= 0) return 0;
  const netExit = calcNetPrice(expectedExitGross);
  return ((netExit - maxBuy) / maxBuy) * 100;
};

// Niezrealizowany P&L = ((Aktualny Ask / 1.15) - Cena Zakupu) * Ilość
export const calcUnrealizedPnl = (currentAsk: number, entryPrice: number, quantity: number): number => {
  return (calcNetPrice(currentAsk) - entryPrice) * quantity;
};

// Niezrealizowany ROI % = (Unrealized P&L / Invested) * 100
export const calcUnrealizedRoi = (unrealizedPnl: number, invested: number): number => {
  if (invested <= 0) return 0;
  return (unrealizedPnl / invested) * 100;
};

// Zrealizowany P&L = ((Cena Sprzedaży / 1.15) - Cena Zakupu) * Ilość
export const calcRealizedPnl = (exitGross: number, entryPrice: number, quantity: number): number => {
  return (calcNetPrice(exitGross) - entryPrice) * quantity;
};

// Zrealizowany ROI % = (Realized P&L / Invested) * 100
export const calcRealizedRoi = (realizedPnl: number, invested: number): number => {
  if (invested <= 0) return 0;
  return (realizedPnl / invested) * 100;
};

// Agregacja kapitału i metryk portfela
export const calcPortfolioMetrics = (state: PortfolioState) => {
  const committedPLN = state.orders
    .filter(o => o.status === 'WAITING')
    .reduce((sum, o) => sum + o.totalCommittedPLN, 0);

  const investedPLN = state.positions
    .reduce((sum, p) => sum + p.investedPLN, 0);

  const availablePLN = Math.max(0, state.capitalPLN - committedPLN - investedPLN);

  const unrealizedPnlPLN = state.positions
    .reduce((sum, p) => sum + p.unrealizedPnlPLN, 0);

  const realizedPnlPLN = state.history
    .reduce((sum, t) => sum + t.realizedPnlPLN, 0);

  const currentNetPortfolioValuePLN = availablePLN + committedPLN + 
    state.positions.reduce((sum, p) => sum + p.currentNetValuePLN, 0);

  return { 
    committedPLN, 
    investedPLN, 
    availablePLN, 
    unrealizedPnlPLN, 
    realizedPnlPLN, 
    currentNetPortfolioValuePLN 
  };
};
