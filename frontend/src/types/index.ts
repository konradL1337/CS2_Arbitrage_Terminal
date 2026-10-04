// Domain types for CS2 Ghost Trading Terminal

export interface MarketItem {
  id: string;
  name: string;
  category: 'case' | 'sticker' | 'skin';
  lowestAsk: number;            // Najniższy sell listing (Steam price)
  highestBid: number;           // Najwyższy buy order (lub fallback = maxBuyPrice)
  spreadPercent: number;        // ((lowestAsk - maxBuyPrice) / lowestAsk) * 100
  volume24h: number;
  liquidityScore: number;       // 10 - 100 (skala logarytmiczna z wolumenu)
  maxBuyPrice: number;          // Sugerowany limit wejścia (Bid Limit)
  expectedExit: number;         // Docelowa cena sprzedaży brutto
  estimatedNetExit: number;     // expectedExit / 1.15
  estimatedEdgePercent: number; // Realny edge netto po podatku Valve (9/13/17/22%)
  delta24h: number;             // Zmiana ceny w oknie 24-30 odczytów (np. -2.45)
  min24h: number;               // Dołek z okna sparkline
  max24h: number;               // Szczyt z okna sparkline
  isPennyStock: boolean;        // lowestAsk < 1.00 PLN (minimalna prowizja Valve)
  dipScore: number;             // Syzyf Dip Score: 0% = dołek 24h, 100% = szczyt
  sparkline: number[];          // Tablica do 30 cen float (chronologicznie)
  updatedAt: string;
}

export interface GhostOrder {
  id: string;
  itemId: string;
  itemName: string;
  targetPrice: number;
  quantity: number;
  totalCommittedPLN: number;    // targetPrice * quantity
  status: 'WAITING' | 'FILLED' | 'CANCELLED';
  createdAt: string;
}

export interface GhostPosition {
  id: string;
  itemId: string;
  itemName: string;
  entryPrice: number;
  quantity: number;
  currentAskPrice: number;
  investedPLN: number;          // entryPrice * quantity
  currentNetValuePLN: number;   // (currentAskPrice / 1.15) * quantity
  unrealizedPnlPLN: number;     // currentNetValuePLN - investedPLN
  unrealizedRoiPercent: number; // (unrealizedPnlPLN / investedPLN) * 100
  openedAt: string;
}

export interface GhostTrade {
  id: string;
  itemId: string;
  itemName: string;
  entryPrice: number;
  exitPrice: number;
  quantity: number;
  realizedPnlPLN: number;       // ((exitPrice / 1.15) - entryPrice) * quantity
  realizedRoiPercent: number;   // (realizedPnlPLN / (entryPrice * quantity)) * 100
  closedAt: string;
}

export interface PortfolioState {
  version: number;
  capitalPLN: number;
  orders: GhostOrder[];
  positions: GhostPosition[];
  history: GhostTrade[];
}

// Backend API response type (/api/market-data)
export interface ApiMarketItem {
  id: string;
  name: string;
  category: 'case' | 'sticker' | 'skin';
  lowestAsk: number;
  highestBid: number;
  spreadPercent: number;
  volume24h: number;
  liquidityScore: number;
  maxBuyPrice: number;
  expectedExit: number;
  estimatedNetExit: number;
  estimatedEdgePercent: number;
  delta24h: number;
  min24h: number;
  max24h: number;
  isPennyStock: boolean;
  sparkline: number[];
  updatedAt: string;
}

// Toast notification types
export interface Toast {
  id: string;
  type: 'success' | 'error' | 'info';
  message: string;
}
