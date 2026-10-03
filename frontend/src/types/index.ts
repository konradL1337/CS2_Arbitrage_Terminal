// Domain types for CS2 Ghost Trading Terminal

export interface MarketItem {
  id: string;
  name: string;
  category: 'case' | 'sticker' | 'skin';
  lowestAsk: number;            // Najniższy sell listing (Steam price)
  highestBid: number;           // Najwyższy buy order (CSFloat price)
  spreadPercent: number;        // ((lowestAsk - highestBid) / lowestAsk) * 100
  volume24h: number;
  buyDepth: number;             // Ilość zleceń w arkuszu
  liquidityScore: number;       // 0 - 100
  maxBuyPrice: number;          // Sugerowany limit wejścia
  expectedExit: number;         // Docelowa cena sprzedaży brutto
  estimatedNetExit: number;     // expectedExit / 1.15
  estimatedEdgePercent: number; // ((estimatedNetExit - maxBuyPrice) / maxBuyPrice) * 100
  delta24h: number;             // Zmiana ceny 24h (np. +3.45 lub -1.20)
  delta7d: number;              // Zmiana ceny 7d (np. +5.12)
  sparkline: number[];          // Tablica 15-30 cen float dla wykresu
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

// Backend API response type
export interface BackendMarketItem {
  item_name: string;
  steam_price: number | null;
  csfloat_price: number | null;
  steam_volume: number | null;
  sparkline: number[];
  delta24h: number;
  delta7d: number;
  timestamp: string;
}

// Toast notification types
export interface Toast {
  id: string;
  type: 'success' | 'error' | 'info';
  message: string;
}
