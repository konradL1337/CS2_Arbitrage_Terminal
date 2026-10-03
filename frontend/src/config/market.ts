// Market configuration constants
export const MARKET_CONFIG = {
  steam: {
    feeMultiplier: 1.15, // Netto = Brutto / 1.15 (13.04% efektywnego podatku)
    minFeePLN: 0.04,
  },
  portfolio: {
    initialCapitalPLN: 500.0,
    storageKey: 'cs2_ghost_portfolio_v1',
    schemaVersion: 1,
  },
  api: {
    baseUrl: 'http://localhost:8000',
    endpoints: {
      marketData: '/api/market-data',
    },
  },
} as const;
