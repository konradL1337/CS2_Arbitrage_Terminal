import { useState, useEffect, useCallback } from 'react';
import { MARKET_CONFIG } from '../config/market';
import type { PortfolioState, GhostOrder, GhostPosition, GhostTrade, MarketItem } from '../types';
import { calcPortfolioMetrics, calcNetPrice, calcUnrealizedPnl, calcUnrealizedRoi, calcRealizedPnl, calcRealizedRoi } from '../utils/finance';

// Initialize default portfolio state
const getInitialState = (): PortfolioState => ({
  version: MARKET_CONFIG.portfolio.schemaVersion,
  capitalPLN: MARKET_CONFIG.portfolio.initialCapitalPLN,
  orders: [],
  positions: [],
  history: [],
});

// Load and validate state from localStorage
const loadState = (): PortfolioState => {
  try {
    const stored = localStorage.getItem(MARKET_CONFIG.portfolio.storageKey);
    if (!stored) {
      return getInitialState();
    }

    const parsed = JSON.parse(stored) as PortfolioState;
    
    // Validate schema version
    if (parsed.version !== MARKET_CONFIG.portfolio.schemaVersion) {
      console.warn(`Portfolio schema mismatch. Expected v${MARKET_CONFIG.portfolio.schemaVersion}, got v${parsed.version}. Resetting.`);
      return getInitialState();
    }

    return parsed;
  } catch (error) {
    console.warn('Failed to load portfolio from localStorage:', error);
    return getInitialState();
  }
};

// Save state to localStorage
const saveState = (state: PortfolioState): void => {
  try {
    localStorage.setItem(MARKET_CONFIG.portfolio.storageKey, JSON.stringify(state));
  } catch (error) {
    console.error('Failed to save portfolio to localStorage:', error);
  }
};

export const usePortfolio = (marketItems: MarketItem[]) => {
  const [state, setState] = useState<PortfolioState>(loadState);

  // Persist state changes
  useEffect(() => {
    saveState(state);
  }, [state]);

  // Update positions with current market prices
  useEffect(() => {
    if (marketItems.length === 0) return;

    setState(prev => ({
      ...prev,
      positions: prev.positions.map(position => {
        const marketItem = marketItems.find(item => item.name === position.itemName);
        if (!marketItem) return position;

        const currentAskPrice = marketItem.lowestAsk;
        const currentNetValuePLN = calcNetPrice(currentAskPrice) * position.quantity;
        const unrealizedPnlPLN = calcUnrealizedPnl(currentAskPrice, position.entryPrice, position.quantity);
        const unrealizedRoiPercent = calcUnrealizedRoi(unrealizedPnlPLN, position.investedPLN);

        return {
          ...position,
          currentAskPrice,
          currentNetValuePLN,
          unrealizedPnlPLN,
          unrealizedRoiPercent,
        };
      }),
    }));
  }, [marketItems]);

  // Create new order
  const createOrder = useCallback((itemId: string, itemName: string, targetPrice: number, quantity: number): { success: boolean; error?: string } => {
    // Validation
    if (targetPrice <= 0) {
      return { success: false, error: 'Cena musi być większa od 0' };
    }
    if (quantity <= 0 || !Number.isInteger(quantity)) {
      return { success: false, error: 'Ilość musi być liczbą całkowitą większą od 0' };
    }

    const totalCost = targetPrice * quantity;
    const metrics = calcPortfolioMetrics(state);

    if (totalCost > metrics.availablePLN) {
      return { success: false, error: 'Brak wystarczających środków' };
    }

    const newOrder: GhostOrder = {
      id: `order-${Date.now()}-${Math.random()}`,
      itemId,
      itemName,
      targetPrice,
      quantity,
      totalCommittedPLN: totalCost,
      status: 'WAITING',
      createdAt: new Date().toISOString(),
    };

    setState(prev => ({
      ...prev,
      orders: [...prev.orders, newOrder],
    }));

    return { success: true };
  }, [state]);

  // Fill order (simulate execution)
  const fillOrder = useCallback((orderId: string, actualFillPrice?: number): { success: boolean; error?: string } => {
    const order = state.orders.find(o => o.id === orderId);
    if (!order || order.status !== 'WAITING') {
      return { success: false, error: 'Zlecenie nie znalezione lub już zrealizowane' };
    }

    const fillPrice = actualFillPrice ?? order.targetPrice;
    const marketItem = marketItems.find(item => item.name === order.itemName);
    const currentAskPrice = marketItem?.lowestAsk ?? fillPrice;

    const newPosition: GhostPosition = {
      id: `position-${Date.now()}-${Math.random()}`,
      itemId: order.itemId,
      itemName: order.itemName,
      entryPrice: fillPrice,
      quantity: order.quantity,
      currentAskPrice,
      investedPLN: fillPrice * order.quantity,
      currentNetValuePLN: calcNetPrice(currentAskPrice) * order.quantity,
      unrealizedPnlPLN: calcUnrealizedPnl(currentAskPrice, fillPrice, order.quantity),
      unrealizedRoiPercent: 0,
      openedAt: new Date().toISOString(),
    };

    newPosition.unrealizedRoiPercent = calcUnrealizedRoi(newPosition.unrealizedPnlPLN, newPosition.investedPLN);

    setState(prev => ({
      ...prev,
      orders: prev.orders.map(o => o.id === orderId ? { ...o, status: 'FILLED' as const } : o),
      positions: [...prev.positions, newPosition],
    }));

    return { success: true };
  }, [state.orders, marketItems]);

  // Cancel order
  const cancelOrder = useCallback((orderId: string): { success: boolean; error?: string } => {
    const order = state.orders.find(o => o.id === orderId);
    if (!order || order.status !== 'WAITING') {
      return { success: false, error: 'Zlecenie nie znalezione lub już zrealizowane' };
    }

    setState(prev => ({
      ...prev,
      orders: prev.orders.map(o => o.id === orderId ? { ...o, status: 'CANCELLED' as const } : o),
    }));

    return { success: true };
  }, [state.orders]);

  // Sell position (close)
  const sellPosition = useCallback((positionId: string, exitGrossPrice: number): { success: boolean; error?: string } => {
    if (exitGrossPrice <= 0) {
      return { success: false, error: 'Cena sprzedaży musi być większa od 0' };
    }

    const position = state.positions.find(p => p.id === positionId);
    if (!position) {
      return { success: false, error: 'Pozycja nie znaleziona' };
    }

    const realizedPnlPLN = calcRealizedPnl(exitGrossPrice, position.entryPrice, position.quantity);
    const realizedRoiPercent = calcRealizedRoi(realizedPnlPLN, position.investedPLN);

    const trade: GhostTrade = {
      id: `trade-${Date.now()}-${Math.random()}`,
      itemId: position.itemId,
      itemName: position.itemName,
      entryPrice: position.entryPrice,
      exitPrice: exitGrossPrice,
      quantity: position.quantity,
      realizedPnlPLN,
      realizedRoiPercent,
      closedAt: new Date().toISOString(),
    };

    setState(prev => ({
      ...prev,
      capitalPLN: prev.capitalPLN + realizedPnlPLN,
      positions: prev.positions.filter(p => p.id !== positionId),
      history: [...prev.history, trade],
    }));

    return { success: true };
  }, [state.positions]);

  // Reset portfolio
  const resetPortfolio = useCallback(() => {
    const initialState = getInitialState();
    setState(initialState);
    saveState(initialState);
  }, []);

  // Calculate metrics
  const metrics = calcPortfolioMetrics(state);

  return {
    state,
    metrics,
    createOrder,
    fillOrder,
    cancelOrder,
    sellPosition,
    resetPortfolio,
  };
};
