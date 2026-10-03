import { useState, useMemo } from 'react';
import { ToastProvider, useToast } from './hooks/useToast';
import { useMarketData } from './hooks/useMarketData';
import { usePortfolio } from './hooks/usePortfolio';
import type { MarketItem, GhostPosition } from './types';

// Layout Components
import { Header } from './components/layout/Header';
import { MetricBar } from './components/layout/MetricBar';

// Radar Components
import { RadarFilters, type FilterState } from './components/radar/RadarFilters';
import { RadarTable } from './components/radar/RadarTable';
import { QuickOrderModal } from './components/radar/QuickOrderModal';

// Ghost Portfolio Components
import { GhostTabs } from './components/ghost/GhostTabs';
import { OrdersTable } from './components/ghost/OrdersTable';
import { PositionsTable } from './components/ghost/PositionsTable';
import { HistoryTable } from './components/ghost/HistoryTable';
import { SellModal } from './components/ghost/SellModal';

// UI Components
import { ToastContainer } from './components/ui/ToastContainer';

function AppContent() {
  const { showToast } = useToast();
  const { items, isLoading, error, lastUpdated, refetch } = useMarketData();
  const portfolio = usePortfolio(items);

  // UI State
  const [mainTab, setMainTab] = useState<'radar' | 'portfolio'>('radar');
  const [ghostTab, setGhostTab] = useState<'orders' | 'positions' | 'history'>('orders');
  const [filters, setFilters] = useState<FilterState>({
    search: '',
    category: 'all',
    minEdge: 0,
    minLiquidity: 0,
    minVolume: 0,
    maxPrice: 999999,
  });

  // Modal State
  const [orderModalItem, setOrderModalItem] = useState<MarketItem | null>(null);
  const [sellModalPosition, setSellModalPosition] = useState<GhostPosition | null>(null);

  // Filter market items
  const filteredItems = useMemo(() => {
    return items.filter(item => {
      if (filters.search && !item.name.toLowerCase().includes(filters.search.toLowerCase())) {
        return false;
      }
      if (filters.category !== 'all' && item.category !== filters.category) {
        return false;
      }
      if (item.estimatedEdgePercent < filters.minEdge) {
        return false;
      }
      if (item.liquidityScore < filters.minLiquidity) {
        return false;
      }
      if (item.volume24h < filters.minVolume) {
        return false;
      }
      if (item.lowestAsk > filters.maxPrice) {
        return false;
      }
      return true;
    });
  }, [items, filters]);

  // Handlers
  const handleCopyPrice = (price: number) => {
    navigator.clipboard.writeText(price.toFixed(2));
    showToast('success', `Skopiowano cenę: ${price.toFixed(2)} zł`);
  };

  const handleCreateOrder = (item: MarketItem) => {
    setOrderModalItem(item);
  };

  const handleSubmitOrder = (targetPrice: number, quantity: number) => {
    if (!orderModalItem) return;

    const result = portfolio.createOrder(
      orderModalItem.id,
      orderModalItem.name,
      targetPrice,
      quantity
    );

    if (result.success) {
      showToast('success', `Złożono zlecenie kupna: ${orderModalItem.name} x${quantity}`);
      setOrderModalItem(null);
    } else {
      showToast('error', result.error || 'Nie udało się złożyć zlecenia');
    }
  };

  const handleFillOrder = (orderId: string) => {
    const result = portfolio.fillOrder(orderId);
    if (result.success) {
      showToast('success', 'Zlecenie zostało zrealizowane');
    } else {
      showToast('error', result.error || 'Nie udało się zrealizować zlecenia');
    }
  };

  const handleCancelOrder = (orderId: string) => {
    const result = portfolio.cancelOrder(orderId);
    if (result.success) {
      showToast('info', 'Zlecenie zostało anulowane');
    } else {
      showToast('error', result.error || 'Nie udało się anulować zlecenia');
    }
  };

  const handleSellPosition = (position: GhostPosition) => {
    setSellModalPosition(position);
  };

  const handleSubmitSell = (exitPrice: number) => {
    if (!sellModalPosition) return;

    const result = portfolio.sellPosition(sellModalPosition.id, exitPrice);
    if (result.success) {
      const pnl = (exitPrice / 1.15 - sellModalPosition.entryPrice) * sellModalPosition.quantity;
      if (pnl >= 0) {
        showToast('success', `Pozycja zamknięta z zyskiem: +${pnl.toFixed(2)} zł`);
      } else {
        showToast('error', `Pozycja zamknięta ze stratą: ${pnl.toFixed(2)} zł`);
      }
      setSellModalPosition(null);
    } else {
      showToast('error', result.error || 'Nie udało się zamknąć pozycji');
    }
  };

  const handleResetPortfolio = () => {
    portfolio.resetPortfolio();
    showToast('info', 'Portfel został zresetowany do stanu początkowego');
  };

  return (
    <div className="min-h-screen bg-[#0b0e14] text-gray-100">
      {/* Header */}
      <Header lastUpdated={lastUpdated} isLoading={isLoading} onRefresh={refetch} />

      {/* Metric Bar */}
      <MetricBar
        capitalPLN={portfolio.state.capitalPLN}
        availablePLN={portfolio.metrics.availablePLN}
        committedPLN={portfolio.metrics.committedPLN}
        investedPLN={portfolio.metrics.investedPLN}
        unrealizedPnlPLN={portfolio.metrics.unrealizedPnlPLN}
        realizedPnlPLN={portfolio.metrics.realizedPnlPLN}
        onReset={handleResetPortfolio}
      />

      {/* Main Tabs */}
      <div className="flex items-center gap-1 bg-[#151a23] border-b border-[#1f2937] px-6">
        <button
          onClick={() => setMainTab('radar')}
          className={`px-6 py-3 text-sm font-medium transition-colors ${
            mainTab === 'radar'
              ? 'text-amber-400 border-b-2 border-amber-500'
              : 'text-gray-400 hover:text-gray-200'
          }`}
        >
          📡 Radar Zleceń
        </button>
        <button
          onClick={() => setMainTab('portfolio')}
          className={`px-6 py-3 text-sm font-medium transition-colors ${
            mainTab === 'portfolio'
              ? 'text-amber-400 border-b-2 border-amber-500'
              : 'text-gray-400 hover:text-gray-200'
          }`}
        >
          👻 Portfel Widmo
        </button>
      </div>

      {/* Content Area */}
      <div className="flex-1">
        {/* Error State */}
        {error && (
          <div className="mx-6 mt-6 bg-rose-500/10 border border-rose-500/30 text-rose-400 px-4 py-3 rounded">
            <strong className="font-bold">Błąd: </strong>
            <span>{error}</span>
          </div>
        )}

        {/* Radar Tab */}
        {mainTab === 'radar' && (
          <div>
            <RadarFilters onFilterChange={setFilters} />
            <div className="p-6">
              <div className="bg-[#151a23] border border-[#1f2937] rounded-lg overflow-hidden">
                {isLoading && !error ? (
                  <div className="flex items-center justify-center py-16 text-gray-400">
                    <div className="animate-pulse">Ładowanie danych rynkowych...</div>
                  </div>
                ) : (
                  <RadarTable
                    items={filteredItems}
                    onCreateOrder={handleCreateOrder}
                    onCopyPrice={handleCopyPrice}
                  />
                )}
              </div>
            </div>
          </div>
        )}

        {/* Portfolio Tab */}
        {mainTab === 'portfolio' && (
          <div>
            <GhostTabs
              activeTab={ghostTab}
              onTabChange={setGhostTab}
              orderCount={portfolio.state.orders.filter(o => o.status === 'WAITING').length}
              positionCount={portfolio.state.positions.length}
              historyCount={portfolio.state.history.length}
            />
            <div className="p-6">
              <div className="bg-[#151a23] border border-[#1f2937] rounded-lg overflow-hidden">
                {ghostTab === 'orders' && (
                  <OrdersTable
                    orders={portfolio.state.orders}
                    onFill={handleFillOrder}
                    onCancel={handleCancelOrder}
                  />
                )}
                {ghostTab === 'positions' && (
                  <PositionsTable
                    positions={portfolio.state.positions}
                    onSell={handleSellPosition}
                  />
                )}
                {ghostTab === 'history' && (
                  <HistoryTable trades={portfolio.state.history} />
                )}
              </div>
            </div>
          </div>
        )}
      </div>

      {/* Modals */}
      {orderModalItem && (
        <QuickOrderModal
          item={orderModalItem}
          availablePLN={portfolio.metrics.availablePLN}
          onClose={() => setOrderModalItem(null)}
          onSubmit={handleSubmitOrder}
        />
      )}

      {sellModalPosition && (
        <SellModal
          position={sellModalPosition}
          onClose={() => setSellModalPosition(null)}
          onSubmit={handleSubmitSell}
        />
      )}

      {/* Toast Notifications */}
      <ToastContainer />
    </div>
  );
}

function App() {
  return (
    <ToastProvider>
      <AppContent />
    </ToastProvider>
  );
}

export default App;
