import { Copy, ShoppingCart, ExternalLink } from 'lucide-react';
import type { MarketItem } from '../../types';
import { formatPLN, formatPercent, formatCompact } from '../../utils/format';
import { Badge } from '../ui/Badge';
import { Sparkline } from './Sparkline';

interface RadarTableProps {
  items: MarketItem[];
  onCreateOrder: (item: MarketItem) => void;
  onCopyPrice: (price: number) => void;
}

export const RadarTable = ({ items, onCreateOrder, onCopyPrice }: RadarTableProps) => {
  if (items.length === 0) {
    return (
      <div className="flex items-center justify-center py-16 text-gray-400">
        <p>Brak przedmiotów spełniających kryteria filtrowania</p>
      </div>
    );
  }

  const openSteamMarket = (itemName: string) => {
    const url = `https://steamcommunity.com/market/listings/730/${encodeURIComponent(itemName)}`;
    window.open(url, '_blank', 'noopener,noreferrer');
  };

  return (
    <div className="overflow-x-auto">
      <table className="w-full text-left">
        <thead className="bg-[#0b0e14] sticky top-0 z-20">
          <tr className="border-b border-[#1f2937]">
            <th className="sticky left-0 z-30 bg-[#0b0e14] px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider">
              Item
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
              Current Ask
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
              Best Bid
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
              Spread %
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
              Vol 24h
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-center">
              Δ 24H
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-center">
              Trend
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-center">
              Liquidity
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
              Max Buy
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
              Edge %
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-center">
              Actions
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-[#1f2937]">
          {items.map((item) => (
            <tr
              key={item.id}
              className="hover:bg-[#1a212d] transition-colors"
            >
              {/* Item Name - Sticky */}
              <td className="sticky left-0 z-10 bg-[#151a23] px-3 py-2 text-sm text-gray-100 font-medium max-w-xs">
                <div className="flex items-center gap-2">
                  <span className="truncate">{item.name}</span>
                  <Badge variant="neutral" className="text-[10px] px-1.5 py-0">
                    {item.category.toUpperCase()}
                  </Badge>
                </div>
              </td>

              {/* Current Ask */}
              <td className="px-3 py-2 text-sm text-gray-300 text-right font-mono">
                {formatPLN(item.lowestAsk)}
              </td>

              {/* Best Bid */}
              <td className="px-3 py-2 text-sm text-gray-300 text-right font-mono">
                {formatPLN(item.highestBid)}
              </td>

              {/* Spread % */}
              <td className="px-3 py-2 text-sm text-right font-mono">
                <span className={item.spreadPercent < 10 ? 'text-emerald-400' : 'text-gray-400'}>
                  {formatPercent(item.spreadPercent, false)}
                </span>
              </td>

              {/* Volume 24h - Kolorowany */}
              <td className="px-3 py-2 text-sm text-right font-mono">
                <span
                  className={
                    item.volume24h >= 1000
                      ? 'text-emerald-400 font-semibold'
                      : item.volume24h >= 100
                      ? 'text-amber-400'
                      : 'text-rose-400'
                  }
                >
                  {formatCompact(item.volume24h)}
                </span>
              </td>

              {/* Delta 24H */}
              <td className="px-3 py-2 text-center">
                <Badge
                  variant={item.delta24h >= 0 ? 'success' : 'error'}
                  className="font-mono text-[11px]"
                >
                  {formatPercent(item.delta24h)}
                </Badge>
              </td>

              {/* Sparkline Trend */}
              <td className="px-3 py-2 text-center">
                <div className="flex items-center justify-center">
                  <Sparkline data={item.sparkline} width={80} height={24} />
                </div>
              </td>

              {/* Liquidity Score */}
              <td className="px-3 py-2 text-center">
                <div className="flex items-center justify-center gap-2">
                  <div className="w-16 h-2 bg-gray-700 rounded-full overflow-hidden">
                    <div
                      className={`h-full transition-all ${
                        item.liquidityScore >= 80
                          ? 'bg-emerald-400'
                          : item.liquidityScore >= 50
                          ? 'bg-amber-400'
                          : 'bg-rose-400'
                      }`}
                      style={{ width: `${item.liquidityScore}%` }}
                    />
                  </div>
                  <span className="text-xs font-mono text-gray-400 w-8">
                    {item.liquidityScore}
                  </span>
                </div>
              </td>

              {/* Max Buy Price */}
              <td className="px-3 py-2 text-sm text-right font-mono">
                <span className="text-blue-400">{formatPLN(item.maxBuyPrice)}</span>
              </td>

              {/* Edge % */}
              <td className="px-3 py-2 text-sm text-right font-mono">
                <span
                  className={
                    item.estimatedEdgePercent >= 15
                      ? 'text-emerald-400 font-bold'
                      : item.estimatedEdgePercent >= 5
                      ? 'text-amber-400'
                      : 'text-gray-400'
                  }
                >
                  {formatPercent(item.estimatedEdgePercent)}
                </span>
              </td>

              {/* Actions */}
              <td className="px-3 py-2">
                <div className="flex items-center justify-center gap-1.5">
                  <button
                    onClick={() => onCopyPrice(item.maxBuyPrice)}
                    className="p-1.5 bg-[#1f2937] hover:bg-[#374151] text-gray-300 rounded transition-colors"
                    title="Kopiuj Max Buy"
                  >
                    <Copy className="w-3.5 h-3.5" />
                  </button>
                  <button
                    onClick={() => onCreateOrder(item)}
                    className="p-1.5 bg-amber-500 hover:bg-amber-600 text-white rounded transition-colors"
                    title="Wystaw zlecenie"
                  >
                    <ShoppingCart className="w-3.5 h-3.5" />
                  </button>
                  <button
                    onClick={() => openSteamMarket(item.name)}
                    className="p-1.5 bg-[#1f2937] hover:bg-[#374151] text-gray-300 rounded transition-colors"
                    title="Otwórz na Steam"
                  >
                    <ExternalLink className="w-3.5 h-3.5" />
                  </button>
                </div>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
};
