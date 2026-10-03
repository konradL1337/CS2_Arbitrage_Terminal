import { TrendingUp, TrendingDown } from 'lucide-react';
import type { GhostTrade } from '../../types';
import { formatPLN, formatPercent, formatDate } from '../../utils/format';

interface HistoryTableProps {
  trades: GhostTrade[];
}

export const HistoryTable = ({ trades }: HistoryTableProps) => {
  if (trades.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-gray-400">
        <p className="text-lg mb-2">Brak zamkniętych transakcji</p>
        <p className="text-sm">Zamknij pozycję z zakładki "Otwarte Pozycje"</p>
      </div>
    );
  }

  const totalPnl = trades.reduce((sum, t) => sum + t.realizedPnlPLN, 0);
  const profitableTrades = trades.filter(t => t.realizedPnlPLN > 0).length;
  const winRate = (profitableTrades / trades.length) * 100;

  return (
    <div>
      {/* Summary Stats */}
      <div className="bg-[#151a23] border-b border-[#1f2937] px-6 py-4">
        <div className="flex items-center gap-8">
          <div>
            <div className="text-xs text-gray-400 uppercase tracking-wide mb-1">Łączny P&L</div>
            <div className={`text-lg font-mono font-bold ${totalPnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
              {formatPLN(totalPnl)}
            </div>
          </div>
          <div>
            <div className="text-xs text-gray-400 uppercase tracking-wide mb-1">Transakcje</div>
            <div className="text-lg font-mono font-bold text-gray-100">{trades.length}</div>
          </div>
          <div>
            <div className="text-xs text-gray-400 uppercase tracking-wide mb-1">Win Rate</div>
            <div className={`text-lg font-mono font-bold ${winRate >= 50 ? 'text-emerald-400' : 'text-rose-400'}`}>
              {formatPercent(winRate, false)}
            </div>
          </div>
          <div>
            <div className="text-xs text-gray-400 uppercase tracking-wide mb-1">Zyskowne</div>
            <div className="text-lg font-mono font-bold text-emerald-400">{profitableTrades}</div>
          </div>
          <div>
            <div className="text-xs text-gray-400 uppercase tracking-wide mb-1">Stratne</div>
            <div className="text-lg font-mono font-bold text-rose-400">{trades.length - profitableTrades}</div>
          </div>
        </div>
      </div>

      {/* Trades Table */}
      <div className="overflow-x-auto">
        <table className="w-full text-left">
          <thead className="bg-[#0b0e14]">
            <tr className="border-b border-[#1f2937]">
              <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider">
                Przedmiot
              </th>
              <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
                Cena Wejścia
              </th>
              <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
                Cena Wyjścia
              </th>
              <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-center">
                Ilość
              </th>
              <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
                Zrealizowany P&L
              </th>
              <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
                ROI %
              </th>
              <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider">
                Zamknięto
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-[#1f2937]">
            {trades.map((trade) => (
              <tr key={trade.id} className="hover:bg-[#1a212d] transition-colors">
                <td className="px-3 py-2 text-sm text-gray-100 font-medium">
                  {trade.itemName}
                </td>
                <td className="px-3 py-2 text-sm text-gray-300 text-right font-mono">
                  {formatPLN(trade.entryPrice)}
                </td>
                <td className="px-3 py-2 text-sm text-gray-300 text-right font-mono">
                  {formatPLN(trade.exitPrice)}
                </td>
                <td className="px-3 py-2 text-sm text-gray-300 text-center font-mono">
                  {trade.quantity}
                </td>
                <td className="px-3 py-2 text-sm text-right font-mono">
                  <div className="flex items-center justify-end gap-1.5">
                    {trade.realizedPnlPLN >= 0 ? (
                      <TrendingUp className="w-4 h-4 text-emerald-400" />
                    ) : (
                      <TrendingDown className="w-4 h-4 text-rose-400" />
                    )}
                    <span className={trade.realizedPnlPLN >= 0 ? 'text-emerald-400 font-semibold' : 'text-rose-400 font-semibold'}>
                      {formatPLN(trade.realizedPnlPLN)}
                    </span>
                  </div>
                </td>
                <td className="px-3 py-2 text-sm text-right font-mono">
                  <span className={trade.realizedRoiPercent >= 0 ? 'text-emerald-400' : 'text-rose-400'}>
                    {formatPercent(trade.realizedRoiPercent)}
                  </span>
                </td>
                <td className="px-3 py-2 text-xs text-gray-400 font-mono">
                  {formatDate(trade.closedAt, true)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};
