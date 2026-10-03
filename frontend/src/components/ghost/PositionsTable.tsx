import { TrendingUp, TrendingDown } from 'lucide-react';
import type { GhostPosition } from '../../types';
import { formatPLN, formatPercent, formatDate } from '../../utils/format';

interface PositionsTableProps {
  positions: GhostPosition[];
  onSell: (position: GhostPosition) => void;
}

export const PositionsTable = ({ positions, onSell }: PositionsTableProps) => {
  if (positions.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-gray-400">
        <p className="text-lg mb-2">Brak otwartych pozycji</p>
        <p className="text-sm">Zrealizuj zlecenie z zakładki "Oczekujące Zlecenia"</p>
      </div>
    );
  }

  return (
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
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-center">
              Ilość
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
              Aktualny Ask
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
              Zainwestowane
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
              Wartość Netto
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
              P&L
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider">
              Otwarto
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-center">
              Akcje
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-[#1f2937]">
          {positions.map((position) => (
            <tr key={position.id} className="hover:bg-[#1a212d] transition-colors">
              <td className="px-3 py-2 text-sm text-gray-100 font-medium">
                {position.itemName}
              </td>
              <td className="px-3 py-2 text-sm text-gray-300 text-right font-mono">
                {formatPLN(position.entryPrice)}
              </td>
              <td className="px-3 py-2 text-sm text-gray-300 text-center font-mono">
                {position.quantity}
              </td>
              <td className="px-3 py-2 text-sm text-gray-300 text-right font-mono">
                {formatPLN(position.currentAskPrice)}
              </td>
              <td className="px-3 py-2 text-sm text-blue-400 text-right font-mono font-semibold">
                {formatPLN(position.investedPLN)}
              </td>
              <td className="px-3 py-2 text-sm text-gray-300 text-right font-mono">
                {formatPLN(position.currentNetValuePLN)}
              </td>
              <td className="px-3 py-2 text-sm text-right font-mono">
                <div className="flex items-center justify-end gap-1.5">
                  {position.unrealizedPnlPLN >= 0 ? (
                    <TrendingUp className="w-4 h-4 text-emerald-400" />
                  ) : (
                    <TrendingDown className="w-4 h-4 text-rose-400" />
                  )}
                  <div>
                    <div className={position.unrealizedPnlPLN >= 0 ? 'text-emerald-400' : 'text-rose-400'}>
                      {formatPLN(position.unrealizedPnlPLN)}
                    </div>
                    <div className={`text-xs ${position.unrealizedRoiPercent >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                      {formatPercent(position.unrealizedRoiPercent)}
                    </div>
                  </div>
                </div>
              </td>
              <td className="px-3 py-2 text-xs text-gray-400 font-mono">
                {formatDate(position.openedAt, true)}
              </td>
              <td className="px-3 py-2">
                <div className="flex items-center justify-center">
                  <button
                    onClick={() => onSell(position)}
                    className="px-3 py-1.5 bg-amber-500 hover:bg-amber-600 text-white text-xs font-semibold rounded transition-colors"
                  >
                    Sprzedaj
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
