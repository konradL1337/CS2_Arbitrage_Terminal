import { X } from 'lucide-react';
import { useState } from 'react';
import type { GhostPosition } from '../../types';
import { formatPLN, formatPercent } from '../../utils/format';
import { calcNetPrice, calcRealizedPnl, calcRealizedRoi } from '../../utils/finance';

interface SellModalProps {
  position: GhostPosition | null;
  onClose: () => void;
  onSubmit: (exitPrice: number) => void;
}

export const SellModal = ({ position, onClose, onSubmit }: SellModalProps) => {
  const [exitPrice, setExitPrice] = useState(position?.currentAskPrice.toFixed(2) || '');

  if (!position) return null;

  const price = parseFloat(exitPrice) || 0;
  const isValid = price > 0;
  
  const projectedPnl = calcRealizedPnl(price, position.entryPrice, position.quantity);
  const projectedRoi = calcRealizedRoi(projectedPnl, position.investedPLN);
  const netReceived = calcNetPrice(price) * position.quantity;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (isValid) {
      onSubmit(price);
      onClose();
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm">
      <div className="bg-[#151a23] border border-[#1f2937] rounded-lg shadow-2xl w-full max-w-md">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-[#1f2937]">
          <h3 className="text-lg font-semibold text-gray-100">Zamknij Pozycję</h3>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-200 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Body */}
        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          {/* Position Info */}
          <div className="bg-[#0b0e14] border border-[#1f2937] rounded p-4">
            <div className="text-xs text-gray-400 uppercase tracking-wide mb-1">Przedmiot</div>
            <div className="text-sm font-medium text-gray-100 mb-3">{position.itemName}</div>
            <div className="grid grid-cols-2 gap-3 text-xs">
              <div>
                <span className="text-gray-400">Cena wejścia: </span>
                <span className="font-mono text-gray-300">{formatPLN(position.entryPrice)}</span>
              </div>
              <div>
                <span className="text-gray-400">Ilość: </span>
                <span className="font-mono text-gray-300">{position.quantity}</span>
              </div>
              <div>
                <span className="text-gray-400">Zainwestowano: </span>
                <span className="font-mono text-blue-400">{formatPLN(position.investedPLN)}</span>
              </div>
              <div>
                <span className="text-gray-400">Aktualny Ask: </span>
                <span className="font-mono text-gray-300">{formatPLN(position.currentAskPrice)}</span>
              </div>
            </div>
          </div>

          {/* Exit Price */}
          <div>
            <label className="block text-sm text-gray-300 mb-2">
              Cena sprzedaży brutto (PLN)
            </label>
            <input
              type="number"
              step="0.01"
              value={exitPrice}
              onChange={(e) => setExitPrice(e.target.value)}
              className="w-full px-4 py-2 bg-[#0b0e14] border border-[#1f2937] rounded text-gray-100 font-mono focus:outline-none focus:border-amber-500"
              placeholder="0.00"
              autoFocus
            />
            <p className="mt-1 text-xs text-gray-400">
              Po prowizji Steam (15%): {formatPLN(calcNetPrice(price))}
            </p>
          </div>

          {/* Projected P&L */}
          <div className="bg-[#0b0e14] border border-[#1f2937] rounded p-4 space-y-2">
            <div className="flex items-center justify-between text-sm">
              <span className="text-gray-400">Otrzymasz netto:</span>
              <span className="font-mono font-semibold text-gray-100">{formatPLN(netReceived)}</span>
            </div>
            <div className="flex items-center justify-between text-sm">
              <span className="text-gray-400">Przewidywany P&L:</span>
              <span className={`font-mono font-semibold ${projectedPnl >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                {formatPLN(projectedPnl)}
              </span>
            </div>
            <div className="flex items-center justify-between text-sm">
              <span className="text-gray-400">Przewidywany ROI:</span>
              <span className={`font-mono font-semibold ${projectedRoi >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                {formatPercent(projectedRoi)}
              </span>
            </div>
          </div>

          {/* Actions */}
          <div className="flex items-center gap-3 pt-2">
            <button
              type="button"
              onClick={onClose}
              className="flex-1 px-4 py-2 bg-[#1f2937] hover:bg-[#374151] text-gray-300 rounded transition-colors"
            >
              Anuluj
            </button>
            <button
              type="submit"
              disabled={!isValid}
              className={`flex-1 px-4 py-2 rounded font-semibold transition-colors ${
                projectedPnl >= 0
                  ? 'bg-emerald-500 hover:bg-emerald-600 text-white'
                  : 'bg-rose-500 hover:bg-rose-600 text-white'
              } disabled:opacity-50 disabled:cursor-not-allowed`}
            >
              {projectedPnl >= 0 ? 'Zamknij z Zyskiem' : 'Zamknij ze Stratą'}
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};
