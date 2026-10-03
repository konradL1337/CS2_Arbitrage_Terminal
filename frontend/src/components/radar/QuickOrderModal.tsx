import { X } from 'lucide-react';
import { useState } from 'react';
import type { MarketItem } from '../../types';
import { formatPLN } from '../../utils/format';

interface QuickOrderModalProps {
  item: MarketItem | null;
  availablePLN: number;
  onClose: () => void;
  onSubmit: (targetPrice: number, quantity: number) => void;
}

export const QuickOrderModal = ({ item, availablePLN, onClose, onSubmit }: QuickOrderModalProps) => {
  const [targetPrice, setTargetPrice] = useState(item?.maxBuyPrice.toFixed(2) || '');
  const [quantity, setQuantity] = useState('1');

  if (!item) return null;

  const price = parseFloat(targetPrice) || 0;
  const qty = parseInt(quantity) || 0;
  const totalCost = price * qty;
  const isValid = price > 0 && qty > 0 && totalCost <= availablePLN;

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    if (isValid) {
      onSubmit(price, qty);
      onClose();
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/70 backdrop-blur-sm">
      <div className="bg-[#151a23] border border-[#1f2937] rounded-lg shadow-2xl w-full max-w-md">
        {/* Header */}
        <div className="flex items-center justify-between px-6 py-4 border-b border-[#1f2937]">
          <h3 className="text-lg font-semibold text-gray-100">Wystaw Zlecenie Kupna</h3>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-200 transition-colors"
          >
            <X className="w-5 h-5" />
          </button>
        </div>

        {/* Body */}
        <form onSubmit={handleSubmit} className="p-6 space-y-4">
          {/* Item Info */}
          <div className="bg-[#0b0e14] border border-[#1f2937] rounded p-4">
            <div className="text-xs text-gray-400 uppercase tracking-wide mb-1">Przedmiot</div>
            <div className="text-sm font-medium text-gray-100">{item.name}</div>
            <div className="mt-2 flex items-center gap-4 text-xs">
              <div>
                <span className="text-gray-400">Max Buy: </span>
                <span className="font-mono text-blue-400">{formatPLN(item.maxBuyPrice)}</span>
              </div>
              <div>
                <span className="text-gray-400">Current Ask: </span>
                <span className="font-mono text-gray-300">{formatPLN(item.lowestAsk)}</span>
              </div>
            </div>
          </div>

          {/* Target Price */}
          <div>
            <label className="block text-sm text-gray-300 mb-2">
              Cena docelowa (PLN)
            </label>
            <input
              type="number"
              step="0.01"
              value={targetPrice}
              onChange={(e) => setTargetPrice(e.target.value)}
              className="w-full px-4 py-2 bg-[#0b0e14] border border-[#1f2937] rounded text-gray-100 font-mono focus:outline-none focus:border-amber-500"
              placeholder="0.00"
              autoFocus
            />
          </div>

          {/* Quantity */}
          <div>
            <label className="block text-sm text-gray-300 mb-2">
              Ilość (sztuki)
            </label>
            <input
              type="number"
              step="1"
              min="1"
              value={quantity}
              onChange={(e) => setQuantity(e.target.value)}
              className="w-full px-4 py-2 bg-[#0b0e14] border border-[#1f2937] rounded text-gray-100 font-mono focus:outline-none focus:border-amber-500"
              placeholder="1"
            />
          </div>

          {/* Summary */}
          <div className="bg-[#0b0e14] border border-[#1f2937] rounded p-4 space-y-2">
            <div className="flex items-center justify-between text-sm">
              <span className="text-gray-400">Całkowity koszt:</span>
              <span className="font-mono font-semibold text-gray-100">{formatPLN(totalCost)}</span>
            </div>
            <div className="flex items-center justify-between text-sm">
              <span className="text-gray-400">Dostępne środki:</span>
              <span className="font-mono font-semibold text-emerald-400">{formatPLN(availablePLN)}</span>
            </div>
            {totalCost > availablePLN && (
              <div className="text-xs text-rose-400 mt-2">
                ⚠ Niewystarczające środki
              </div>
            )}
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
              className="flex-1 px-4 py-2 bg-amber-500 hover:bg-amber-600 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold rounded transition-colors"
            >
              Złóż Zlecenie
            </button>
          </div>
        </form>
      </div>
    </div>
  );
};
