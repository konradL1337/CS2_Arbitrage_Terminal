import { TrendingUp, TrendingDown, Wallet, Lock, DollarSign, RotateCcw } from 'lucide-react';
import { formatPLN, formatPercent } from '../../utils/format';
import { useState } from 'react';

interface MetricBarProps {
  capitalPLN: number;
  availablePLN: number;
  committedPLN: number;
  investedPLN: number;
  unrealizedPnlPLN: number;
  realizedPnlPLN: number;
  onReset: () => void;
}

export const MetricBar = ({
  capitalPLN,
  availablePLN,
  committedPLN,
  investedPLN,
  unrealizedPnlPLN,
  realizedPnlPLN,
  onReset,
}: MetricBarProps) => {
  const [showResetConfirm, setShowResetConfirm] = useState(false);

  const unrealizedRoi = investedPLN > 0 ? (unrealizedPnlPLN / investedPLN) * 100 : 0;

  const handleReset = () => {
    onReset();
    setShowResetConfirm(false);
  };

  return (
    <div className="bg-[#151a23] border-b border-[#1f2937] px-6 py-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-6">
          {/* Capital */}
          <div className="flex items-center gap-2">
            <Wallet className="w-4 h-4 text-gray-400" />
            <div>
              <div className="text-xs text-gray-400 uppercase tracking-wide">Kapitał</div>
              <div className="text-sm font-mono font-semibold text-gray-100">
                {formatPLN(capitalPLN)}
              </div>
            </div>
          </div>

          {/* Available */}
          <div className="flex items-center gap-2">
            <DollarSign className="w-4 h-4 text-emerald-400" />
            <div>
              <div className="text-xs text-gray-400 uppercase tracking-wide">Dostępne</div>
              <div className="text-sm font-mono font-semibold text-emerald-400">
                {formatPLN(availablePLN)}
              </div>
            </div>
          </div>

          {/* Committed */}
          <div className="flex items-center gap-2">
            <Lock className="w-4 h-4 text-amber-400" />
            <div>
              <div className="text-xs text-gray-400 uppercase tracking-wide">Zablokowane</div>
              <div className="text-sm font-mono font-semibold text-amber-400">
                {formatPLN(committedPLN)}
              </div>
            </div>
          </div>

          {/* Invested */}
          <div className="flex items-center gap-2">
            <TrendingUp className="w-4 h-4 text-blue-400" />
            <div>
              <div className="text-xs text-gray-400 uppercase tracking-wide">Zainwestowane</div>
              <div className="text-sm font-mono font-semibold text-blue-400">
                {formatPLN(investedPLN)}
              </div>
            </div>
          </div>

          {/* Unrealized P&L */}
          <div className="flex items-center gap-2">
            {unrealizedPnlPLN >= 0 ? (
              <TrendingUp className="w-4 h-4 text-emerald-400" />
            ) : (
              <TrendingDown className="w-4 h-4 text-rose-400" />
            )}
            <div>
              <div className="text-xs text-gray-400 uppercase tracking-wide">Niezrealizowany P&L</div>
              <div className={`text-sm font-mono font-semibold ${unrealizedPnlPLN >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                {formatPLN(unrealizedPnlPLN)} ({formatPercent(unrealizedRoi)})
              </div>
            </div>
          </div>

          {/* Realized P&L */}
          <div className="flex items-center gap-2">
            {realizedPnlPLN >= 0 ? (
              <TrendingUp className="w-4 h-4 text-emerald-400" />
            ) : (
              <TrendingDown className="w-4 h-4 text-rose-400" />
            )}
            <div>
              <div className="text-xs text-gray-400 uppercase tracking-wide">Zrealizowany P&L</div>
              <div className={`text-sm font-mono font-semibold ${realizedPnlPLN >= 0 ? 'text-emerald-400' : 'text-rose-400'}`}>
                {formatPLN(realizedPnlPLN)}
              </div>
            </div>
          </div>
        </div>

        {/* Reset Button */}
        <div className="relative">
          {showResetConfirm ? (
            <div className="flex items-center gap-2">
              <span className="text-xs text-gray-400">Czy na pewno?</span>
              <button
                onClick={handleReset}
                className="px-3 py-1.5 bg-rose-500 hover:bg-rose-600 text-white text-xs rounded transition-colors"
              >
                Tak, resetuj
              </button>
              <button
                onClick={() => setShowResetConfirm(false)}
                className="px-3 py-1.5 bg-gray-700 hover:bg-gray-600 text-gray-300 text-xs rounded transition-colors"
              >
                Anuluj
              </button>
            </div>
          ) : (
            <button
              onClick={() => setShowResetConfirm(true)}
              className="flex items-center gap-2 px-3 py-1.5 bg-[#1f2937] hover:bg-[#374151] text-gray-300 text-xs rounded border border-gray-600 transition-colors"
            >
              <RotateCcw className="w-3.5 h-3.5" />
              <span>Reset Portfela</span>
            </button>
          )}
        </div>
      </div>
    </div>
  );
};
