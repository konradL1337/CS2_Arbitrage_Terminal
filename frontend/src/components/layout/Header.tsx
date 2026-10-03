import { Activity, RefreshCw } from 'lucide-react';
import { Badge } from '../ui/Badge';
import { formatDate } from '../../utils/format';

interface HeaderProps {
  lastUpdated: Date | null;
  isLoading: boolean;
  onRefresh: () => void;
}

export const Header = ({ lastUpdated, isLoading, onRefresh }: HeaderProps) => {
  return (
    <header className="bg-[#151a23] border-b border-[#1f2937] px-6 py-4">
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-4">
          <h1 className="text-xl font-bold text-gray-100 tracking-tight">
            CS2 ORDER RADAR & GHOST TERMINAL
          </h1>
          <Badge variant="success" className="flex items-center gap-1.5">
            <Activity className="w-3 h-3" />
            <span>LIVE</span>
          </Badge>
        </div>

        <div className="flex items-center gap-4">
          {lastUpdated && (
            <span className="text-xs text-gray-400 font-mono">
              Ostatnia aktualizacja: {formatDate(lastUpdated.toISOString(), false)}
            </span>
          )}
          <button
            onClick={onRefresh}
            disabled={isLoading}
            className="flex items-center gap-2 px-3 py-1.5 bg-[#1f2937] hover:bg-[#374151] disabled:opacity-50 disabled:cursor-not-allowed text-gray-300 text-sm rounded border border-gray-600 transition-colors"
          >
            <RefreshCw className={`w-4 h-4 ${isLoading ? 'animate-spin' : ''}`} />
            <span>Odśwież</span>
          </button>
        </div>
      </div>
    </header>
  );
};
