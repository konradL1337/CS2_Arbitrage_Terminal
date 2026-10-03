import { Search } from 'lucide-react';
import { useState } from 'react';

interface RadarFiltersProps {
  onFilterChange: (filters: FilterState) => void;
}

export interface FilterState {
  search: string;
  category: 'all' | 'case' | 'sticker' | 'skin';
  minEdge: number;
  minLiquidity: number;
  minVolume: number;
  maxPrice: number;
}

export const RadarFilters = ({ onFilterChange }: RadarFiltersProps) => {
  const [filters, setFilters] = useState<FilterState>({
    search: '',
    category: 'all',
    minEdge: 0,
    minLiquidity: 0,
    minVolume: 0,
    maxPrice: 999999,
  });

  const updateFilter = (key: keyof FilterState, value: string | number) => {
    const newFilters = { ...filters, [key]: value };
    setFilters(newFilters);
    onFilterChange(newFilters);
  };

  const applyQuickFilter = (preset: 'all' | 'highEdge' | 'highLiquidity') => {
    let newFilters = { ...filters };
    
    switch (preset) {
      case 'all':
        newFilters = { ...filters, minEdge: 0, minLiquidity: 0, category: 'all' };
        break;
      case 'highEdge':
        newFilters = { ...filters, minEdge: 15, minLiquidity: 0 };
        break;
      case 'highLiquidity':
        newFilters = { ...filters, minEdge: 0, minLiquidity: 80 };
        break;
    }
    
    setFilters(newFilters);
    onFilterChange(newFilters);
  };

  return (
    <div className="bg-[#151a23] border-b border-[#1f2937] px-6 py-4">
      <div className="flex items-center gap-4 mb-4">
        {/* Quick Filters */}
        <div className="flex items-center gap-2">
          <span className="text-xs text-gray-400 uppercase tracking-wide">Szybkie filtry:</span>
          <button
            onClick={() => applyQuickFilter('all')}
            className={`px-3 py-1.5 text-xs rounded transition-colors ${
              filters.minEdge === 0 && filters.minLiquidity === 0
                ? 'bg-amber-500 text-white'
                : 'bg-[#1f2937] text-gray-300 hover:bg-[#374151]'
            }`}
          >
            Wszystkie
          </button>
          <button
            onClick={() => applyQuickFilter('highEdge')}
            className={`px-3 py-1.5 text-xs rounded transition-colors ${
              filters.minEdge >= 15
                ? 'bg-emerald-500 text-white'
                : 'bg-[#1f2937] text-gray-300 hover:bg-[#374151]'
            }`}
          >
            High Edge &gt; 15%
          </button>
          <button
            onClick={() => applyQuickFilter('highLiquidity')}
            className={`px-3 py-1.5 text-xs rounded transition-colors ${
              filters.minLiquidity >= 80
                ? 'bg-blue-500 text-white'
                : 'bg-[#1f2937] text-gray-300 hover:bg-[#374151]'
            }`}
          >
            High Liquidity &gt; 80
          </button>
        </div>

        {/* Category Filters */}
        <div className="flex items-center gap-2 ml-4">
          <button
            onClick={() => updateFilter('category', 'case')}
            className={`px-3 py-1.5 text-xs rounded transition-colors ${
              filters.category === 'case'
                ? 'bg-amber-500 text-white'
                : 'bg-[#1f2937] text-gray-300 hover:bg-[#374151]'
            }`}
          >
            Cases
          </button>
          <button
            onClick={() => updateFilter('category', 'sticker')}
            className={`px-3 py-1.5 text-xs rounded transition-colors ${
              filters.category === 'sticker'
                ? 'bg-amber-500 text-white'
                : 'bg-[#1f2937] text-gray-300 hover:bg-[#374151]'
            }`}
          >
            Stickers
          </button>
        </div>
      </div>

      <div className="flex items-center gap-4">
        {/* Search */}
        <div className="flex-1 relative">
          <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400" />
          <input
            type="text"
            placeholder="Szukaj przedmiotu..."
            value={filters.search}
            onChange={(e) => updateFilter('search', e.target.value)}
            className="w-full pl-10 pr-4 py-2 bg-[#0b0e14] border border-[#1f2937] rounded text-sm text-gray-100 placeholder-gray-500 focus:outline-none focus:border-amber-500"
          />
        </div>

        {/* Min Volume */}
        <div className="flex items-center gap-2">
          <label className="text-xs text-gray-400 whitespace-nowrap">Min Wolumen:</label>
          <input
            type="number"
            value={filters.minVolume}
            onChange={(e) => updateFilter('minVolume', Number(e.target.value))}
            className="w-24 px-3 py-2 bg-[#0b0e14] border border-[#1f2937] rounded text-sm text-gray-100 font-mono focus:outline-none focus:border-amber-500"
          />
        </div>

        {/* Max Price */}
        <div className="flex items-center gap-2">
          <label className="text-xs text-gray-400 whitespace-nowrap">Max Cena:</label>
          <input
            type="number"
            value={filters.maxPrice === 999999 ? '' : filters.maxPrice}
            onChange={(e) => updateFilter('maxPrice', e.target.value ? Number(e.target.value) : 999999)}
            placeholder="Bez limitu"
            className="w-28 px-3 py-2 bg-[#0b0e14] border border-[#1f2937] rounded text-sm text-gray-100 font-mono focus:outline-none focus:border-amber-500"
          />
        </div>
      </div>
    </div>
  );
};
