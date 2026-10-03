interface GhostTabsProps {
  activeTab: 'orders' | 'positions' | 'history';
  onTabChange: (tab: 'orders' | 'positions' | 'history') => void;
  orderCount: number;
  positionCount: number;
  historyCount: number;
}

export const GhostTabs = ({ activeTab, onTabChange, orderCount, positionCount, historyCount }: GhostTabsProps) => {
  const tabs = [
    { id: 'orders' as const, label: 'Oczekujące Zlecenia', count: orderCount },
    { id: 'positions' as const, label: 'Otwarte Pozycje', count: positionCount },
    { id: 'history' as const, label: 'Historia Transakcji', count: historyCount },
  ];

  return (
    <div className="flex items-center gap-1 bg-[#151a23] border-b border-[#1f2937] px-6">
      {tabs.map((tab) => (
        <button
          key={tab.id}
          onClick={() => onTabChange(tab.id)}
          className={`px-4 py-3 text-sm font-medium transition-colors relative ${
            activeTab === tab.id
              ? 'text-amber-400 border-b-2 border-amber-500'
              : 'text-gray-400 hover:text-gray-200'
          }`}
        >
          {tab.label}
          {tab.count > 0 && (
            <span
              className={`ml-2 px-2 py-0.5 rounded-full text-xs font-semibold ${
                activeTab === tab.id
                  ? 'bg-amber-500/20 text-amber-400'
                  : 'bg-gray-700 text-gray-400'
              }`}
            >
              {tab.count}
            </span>
          )}
        </button>
      ))}
    </div>
  );
};
