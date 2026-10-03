// Polish currency and number formatting utilities

// Format PLN currency: 1234.56 -> "1 234,56 zł"
export const formatPLN = (value: number): string => {
  const formatted = value.toFixed(2).replace('.', ',');
  const parts = formatted.split(',');
  parts[0] = parts[0].replace(/\B(?=(\d{3})+(?!\d))/g, ' ');
  return `${parts.join(',')} zł`;
};

// Format percentage with optional sign: 14.234 -> "+14,23%"
export const formatPercent = (value: number, showSign: boolean = true): string => {
  const sign = showSign && value > 0 ? '+' : '';
  const decimals = Math.abs(value) >= 100 ? 1 : 2;
  return `${sign}${value.toFixed(decimals).replace('.', ',')}%`;
};

// Format compact numbers: 1234 -> "1,2K", 1234567 -> "1,2M"
export const formatCompact = (value: number): string => {
  if (value >= 1_000_000) {
    return `${(value / 1_000_000).toFixed(1).replace('.', ',')}M`;
  }
  if (value >= 1_000) {
    return `${(value / 1_000).toFixed(1).replace('.', ',')}K`;
  }
  return value.toString();
};

// Format date from ISO string: "2024-01-15T14:30:00" -> "14:30:00" or "15.01 14:30"
export const formatDate = (isoString: string, includeDate: boolean = false): string => {
  const date = new Date(isoString);
  const hours = date.getHours().toString().padStart(2, '0');
  const minutes = date.getMinutes().toString().padStart(2, '0');
  const seconds = date.getSeconds().toString().padStart(2, '0');
  
  if (includeDate) {
    const day = date.getDate().toString().padStart(2, '0');
    const month = (date.getMonth() + 1).toString().padStart(2, '0');
    return `${day}.${month} ${hours}:${minutes}`;
  }
  
  return `${hours}:${minutes}:${seconds}`;
};

// Format time ago: "2 min ago", "1h ago", "3d ago"
export const formatTimeAgo = (isoString: string): string => {
  const now = new Date();
  const date = new Date(isoString);
  const diffMs = now.getTime() - date.getTime();
  const diffSec = Math.floor(diffMs / 1000);
  const diffMin = Math.floor(diffSec / 60);
  const diffHour = Math.floor(diffMin / 60);
  const diffDay = Math.floor(diffHour / 24);

  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffMin < 60) return `${diffMin}m ago`;
  if (diffHour < 24) return `${diffHour}h ago`;
  return `${diffDay}d ago`;
};
