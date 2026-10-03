import { CheckCircle, XCircle } from 'lucide-react';
import type { GhostOrder } from '../../types';
import { formatPLN, formatDate } from '../../utils/format';
import { Badge } from '../ui/Badge';

interface OrdersTableProps {
  orders: GhostOrder[];
  onFill: (orderId: string) => void;
  onCancel: (orderId: string) => void;
}

export const OrdersTable = ({ orders, onFill, onCancel }: OrdersTableProps) => {
  const waitingOrders = orders.filter(o => o.status === 'WAITING');

  if (waitingOrders.length === 0) {
    return (
      <div className="flex flex-col items-center justify-center py-16 text-gray-400">
        <p className="text-lg mb-2">Brak oczekujących zleceń</p>
        <p className="text-sm">Wystaw zlecenie kupna z zakładki "Radar Zleceń"</p>
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
              Cena Docelowa
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-center">
              Ilość
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-right">
              Zablokowane PLN
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-center">
              Status
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider">
              Utworzono
            </th>
            <th className="px-3 py-3 text-xs font-semibold text-gray-300 uppercase tracking-wider text-center">
              Akcje
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-[#1f2937]">
          {waitingOrders.map((order) => (
            <tr key={order.id} className="hover:bg-[#1a212d] transition-colors">
              <td className="px-3 py-2 text-sm text-gray-100 font-medium">
                {order.itemName}
              </td>
              <td className="px-3 py-2 text-sm text-gray-300 text-right font-mono">
                {formatPLN(order.targetPrice)}
              </td>
              <td className="px-3 py-2 text-sm text-gray-300 text-center font-mono">
                {order.quantity}
              </td>
              <td className="px-3 py-2 text-sm text-amber-400 text-right font-mono font-semibold">
                {formatPLN(order.totalCommittedPLN)}
              </td>
              <td className="px-3 py-2 text-center">
                <Badge variant="warning">WAITING</Badge>
              </td>
              <td className="px-3 py-2 text-xs text-gray-400 font-mono">
                {formatDate(order.createdAt, true)}
              </td>
              <td className="px-3 py-2">
                <div className="flex items-center justify-center gap-2">
                  <button
                    onClick={() => onFill(order.id)}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-emerald-500 hover:bg-emerald-600 text-white text-xs rounded transition-colors"
                    title="Symuluj realizację"
                  >
                    <CheckCircle className="w-3.5 h-3.5" />
                    <span>Zrealizuj</span>
                  </button>
                  <button
                    onClick={() => onCancel(order.id)}
                    className="flex items-center gap-1.5 px-3 py-1.5 bg-rose-500 hover:bg-rose-600 text-white text-xs rounded transition-colors"
                    title="Anuluj zlecenie"
                  >
                    <XCircle className="w-3.5 h-3.5" />
                    <span>Anuluj</span>
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
