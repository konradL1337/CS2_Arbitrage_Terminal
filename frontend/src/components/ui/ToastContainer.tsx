import { CheckCircle, XCircle, Info, X } from 'lucide-react';
import { useToast } from '../../hooks/useToast';

export const ToastContainer = () => {
  const { toasts, removeToast } = useToast();

  if (toasts.length === 0) return null;

  return (
    <div className="fixed top-4 right-4 z-50 flex flex-col gap-2 max-w-sm">
      {toasts.map(toast => {
        const icons = {
          success: <CheckCircle className="w-5 h-5 text-emerald-400" />,
          error: <XCircle className="w-5 h-5 text-rose-400" />,
          info: <Info className="w-5 h-5 text-amber-400" />,
        };

        const bgColors = {
          success: 'bg-emerald-500/10 border-emerald-500/30',
          error: 'bg-rose-500/10 border-rose-500/30',
          info: 'bg-amber-500/10 border-amber-500/30',
        };

        return (
          <div
            key={toast.id}
            className={`flex items-start gap-3 p-4 rounded-lg border backdrop-blur-sm shadow-lg animate-in slide-in-from-right ${bgColors[toast.type]}`}
          >
            <div className="flex-shrink-0 mt-0.5">{icons[toast.type]}</div>
            <p className="flex-1 text-sm text-gray-100">{toast.message}</p>
            <button
              onClick={() => removeToast(toast.id)}
              className="flex-shrink-0 text-gray-400 hover:text-gray-200 transition-colors"
            >
              <X className="w-4 h-4" />
            </button>
          </div>
        );
      })}
    </div>
  );
};
