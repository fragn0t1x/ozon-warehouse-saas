'use client';

import { useStoreContext } from '@/lib/context/StoreContext';

interface StoreSelectorProps {
  value?: number | 'all';
  onChange: (storeId: number | 'all') => void;
  showAll?: boolean;
  label?: string;
}

export function StoreSelector({
  value,
  onChange,
  showAll = true,
  label = "Магазин"
}: StoreSelectorProps) {
  const { stores, isLoading } = useStoreContext();

  if (isLoading) {
    return (
      <div className="space-y-1">
        {label && <label className="block text-sm font-medium text-gray-700">{label}</label>}
        <div className="animate-pulse h-10 bg-gray-200 rounded"></div>
      </div>
    );
  }

  if (stores.length === 0) {
    return (
      <div className="space-y-1">
        {label && <label className="block text-sm font-medium text-gray-700">{label}</label>}
        <div className="text-sm text-gray-500 py-2 px-3 border border-gray-300 rounded-md bg-white">
          Нет доступных магазинов
        </div>
      </div>
    );
  }

  return (
    <div className="space-y-1">
      {label && (
        <label className="block text-sm font-medium text-gray-700">
          {label}
        </label>
      )}
      <select
        value={value ?? 'all'}
        onChange={(e) => onChange(e.target.value === 'all' ? 'all' : parseInt(e.target.value))}
        className="block w-full pl-3 pr-10 py-2 text-base border border-gray-300 focus:outline-none focus:ring-2 focus:ring-primary-500 focus:border-primary-500 sm:text-sm rounded-md bg-white text-gray-900"
      >
        {showAll && <option value="all" className="text-gray-900">Все магазины</option>}
        {stores.map(store => (
          <option key={store.id} value={store.id} className="text-gray-900">
            {store.name}
          </option>
        ))}
      </select>
    </div>
  );
}
