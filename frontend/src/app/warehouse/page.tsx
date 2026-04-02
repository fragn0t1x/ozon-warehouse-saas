'use client';

import Link from 'next/link';
import { memo, useCallback, useEffect, useMemo, useState } from 'react';
import {
  ArchiveBoxIcon,
  ArrowUpIcon,
  ChevronDownIcon,
  ChevronRightIcon,
  TrashIcon,
} from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { ExportJobCard } from '@/components/ExportJobCard';
import { Layout } from '@/components/Layout';
import { IncomeForm } from '@/components/Warehouse/IncomeForm';
import { PackForm } from '@/components/Warehouse/PackForm';
import {
  warehouseAPI,
  type ExportJobStatus,
  type Transaction,
  type WarehouseOverview,
  type WarehouseOverviewProduct,
} from '@/lib/api/warehouse';
import { useStoreContext } from '@/lib/context/StoreContext';
import { compareVariantPresentation } from '@/lib/utils/variantPresentation';

const transactionNames: Record<string, string> = {
  INCOME: 'Приход',
  PACK: 'Упаковка',
  RESERVE: 'Резерв',
  SHIP: 'Отгрузка',
  UNRESERVE: 'Снятие резерва',
  RETURN: 'Возврат',
};

const transactionColors: Record<string, string> = {
  INCOME: 'bg-emerald-100 text-emerald-800',
  PACK: 'bg-sky-100 text-sky-800',
  RESERVE: 'bg-amber-100 text-amber-800',
  SHIP: 'bg-indigo-100 text-indigo-800',
  UNRESERVE: 'bg-orange-100 text-orange-800',
  RETURN: 'bg-slate-200 text-slate-700',
};

function formatDate(value?: string | null) {
  if (!value) {
    return '—';
  }

  return new Date(value).toLocaleString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatCurrency(value?: number | null) {
  if (value === null || value === undefined) {
    return '—';
  }

  return `${new Intl.NumberFormat('ru-RU', {
    maximumFractionDigits: value % 1 === 0 ? 0 : 2,
  }).format(value)} ₽`;
}

function formatInteger(value?: number | null) {
  if (value === null || value === undefined) {
    return '—';
  }
  return new Intl.NumberFormat('ru-RU', { maximumFractionDigits: 0 }).format(value);
}

function formatTransactionQuantity(tx: Transaction) {
  if (tx.type === 'PACK') {
    return `${tx.quantity} уп.`;
  }
  return `${tx.quantity} шт`;
}

function getOrderWindowLabel(days: number) {
  if (days >= 90) {
    return 'Заказано за 3 месяца';
  }
  if (days >= 30) {
    return 'Заказано за месяц';
  }
  return 'Заказано за неделю';
}

type GroupedTransactionGroup = {
  key: string;
  type: string;
  createdAt: string;
  products: Array<{
    productName: string;
    items: Transaction[];
  }>;
};

const heavySectionStyle = {
  contentVisibility: 'auto',
  containIntrinsicSize: '420px',
} as const;

const ProductCard = memo(function ProductCard({
  product,
  warehouseHeader,
  storeHeader,
  orderWindowDays,
}: {
  product: WarehouseOverviewProduct;
  warehouseHeader: string;
  storeHeader: string;
  orderWindowDays: number;
}) {
  const allVariants = product.colors.flatMap((group) => group.variants);
  const showColorGroups = product.colors.some((group) => {
    const normalized = String(group.color || '').trim().toLowerCase();
    return normalized && normalized !== 'без цвета';
  });
  const showSizeColumn = allVariants.some((variant) => String(variant.size || '').trim() !== '');
  const showPackColumn = allVariants.some((variant) => Number(variant.pack_size || 1) > 1);
  const warehouseColumnCount = 4 + (showSizeColumn ? 1 : 0) + (showPackColumn ? 1 : 0);
  const totalColumnCount = warehouseColumnCount + 7 + 1;
  const displayColorGroups = showColorGroups
    ? product.colors
    : [
        {
          color: '',
          variants: allVariants.slice(),
        },
      ];

  return (
    <article
      className="overflow-hidden rounded-[26px] border border-slate-200 bg-white shadow-[0_12px_28px_rgba(15,23,42,0.06)]"
      style={heavySectionStyle}
    >
      <div className="flex flex-col gap-3 border-b border-slate-200 bg-slate-50/90 px-4 py-3 lg:flex-row lg:items-start lg:justify-between">
        <div className="min-w-0">
          <h2 className="text-base font-semibold text-slate-950">{product.product_name}</h2>
          <div className="mt-2 flex flex-wrap gap-1.5 text-[11px]">
            <span className="rounded-full border border-slate-200 bg-white px-2.5 py-1 font-medium text-slate-700">
              Неупак.: {formatInteger(product.warehouse_unpacked)}
            </span>
            <span className="rounded-full border border-slate-200 bg-white px-2.5 py-1 font-medium text-slate-700">
              Резерв: {formatInteger(product.warehouse_reserved)}
            </span>
            <span className="rounded-full border border-emerald-200 bg-emerald-50 px-2.5 py-1 font-medium text-emerald-700">
              Доступно: {formatInteger(product.warehouse_available)}
            </span>
            <span className="rounded-full border border-sky-200 bg-sky-50 px-2.5 py-1 font-medium text-sky-700">
              OZON: {formatInteger(product.ozon_available)}
            </span>
            <span className="rounded-full border border-violet-200 bg-violet-50 px-2.5 py-1 font-medium text-violet-700">
              {getOrderWindowLabel(orderWindowDays)}: {formatInteger(product.ordered_units)}
            </span>
            <span className="rounded-full border border-amber-200 bg-amber-50 px-2.5 py-1 font-medium text-amber-700">
              Всего товара: {formatInteger(product.total_units)}
            </span>
          </div>
        </div>
      </div>

      <div className="overflow-x-auto overflow-y-visible px-2 py-2">
        <table className="w-max min-w-max border-collapse">
          <thead>
            <tr>
              <th colSpan={warehouseColumnCount} className="border-b border-r border-emerald-200 bg-emerald-50 px-2 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.1em] text-emerald-700">
                {warehouseHeader}
              </th>
              <th colSpan={7} className="border-b border-r border-sky-200 bg-sky-50 px-2 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.1em] text-sky-700">
                {storeHeader}
              </th>
              <th className="border-b border-amber-200 bg-amber-50 px-2 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.1em] text-amber-700">
                Общее
              </th>
            </tr>
            <tr>
              <th className="w-px whitespace-nowrap border border-emerald-200 bg-emerald-50 px-2 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.1em] text-emerald-700">
                Артикул
              </th>
              {showSizeColumn ? (
                <th className="w-[68px] min-w-[68px] border border-emerald-200 bg-emerald-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.08em] text-emerald-700">
                  Размер
                </th>
              ) : null}
              {showPackColumn ? (
                <th className="w-[52px] min-w-[52px] border border-emerald-200 bg-emerald-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.08em] text-emerald-700">
                  Уп.
                </th>
              ) : null}
              <th className="w-[64px] min-w-[64px] border border-emerald-200 bg-emerald-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.08em] text-emerald-700">
                Неупак.
              </th>
              <th className="w-[64px] min-w-[64px] border border-emerald-200 bg-emerald-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.08em] text-emerald-700">
                Резерв
              </th>
              <th className="w-[68px] min-w-[68px] border border-emerald-200 bg-emerald-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.08em] text-emerald-700">
                Доступн
              </th>
              <th className="w-[64px] min-w-[64px] border border-sky-200 bg-sky-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.08em] text-sky-700">
                OZON
              </th>
              <th className="w-[78px] min-w-[78px] border border-sky-200 bg-sky-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.05em] text-sky-700">
                Готовим к продаже
              </th>
              <th className="w-[82px] min-w-[82px] border border-sky-200 bg-sky-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.05em] text-sky-700">
                В заявках на поставку
              </th>
              <th className="w-[82px] min-w-[82px] border border-sky-200 bg-sky-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.05em] text-sky-700">
                В поставках в пути
              </th>
              <th className="w-[88px] min-w-[88px] border border-sky-200 bg-sky-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.05em] text-sky-700">
                Возвращаются от покупателей
              </th>
              <th className="w-[78px] min-w-[78px] border border-sky-200 bg-sky-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.05em] text-sky-700">
                {getOrderWindowLabel(orderWindowDays)}
              </th>
              <th className="w-[88px] min-w-[88px] border border-sky-200 bg-sky-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.05em] text-sky-700">
                Текущая цена
              </th>
              <th className="w-[82px] min-w-[82px] border border-amber-200 bg-amber-50 px-1 py-2 text-center text-[10px] font-semibold uppercase tracking-[0.08em] text-amber-700">
                Общее кол-во товара
              </th>
            </tr>
          </thead>
          <tbody className="bg-white/70">
            {displayColorGroups.flatMap((colorGroup) => {
              const sizeGroups = colorGroup.variants
                .slice()
                .sort(compareVariantPresentation)
                .reduce<Array<{ size: string; items: typeof colorGroup.variants }>>((groups, variant) => {
                  if (!showSizeColumn) {
                    if (groups.length === 0) {
                      groups.push({ size: '', items: [] });
                    }
                    groups[0].items.push(variant);
                    return groups;
                  }

                  const size = variant.size || '—';
                  const lastGroup = groups[groups.length - 1];
                  if (lastGroup && lastGroup.size === size) {
                    lastGroup.items.push(variant);
                  } else {
                    groups.push({ size, items: [variant] });
                  }
                  return groups;
                }, []);

              return [
                ...(showColorGroups
                  ? [
                      <tr key={`color-${product.product_id}-${colorGroup.color}`} className="bg-[linear-gradient(90deg,rgba(239,246,255,0.92),rgba(248,250,252,0.92))]">
                        <td colSpan={totalColumnCount} className="border border-slate-200 px-2 py-1.5 text-left text-[10px] font-semibold uppercase tracking-[0.1em] text-sky-800">
                          Цвет: {colorGroup.color}
                        </td>
                      </tr>,
                    ]
                  : []),
                ...sizeGroups.flatMap((sizeGroup) =>
                  sizeGroup.items.map((variant, index) => (
                    <tr key={variant.variant_id} className="transition hover:bg-slate-50/80 [&>td:not(:last-child)]:border-r [&>td:not(:last-child)]:border-slate-200">
                      <td className="w-px whitespace-nowrap border border-slate-200 px-2 py-2 text-left text-[11px] text-slate-700">
                        <div className="font-medium text-slate-950">{variant.offer_id}</div>
                      </td>
                      {showSizeColumn && index === 0 ? (
                        <td
                          rowSpan={sizeGroup.items.length}
                          className="border border-slate-200 px-1 py-2 align-middle text-center text-[11px] text-slate-600"
                        >
                          {sizeGroup.size}
                        </td>
                      ) : null}
                      {showPackColumn ? (
                        <td className="border border-slate-200 px-1 py-2 text-center text-[11px] text-slate-600">{formatInteger(variant.pack_size)}</td>
                      ) : null}
                      <td className="border border-slate-200 px-1 py-2 text-center text-[11px] text-slate-900">{formatInteger(variant.warehouse_unpacked)}</td>
                      <td className="border border-slate-200 px-1 py-2 text-center text-[11px] text-slate-900">{formatInteger(variant.warehouse_reserved)}</td>
                      <td className="border border-slate-200 px-1 py-2 text-center text-[11px] font-medium text-emerald-700">{formatInteger(variant.warehouse_available)}</td>
                      <td className="border border-slate-200 px-1 py-2 text-center text-[11px] text-slate-900">{formatInteger(variant.ozon_available)}</td>
                      <td className="border border-slate-200 px-1 py-2 text-center text-[11px] text-sky-700">{formatInteger(variant.ozon_ready_for_sale)}</td>
                      <td className="border border-slate-200 px-1 py-2 text-center text-[11px] text-indigo-700">{formatInteger(variant.ozon_requested_to_supply)}</td>
                      <td className="border border-slate-200 px-1 py-2 text-center text-[11px] text-amber-700">{formatInteger(variant.ozon_in_transit)}</td>
                      <td className="border border-slate-200 px-1 py-2 text-center text-[11px] text-violet-700">{formatInteger(variant.ozon_returning)}</td>
                      <td className="border border-slate-200 px-1 py-2 text-center text-[11px] font-medium text-slate-700">{formatInteger(variant.ordered_units)}</td>
                      <td className="border border-slate-200 px-1 py-2 text-center text-[11px] text-slate-700">{formatCurrency(variant.current_price)}</td>
                      <td className="border border-slate-200 px-1 py-2 text-center text-[11px] font-semibold text-amber-700">{formatInteger(variant.total_units)}</td>
                    </tr>
                  ))
                ),
              ];
            })}
          </tbody>
        </table>
      </div>
    </article>
  );
});

export default function WarehousePage() {
  const { stores, selectedStore, selectedStoreId, isLoading: storesLoading } = useStoreContext();
  const [overview, setOverview] = useState<WarehouseOverview | null>(null);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(true);
  const [pageError, setPageError] = useState<string | null>(null);
  const [showIncomeForm, setShowIncomeForm] = useState(false);
  const [showPackForm, setShowPackForm] = useState(false);
  const [expandedTransactionGroups, setExpandedTransactionGroups] = useState<Record<string, boolean>>({});
  const [orderWindowDays, setOrderWindowDays] = useState(7);
  const [exportStatus, setExportStatus] = useState<ExportJobStatus | null>(null);
  const [isStartingExport, setIsStartingExport] = useState(false);
  const [isDownloadingExport, setIsDownloadingExport] = useState(false);
  const [isClearingExport, setIsClearingExport] = useState(false);

  const loadWarehousePage = useCallback(async () => {
    if (!selectedStoreId) {
      setOverview(null);
      setTransactions([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    setPageError(null);
    try {
      const [overviewData, transactionData] = await Promise.all([
        warehouseAPI.getOverview(selectedStoreId, orderWindowDays),
        warehouseAPI.getTransactions(selectedStoreId, undefined, 250),
      ]);
      setOverview(overviewData);
      setTransactions(transactionData);
      if (!overviewData.available_order_windows.includes(orderWindowDays)) {
        setOrderWindowDays(overviewData.available_order_windows.at(-1) ?? 7);
      }
    } catch {
      setOverview(null);
      setTransactions([]);
      setPageError('Не удалось загрузить склад активного магазина.');
      toast.error('Ошибка загрузки склада');
    } finally {
      setLoading(false);
    }
  }, [orderWindowDays, selectedStoreId]);

  useEffect(() => {
    void loadWarehousePage();
  }, [loadWarehousePage]);

  useEffect(() => {
    let cancelled = false;
    if (!selectedStoreId) {
      setExportStatus(null);
      return;
    }
    setExportStatus(null);

    void warehouseAPI.getExportStatus(selectedStoreId)
      .then((status) => {
        if (!cancelled) {
          setExportStatus(status);
        }
      })
      .catch(() => {});

    return () => {
      cancelled = true;
    };
  }, [selectedStoreId]);

  useEffect(() => {
    if (!selectedStoreId || !exportStatus || !['queued', 'running'].includes(exportStatus.status)) {
      return;
    }

    const timer = window.setInterval(async () => {
      try {
        const status = await warehouseAPI.getExportStatus(selectedStoreId);
        setExportStatus(status);
      } catch {}
    }, 2000);

    return () => window.clearInterval(timer);
  }, [exportStatus, selectedStoreId]);

  const handleStartExport = useCallback(async () => {
    if (!selectedStoreId) {
      toast.error('Сначала выбери магазин');
      return;
    }
    setIsStartingExport(true);
    try {
      const status = await warehouseAPI.startExport(selectedStoreId, orderWindowDays);
      setExportStatus(status);
      if (status.duplicate_request) {
        toast('Excel по складу уже формируется');
      } else if (status.status === 'queued' || status.status === 'running') {
        toast.success('Excel по складу поставлен в очередь');
      }
    } catch {
      toast.error('Не удалось запустить экспорт склада');
    } finally {
      setIsStartingExport(false);
    }
  }, [orderWindowDays, selectedStoreId]);

  const handleDownloadExport = useCallback(async () => {
    if (!selectedStoreId || !exportStatus?.file_name) {
      return;
    }
    setIsDownloadingExport(true);
    try {
      await warehouseAPI.downloadExport(selectedStoreId, exportStatus.file_name);
    } catch {
      toast.error('Не удалось скачать Excel по складу');
    } finally {
      setIsDownloadingExport(false);
    }
  }, [exportStatus?.file_name, selectedStoreId]);

  const handleClearExport = useCallback(async () => {
    if (!selectedStoreId) {
      return;
    }
    setIsClearingExport(true);
    try {
      const status = await warehouseAPI.clearExport(selectedStoreId);
      setExportStatus(status);
      toast.success('Выгрузки по складу очищены');
    } catch {
      toast.error('Не удалось очистить выгрузки по складу');
    } finally {
      setIsClearingExport(false);
    }
  }, [selectedStoreId]);

  const groupedTransactions = useMemo<GroupedTransactionGroup[]>(() => {
    const groups = new Map<string, { key: string; type: string; createdAt: string; items: Transaction[] }>();

    transactions.forEach((tx) => {
      const key = tx.batch_key;
      if (!groups.has(key)) {
        groups.set(key, {
          key,
          type: tx.type,
          createdAt: tx.created_at,
          items: [],
        });
      }
      groups.get(key)!.items.push(tx);
    });

    return Array.from(groups.values()).map((group) => {
      const products = new Map<string, Transaction[]>();
      group.items.forEach((tx) => {
        const productName = tx.product_name || 'Без названия';
        if (!products.has(productName)) {
          products.set(productName, []);
        }
        products.get(productName)!.push(tx);
      });

      return {
        key: group.key,
        type: group.type,
        createdAt: group.createdAt,
        products: Array.from(products.entries()).map(([productName, items]) => ({
          productName,
          items: items.slice().sort((left, right) => compareVariantPresentation(left, right)),
        })),
      };
    });
  }, [transactions]);

  const totals = useMemo(() => {
    if (!overview) {
      return {
        warehouseAvailable: 0,
        ozonAvailable: 0,
        pendingSupply: 0,
        orderedUnits: 0,
        totalUnits: 0,
      };
    }

    return overview.products.reduce(
      (acc, product) => {
        acc.warehouseAvailable += product.warehouse_available;
        acc.ozonAvailable += product.ozon_available;
        acc.pendingSupply += product.ozon_requested_to_supply;
        acc.orderedUnits += product.ordered_units;
        acc.totalUnits += product.total_units;
        return acc;
      },
      {
        warehouseAvailable: 0,
        ozonAvailable: 0,
        pendingSupply: 0,
        orderedUnits: 0,
        totalUnits: 0,
      },
    );
  }, [overview]);

  const handleDeleteTransaction = async (tx: Transaction) => {
    const isConfirmed = window.confirm('Точно удалить эту операцию? Остатки склада будут пересчитаны.');
    if (!isConfirmed) {
      return;
    }

    try {
      await warehouseAPI.deleteTransaction(tx.id);
      toast.success('Операция удалена');
      await loadWarehousePage();
    } catch {
      toast.error('Не удалось удалить операцию');
    }
  };

  const toggleTransactionGroup = (groupKey: string) => {
    setExpandedTransactionGroups((prev) => ({
      ...prev,
      [groupKey]: !prev[groupKey],
    }));
  };

  if ((loading && !overview) || storesLoading) {
    return (
      <ProtectedRoute>
        <Layout>
          <div className="flex h-64 items-center justify-center">
            <div className="h-12 w-12 animate-spin rounded-full border-b-2 border-primary-600" />
          </div>
        </Layout>
      </ProtectedRoute>
    );
  }

  return (
    <ProtectedRoute>
      <Layout>
        {stores.length === 0 ? (
          <div className="rounded-3xl border border-dashed border-slate-300 bg-white px-6 py-12 text-center shadow-sm">
            <h2 className="text-lg font-semibold text-slate-900">Склад появится после добавления магазина</h2>
            <p className="mt-2 text-sm text-slate-500">Когда добавишь магазин, здесь будут остатки, Ozon-статусы и движения товара по активному магазину.</p>
            <Link href="/stores" className="mt-5 inline-flex rounded-2xl bg-slate-950 px-4 py-3 text-sm font-medium text-white transition hover:bg-slate-800">
              Открыть управление магазинами
            </Link>
          </div>
        ) : pageError ? (
          <div className="rounded-3xl border border-amber-200 bg-amber-50 px-6 py-12 text-center shadow-sm">
            <h2 className="text-lg font-semibold text-amber-950">Склад временно недоступен</h2>
            <p className="mt-2 text-sm text-amber-800">{pageError}</p>
            <button
              type="button"
              onClick={() => void loadWarehousePage()}
              className="mt-5 inline-flex rounded-2xl bg-slate-950 px-4 py-3 text-sm font-medium text-white transition hover:bg-slate-800"
            >
              Повторить попытку
            </button>
          </div>
        ) : (
          <div className="space-y-8">
            <section className="rounded-[32px] border border-white/70 bg-[radial-gradient(circle_at_top_left,_rgba(255,255,255,0.96),_rgba(237,246,255,0.9)_35%,_rgba(240,249,255,0.86)_100%)] p-8 shadow-[0_24px_60px_rgba(15,23,42,0.08)]">
              <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
                <div className="max-w-3xl">
                  <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Склад по активному магазину</p>
                  <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-950 sm:text-4xl">
                    {selectedStore?.name || overview?.store_name || 'Активный магазин'}
                  </h1>
                  <p className="mt-3 text-sm leading-6 text-slate-600 sm:text-base">
                    Здесь видны наши остатки, текущий контур в Ozon и движение товара по активному магазину. Собственный склад: {overview?.warehouse_name || '—'}.
                  </p>
                  <div className="mt-4 flex flex-wrap gap-2 text-xs">
                    <span className="rounded-full border border-slate-200 bg-white px-3 py-1 font-medium text-slate-700">
                      Режим склада: {overview?.warehouse_scope === 'shared' ? 'общий' : 'отдельный'}
                    </span>
                    <span className="rounded-full border border-slate-200 bg-white px-3 py-1 font-medium text-slate-700">
                      Обновлено: {formatDate(overview?.orders_updated_at)}
                    </span>
                  </div>
                </div>

                <div className="flex flex-wrap gap-3">
                  <button
                    type="button"
                    onClick={() => setShowIncomeForm(true)}
                    disabled={!selectedStoreId}
                    className="btn-primary gap-2 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    <ArrowUpIcon className="h-5 w-5" />
                    Приход
                  </button>
                  {overview?.packing_mode === 'advanced' && (
                    <button
                      type="button"
                      onClick={() => setShowPackForm(true)}
                      disabled={!selectedStoreId}
                      className="btn-secondary gap-2 disabled:cursor-not-allowed disabled:opacity-60"
                    >
                      <ArchiveBoxIcon className="h-5 w-5" />
                      Упаковка
                    </button>
                  )}
                </div>
              </div>
            </section>

            <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-5">
              <article className="card">
                <div className="text-sm font-medium text-slate-500">Доступно на нашем складе</div>
                <div className="mt-3 text-3xl font-semibold text-slate-950">{formatInteger(totals.warehouseAvailable)}</div>
                <div className="mt-2 text-sm text-slate-500">Штук готовы к новым поставкам</div>
              </article>
              <article className="card">
                <div className="text-sm font-medium text-slate-500">Доступно в OZON</div>
                <div className="mt-3 text-3xl font-semibold text-slate-950">{formatInteger(totals.ozonAvailable)}</div>
                <div className="mt-2 text-sm text-slate-500">Активные остатки по магазину</div>
              </article>
              <article className="card">
                <div className="text-sm font-medium text-slate-500">В заявках на поставку</div>
                <div className="mt-3 text-3xl font-semibold text-slate-950">{formatInteger(totals.pendingSupply)}</div>
                <div className="mt-2 text-sm text-slate-500">По аналитике остатков Ozon</div>
              </article>
              <article className="card">
                <div className="text-sm font-medium text-slate-500">{getOrderWindowLabel(orderWindowDays)}</div>
                <div className="mt-3 text-3xl font-semibold text-slate-950">{formatInteger(totals.orderedUnits)}</div>
                <div className="mt-2 text-sm text-slate-500">По отчетам Ozon за выбранный период</div>
              </article>
              <article className="rounded-[28px] border border-sky-200 bg-sky-600 p-6 text-white shadow-[0_18px_44px_rgba(14,116,211,0.22)]">
                <div className="text-sm font-medium text-sky-100">Общее кол-во товара</div>
                <div className="mt-3 text-3xl font-semibold">{formatInteger(totals.totalUnits)}</div>
                <div className="mt-2 text-sm text-sky-100">Доступно на нашем складе + доступно в OZON</div>
              </article>
            </section>

            <ExportJobCard
              title="Excel по складу"
              description="Формируем фоновый Excel по текущему складу активного магазина. Когда отчет будет готов, здесь появится кнопка скачивания."
              status={exportStatus}
              onStart={handleStartExport}
              onDownload={handleDownloadExport}
              onClear={handleClearExport}
              isStarting={isStartingExport}
              isDownloading={isDownloadingExport}
              isClearing={isClearingExport}
            />

            <section className="space-y-4">
              <div className="flex flex-col gap-2 lg:flex-row lg:items-end lg:justify-between">
                <div>
                  <h2 className="text-xl font-semibold text-slate-950">Товары активного магазина</h2>
                  <p className="mt-1 text-sm text-slate-500">
                    Наш склад и Ozon собраны по активному магазину в одной строке товара. Текущая цена — из Ozon API по актуальной карточке товара.
                  </p>
                </div>
                <div className="flex flex-col items-start gap-2 lg:items-end">
                  <div className="w-full overflow-x-auto rounded-2xl border border-slate-200 bg-white p-2 shadow-sm lg:w-auto">
                    <div className="flex flex-nowrap gap-2">
                      {(overview?.available_order_windows ?? [7, 30]).map((days) => {
                        const isActive = days === orderWindowDays;
                        const label = days >= 90 ? '3 месяца' : days >= 30 ? 'Месяц' : 'Неделя';
                        return (
                          <button
                            key={days}
                            type="button"
                            onClick={() => setOrderWindowDays(days)}
                            className={`shrink-0 rounded-2xl px-4 py-2 text-sm font-medium transition ${
                              isActive
                                ? 'bg-sky-600 text-white shadow-sm'
                                : 'bg-slate-50 text-slate-700 hover:bg-slate-100'
                            }`}
                          >
                            {label}
                          </button>
                        );
                      })}
                    </div>
                  </div>
                </div>
              </div>

              {overview?.products.length ? (
                <div className="space-y-4">
                  {overview.products.map((product) => (
                  <ProductCard
                    key={product.product_id}
                    product={product}
                    warehouseHeader={overview.warehouse_name}
                    storeHeader={`${overview.store_name} OZON`}
                    orderWindowDays={orderWindowDays}
                  />
                  ))}
                </div>
              ) : (
                <div className="rounded-3xl border border-dashed border-slate-300 bg-white px-6 py-12 text-center text-sm text-slate-500 shadow-sm">
                  По активному магазину пока нет товаров с остатками, Ozon-остатками, заявками на поставку или заказами в выбранном окне.
                </div>
              )}
            </section>

            <section className="card overflow-hidden p-0">
              <div className="border-b border-slate-200/70 px-6 py-5">
                <h2 className="text-lg font-semibold text-slate-950">Последние операции</h2>
                <p className="mt-1 text-sm text-slate-500">Показываем операции активного магазина списком товаров. Для прихода и упаковки можно удалить строку и сразу откатить остаток.</p>
              </div>

              <div className="space-y-4 p-6">
                {groupedTransactions.map((group) => (
                  <article key={group.key} className="rounded-[24px] border border-slate-200 bg-slate-50/60">
                    <div className="flex items-center justify-between border-b border-slate-200 px-4 py-3">
                      <div className="text-sm text-slate-500">{formatDate(group.createdAt)}</div>
                      <span className={`inline-flex rounded-full px-3 py-1 text-xs font-medium ${transactionColors[group.type] || 'bg-slate-200 text-slate-700'}`}>
                        {transactionNames[group.type] || group.type}
                      </span>
                    </div>

                    <div className="space-y-4 p-4">
                      {group.products.map((product) => (
                        <div key={`${group.key}-${product.productName}`}>
                          <div className="overflow-hidden rounded-2xl border border-slate-200 bg-white">
                            <button
                              type="button"
                              onClick={() => toggleTransactionGroup(`${group.key}-${product.productName}`)}
                              className="flex w-full items-center justify-between gap-3 border-b border-slate-200 bg-slate-50 px-4 py-3 text-left transition hover:bg-slate-100/80"
                            >
                              <div className="min-w-0">
                                <div className="text-sm font-semibold text-slate-900">{product.productName}</div>
                                <div className="mt-1 text-xs text-slate-500">
                                  {product.items.length} вариаций
                                </div>
                              </div>
                              <div className="flex items-center gap-3">
                                <div className="text-sm font-medium text-slate-700">
                                  {product.items.reduce((sum, tx) => sum + tx.quantity, 0)}
                                  {group.type === 'PACK' ? ' уп.' : ' шт'}
                                </div>
                                {expandedTransactionGroups[`${group.key}-${product.productName}`] ? (
                                  <ChevronDownIcon className="h-5 w-5 text-slate-500" />
                                ) : (
                                  <ChevronRightIcon className="h-5 w-5 text-slate-500" />
                                )}
                              </div>
                            </button>

                            {expandedTransactionGroups[`${group.key}-${product.productName}`] && (
                              <>
                                <div className="grid grid-cols-[1.6fr_140px_56px] gap-3 border-b border-slate-200 bg-slate-50 px-4 py-3 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">
                                  <div>Цвет / Размер</div>
                                  <div className="text-right">Количество</div>
                                  <div />
                                </div>
                                <div className="max-h-[420px] overflow-y-auto divide-y divide-slate-100">
                                  {product.items.map((tx) => (
                                    <div key={tx.id} className="grid grid-cols-[1.6fr_140px_56px] items-center gap-3 px-4 py-3 text-sm text-slate-700">
                                      <div>
                                        <div className="font-medium text-slate-900">
                                          {(tx.color || 'Без цвета') + ' · ' + (tx.size || 'Без размера')}
                                        </div>
                                        <div className="mt-1 text-xs text-slate-500">
                                          Артикул: {tx.offer_id}
                                          {tx.type === 'PACK' ? ` · ${tx.pack_size} шт в упаковке` : ''}
                                        </div>
                                      </div>
                                      <div className="text-right font-medium text-slate-900">{formatTransactionQuantity(tx)}</div>
                                      <div className="flex justify-end">
                                        {tx.can_delete ? (
                                          <button
                                            type="button"
                                            onClick={() => void handleDeleteTransaction(tx)}
                                            className="inline-flex h-9 w-9 items-center justify-center rounded-xl border border-rose-200 bg-rose-50 text-rose-600 transition hover:bg-rose-100"
                                            title="Удалить операцию"
                                          >
                                            <TrashIcon className="h-4 w-4" />
                                          </button>
                                        ) : (
                                          <span className="text-xs text-slate-300">—</span>
                                        )}
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              </>
                            )}
                          </div>
                        </div>
                      ))}
                    </div>
                  </article>
                ))}

                {groupedTransactions.length === 0 && (
                  <div className="px-6 py-10 text-center text-sm text-slate-500">
                    Операций по активному магазину пока нет.
                  </div>
                )}
              </div>
            </section>

            {showIncomeForm && (
              <IncomeForm
                storeId={selectedStoreId ?? undefined}
                onClose={() => setShowIncomeForm(false)}
                onSuccess={() => {
                  void loadWarehousePage();
                }}
              />
            )}

            {showPackForm && (
              <PackForm
                storeId={selectedStoreId ?? undefined}
                onClose={() => setShowPackForm(false)}
                onSuccess={() => {
                  void loadWarehousePage();
                }}
              />
            )}
          </div>
        )}
      </Layout>
    </ProtectedRoute>
  );
}
