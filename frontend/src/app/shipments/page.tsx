'use client';

import Link from 'next/link';
import { useCallback, useDeferredValue, useEffect, useMemo, useRef, useState } from 'react';
import {
  ClipboardDocumentListIcon,
  ChevronDownIcon,
  ChevronUpIcon,
  CubeIcon,
  MagnifyingGlassIcon,
  PaperAirplaneIcon,
  XMarkIcon,
} from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { ExportJobCard } from '@/components/ExportJobCard';
import { Layout } from '@/components/Layout';
import { SyncFreshnessPanel } from '@/components/SyncFreshnessPanel';
import { getSupplyStatusLabel, getSupplyStatusStyle } from '@/lib/constants/supplyStatus';
import { suppliesAPI, type SupplyItem } from '@/lib/api/supplies';
import { useStoreContext } from '@/lib/context/StoreContext';
import { shipmentsAPI, type ShipmentCluster, type ShipmentSupplySummary, type ShipmentWarehouse } from '@/lib/api/shipments';
import type { ExportJobStatus } from '@/lib/api/warehouse';
import {
  getVariantCharacteristicText,
  getVariantDisplayTitle,
  groupVariantEntries,
} from '@/lib/utils/variantPresentation';

function formatTime(value?: string | null) {
  if (!value) return '-';
  return new Date(value).toLocaleTimeString('ru-RU', {
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatTimeslotRange(from?: string | null, to?: string | null) {
  if (!from) return '-';
  const fromLabel = formatTime(from);
  const toLabel = to ? formatTime(to) : null;
  return toLabel ? `${fromLabel} - ${toLabel}` : fromLabel;
}

function formatTimeslotWithDate(from?: string | null, to?: string | null) {
  if (!from) return '-';
  const dateLabel = new Date(from).toLocaleDateString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
  });
  return `${dateLabel} · ${formatTimeslotRange(from, to)}`;
}

function getShortOrderNumber(orderNumber: string) {
  const digits = orderNumber.replace(/\D/g, '');
  return digits ? digits.slice(-4) : orderNumber.slice(-4);
}

function formatEtaLabel(item: Pick<ShipmentSupplySummary, 'eta_date'>, avgDays?: number) {
  if (!item.eta_date) return avgDays ? `~ ${avgDays} дн.` : '-';
  const etaDate = new Date(`${item.eta_date}T00:00:00`);
  const dateLabel = etaDate.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' });
  if (typeof avgDays === 'number' && avgDays > 0) {
    return `${dateLabel} (${avgDays} дн.)`;
  }
  return dateLabel;
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

type SelectedWarehouseDetails = {
  clusterName: string;
  warehouse: ShipmentWarehouse;
};

export default function ShipmentsPage() {
  const { selectedStore, selectedStoreId, stores, isLoading: storesLoading } = useStoreContext();
  const [clusters, setClusters] = useState<ShipmentCluster[]>([]);
  const [ourWarehouseName, setOurWarehouseName] = useState<string | null>(null);
  const [packingMode, setPackingMode] = useState<'simple' | 'advanced' | null>(null);
  const [dataSource, setDataSource] = useState<string | null>(null);
  const [dataNote, setDataNote] = useState<string | null>(null);
  const [ordersDataNote, setOrdersDataNote] = useState<string | null>(null);
  const [ordersPeriodFrom, setOrdersPeriodFrom] = useState<string | null>(null);
  const [ordersPeriodTo, setOrdersPeriodTo] = useState<string | null>(null);
  const [ordersUpdatedAt, setOrdersUpdatedAt] = useState<string | null>(null);
  const [orderWindowDays, setOrderWindowDays] = useState(30);
  const [availableOrderWindows, setAvailableOrderWindows] = useState<number[]>([7, 30]);
  const [selectedWarehouseDetails, setSelectedWarehouseDetails] = useState<SelectedWarehouseDetails | null>(null);
  const [openedWarehouseSupplyId, setOpenedWarehouseSupplyId] = useState<number | null>(null);
  const [supplyItemsMap, setSupplyItemsMap] = useState<Record<number, SupplyItem[]>>({});
  const [loadingSupplyItems, setLoadingSupplyItems] = useState<Record<number, boolean>>({});
  const [productFilter, setProductFilter] = useState('');
  const [selectedProductNames, setSelectedProductNames] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [collapsedClusters, setCollapsedClusters] = useState<Record<string, boolean>>({});
  const [scrollHints, setScrollHints] = useState<Record<string, { left: boolean; right: boolean }>>({});
  const [exportStatus, setExportStatus] = useState<ExportJobStatus | null>(null);
  const [isStartingExport, setIsStartingExport] = useState(false);
  const [isDownloadingExport, setIsDownloadingExport] = useState(false);
  const [isClearingExport, setIsClearingExport] = useState(false);
  const scrollRefs = useRef<Record<string, HTMLDivElement | null>>({});
  const clusterSectionRefs = useRef<Record<string, HTMLElement | null>>({});
  const deferredProductFilter = useDeferredValue(productFilter);
  const loadShipments = useCallback(async () => {
    if (!selectedStoreId) {
      setClusters([]);
      setOurWarehouseName(null);
      setPackingMode(null);
      setDataSource(null);
      setDataNote(null);
      setOrdersDataNote(null);
      setOrdersPeriodFrom(null);
      setOrdersPeriodTo(null);
      setOrdersUpdatedAt(null);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      const data = await shipmentsAPI.getShipments(selectedStoreId, orderWindowDays);
      setClusters(data.clusters);
      setOurWarehouseName(data.our_warehouse_name);
      setPackingMode(data.packing_mode);
      setAvailableOrderWindows(data.available_order_windows.length ? data.available_order_windows : [7, 30]);
      setDataSource(data.data_source ?? null);
      setDataNote(data.data_note ?? null);
      setOrdersDataNote(data.orders_data_note ?? null);
      setOrdersPeriodFrom(data.orders_period_from ?? null);
      setOrdersPeriodTo(data.orders_period_to ?? null);
      setOrdersUpdatedAt(data.orders_updated_at ?? null);
      if (!data.available_order_windows.includes(orderWindowDays)) {
        setOrderWindowDays(data.available_order_windows.at(-1) ?? 30);
      }
    } catch {
      toast.error('Ошибка загрузки отправок');
    } finally {
      setLoading(false);
    }
  }, [orderWindowDays, selectedStoreId]);

  const ourStockLabel = useMemo(() => {
    if (!ourWarehouseName) {
      return 'Наш';
    }
    return ourWarehouseName;
  }, [ourWarehouseName]);

  const normalizedProductFilter = useMemo(
    () => deferredProductFilter.trim().toLowerCase(),
    [deferredProductFilter]
  );

  const productOptions = useMemo(() => {
    const productMap = new Map<string, { name: string; offers: Set<string> }>();

    for (const cluster of clusters) {
      for (const warehouse of cluster.warehouses) {
        for (const product of warehouse.products) {
          const existing = productMap.get(product.product_name) ?? {
            name: product.product_name,
            offers: new Set<string>(),
          };
          for (const variant of product.variants) {
            if (variant.offer_id) {
              existing.offers.add(variant.offer_id);
            }
          }
          productMap.set(product.product_name, existing);
        }
      }
    }

    return Array.from(productMap.values())
      .filter((product) => {
        if (!normalizedProductFilter) {
          return true;
        }
        if (product.name.toLowerCase().includes(normalizedProductFilter)) {
          return true;
        }
        return Array.from(product.offers).some((offerId) =>
          offerId.toLowerCase().includes(normalizedProductFilter)
        );
      })
      .sort((left, right) => left.name.localeCompare(right.name, 'ru', { sensitivity: 'base' }));
  }, [clusters, normalizedProductFilter]);

  const filteredClusters = useMemo(() => {
    if (!normalizedProductFilter && selectedProductNames.length === 0) {
      return clusters
        .map((cluster) => {
          const visibleWarehouses = cluster.warehouses.filter((warehouse) => {
            const normalizedName = String(warehouse.name || '').trim().toLowerCase();
            const looksUnknown = !warehouse.ozon_id || normalizedName === 'unknown';
            const hasOzonSignal =
              warehouse.total_ozon_available > 0 ||
              warehouse.total_in_pipeline > 0 ||
              warehouse.total_ordered_30d > 0 ||
              warehouse.pending_departure_supplies.length > 0 ||
              warehouse.in_transit_supplies.length > 0;
            return !looksUnknown || hasOzonSignal;
          });

          if (!visibleWarehouses.length) {
            return null;
          }

          return {
            ...cluster,
            warehouses: visibleWarehouses,
            total_ozon_available: visibleWarehouses.reduce((sum, warehouse) => sum + warehouse.total_ozon_available, 0),
            total_in_pipeline: visibleWarehouses.reduce((sum, warehouse) => sum + warehouse.total_in_pipeline, 0),
            total_ordered_30d: visibleWarehouses.reduce((sum, warehouse) => sum + warehouse.total_ordered_30d, 0),
          };
        })
        .filter(Boolean) as ShipmentCluster[];
    }

    const selectedNamesSet = new Set(selectedProductNames);

    return clusters
      .map((cluster) => {
        const filteredWarehouses = cluster.warehouses
          .filter((warehouse) => {
            const normalizedName = String(warehouse.name || '').trim().toLowerCase();
            const looksUnknown = !warehouse.ozon_id || normalizedName === 'unknown';
            const hasOzonSignal =
              warehouse.total_ozon_available > 0 ||
              warehouse.total_in_pipeline > 0 ||
              warehouse.total_ordered_30d > 0 ||
              warehouse.pending_departure_supplies.length > 0 ||
              warehouse.in_transit_supplies.length > 0;
            return !looksUnknown || hasOzonSignal;
          })
          .map((warehouse) => {
            const filteredProducts = warehouse.products.filter((product) => {
              const selectedMatch = selectedNamesSet.size === 0 || selectedNamesSet.has(product.product_name);
              if (!selectedMatch) {
                return false;
              }
              if (!normalizedProductFilter) {
                return true;
              }
              const productNameMatch = product.product_name.toLowerCase().includes(normalizedProductFilter);
              const offerMatch = product.variants.some((variant) =>
                (variant.offer_id || '').toLowerCase().includes(normalizedProductFilter)
              );
              return productNameMatch || offerMatch;
            });

            if (!filteredProducts.length) {
              return null;
            }

            return {
              ...warehouse,
              products: filteredProducts,
              total_ozon_available: filteredProducts.reduce(
                (sum, product) => sum + product.variants.reduce((inner, variant) => inner + variant.ozon_available, 0),
                0
              ),
              total_in_pipeline: filteredProducts.reduce(
                (sum, product) => sum + product.variants.reduce((inner, variant) => inner + variant.ozon_in_pipeline, 0),
                0
              ),
              total_ordered_30d: filteredProducts.reduce(
                (sum, product) => sum + product.variants.reduce((inner, variant) => inner + variant.ordered_30d, 0),
                0
              ),
            };
          })
          .filter(Boolean) as ShipmentWarehouse[];

        if (!filteredWarehouses.length) {
          return null;
        }

        return {
          ...cluster,
          warehouses: filteredWarehouses,
          total_ozon_available: filteredWarehouses.reduce((sum, warehouse) => sum + warehouse.total_ozon_available, 0),
          total_in_pipeline: filteredWarehouses.reduce((sum, warehouse) => sum + warehouse.total_in_pipeline, 0),
          total_ordered_30d: filteredWarehouses.reduce((sum, warehouse) => sum + warehouse.total_ordered_30d, 0),
        };
      })
      .filter(Boolean) as ShipmentCluster[];
  }, [clusters, normalizedProductFilter, selectedProductNames]);

  useEffect(() => {
    setCollapsedClusters((prev) => {
      const next: Record<string, boolean> = {};
      for (const cluster of filteredClusters) {
        next[cluster.key] = prev[cluster.key] ?? true;
      }
      return next;
    });
  }, [filteredClusters]);

  useEffect(() => {
    void loadShipments();
  }, [loadShipments]);

  useEffect(() => {
    let cancelled = false;
    if (!selectedStoreId) {
      setExportStatus(null);
      return;
    }
    setExportStatus(null);

    void shipmentsAPI.getExportStatus(selectedStoreId)
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
        const status = await shipmentsAPI.getExportStatus(selectedStoreId);
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
      const status = await shipmentsAPI.startExport(selectedStoreId, orderWindowDays, {
        productFilter,
        selectedProductNames,
      });
      setExportStatus(status);
      if (status.duplicate_request) {
        toast('Excel по отправкам уже формируется');
      } else if (status.status === 'queued' || status.status === 'running') {
        toast.success('Excel по отправкам поставлен в очередь');
      }
    } catch {
      toast.error('Не удалось запустить экспорт отправок');
    } finally {
      setIsStartingExport(false);
    }
  }, [orderWindowDays, productFilter, selectedProductNames, selectedStoreId]);

  const handleDownloadExport = useCallback(async () => {
    if (!selectedStoreId || !exportStatus?.file_name) {
      return;
    }
    setIsDownloadingExport(true);
    try {
      await shipmentsAPI.downloadExport(selectedStoreId, exportStatus.file_name);
    } catch {
      toast.error('Не удалось скачать Excel по отправкам');
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
      const status = await shipmentsAPI.clearExport(selectedStoreId);
      setExportStatus(status);
      toast.success('Выгрузки по отправкам очищены');
    } catch {
      toast.error('Не удалось очистить выгрузки по отправкам');
    } finally {
      setIsClearingExport(false);
    }
  }, [selectedStoreId]);

  useEffect(() => {
    setOpenedWarehouseSupplyId(null);
  }, [selectedWarehouseDetails?.warehouse.id]);

  const toggleCluster = (clusterId: string) => {
    setCollapsedClusters((prev) => {
      const next: Record<string, boolean> = {};
      const willOpen = prev[clusterId];

      for (const key of Object.keys(prev)) {
        next[key] = true;
      }

      next[clusterId] = !willOpen;
      return next;
    });

    const isOpening = collapsedClusters[clusterId] ?? true;
    if (isOpening) {
      requestAnimationFrame(() => {
        const sectionNode = clusterSectionRefs.current[clusterId];
        if (!sectionNode) {
          return;
        }
        const top = sectionNode.getBoundingClientRect().top + window.scrollY - 112;
        window.scrollTo({ top: Math.max(top, 0), behavior: 'smooth' });
      });
    }
  };

  const updateScrollHint = useCallback((clusterId: string) => {
    const node = scrollRefs.current[clusterId];
    if (!node) {
      return;
    }

    const left = node.scrollLeft > 8;
    const right = node.scrollLeft + node.clientWidth < node.scrollWidth - 8;
    setScrollHints((prev) => ({
      ...prev,
      [clusterId]: { left, right },
    }));
  }, []);

  const attachScrollNode = useCallback((clusterId: string, node: HTMLDivElement | null) => {
    scrollRefs.current[clusterId] = node;
    if (node) {
      requestAnimationFrame(() => updateScrollHint(clusterId));
    }
  }, [updateScrollHint]);

  const stats = useMemo(() => {
    const warehouseCount = filteredClusters.reduce((sum, cluster) => sum + cluster.warehouses.length, 0);
    const productCount = filteredClusters.reduce(
      (sum, cluster) => sum + cluster.warehouses.reduce((inner, warehouse) => inner + warehouse.products.length, 0),
      0
    );
    const totalAvailable = filteredClusters.reduce((sum, cluster) => sum + cluster.total_ozon_available, 0);
    const totalPipeline = filteredClusters.reduce((sum, cluster) => sum + cluster.total_in_pipeline, 0);
    const totalOrdered30d = filteredClusters.reduce((sum, cluster) => sum + cluster.total_ordered_30d, 0);
    const avgDelivery = (() => {
      const values = filteredClusters.flatMap((cluster) => cluster.warehouses.map((warehouse) => warehouse.avg_delivery_days));
      if (!values.length) {
        return '-';
      }
      return (values.reduce((sum, value) => sum + value, 0) / values.length).toFixed(1);
    })();

    return { warehouseCount, productCount, totalAvailable, totalPipeline, totalOrdered30d, avgDelivery };
  }, [filteredClusters]);

  const toggleSelectedProduct = useCallback((productName: string) => {
    setSelectedProductNames((current) =>
      current.includes(productName)
        ? current.filter((name) => name !== productName)
        : [...current, productName]
    );
  }, []);

  const clearProductFilters = useCallback(() => {
    setProductFilter('');
    setSelectedProductNames([]);
  }, []);

  const warehouseCardStyle = packingMode === 'advanced'
    ? {
        width: 'max-content',
        minWidth: '760px',
        maxWidth: '840px',
      }
    : {
        width: 'max-content',
        minWidth: '640px',
        maxWidth: '700px',
      };

  const selectedWarehouseSupplyCount = selectedWarehouseDetails
    ? selectedWarehouseDetails.warehouse.pending_departure_supplies.length + selectedWarehouseDetails.warehouse.in_transit_supplies.length
    : 0;

  const toggleWarehouseSupply = useCallback(async (supplyId: number) => {
    if (openedWarehouseSupplyId === supplyId) {
      setOpenedWarehouseSupplyId(null);
      return;
    }

    setOpenedWarehouseSupplyId(supplyId);
    if (supplyItemsMap[supplyId] || loadingSupplyItems[supplyId]) {
      return;
    }

    setLoadingSupplyItems((prev) => ({ ...prev, [supplyId]: true }));
    try {
      const items = await suppliesAPI.getSupplyItems(supplyId);
      setSupplyItemsMap((prev) => ({ ...prev, [supplyId]: items }));
    } catch {
      toast.error('Не удалось загрузить состав накладной');
    } finally {
      setLoadingSupplyItems((prev) => ({ ...prev, [supplyId]: false }));
    }
  }, [loadingSupplyItems, openedWarehouseSupplyId, supplyItemsMap]);

  return (
    <ProtectedRoute>
      <Layout>
        {storesLoading ? (
          <div className="card px-6 py-12 text-center text-sm text-slate-500">Загрузка магазинов...</div>
        ) : stores.length === 0 ? (
          <div className="rounded-3xl border border-dashed border-slate-300 bg-white px-6 py-12 text-center shadow-sm">
            <h2 className="text-lg font-semibold text-slate-900">Сначала подключим магазин</h2>
            <p className="mt-2 text-sm text-slate-500">После добавления магазина здесь появятся его кластеры, склады OZON и остатки для отправки.</p>
            <Link href="/stores" className="mt-5 inline-flex rounded-2xl bg-slate-950 px-4 py-3 text-sm font-medium text-white transition hover:bg-slate-800">
              Открыть управление магазинами
            </Link>
          </div>
        ) : (
        <div className="space-y-8">
          <section className="rounded-[32px] border border-white/70 bg-[radial-gradient(circle_at_top_left,_rgba(255,255,255,0.96),_rgba(239,246,255,0.92)_36%,_rgba(240,249,255,0.88)_100%)] p-8 shadow-[0_24px_60px_rgba(15,23,42,0.08)]">
            <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
              <div className="max-w-3xl">
                <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Отправки</p>
                <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-950 sm:text-4xl">
                  Кластеры, склады OZON и наш запас в одном срезе
                </h1>
                <p className="mt-3 text-sm leading-6 text-slate-600 sm:text-base">
                  Страница помогает решать, куда и что отправлять, с опорой на среднее время доставки и остатки.
                </p>
              </div>
              <div className="card max-w-sm p-4">
                <div className="text-xs font-semibold uppercase tracking-[0.18em] text-sky-700">Активный магазин</div>
                <div className="mt-2 text-lg font-semibold text-slate-950">{selectedStore?.name || 'Не выбран'}</div>
                <div className="mt-1 text-sm text-slate-500">
                  Сводка строится только по нему.
                  {ourWarehouseName ? ` Наш склад учёта: ${ourWarehouseName}.` : ''}
                </div>
                {dataSource === 'pipeline_only' && dataNote ? (
                  <div className="mt-3 rounded-2xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs leading-5 text-amber-800">
                    {dataNote}
                  </div>
                ) : null}
                {ordersDataNote ? (
                  <div className="mt-3 rounded-2xl border border-amber-200 bg-amber-50 px-3 py-2 text-xs leading-5 text-amber-800">
                    {ordersDataNote}
                  </div>
                ) : null}
              </div>
            </div>
          </section>

          <SyncFreshnessPanel
            storeIds={selectedStoreId ? [selectedStoreId] : []}
            kinds={['supplies', 'stocks', 'reports']}
            title="Отправки, остатки OZON и товары в пути"
            description="Экран отправок опирается на поставки, остатки и отчет заказов Ozon, поэтому здесь важна свежесть всех трех синхронизаций по выбранному магазину."
          />

          <section className="card p-4 sm:p-5">
            <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
              <div>
                <div className="text-sm font-semibold text-slate-900">Фильтр по товарам</div>
                <div className="mt-1 text-sm text-slate-500">
                  Ищи по названию товара или по артикулу `offer_id`. Пустые склады и кластеры автоматически скрываются.
                </div>
              </div>
              <div className="flex w-full max-w-3xl flex-col gap-3 lg:items-end">
                <div className="rounded-2xl border border-slate-200 bg-white p-2 shadow-sm">
                  <div className="flex flex-wrap gap-2">
                    {availableOrderWindows.map((days) => {
                      const isActive = days === orderWindowDays;
                      const label = days >= 90 ? '3 месяца' : days >= 30 ? 'Месяц' : 'Неделя';
                      return (
                        <button
                          key={days}
                          type="button"
                          onClick={() => setOrderWindowDays(days)}
                          className={`rounded-2xl px-4 py-2 text-sm font-medium transition ${
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
                <div className="flex w-full items-center gap-2 rounded-2xl border border-slate-200 bg-slate-50 px-3 py-2">
                  <MagnifyingGlassIcon className="h-5 w-5 flex-none text-slate-400" />
                  <input
                    value={productFilter}
                    onChange={(event) => setProductFilter(event.target.value)}
                    placeholder="Например: Носки или белые/10пар/36-41"
                    className="w-full bg-transparent text-sm text-slate-900 outline-none placeholder:text-slate-400"
                  />
                  {productFilter ? (
                    <button
                      type="button"
                      onClick={() => setProductFilter('')}
                      className="rounded-full border border-slate-200 bg-white px-2.5 py-1 text-xs font-medium text-slate-600 transition hover:border-slate-300 hover:text-slate-900"
                    >
                      Очистить поиск
                    </button>
                  ) : null}
                </div>
              </div>
            </div>
            {selectedProductNames.length > 0 ? (
              <div className="mt-4">
                <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                  Выбраны товары
                </div>
                <div className="flex flex-wrap items-center gap-2">
                  {selectedProductNames.map((productName) => (
                    <button
                      key={productName}
                      type="button"
                      onClick={() => toggleSelectedProduct(productName)}
                      className="inline-flex items-center gap-2 rounded-full border border-sky-300 bg-sky-50 px-3 py-1.5 text-sm font-medium text-sky-800"
                    >
                      <span>{productName}</span>
                      <span className="text-sky-500">×</span>
                    </button>
                  ))}
                  <button
                    type="button"
                    onClick={clearProductFilters}
                    className="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-sm text-slate-600 transition hover:border-slate-300 hover:text-slate-900"
                  >
                    Сбросить все
                  </button>
                </div>
              </div>
            ) : null}
            {productOptions.length > 0 ? (
              <div className="mt-4">
                <div className="mb-2 text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-400">
                  Все товары
                </div>
                <div className="flex flex-wrap gap-2">
                  {productOptions.map((product) => {
                    const isActive = selectedProductNames.includes(product.name);
                    return (
                      <button
                        key={product.name}
                        type="button"
                        onClick={() => toggleSelectedProduct(product.name)}
                        className={`rounded-full border px-3 py-1.5 text-sm transition ${
                          isActive
                            ? 'border-sky-300 bg-sky-50 font-medium text-sky-800'
                            : 'border-slate-200 bg-white text-slate-700 hover:border-slate-300 hover:text-slate-950'
                        }`}
                        title={Array.from(product.offers).slice(0, 6).join(', ')}
                      >
                        {product.name}
                      </button>
                    );
                  })}
                </div>
              </div>
            ) : null}
          </section>

          <section className="grid gap-4 md:grid-cols-2 2xl:grid-cols-4">
            <article className="card">
              <div className="text-sm font-medium text-slate-500">Кластеров</div>
              <div className="mt-3 text-3xl font-semibold text-slate-950">{filteredClusters.length}</div>
              <div className="mt-2 text-sm text-slate-500">
                {normalizedProductFilter ? 'После фильтрации' : 'В выборке магазина'}
              </div>
            </article>
            <article className="card">
              <div className="text-sm font-medium text-slate-500">Складов OZON</div>
              <div className="mt-3 text-3xl font-semibold text-slate-950">{stats.warehouseCount}</div>
              <div className="mt-2 text-sm text-slate-500">Доступны для анализа</div>
            </article>
            <article className="card">
              <div className="text-sm font-medium text-slate-500">В поставках в пути</div>
              <div className="mt-3 text-3xl font-semibold text-slate-950">{stats.totalPipeline}</div>
              <div className="mt-2 text-sm text-slate-500">По аналитике остатков OZON для выбранных складов</div>
            </article>
            <article className="rounded-[28px] border border-sky-200 bg-sky-600 p-6 text-white shadow-[0_18px_44px_rgba(14,116,211,0.22)]">
              <div className="text-sm font-medium text-sky-100">Средняя доставка</div>
              <div className="mt-3 text-3xl font-semibold">{stats.avgDelivery}</div>
              <div className="mt-2 text-sm text-sky-100">дн. по активным складам</div>
            </article>
          </section>

          <ExportJobCard
            title="Excel по отправкам"
            description="Формируем Excel по текущей странице отправок в фоне. Каждый кластер попадет в отдельный лист, а внутри сохранятся блоки складов и товаров."
            status={exportStatus}
            onStart={handleStartExport}
            onDownload={handleDownloadExport}
            onClear={handleClearExport}
            isStarting={isStartingExport}
            isDownloading={isDownloadingExport}
            isClearing={isClearingExport}
          />

          {loading ? (
            <div className="card px-6 py-12 text-center text-sm text-slate-500">Загрузка отправок...</div>
          ) : (
            <div className="space-y-6">
              {clusters.length === 0 && (
                <div className="card px-6 py-12 text-center text-sm text-slate-500">
                  Данных по отправкам пока нет.
                </div>
              )}

              {clusters.length > 0 && filteredClusters.length === 0 && (
                <div className="card px-6 py-12 text-center text-sm text-slate-500">
                  По текущему фильтру товары не найдены. Попробуй часть названия или артикул `offer_id`.
                </div>
              )}

              {filteredClusters.map((cluster) => {
                const clusterId = cluster.key;
                const isCollapsed = collapsedClusters[clusterId];
                const warehouseCount = cluster.warehouses.length;
                const productCount = cluster.warehouses.reduce((sum, warehouse) => sum + warehouse.products.length, 0);

                return (
                  <section
                    key={clusterId}
                    ref={(node) => {
                      clusterSectionRefs.current[clusterId] = node;
                    }}
                    className="card overflow-hidden p-0"
                  >
                    <button
                      type="button"
                      onClick={() => toggleCluster(clusterId)}
                      className="flex w-full flex-col gap-4 border-b border-slate-200/70 px-6 py-5 text-left lg:flex-row lg:items-center lg:justify-between"
                    >
                      <div>
                        <h2 className="text-xl font-semibold text-slate-950">{cluster.name}</h2>
                        <p className="mt-1 text-sm text-slate-500">
                          В OZON доступно {cluster.total_ozon_available} шт. · в поставках в пути {cluster.total_in_pipeline} шт. · {getOrderWindowLabel(orderWindowDays).toLowerCase()} {cluster.total_ordered_30d} шт. · складов: {warehouseCount} · групп товаров: {productCount}
                        </p>
                      </div>

                      <div className="flex items-center gap-3">
                        <div className="rounded-full bg-slate-100 px-4 py-2 text-sm text-slate-600">
                          Остаток / в поставках в пути / заказано: {cluster.total_ozon_available} / {cluster.total_in_pipeline} / {cluster.total_ordered_30d}
                        </div>
                        {isCollapsed ? (
                          <ChevronDownIcon className="h-5 w-5 text-slate-400" />
                        ) : (
                          <ChevronUpIcon className="h-5 w-5 text-slate-400" />
                        )}
                      </div>
                    </button>

                    {!isCollapsed && (
                      <div className="relative px-3 py-3">
                        {scrollHints[clusterId]?.left && (
                          <div className="pointer-events-none absolute inset-y-3 left-3 z-10 w-10 bg-gradient-to-r from-white via-white/90 to-transparent" />
                        )}
                        {scrollHints[clusterId]?.right && (
                          <div className="pointer-events-none absolute inset-y-3 right-3 z-10 w-10 bg-gradient-to-l from-white via-white/90 to-transparent" />
                        )}

                        <div className="mb-2 flex items-center justify-between px-1">
                          <div className="text-[11px] font-medium uppercase tracking-[0.14em] text-slate-400">
                            Склады кластера
                          </div>
                          <div className="text-[11px] text-slate-500">
                            Листай вправо карточки складов
                          </div>
                        </div>

                        <div
                          ref={(node) => attachScrollNode(clusterId, node)}
                          onScroll={() => updateScrollHint(clusterId)}
                          className="overflow-x-auto px-1 pb-1"
                        >
                        <div className="flex min-w-full gap-3 snap-x snap-mandatory">
                        {cluster.warehouses.map((warehouse) => (
                          (() => {
                            return (
                          <article
                            key={warehouse.id}
                            className="flex-shrink-0 snap-start overflow-hidden rounded-[20px] border border-slate-200 bg-white"
                            style={warehouseCardStyle}
                          >
                            <div className="flex flex-col items-center gap-2 border-b border-slate-200/70 px-3 py-2.5 text-center">
                              <div>
                                <div className="text-[13px] font-semibold text-slate-950">{warehouse.name}</div>
                                <div className="mt-0.5 text-[11px] text-slate-500">OZON ID: {warehouse.ozon_id}</div>
                              </div>

                              <div className="flex flex-wrap justify-center gap-1.5">
                                <span className="inline-flex items-center gap-1 rounded-full bg-sky-50 px-2.5 py-1 text-[11px] font-medium text-sky-700">
                                  <PaperAirplaneIcon className="h-3.5 w-3.5" />
                                  {warehouse.avg_delivery_days} дн.
                                </span>
                                <span className="inline-flex items-center gap-1 rounded-full bg-slate-100 px-2.5 py-1 text-[11px] text-slate-600">
                                  <CubeIcon className="h-3.5 w-3.5" />
                                  {warehouse.products.length}
                                </span>
                                <span className="inline-flex rounded-full bg-emerald-50 px-2.5 py-1 text-[11px] font-medium text-emerald-700">
                                  OZON {warehouse.total_ozon_available}
                                </span>
                                <span className="inline-flex rounded-full bg-amber-50 px-2.5 py-1 text-[11px] font-medium text-amber-700">
                                  В поставках в пути {warehouse.total_in_pipeline}
                                </span>
                                <span className="inline-flex rounded-full bg-slate-100 px-2.5 py-1 text-[11px] font-medium text-slate-700">
                                  {getOrderWindowLabel(orderWindowDays).replace('Заказано ', '')} {warehouse.total_ordered_30d}
                                </span>
                              </div>

                              <button
                                type="button"
                                onClick={() => setSelectedWarehouseDetails({ clusterName: cluster.name, warehouse })}
                                className="inline-flex items-center gap-1.5 rounded-full border border-slate-200 bg-white px-3 py-1.5 text-[11px] font-medium text-slate-700 transition hover:border-sky-200 hover:text-sky-700"
                              >
                                <ClipboardDocumentListIcon className="h-3.5 w-3.5" />
                                Поставки ({warehouse.pending_departure_supplies.length + warehouse.in_transit_supplies.length})
                              </button>
                            </div>

                            <div className="space-y-3 p-3">
                              {warehouse.products.length === 0 ? (
                                <div className="rounded-2xl border border-slate-200 px-4 py-8 text-center text-sm text-slate-500">
                                  Для этого склада данных пока нет.
                                </div>
                              ) : (
                                warehouse.products
                                  .slice()
                                  .sort((left, right) => left.product_name.localeCompare(right.product_name, 'ru', { sensitivity: 'base' }))
                                  .map((product) => {
                                    const groupedProduct = groupVariantEntries(
                                      product.variants.map((variant) => ({
                                        ...variant,
                                        product_name: product.product_name,
                                      }))
                                    )[0];
                                    const colorGroups = groupedProduct?.colors || [];
                                    const productVariants = colorGroups.flatMap((group) =>
                                      group.sizes.flatMap((sizeGroup) => sizeGroup.items)
                                    );
                                    const showColorGroups = colorGroups.some((group) => {
                                      const normalized = String(group.color || '').trim().toLowerCase();
                                      return normalized && normalized !== 'без цвета';
                                    });
                                    const showSizeColumn = productVariants.some((variant) => {
                                      const size = String(variant.attributes?.['Размер'] || '').trim();
                                      return size.length > 0;
                                    });
                                    const showPackColumn = productVariants.some((variant) => Number(variant.pack_size || 1) > 1);
                                    const tableColumnCount =
                                      packingMode === 'advanced'
                                        ? 8 + (showSizeColumn ? 1 : 0) + (showPackColumn ? 1 : 0)
                                        : 7 + (showSizeColumn ? 1 : 0) + (showPackColumn ? 1 : 0);
                                    const displayColorGroups = showColorGroups
                                      ? colorGroups
                                      : [
                                          {
                                            color: '',
                                            sizes: [
                                              {
                                                size: '',
                                                items: productVariants,
                                              },
                                            ],
                                          },
                                        ];

                                    return (
                                      <section
                                        key={product.product_id}
                                        className="mx-auto block w-fit max-w-full overflow-hidden rounded-[18px] border border-slate-200 bg-white"
                                      >
                                        <div className="border-b border-slate-200 bg-sky-50/70 px-3 py-2 text-center text-[11px] font-semibold text-sky-900">
                                          {product.product_name}
                                        </div>
                                        <div className="overflow-x-auto">
                                          <table className="w-max border-collapse border border-slate-200 bg-white/80">
                                            <thead className="bg-slate-50/90">
                                              {packingMode === 'advanced' ? (
                                                <>
                                                  <tr>
                                                    <th rowSpan={2} className="min-w-[132px] whitespace-nowrap border border-slate-200 px-1 py-1.5 text-center align-middle text-[9px] font-semibold uppercase tracking-[0.05em] leading-tight text-slate-500">Артикул</th>
                                                    {showSizeColumn ? (
                                                      <th rowSpan={2} className="w-[48px] border border-slate-200 px-1 py-1.5 text-center align-middle text-[9px] font-semibold uppercase tracking-[0.05em] leading-tight text-slate-500">Размер</th>
                                                    ) : null}
                                                    {showPackColumn ? (
                                                      <th rowSpan={2} className="w-[34px] border border-slate-200 px-1 py-1.5 text-center align-middle text-[9px] font-semibold uppercase tracking-[0.05em] leading-tight text-slate-500">Уп.</th>
                                                    ) : null}
                                                    <th
                                                      colSpan={2}
                                                      className="border border-emerald-200 bg-emerald-50 px-1 py-1.5 text-center text-[9px] font-semibold uppercase tracking-[0.06em] leading-tight text-emerald-700"
                                                      title={ourStockLabel}
                                                    >
                                                      {ourStockLabel}
                                                    </th>
                                                    <th rowSpan={2} className="w-[48px] border border-sky-200 bg-sky-50 px-1 py-1.5 text-center align-middle text-[9px] font-semibold uppercase tracking-[0.05em] leading-tight text-sky-700">OZON</th>
                                                    <th rowSpan={2} className="w-[74px] border border-sky-200 bg-sky-50 px-1 py-1.5 text-center align-middle text-[9px] font-semibold uppercase tracking-[0.02em] leading-tight text-sky-700">В заявках на поставку</th>
                                                    <th rowSpan={2} className="w-[72px] border border-sky-200 bg-sky-50 px-1 py-1.5 text-center align-middle text-[9px] font-semibold uppercase tracking-[0.02em] leading-tight text-sky-700">В поставках в пути</th>
                                                    <th rowSpan={2} className="w-[84px] border border-sky-200 bg-sky-50 px-1 py-1.5 text-center align-middle text-[9px] font-semibold uppercase tracking-[0.02em] leading-tight text-sky-700">Возвращаются от покупателей</th>
                                                    <th rowSpan={2} className="w-[64px] border border-sky-200 bg-sky-50 px-1 py-1.5 text-center align-middle text-[9px] font-semibold uppercase tracking-[0.02em] leading-tight text-sky-700">{getOrderWindowLabel(orderWindowDays)}</th>
                                                  </tr>
                                                  <tr>
                                                    <th className="w-[48px] border border-emerald-200 bg-emerald-50 px-1 py-1.5 text-center text-[9px] font-semibold uppercase tracking-[0.05em] leading-tight text-emerald-700">Неупак.</th>
                                                    <th className="w-[48px] border border-emerald-200 bg-emerald-50 px-1 py-1.5 text-center text-[9px] font-semibold uppercase tracking-[0.05em] leading-tight text-emerald-700">Упак.</th>
                                                  </tr>
                                                </>
                                              ) : (
                                                <tr>
                                                  <th className="min-w-[132px] whitespace-nowrap border border-slate-200 px-1 py-1.5 text-center text-[9px] font-semibold uppercase tracking-[0.05em] leading-tight text-slate-500">Артикул</th>
                                                  {showSizeColumn ? (
                                                    <th className="w-[48px] border border-slate-200 px-1 py-1.5 text-center text-[9px] font-semibold uppercase tracking-[0.05em] leading-tight text-slate-500">Размер</th>
                                                  ) : null}
                                                  {showPackColumn ? (
                                                    <th className="w-[34px] border border-slate-200 px-1 py-1.5 text-center text-[9px] font-semibold uppercase tracking-[0.05em] leading-tight text-slate-500">Уп.</th>
                                                  ) : null}
                                                  <th
                                                    className="w-[56px] border border-emerald-200 bg-emerald-50 px-1 py-1.5 text-center text-[9px] font-semibold uppercase tracking-[0.05em] leading-tight text-emerald-700"
                                                    title={ourStockLabel}
                                                  >
                                                    {ourStockLabel}
                                                  </th>
                                                  <th className="w-[48px] border border-sky-200 bg-sky-50 px-1 py-1.5 text-center text-[9px] font-semibold uppercase tracking-[0.05em] leading-tight text-sky-700">OZON</th>
                                                  <th className="w-[74px] border border-sky-200 bg-sky-50 px-1 py-1.5 text-center text-[9px] font-semibold uppercase tracking-[0.02em] leading-tight text-sky-700">В заявках на поставку</th>
                                                  <th className="w-[72px] border border-sky-200 bg-sky-50 px-1 py-1.5 text-center text-[9px] font-semibold uppercase tracking-[0.02em] leading-tight text-sky-700">В поставках в пути</th>
                                                  <th className="w-[84px] border border-sky-200 bg-sky-50 px-1 py-1.5 text-center text-[9px] font-semibold uppercase tracking-[0.02em] leading-tight text-sky-700">Возвращаются от покупателей</th>
                                                  <th className="w-[64px] border border-sky-200 bg-sky-50 px-1 py-1.5 text-center text-[9px] font-semibold uppercase tracking-[0.02em] leading-tight text-sky-700">{getOrderWindowLabel(orderWindowDays)}</th>
                                                </tr>
                                              )}
                                            </thead>
                                            <tbody className="bg-white/60">
                                              {displayColorGroups.flatMap((colorGroup) => [
                                                ...(showColorGroups
                                                  ? [
                                                      <tr key={`color-${product.product_id}-${colorGroup.color}`} className="border-t border-slate-200 bg-white">
                                                        <td colSpan={tableColumnCount} className="border border-slate-200 px-1.5 py-1.5 text-center text-[10px] font-medium uppercase tracking-[0.08em] text-slate-500">
                                                          Цвет: {colorGroup.color}
                                                        </td>
                                                      </tr>,
                                                    ]
                                                  : []),
                                                ...colorGroup.sizes.flatMap((sizeGroup) => {
                                                  const simpleModeStock = sizeGroup.items.reduce(
                                                    (sum, variant) => sum + variant.our_unpacked,
                                                    0
                                                  );

                                                  return sizeGroup.items.map((variant, index) => (
                                                    <tr key={variant.variant_id} className="hover:bg-slate-50/80">
                                                      <td className="min-w-[132px] whitespace-nowrap border border-slate-200 px-1 py-1.5 text-left text-[10px] leading-tight text-slate-900">
                                                        <span className="block font-medium">{variant.offer_id}</span>
                                                      </td>
                                                      {showSizeColumn && index === 0 ? (
                                                        <td
                                                          rowSpan={sizeGroup.items.length}
                                                          className="border border-slate-200 px-1 py-1.5 align-middle text-center text-[10px] text-slate-600"
                                                        >
                                                          {sizeGroup.size || '—'}
                                                        </td>
                                                      ) : null}
                                                      {showPackColumn ? (
                                                        <td className="border border-slate-200 px-1 py-1.5 text-center text-[10px] text-slate-600">{variant.pack_size}</td>
                                                      ) : null}
                                                      {packingMode === 'simple' ? (
                                                        index === 0 && (
                                                          <td
                                                            rowSpan={sizeGroup.items.length}
                                                            className="border border-slate-200 px-1 py-1.5 align-middle text-center text-[10px] font-medium text-emerald-600"
                                                          >
                                                            {simpleModeStock}
                                                          </td>
                                                        )
                                                      ) : (
                                                        <>
                                                          {index === 0 && (
                                                            <td
                                                              rowSpan={sizeGroup.items.length}
                                                              className="border border-slate-200 px-1 py-1.5 align-middle text-center text-[10px] font-medium text-slate-900"
                                                            >
                                                              {sizeGroup.items[0]?.our_unpacked ?? 0}
                                                            </td>
                                                          )}
                                                          <td className="border border-slate-200 px-1 py-1.5 text-center text-[10px] font-medium text-emerald-600">
                                                            {variant.our_packed}
                                                          </td>
                                                        </>
                                                      )}
                                                      <td className="border border-slate-200 px-1 py-1.5 text-center text-[10px] text-slate-900">{variant.ozon_available}</td>
                                                      <td className="border border-slate-200 px-1 py-1.5 text-center text-[10px] text-sky-700">{variant.ozon_requested_to_supply}</td>
                                                      <td className="border border-slate-200 px-1 py-1.5 text-center text-[10px] text-amber-700">{variant.ozon_in_pipeline}</td>
                                                      <td className="border border-slate-200 px-1 py-1.5 text-center text-[10px] text-violet-700">{variant.ozon_returning}</td>
                                                      <td className="w-[64px] border border-slate-200 px-1 py-1.5 text-center text-[10px] font-medium text-slate-700">{variant.ordered_30d}</td>
                                                    </tr>
                                                  ));
                                                }),
                                              ])}
                                            </tbody>
                                          </table>
                                        </div>
                                      </section>
                                    );
                                  })
                              )}
                            </div>
                          </article>
                            );
                          })()
                        ))}
                        </div>
                        </div>
                      </div>
                    )}
                  </section>
                );
              })}

              {clusters.length > 0 && (
                <div className="card flex items-center justify-between">
                  <div>
                    <div className="text-sm font-medium text-slate-500">Суммарный доступный остаток OZON</div>
                    <div className="mt-2 text-3xl font-semibold text-slate-950">{stats.totalAvailable}</div>
                  </div>
                  <div className="text-sm text-slate-500">
                    В поставках в пути сейчас: {stats.totalPipeline}. {getOrderWindowLabel(orderWindowDays)}: {stats.totalOrdered30d}. Полезно для планирования следующей волны отправок.
                  </div>
                </div>
              )}
            </div>
          )}
        </div>
        )}

        {selectedWarehouseDetails ? (
          <div className="fixed inset-0 z-50">
            <button
              type="button"
              aria-label="Закрыть окно склада"
              onClick={() => setSelectedWarehouseDetails(null)}
              className="absolute inset-0 h-full w-full bg-slate-950/35 backdrop-blur-sm"
            />
            <div className="absolute inset-0 overflow-y-auto p-4 sm:p-6">
              <div className="flex min-h-full items-center justify-center">
                <div className="relative w-full max-w-5xl rounded-[32px] border border-white/70 bg-white p-6 shadow-[0_28px_80px_rgba(15,23,42,0.22)]">
                  <div className="flex items-start justify-between gap-4">
                    <div>
                      <div className="text-xl font-semibold text-slate-950">
                        {selectedWarehouseDetails.warehouse.name}
                      </div>
                      <p className="mt-2 text-sm text-slate-500">
                        {selectedWarehouseDetails.clusterName} · всего накладных: {selectedWarehouseSupplyCount}. Нажми на строку в списке поставок на странице `Поставки`, если нужен полный состав.
                      </p>
                    </div>
                    <button
                      type="button"
                      onClick={() => setSelectedWarehouseDetails(null)}
                      className="rounded-full border border-slate-200 p-3 text-slate-500 transition hover:border-slate-300 hover:text-slate-700"
                    >
                      <XMarkIcon className="h-6 w-6" />
                    </button>
                  </div>

                  <div className="mt-6 grid gap-5 xl:grid-cols-2">
                    {[
                      {
                        key: 'pending',
                        title: 'Еще не ушли в путь',
                        description: 'Накладные еще не в дороге до склада OZON.',
                        items: selectedWarehouseDetails.warehouse.pending_departure_supplies,
                        empty: 'Таких накладных сейчас нет.',
                      },
                      {
                        key: 'transit',
                        title: 'Сейчас доставляются',
                        description: 'Накладные уже едут до склада OZON или находятся на приемке.',
                        items: selectedWarehouseDetails.warehouse.in_transit_supplies,
                        empty: 'Сейчас нет накладных в пути до этого склада.',
                      },
                    ].map((section) => (
                      <section key={section.key} className="rounded-[28px] border border-slate-200 bg-slate-50/60 p-4">
                        <div className="flex flex-col gap-1 border-b border-slate-200/80 pb-3">
                          <h3 className="text-base font-semibold text-slate-950">{section.title}</h3>
                          <p className="text-sm text-slate-500">{section.description}</p>
                        </div>

                        {section.items.length > 0 ? (
                          <div className="mt-4 space-y-3">
                            <div className="hidden rounded-2xl bg-slate-100 px-4 py-3 md:grid md:grid-cols-[110px_170px_minmax(0,1fr)] md:gap-3">
                              <div className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Накладная</div>
                              <div className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Таймслот</div>
                              <div className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-500">Статус</div>
                            </div>

                            {section.items.map((item) => (
                              <div key={item.id} className="overflow-hidden rounded-[24px] border border-slate-200 bg-white shadow-sm">
                                <button
                                  type="button"
                                  onClick={() => void toggleWarehouseSupply(item.id)}
                                  className="w-full px-4 py-4 text-left transition hover:bg-slate-50"
                                >
                                  <div className="grid gap-3 md:grid-cols-[110px_170px_minmax(0,1fr)] md:items-start md:gap-3">
                                    <div className="min-w-0">
                                      <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400 md:hidden">Накладная</div>
                                      <div className="mt-1 text-base font-semibold text-slate-950 md:mt-0">#{getShortOrderNumber(item.order_number)}</div>
                                    </div>

                                    <div className="min-w-0">
                                      <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400 md:hidden">Таймслот</div>
                                      <div className="mt-1 text-sm text-slate-800 md:mt-0">{formatTimeslotWithDate(item.timeslot_from, item.timeslot_to)}</div>
                                      <div className="mt-1 text-xs text-slate-500">ETA: {formatEtaLabel(item, selectedWarehouseDetails.warehouse.avg_delivery_days)}</div>
                                    </div>

                                    <div className="min-w-0">
                                      <div className="text-[11px] font-semibold uppercase tracking-[0.12em] text-slate-400 md:hidden">Статус</div>
                                      <div className="mt-1 md:mt-0">
                                        <span
                                          className="inline-block max-w-full rounded-full px-3 py-1 text-center text-xs font-medium leading-4 whitespace-normal break-words"
                                          style={getSupplyStatusStyle(item.status)}
                                        >
                                          {getSupplyStatusLabel(item.status)}
                                        </span>
                                      </div>
                                    </div>
                                  </div>
                                </button>

                                {openedWarehouseSupplyId === item.id && (
                                  <div className="border-t border-slate-200 bg-slate-50/70 px-4 py-4">
                                    {loadingSupplyItems[item.id] ? (
                                      <div className="text-sm text-slate-500">Загрузка состава накладной...</div>
                                    ) : (supplyItemsMap[item.id] || []).length === 0 ? (
                                      <div className="text-sm text-slate-500">Состав накладной пока не загружен.</div>
                                    ) : (
                                      <div className="space-y-3">
                                        {groupVariantEntries((supplyItemsMap[item.id] || []).map((entry) => ({
                                          ...entry,
                                          accepted_quantity: entry.accepted_quantity ?? null,
                                        }))).map((group) => (
                                          <details key={`${item.id}-${group.productName}`} className="group overflow-hidden rounded-2xl border border-slate-200 bg-white open:bg-white">
                                            <summary className="flex cursor-pointer list-none items-center justify-between gap-3 px-4 py-3">
                                              <div>
                                                <div className="text-sm font-semibold text-slate-900">{group.productName}</div>
                                                <div className="mt-1 text-xs text-slate-500">
                                                  Вариаций: {group.colors.reduce((sum, colorGroup) => sum + colorGroup.sizes.reduce((sizeSum, sizeGroup) => sizeSum + sizeGroup.items.length, 0), 0)} ·
                                                  {' '}Штук: {group.colors.reduce((sum, colorGroup) => sum + colorGroup.sizes.reduce((sizeSum, sizeGroup) => sizeSum + sizeGroup.items.reduce((itemSum, groupedItem) => itemSum + (groupedItem.quantity || 0), 0), 0), 0)}
                                                </div>
                                              </div>
                                              <div className="text-xs font-medium text-slate-500 transition group-open:rotate-180">⌄</div>
                                            </summary>
                                            <div className="space-y-3 border-t border-slate-200 px-4 py-4">
                                              {group.colors.map((colorGroup) => (
                                                <div key={`${item.id}-${group.productName}-${colorGroup.color}`} className="rounded-2xl border border-slate-200 bg-slate-50 p-3">
                                                  <div className="mb-2 inline-flex rounded-full bg-white px-3 py-1 text-xs font-medium text-slate-700">{colorGroup.color}</div>
                                                  <div className="space-y-2">
                                                    {colorGroup.sizes.map((sizeGroup) => (
                                                      <div key={`${item.id}-${colorGroup.color}-${sizeGroup.size}`} className="rounded-2xl bg-white px-3 py-3">
                                                        <div className="mb-2 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">Размер: {sizeGroup.size}</div>
                                                        <div className="space-y-2">
                                                          {sizeGroup.items.map((entry) => (
                                                            <div key={`${item.id}-${entry.offer_id}`} className="flex items-start justify-between gap-4 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2">
                                                              <div className="min-w-0">
                                                                <div className="truncate text-sm font-medium text-slate-900">{getVariantDisplayTitle(entry)}</div>
                                                                <div className="mt-1 text-xs text-slate-500">{getVariantCharacteristicText(entry)}</div>
                                                              </div>
                                                              <div className="shrink-0 rounded-full bg-sky-50 px-3 py-1 text-sm font-semibold text-sky-700">{entry.quantity} шт.</div>
                                                            </div>
                                                          ))}
                                                        </div>
                                                      </div>
                                                    ))}
                                                  </div>
                                                </div>
                                              ))}
                                            </div>
                                          </details>
                                        ))}
                                      </div>
                                    )}
                                  </div>
                                )}
                              </div>
                            ))}
                          </div>
                        ) : (
                          <div className="mt-4 rounded-2xl border border-dashed border-slate-200 bg-white px-4 py-6 text-sm text-slate-500">
                            {section.empty}
                          </div>
                        )}
                      </section>
                    ))}
                  </div>
                </div>
              </div>
            </div>
          </div>
        ) : null}
      </Layout>
    </ProtectedRoute>
  );
}
