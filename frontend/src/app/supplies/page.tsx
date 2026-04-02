'use client';

import Link from 'next/link';
import { useRouter, useSearchParams } from 'next/navigation';
import { Fragment, Suspense, useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { Dialog, Transition } from '@headlessui/react';
import {
  CalendarDaysIcon,
  CubeIcon,
  FunnelIcon,
  ListBulletIcon,
  TruckIcon,
  XMarkIcon,
} from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { Layout } from '@/components/Layout';
import { StoreSelector } from '@/components/StoreSelector';
import { SyncFreshnessPanel } from '@/components/SyncFreshnessPanel';
import { suppliesAPI, type Supply } from '@/lib/api/supplies';
import { calendarAPI, type CalendarItem, type CalendarSupplyItem } from '@/lib/api/calendar';
import { useStoreContext } from '@/lib/context/StoreContext';
import {
  SUPPLY_STATUS_OPTIONS,
  getSupplyStatusLabel,
  getSupplyStatusStyle,
} from '@/lib/constants/supplyStatus';
import { getVariantCharacteristicText, getVariantDisplayTitle, groupVariantEntries } from '@/lib/utils/variantPresentation';

function formatDateTime(value?: string | null) {
  if (!value) return '-';
  return new Date(value).toLocaleString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatDate(value?: string | null) {
  if (!value) return '-';
  return new Date(value).toLocaleDateString('ru-RU');
}

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
  const dateLabel = new Date(from).toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit', year: 'numeric' });
  return `${dateLabel} · ${formatTimeslotRange(from, to)}`;
}

function getShortOrderNumber(orderNumber: string) {
  const digits = orderNumber.replace(/\D/g, '');
  return digits ? digits.slice(-4) : orderNumber.slice(-4);
}

function formatInputDate(value: Date) {
  const year = value.getFullYear();
  const month = String(value.getMonth() + 1).padStart(2, '0');
  const day = String(value.getDate()).padStart(2, '0');
  return `${year}-${month}-${day}`;
}

function parseInputDate(value: string) {
  const [year, month, day] = value.split('-').map(Number);
  return new Date(year, month - 1, day);
}

function addDays(value: Date, days: number) {
  const next = new Date(value);
  next.setDate(next.getDate() + days);
  return next;
}

function startOfWeek(value: Date) {
  const day = value.getDay();
  const diff = day === 0 ? -6 : 1 - day;
  return addDays(value, diff);
}

function endOfWeek(value: Date) {
  return addDays(startOfWeek(value), 6);
}

function buildVisibleDays(dateFrom: string, dateTo: string) {
  const from = parseInputDate(dateFrom);
  const to = parseInputDate(dateTo);
  const start = startOfWeek(from);
  const end = endOfWeek(to);
  const days: Date[] = [];
  for (let cursor = new Date(start); cursor <= end; cursor = addDays(cursor, 1)) {
    days.push(new Date(cursor));
  }
  return days;
}

function formatDateLabel(value: Date) {
  return value.toLocaleDateString('ru-RU', { day: 'numeric', month: 'long' });
}

function formatEtaLabel(item: CalendarItem) {
  if (!item.eta_date) return '-';
  const etaDate = new Date(`${item.eta_date}T00:00:00`);
  const dateLabel = etaDate.toLocaleDateString('ru-RU', { day: '2-digit', month: '2-digit' });
  if (typeof item.avg_delivery_days === 'number' && item.avg_delivery_days > 0) {
    return `${dateLabel} (${item.avg_delivery_days} дн.)`;
  }
  return dateLabel;
}


function getReservationWaitMessage(
  item: Pick<Supply, 'status' | 'reservation_waiting_for_stock' | 'reservation_wait_message'>
) {
  if (!item.reservation_waiting_for_stock || item.status !== 'READY_TO_SUPPLY') {
    return null;
  }
  return item.reservation_wait_message || 'Поставка готова к отгрузке, но резерв начнется после внесения прихода.';
}


function groupCalendarSupplyEntries(items: CalendarSupplyItem[]) {
  return groupVariantEntries(items.map((item) => ({
    offer_id: item.offer_id ?? '',
    product_name: item.product_name,
    attributes: item.attributes || {},
    quantity: item.quantity,
    accepted_quantity: null,
    pack_size: item.pack_size ?? 1,
  })));
}

const weekdayLabels = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'];

type ViewMode = 'list' | 'calendar';
type SelectedStoreGroup = { dayKey: string; storeName: string; items: CalendarItem[] };

function SuppliesPageInner() {
  const searchParams = useSearchParams();
  const search = searchParams?.toString() || '';
  const router = useRouter();
  const initialView = searchParams?.get('view') === 'list' ? 'list' : 'calendar';

  const { stores, isLoading: storesLoading } = useStoreContext();
  const [viewMode, setViewMode] = useState<ViewMode>(initialView);
  const [storeFilter, setStoreFilter] = useState<number | 'all'>('all');
  const [selectedStatuses, setSelectedStatuses] = useState<string[]>([]);

  const [supplies, setSupplies] = useState<Supply[]>([]);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [page, setPage] = useState(1);
  const [pageSize] = useState(15);
  const [total, setTotal] = useState(0);

  const [calendarItems, setCalendarItems] = useState<CalendarItem[]>([]);
  const [selectedStoreGroup, setSelectedStoreGroup] = useState<SelectedStoreGroup | null>(null);
  const [openedSupplyId, setOpenedSupplyId] = useState<number | null>(null);
  const latestCalendarRequestRef = useRef(0);
  const [dateFrom, setDateFrom] = useState(() => formatInputDate(new Date()));
  const [dateTo, setDateTo] = useState(() => formatInputDate(addDays(new Date(), 31)));

  const [loading, setLoading] = useState(true);

  const updateViewMode = (nextView: ViewMode) => {
    setViewMode(nextView);
    const query = new URLSearchParams(search);
    if (nextView === 'list') {
      query.set('view', 'list');
    } else {
      query.delete('view');
    }
    const nextQuery = query.toString();
    router.replace(nextQuery ? `/supplies?${nextQuery}` : '/supplies');
  };

  const activeStoreId = storeFilter === 'all' ? undefined : storeFilter;

  const loadSupplies = useCallback(async () => {
    setLoading(true);
    try {
      const data = await suppliesAPI.getSupplies({
        statuses: selectedStatuses.length > 0 ? selectedStatuses : undefined,
        page,
        pageSize,
        includeItems: true,
        storeId: activeStoreId,
      });
      setSupplies(data.items);
      setTotal(data.total);
    } catch {
      toast.error('Ошибка загрузки поставок');
    } finally {
      setLoading(false);
    }
  }, [activeStoreId, page, pageSize, selectedStatuses]);

  const loadCalendar = useCallback(async () => {
    const requestId = latestCalendarRequestRef.current + 1;
    latestCalendarRequestRef.current = requestId;
    setLoading(true);
    try {
      const data = await calendarAPI.getCalendar({
        storeId: activeStoreId,
        statuses: selectedStatuses.length > 0 ? selectedStatuses : undefined,
        dateFrom,
        dateTo,
      });
      if (latestCalendarRequestRef.current !== requestId) return;
      setCalendarItems(data.items);
    } catch {
      if (latestCalendarRequestRef.current === requestId) toast.error('Ошибка загрузки календаря поставок');
    } finally {
      if (latestCalendarRequestRef.current === requestId) setLoading(false);
    }
  }, [activeStoreId, dateFrom, dateTo, selectedStatuses]);

  useEffect(() => {
    if (viewMode === 'list') {
      void loadSupplies();
    } else {
      void loadCalendar();
    }
  }, [loadCalendar, loadSupplies, viewMode]);

  useEffect(() => {
    setPage(1);
    setExpanded(null);
    setSelectedStoreGroup(null);
    setOpenedSupplyId(null);
  }, [storeFilter]);

  const totalPages = useMemo(() => Math.max(1, Math.ceil(total / pageSize)), [total, pageSize]);
  const pageStats = useMemo(() => {
    const itemsCount = supplies.reduce((sum, supply) => sum + (supply.items?.length || 0), 0);
    const readyCount = supplies.filter((supply) => supply.status === 'READY_TO_SUPPLY').length;
    const waitingForStockCount = supplies.filter((supply) => supply.reservation_waiting_for_stock).length;
    const problemCount = supplies.filter((supply) => ['OVERDUE', 'REJECTED_AT_SUPPLY_WAREHOUSE'].includes(supply.status)).length;
    return { itemsCount, readyCount, waitingForStockCount, problemCount };
  }, [supplies]);

  const toggleStatus = (status: string) => {
    setPage(1);
    setExpanded(null);
    setSelectedStoreGroup(null);
    setOpenedSupplyId(null);
    setSelectedStatuses((prev) => (prev.includes(status) ? prev.filter((item) => item !== status) : [...prev, status]));
  };

  const visibleDays = useMemo(() => buildVisibleDays(dateFrom, dateTo), [dateFrom, dateTo]);
  const visibleDayKeys = useMemo(() => new Set(visibleDays.map((day) => formatInputDate(day))), [visibleDays]);
  const calendarItemsByDay = useMemo(() => {
    const grouped = new Map<string, CalendarItem[]>();
    calendarItems.forEach((item) => {
      const dayKey = item.timeslot_from ? item.timeslot_from.slice(0, 10) : null;
      if (!dayKey || dayKey < dateFrom || dayKey > dateTo) return;
      if (!grouped.has(dayKey)) grouped.set(dayKey, []);
      grouped.get(dayKey)!.push(item);
    });
    grouped.forEach((dayItems) => dayItems.sort((left, right) => (left.timeslot_from || '').localeCompare(right.timeslot_from || '')));
    return grouped;
  }, [calendarItems, dateFrom, dateTo]);

  const calendarStats = useMemo(() => {
    const avgValues = calendarItems
      .map((item) => item.avg_delivery_days)
      .filter((value): value is number => typeof value === 'number' && value > 0);
    return {
      total: calendarItems.length,
      daysWithSupplies: Array.from(calendarItemsByDay.keys()).filter((key) => visibleDayKeys.has(key)).length,
      avgDeliveryDays: avgValues.length ? (avgValues.reduce((sum, value) => sum + value, 0) / avgValues.length).toFixed(1) : '-',
      overdue: calendarItems.filter((item) => item.status === 'OVERDUE').length,
    };
  }, [calendarItems, calendarItemsByDay, visibleDayKeys]);

  const modalTitle = selectedStoreGroup ? `${selectedStoreGroup.storeName} · ${formatDateLabel(parseInputDate(selectedStoreGroup.dayKey))}` : '';

  return (
    <ProtectedRoute>
      <Layout>
        {storesLoading ? (
          <div className="card px-6 py-12 text-center text-sm text-slate-500">Загрузка магазинов...</div>
        ) : stores.length === 0 ? (
          <div className="rounded-3xl border border-dashed border-slate-300 bg-white px-6 py-12 text-center shadow-sm">
            <h2 className="text-lg font-semibold text-slate-900">Поставки появятся после подключения магазина</h2>
            <p className="mt-2 text-sm text-slate-500">Добавь магазин OZON, и здесь будут только его заявки и их состав.</p>
            <Link href="/stores" className="mt-5 inline-flex rounded-2xl bg-slate-950 px-4 py-3 text-sm font-medium text-white transition hover:bg-slate-800">
              Открыть магазины
            </Link>
          </div>
        ) : (
          <div className="space-y-8">
            <section className="rounded-[32px] border border-white/70 bg-[radial-gradient(circle_at_top_left,_rgba(255,255,255,0.96),_rgba(239,246,255,0.94)_38%,_rgba(255,247,237,0.92)_100%)] p-8 shadow-[0_24px_60px_rgba(15,23,42,0.08)]">
              <div className="flex flex-col gap-6 xl:flex-row xl:items-end xl:justify-between">
                <div className="max-w-3xl">
                  <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Поставки OZON</p>
                  <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-950 sm:text-4xl">
                    Один экран для списка и календаря поставок
                  </h1>
                  <p className="mt-3 text-sm leading-6 text-slate-600 sm:text-base">
                    Переключайся между списком и календарной сеткой, не теряя фильтры по магазинам и статусам.
                  </p>
                </div>

                <div className="grid gap-3 lg:min-w-[420px] lg:grid-cols-2 xl:grid-cols-1">
                  <div className="card p-4">
                    <StoreSelector value={storeFilter} onChange={setStoreFilter} label="Магазин" />
                  </div>
                  <div className="rounded-[24px] border border-slate-200 bg-white p-2 shadow-sm">
                    <div className="grid grid-cols-2 gap-2">
                      <button
                        type="button"
                        onClick={() => updateViewMode('list')}
                        className={`inline-flex items-center justify-center gap-2 rounded-2xl px-4 py-3 text-sm font-medium transition ${viewMode === 'list' ? 'bg-slate-950 text-white' : 'text-slate-600 hover:bg-slate-100'}`}
                      >
                        <ListBulletIcon className="h-4 w-4" />
                        Список
                      </button>
                      <button
                        type="button"
                        onClick={() => updateViewMode('calendar')}
                        className={`inline-flex items-center justify-center gap-2 rounded-2xl px-4 py-3 text-sm font-medium transition ${viewMode === 'calendar' ? 'bg-slate-950 text-white' : 'text-slate-600 hover:bg-slate-100'}`}
                      >
                        <CalendarDaysIcon className="h-4 w-4" />
                        Календарь
                      </button>
                    </div>
                  </div>
                </div>
              </div>
            </section>

            <SyncFreshnessPanel
              storeIds={storeFilter === 'all' ? stores.map((store) => store.id) : [storeFilter]}
              kinds={['supplies']}
              title={viewMode === 'calendar' ? 'Календарь поставок' : 'Список поставок'}
              description={
                viewMode === 'calendar'
                  ? 'Календарная сетка строится по последней успешной синхронизации поставок по выбранному набору магазинов.'
                  : 'Список и состав поставок показывают, когда заявки в последний раз успешно подтянулись из OZON.'
              }
            />

            {viewMode === 'calendar' ? (
              <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <article className="card"><div className="text-sm font-medium text-slate-500">Поставок в диапазоне</div><div className="mt-3 text-3xl font-semibold text-slate-950">{calendarStats.total}</div><div className="mt-2 text-sm text-slate-500">От {formatDateLabel(parseInputDate(dateFrom))} до {formatDateLabel(parseInputDate(dateTo))}</div></article>
                <article className="card"><div className="text-sm font-medium text-slate-500">Дней с поставками</div><div className="mt-3 text-3xl font-semibold text-slate-950">{calendarStats.daysWithSupplies}</div><div className="mt-2 text-sm text-slate-500">В видимой сетке календаря</div></article>
                <article className="card"><div className="text-sm font-medium text-slate-500">Средняя доставка</div><div className="mt-3 text-3xl font-semibold text-slate-950">{calendarStats.avgDeliveryDays}</div><div className="mt-2 text-sm text-slate-500">дн. до склада OZON</div></article>
                <article className="rounded-[28px] border border-orange-200 bg-orange-50 p-6 shadow-[0_18px_44px_rgba(249,115,22,0.10)]"><div className="text-sm font-medium text-orange-700">Просрочено</div><div className="mt-3 text-3xl font-semibold text-orange-900">{calendarStats.overdue}</div><div className="mt-2 text-sm text-orange-700">Нужна ручная проверка</div></article>
              </section>
            ) : (
              <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <article className="card"><div className="text-sm font-medium text-slate-500">Всего заявок</div><div className="mt-3 text-3xl font-semibold text-slate-950">{total}</div><div className="mt-2 text-sm text-slate-500">С учётом фильтров и пагинации</div></article>
                <article className="card"><div className="text-sm font-medium text-slate-500">На странице</div><div className="mt-3 text-3xl font-semibold text-slate-950">{supplies.length}</div><div className="mt-2 text-sm text-slate-500">Загружено сейчас</div></article>
                <article className="card"><div className="text-sm font-medium text-slate-500">Готовы к отгрузке</div><div className="mt-3 text-3xl font-semibold text-slate-950">{pageStats.readyCount}</div><div className="mt-2 text-sm text-slate-500">{pageStats.waitingForStockCount > 0 ? `Из них ${pageStats.waitingForStockCount} ждут внесения прихода` : `Учитываем статус: ${getSupplyStatusLabel('READY_TO_SUPPLY')}`}</div></article>
                <article className="rounded-[28px] border border-rose-200 bg-rose-50 p-6 shadow-[0_18px_44px_rgba(244,63,94,0.10)]"><div className="text-sm font-medium text-rose-700">Риски</div><div className="mt-3 text-3xl font-semibold text-rose-900">{pageStats.problemCount}</div><div className="mt-2 text-sm text-rose-700">Просрочено или с отказом</div></article>
              </section>
            )}

            
            <section className="card">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-slate-950">Фильтр по статусам</h2>
                  <p className="mt-1 text-sm text-slate-500">Используем одинаковые статусы и одинаковый внешний вид в списке и календаре.</p>
                </div>
                <div className="flex flex-wrap gap-2">
                  {viewMode === 'calendar' && (
                    <>
                      <input type="date" value={dateFrom} onChange={(event) => setDateFrom(event.target.value)} className="input-field min-w-[170px]" />
                      <input type="date" value={dateTo} onChange={(event) => setDateTo(event.target.value)} className="input-field min-w-[170px]" />
                    </>
                  )}
                  <button type="button" onClick={() => setSelectedStatuses([])} className="btn-secondary gap-2">
                    <FunnelIcon className="h-4 w-4" />
                    Сбросить фильтр
                  </button>
                </div>
              </div>

              <div className="mt-5 flex flex-wrap gap-2">
                {SUPPLY_STATUS_OPTIONS.map((option) => {
                  const active = selectedStatuses.includes(option.value);
                  return (
                    <button
                      key={option.value}
                      type="button"
                      onClick={() => toggleStatus(option.value)}
                      className={`rounded-full border px-4 py-2 text-xs font-medium transition ${
                        active ? 'ring-2 ring-offset-2 ring-slate-300 shadow-sm' : 'hover:-translate-y-0.5 hover:shadow-sm'
                      }`}
                      style={getSupplyStatusStyle(option.value)}
                    >
                      <span className="whitespace-nowrap">{option.label}</span>
                    </button>
                  );
                })}
              </div>
            </section>

            {viewMode === 'list' ? (
              <section className="card overflow-hidden p-0">
                <div className="flex items-center justify-between border-b border-slate-200/70 px-6 py-5">
                  <div>
                    <h2 className="text-lg font-semibold text-slate-950">Список поставок</h2>
                    <p className="mt-1 text-sm text-slate-500">{storeFilter === 'all' ? 'Показываем все магазины.' : 'Показываем выбранный магазин.'}</p>
                  </div>
                  <div className="hidden text-sm text-slate-500 sm:block">Товарных строк на странице: {pageStats.itemsCount}</div>
                </div>

                {loading ? (
                  <div className="px-6 py-12 text-center text-sm text-slate-500">Загрузка поставок...</div>
                ) : (
                  <div className="overflow-x-auto">
                    <table className="min-w-full divide-y divide-slate-200">
                      <thead className="bg-slate-50/80">
                        <tr>
                          <th className="px-6 py-3 text-left text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Заявка</th>
                          <th className="px-6 py-3 text-left text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Магазин</th>
                          <th className="px-6 py-3 text-left text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Таймслот</th>
                          <th className="px-6 py-3 text-left text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">ETA</th>
                          <th className="px-6 py-3 text-left text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Склад</th>
                          <th className="px-6 py-3 text-left text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Статус</th>
                        </tr>
                      </thead>
                      <tbody className="divide-y divide-slate-200 bg-white">
                        {supplies.map((supply) => {
                          const expandedRow = expanded === supply.id;
                          const reservationWaitMessage = getReservationWaitMessage(supply);
                          const groupedItems = groupVariantEntries((supply.items || []).map((item) => ({
                            offer_id: item.offer_id ?? '',
                            product_name: item.product_name,
                            attributes: item.attributes || {},
                            quantity: item.quantity,
                            accepted_quantity: item.accepted_quantity ?? null,
                            pack_size: item.pack_size ?? 1,
                          })));
                          return (
                            <Fragment key={supply.id}>
                              <tr className="cursor-pointer transition hover:bg-slate-50" onClick={() => setExpanded(expandedRow ? null : supply.id)}>
                                <td className="px-6 py-4 text-sm font-medium text-slate-900">#{getShortOrderNumber(supply.order_number)}</td>
                                <td className="px-6 py-4 text-sm text-slate-600">
                                  <div className="font-medium text-slate-900">{supply.store_name || '—'}</div>
                                  <div className="mt-1 text-xs text-slate-500">Создана: {formatDateTime(supply.created_at)}</div>
                                </td>
                                <td className="px-6 py-4 text-sm text-slate-600">{formatTimeslotWithDate(supply.timeslot_from, supply.timeslot_to)}</td>
                                <td className="px-6 py-4 text-sm text-slate-600">{formatDate(supply.eta_date)}</td>
                                <td className="px-6 py-4 text-sm text-slate-600">{supply.storage_warehouse_name || supply.dropoff_warehouse_name || '—'}</td>
                                <td className="px-6 py-4 text-sm">
                                  <div className="flex flex-col items-start gap-2">
                                    <span className="inline-flex whitespace-nowrap rounded-full px-3 py-1 text-xs font-medium" style={getSupplyStatusStyle(supply.status)}>{getSupplyStatusLabel(supply.status)}</span>
                                    {reservationWaitMessage && (
                                      <span className="inline-flex rounded-full border border-amber-200 bg-amber-50 px-3 py-1 text-xs font-medium text-amber-800">
                                        Ждет приход
                                      </span>
                                    )}
                                  </div>
                                </td>
                              </tr>
                              {expandedRow && (
                                <tr className="bg-slate-50/70">
                                  <td colSpan={6} className="px-6 py-5">
                                    <div className="rounded-[24px] border border-slate-200 bg-white p-4">
                                      {reservationWaitMessage && (
                                        <div className="mb-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
                                          <div className="font-medium">Ожидает внесения прихода</div>
                                          <div className="mt-1 text-amber-800">{reservationWaitMessage}</div>
                                        </div>
                                      )}
                                      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 pb-4">
                                        <div>
                                          <div className="text-sm font-semibold text-slate-900">Товары в поставке</div>
                                          <div className="mt-1 text-xs text-slate-500">Склад: {supply.storage_warehouse_name || supply.dropoff_warehouse_name || '—'} · ETA: {formatDate(supply.eta_date)}</div>
                                        </div>
                                        <div className="text-xs text-slate-500">Нажми на товар, чтобы раскрыть вариации</div>
                                      </div>
                                      <div className="mt-4 space-y-3">
                                        {groupedItems.length === 0 ? (
                                          <div className="text-sm text-slate-500">Состав поставки ещё не загружен.</div>
                                        ) : (
                                          groupedItems.map((group) => (
                                            <details key={group.productName} className="group overflow-hidden rounded-2xl border border-slate-200 bg-slate-50 open:bg-white">
                                              <summary className="flex cursor-pointer list-none items-center justify-between gap-3 px-4 py-3">
                                                <div>
                                                  <div className="text-sm font-semibold text-slate-900">{group.productName}</div>
                                                  <div className="mt-1 text-xs text-slate-500">Вариаций: {group.colors.reduce((sum, colorGroup) => sum + colorGroup.sizes.reduce((sizeSum, sizeGroup) => sizeSum + sizeGroup.items.length, 0), 0)} · Штук: {group.colors.reduce((sum, colorGroup) => sum + colorGroup.sizes.reduce((sizeSum, sizeGroup) => sizeSum + sizeGroup.items.reduce((itemSum, entry) => itemSum + (entry.quantity || 0), 0), 0), 0)}</div>
                                                </div>
                                                <div className="text-xs font-medium text-slate-500 transition group-open:rotate-180">⌄</div>
                                              </summary>
                                              <div className="space-y-3 border-t border-slate-200 px-4 py-4">
                                                {group.colors.map((colorGroup) => (
                                                  <div key={`${group.productName}-${colorGroup.color}`} className="rounded-2xl border border-slate-200 bg-white p-3">
                                                    <div className="mb-2 inline-flex rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700">{colorGroup.color}</div>
                                                    <div className="space-y-2">
                                                      {colorGroup.sizes.map((sizeGroup) => (
                                                        <div key={`${colorGroup.color}-${sizeGroup.size}`} className="rounded-2xl bg-slate-50 px-3 py-3">
                                                          <div className="mb-2 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">Размер: {sizeGroup.size}</div>
                                                          <div className="space-y-2">
                                                            {sizeGroup.items.map((entry) => (
                                                              <div key={entry.offer_id} className="flex items-start justify-between gap-4 rounded-xl border border-slate-200 bg-white px-3 py-2">
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
                                          ))
                                        )}
                                      </div>
                                    </div>
                                  </td>
                                </tr>
                              )}
                            </Fragment>
                          );
                        })}
                      </tbody>
                    </table>
                  </div>
                )}

                <div className="flex items-center justify-between border-t border-slate-200/70 px-6 py-4 text-sm text-slate-500">
                  <div>Страница {page} из {totalPages}</div>
                  <div className="flex gap-2">
                    <button type="button" disabled={page <= 1} onClick={() => setPage((prev) => Math.max(1, prev - 1))} className="btn-secondary disabled:opacity-50">Назад</button>
                    <button type="button" disabled={page >= totalPages} onClick={() => setPage((prev) => Math.min(totalPages, prev + 1))} className="btn-secondary disabled:opacity-50">Вперёд</button>
                  </div>
                </div>
              </section>
            ) : loading ? (
              <div className="card px-6 py-12 text-center text-sm text-slate-500">Загрузка календаря...</div>
            ) : (
              <section className="rounded-[32px] border border-white/70 bg-white/90 p-4 shadow-[0_24px_60px_rgba(15,23,42,0.08)]">
                <div className="mb-4 flex items-center justify-between px-2">
                  <div>
                    <h2 className="text-lg font-semibold text-slate-950">Календарная сетка</h2>
                    <p className="mt-1 text-sm text-slate-500">Под датой показаны только магазины, у которых на этот день есть поставки.</p>
                  </div>
                  <div className="hidden items-center gap-2 rounded-full bg-slate-100 px-4 py-2 text-sm text-slate-600 md:inline-flex">
                    <CalendarDaysIcon className="h-4 w-4" />
                    {formatDateLabel(parseInputDate(dateFrom))} - {formatDateLabel(parseInputDate(dateTo))}
                  </div>
                </div>
                <div className="grid grid-cols-7 gap-2">
                  {weekdayLabels.map((label) => (
                    <div key={label} className="rounded-2xl bg-slate-100 px-3 py-2 text-center text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">{label}</div>
                  ))}

                  {visibleDays.map((day) => {
                    const dayKey = formatInputDate(day);
                    const dayItems = calendarItemsByDay.get(dayKey) || [];
                    const inSelectedRange = dayKey >= dateFrom && dayKey <= dateTo;
                    const storeGroups = Object.entries(dayItems.reduce<Record<string, CalendarItem[]>>((acc, item) => {
                      const storeName = item.store_name || 'Без магазина';
                      acc[storeName] = acc[storeName] || [];
                      acc[storeName].push(item);
                      return acc;
                    }, {}));

                    return (
                      <div key={dayKey} className={`min-h-[184px] rounded-[24px] border p-2.5 ${inSelectedRange ? 'border-slate-200 bg-white' : 'border-slate-100 bg-slate-50/70 text-slate-400'}`}>
                        <div className="flex items-start justify-between gap-2">
                          <div>
                            <div className={`text-lg font-semibold ${inSelectedRange ? 'text-slate-950' : 'text-slate-400'}`}>{day.getDate()}</div>
                            <div className="text-xs text-slate-500">{day.toLocaleDateString('ru-RU', { month: 'short' })}</div>
                          </div>
                          {dayItems.length > 0 && <div className="rounded-full bg-slate-100 px-2 py-0.5 text-[11px] font-medium text-slate-600">{dayItems.length}</div>}
                        </div>

                        <div className="mt-3 space-y-2">
                          {storeGroups.map(([storeName, storeItems]) => (
                            <button
                              key={`${dayKey}-${storeName}`}
                              type="button"
                              onClick={() => {
                                setOpenedSupplyId(null);
                                setSelectedStoreGroup({ dayKey, storeName, items: storeItems });
                              }}
                              className="flex w-full items-center justify-between gap-2 rounded-2xl border border-slate-200 bg-slate-50 px-3 py-2 text-left transition hover:border-sky-300 hover:bg-sky-50"
                              title={storeName}
                            >
                              <span className="min-w-0 flex-1 truncate text-sm font-medium text-slate-700">{storeName}</span>
                              <span className="shrink-0 rounded-full bg-white px-2 py-0.5 text-[11px] font-semibold text-slate-500 ring-1 ring-inset ring-slate-200">{storeItems.length}</span>
                            </button>
                          ))}
                          {storeGroups.length === 0 && inSelectedRange && <div className="pt-6 text-sm text-slate-400">Поставок нет</div>}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </section>
            )}
          </div>
        )}

        <Transition appear show={Boolean(selectedStoreGroup)} as={Fragment}>
          <Dialog as="div" className="relative z-50" onClose={() => setSelectedStoreGroup(null)}>
            <Transition.Child
              as={Fragment}
              enter="ease-out duration-200"
              enterFrom="opacity-0"
              enterTo="opacity-100"
              leave="ease-in duration-150"
              leaveFrom="opacity-100"
              leaveTo="opacity-0"
            >
              <div className="fixed inset-0 bg-slate-950/35 backdrop-blur-sm" />
            </Transition.Child>

            <div className="fixed inset-0 overflow-y-auto p-4">
              <div className="flex min-h-full items-center justify-center">
                <Transition.Child
                  as={Fragment}
                  enter="ease-out duration-200"
                  enterFrom="opacity-0 translate-y-4 scale-95"
                  enterTo="opacity-100 translate-y-0 scale-100"
                  leave="ease-in duration-150"
                  leaveFrom="opacity-100 translate-y-0 scale-100"
                  leaveTo="opacity-0 translate-y-4 scale-95"
                >
                  <Dialog.Panel className="w-full max-w-5xl rounded-[32px] border border-white/70 bg-white p-6 shadow-[0_28px_80px_rgba(15,23,42,0.22)]">
                    <div className="flex items-start justify-between gap-4">
                      <div>
                        <Dialog.Title className="text-xl font-semibold text-slate-950">{modalTitle}</Dialog.Title>
                        <p className="mt-1 text-sm text-slate-500">Список заявок выбранного магазина на одну дату. Нажми на строку, чтобы раскрыть состав.</p>
                      </div>
                      <button type="button" onClick={() => setSelectedStoreGroup(null)} className="rounded-2xl border border-slate-200 p-2 text-slate-500 transition hover:text-slate-950">
                        <XMarkIcon className="h-5 w-5" />
                      </button>
                    </div>

                    <div className="mt-6 space-y-3 max-h-[70vh] overflow-y-auto pr-1">
                      <div className="hidden rounded-[22px] border border-slate-200 bg-slate-50 px-4 py-3 md:grid md:grid-cols-[120px_150px_minmax(0,1fr)_220px] md:items-center md:gap-4">
                        <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Номер поставки</div>
                        <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Таймслот</div>
                        <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">На склад</div>
                        <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Статус</div>
                      </div>

                      {selectedStoreGroup?.items.map((item) => {
                        const expandedRow = openedSupplyId === item.id;
                        const reservationWaitMessage = getReservationWaitMessage(item);
                        return (
                          <div key={item.id} className="overflow-hidden rounded-[24px] border border-slate-200">
                            <button
                              type="button"
                              onClick={() => setOpenedSupplyId(expandedRow ? null : item.id)}
                              className="w-full px-4 py-4 text-left transition hover:bg-slate-50"
                              style={getSupplyStatusStyle(item.status)}
                            >
                              <div className="grid gap-3 md:grid-cols-[120px_150px_minmax(0,1fr)_220px] md:items-center md:gap-4">
                                <div>
                                  <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">Номер поставки</div>
                                  <div className="mt-1 text-sm font-semibold text-slate-950 md:mt-0">#{getShortOrderNumber(item.order_number)}</div>
                                </div>
                                <div>
                                  <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">Таймслот</div>
                                  <div className="mt-1 text-sm text-slate-800 md:mt-0">{formatTimeslotRange(item.timeslot_from, item.timeslot_to)}</div>
                                </div>
                                <div className="min-w-0">
                                  <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">На склад</div>
                                  <div className="mt-1 truncate text-sm text-slate-800 md:mt-0">{item.storage_warehouse_name || '—'}</div>
                                  <div className="mt-1 text-xs text-slate-500">ETA {formatEtaLabel(item)}</div>
                                </div>
                                <div className="space-y-1">
                                  <div className="text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500 md:hidden">Статус</div>
                                  <div className="text-sm font-medium text-slate-900">{getSupplyStatusLabel(item.status)}</div>
                                  {reservationWaitMessage && <div className="text-xs font-medium text-amber-800">Ждет приход</div>}
                                </div>
                              </div>
                            </button>
                            {expandedRow && (
                              <div className="border-t border-slate-200 bg-white px-4 py-4">
                                {reservationWaitMessage && (
                                  <div className="mb-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
                                    <div className="font-medium">Ожидает внесения прихода</div>
                                    <div className="mt-1 text-amber-800">{reservationWaitMessage}</div>
                                  </div>
                                )}
                                {(item.items || []).length === 0 ? (
                                  <div className="text-sm text-slate-500">Состав заявки пока не загружен.</div>
                                ) : (
                                  <div className="space-y-3">
                                    {groupCalendarSupplyEntries(item.items || []).map((group) => (
                                      <details key={group.productName} className="group overflow-hidden rounded-2xl border border-slate-200 bg-slate-50 open:bg-white" open>
                                        <summary className="flex cursor-pointer list-none items-center justify-between gap-3 px-4 py-3">
                                          <div>
                                            <div className="text-sm font-semibold text-slate-900">{group.productName}</div>
                                            <div className="mt-1 text-xs text-slate-500">Вариаций: {group.colors.reduce((sum, colorGroup) => sum + colorGroup.sizes.reduce((sizeSum, sizeGroup) => sizeSum + sizeGroup.items.length, 0), 0)} · Штук: {group.colors.reduce((sum, colorGroup) => sum + colorGroup.sizes.reduce((sizeSum, sizeGroup) => sizeSum + sizeGroup.items.reduce((itemSum, entry) => itemSum + (entry.quantity || 0), 0), 0), 0)}</div>
                                          </div>
                                          <div className="text-xs font-medium text-slate-500 transition group-open:rotate-180">⌄</div>
                                        </summary>
                                        <div className="space-y-3 border-t border-slate-200 px-4 py-4">
                                          {group.colors.map((colorGroup) => (
                                            <div key={`${group.productName}-${colorGroup.color}`} className="rounded-2xl border border-slate-200 bg-white p-3">
                                              <div className="mb-2 inline-flex rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700">{colorGroup.color}</div>
                                              <div className="space-y-2">
                                                {colorGroup.sizes.map((sizeGroup) => (
                                                  <div key={`${colorGroup.color}-${sizeGroup.size}`} className="rounded-2xl bg-slate-50 px-3 py-3">
                                                    <div className="mb-2 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500">Размер: {sizeGroup.size}</div>
                                                    <div className="space-y-2">
                                                      {sizeGroup.items.map((entry) => (
                                                        <div key={entry.offer_id} className="flex items-start justify-between gap-4 rounded-xl border border-slate-200 bg-white px-3 py-2">
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
                        );
                      })}
                    </div>
                  </Dialog.Panel>
                </Transition.Child>
              </div>
            </div>
          </Dialog>
        </Transition>
      </Layout>
    </ProtectedRoute>
  );
}


function SuppliesPageFallback() {
  return <div className="min-h-screen bg-slate-50" />;
}

export default function SuppliesPage() {
  return (
    <Suspense fallback={<SuppliesPageFallback />}>
      <SuppliesPageInner />
    </Suspense>
  );
}
