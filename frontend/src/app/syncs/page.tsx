'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import {
  ArchiveBoxIcon,
  ArrowPathIcon,
  BanknotesIcon,
  ChartBarSquareIcon,
  CheckCircleIcon,
  ClockIcon,
  CubeIcon,
  ExclamationTriangleIcon,
  TruckIcon,
} from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { Layout } from '@/components/Layout';
import { settingsAPI, type UserSettings } from '@/lib/api/settings';
import { closedMonthsAPI } from '@/lib/api/closedMonths';
import { storesAPI, type Store, type StoreSyncKindStatus, type StoreSyncStatus } from '@/lib/api/stores';
import { useStoreContext } from '@/lib/context/StoreContext';

type SyncKind = 'products' | 'stocks' | 'supplies' | 'reports' | 'finance' | 'closed_months' | 'full';

type SyncRow = {
  key: SyncKind;
  label: string;
  isRunning: boolean;
  canRunManually: boolean;
  currentLabel: string;
  lastRunLabel: string;
  resultLabel: string;
  resultTone: string;
  cadenceLabel: string;
  progressPercent?: number | null;
  helperText?: string;
};

const SYNC_KIND_ORDER: SyncKind[] = ['products', 'stocks', 'supplies', 'reports', 'finance', 'closed_months', 'full'];

const SYNC_KIND_LABELS: Record<SyncKind, string> = {
  products: 'Товары',
  stocks: 'Остатки',
  supplies: 'Поставки',
  reports: 'Отчёты',
  finance: 'Финансы',
  closed_months: 'Закрытые месяцы',
  full: 'Полная синхронизация',
};

const SYNC_KIND_ICONS: Record<SyncKind, typeof CubeIcon> = {
  products: CubeIcon,
  stocks: ArchiveBoxIcon,
  supplies: TruckIcon,
  reports: ChartBarSquareIcon,
  finance: BanknotesIcon,
  closed_months: ClockIcon,
  full: ArrowPathIcon,
};

const STATUS_BADGE: Record<StoreSyncStatus['status'], string> = {
  idle: 'bg-slate-100 text-slate-700',
  queued: 'bg-amber-100 text-amber-800',
  running: 'bg-sky-100 text-sky-800',
  success: 'bg-emerald-100 text-emerald-800',
  failed: 'bg-rose-100 text-rose-800',
  skipped: 'bg-slate-100 text-slate-700',
  cancelled: 'bg-slate-100 text-slate-700',
};

const STATUS_LABEL: Record<StoreSyncStatus['status'], string> = {
  idle: 'Нет запусков',
  queued: 'В очереди',
  running: 'Идёт сейчас',
  success: 'Последний запуск успешный',
  failed: 'Последний запуск с ошибкой',
  skipped: 'Запуск пропущен',
  cancelled: 'Запуск остановлен',
};

function formatMoment(value?: string | null) {
  if (!value) return '—';
  return new Date(value).toLocaleString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function getOverallLastRun(status: StoreSyncStatus) {
  return status.finished_at || status.started_at || status.queued_at || null;
}

function getResultFromKind(kindStatus?: StoreSyncKindStatus) {
  if (!kindStatus) {
    return { label: 'Не запущено', tone: 'bg-slate-100 text-slate-600 ring-slate-200' };
  }
  if (kindStatus.status === 'success') {
    return { label: 'Успешно', tone: 'bg-emerald-50 text-emerald-700 ring-emerald-200' };
  }
  if (kindStatus.status === 'failed') {
    return { label: 'Ошибка', tone: 'bg-rose-50 text-rose-700 ring-rose-200' };
  }

  const lastSuccessAt = kindStatus.last_success_at ? new Date(kindStatus.last_success_at).getTime() : null;
  const lastFailureAt = kindStatus.last_failure_at ? new Date(kindStatus.last_failure_at).getTime() : null;

  if (lastSuccessAt !== null || lastFailureAt !== null) {
    if (lastSuccessAt !== null && (lastFailureAt === null || lastSuccessAt >= lastFailureAt)) {
      return { label: 'Успешно', tone: 'bg-emerald-50 text-emerald-700 ring-emerald-200' };
    }
    return { label: 'Ошибка', tone: 'bg-rose-50 text-rose-700 ring-rose-200' };
  }

  return { label: 'Не запущено', tone: 'bg-slate-100 text-slate-600 ring-slate-200' };
}

function getOverallResult(status: StoreSyncStatus) {
  if (Object.values(status.sync_kinds || {}).some((kindStatus) => kindStatus?.status === 'failed')) {
    return { label: 'Ошибка', tone: 'bg-rose-50 text-rose-700 ring-rose-200' };
  }
  if (status.status === 'success') {
    return { label: 'Успешно', tone: 'bg-emerald-50 text-emerald-700 ring-emerald-200' };
  }
  if (status.status === 'failed') {
    return { label: 'Ошибка', tone: 'bg-rose-50 text-rose-700 ring-rose-200' };
  }
  if (
    status.bootstrap_state === 'failed' ||
    status.bootstrap_state === 'pending' ||
    status.bootstrap_state === 'running'
  ) {
    return { label: 'Не запущено', tone: 'bg-slate-100 text-slate-600 ring-slate-200' };
  }
  return { label: 'Не запущено', tone: 'bg-slate-100 text-slate-600 ring-slate-200' };
}

function getStoreBadge(status: StoreSyncStatus) {
  if (status.bootstrap_state === 'failed') {
    return { tone: STATUS_BADGE.failed, label: 'Ошибка первой загрузки' };
  }
  if (status.bootstrap_state === 'pending') {
    return { tone: STATUS_BADGE.queued, label: 'В очереди' };
  }
  if (status.bootstrap_state === 'running') {
    return { tone: STATUS_BADGE.running, label: 'Выполняется' };
  }
  if (Object.values(status.sync_kinds || {}).some((kindStatus) => kindStatus?.status === 'failed')) {
    return { tone: STATUS_BADGE.failed, label: 'Есть ошибка' };
  }
  if (status.status === 'running') {
    return { tone: STATUS_BADGE.running, label: 'Выполняется' };
  }
  if (status.status === 'queued') {
    return { tone: STATUS_BADGE.queued, label: 'В очереди' };
  }
  if (status.status === 'cancelled') {
    return { tone: STATUS_BADGE.cancelled, label: 'Остановлено' };
  }
  return {
    tone: STATUS_BADGE.idle,
    label: 'Не выполняется',
  };
}

type SyncCadenceSettings = Pick<
  UserSettings,
  | 'sync_products_interval_minutes'
  | 'sync_supplies_interval_minutes'
  | 'sync_stocks_interval_minutes'
  | 'sync_reports_interval_minutes'
  | 'sync_finance_interval_minutes'
>;

function formatCadenceLabel(minutes: number) {
  if (minutes % (24 * 60) === 0) {
    const days = minutes / (24 * 60);
    const suffix = days === 1 ? 'день' : days >= 2 && days <= 4 ? 'дня' : 'дней';
    return days === 1 ? 'Фон: раз в день' : `Фон: каждые ${days} ${suffix}`;
  }
  if (minutes % 60 === 0) {
    const hours = minutes / 60;
    const suffix = hours === 1 ? 'час' : hours >= 2 && hours <= 4 ? 'часа' : 'часов';
    return hours === 1 ? 'Фон: каждый час' : `Фон: каждые ${hours} ${suffix}`;
  }
  return `Фон: каждые ${minutes} минут`;
}

function getCadenceLabels(settings?: SyncCadenceSettings | null): Record<SyncKind, string> {
  return {
    products: formatCadenceLabel(settings?.sync_products_interval_minutes ?? 360),
    stocks: formatCadenceLabel(settings?.sync_stocks_interval_minutes ?? 20),
    supplies: formatCadenceLabel(settings?.sync_supplies_interval_minutes ?? 5),
    reports: formatCadenceLabel(settings?.sync_reports_interval_minutes ?? 180),
    finance: formatCadenceLabel(settings?.sync_finance_interval_minutes ?? 360),
    closed_months: 'Только вручную',
    full: 'Первый запуск и ручной запуск',
  };
}

function buildSyncRows(status: StoreSyncStatus, cadenceLabels: Record<SyncKind, string>): SyncRow[] {
  const activeKinds = new Set((status.active_sync_kinds || []) as SyncKind[]);
  const kindStatuses = status.sync_kinds || {};

  return SYNC_KIND_ORDER.map((kind) => {
    const kindStatus = kindStatuses[kind];

    const hasActiveRuntime = activeKinds.has(kind);
    const isRunning = kindStatus?.status === 'running' || hasActiveRuntime;
    const isQueued = !isRunning && kindStatus?.status === 'queued';
    const fullIsActive = activeKinds.has('full') || kindStatuses.full?.status === 'running' || kindStatuses.full?.status === 'queued';

    if (kindStatus) {
      const result = getResultFromKind(kindStatus);
      const lastMeaningfulRunAt =
        kindStatus.status === 'skipped'
          ? kindStatus.last_success_at || kindStatus.last_failure_at || kindStatus.started_at || kindStatus.queued_at
          : kindStatus.finished_at ||
            kindStatus.last_success_at ||
            kindStatus.last_failure_at ||
            kindStatus.started_at ||
            kindStatus.queued_at;
      const runningPhaseLabel =
        kindStatus.phase_label && kindStatus.phase_label !== 'В очереди'
          ? kindStatus.phase_label
          : kindStatus.status === 'running' || hasActiveRuntime
            ? 'Выполняется'
            : null;
      const currentLabel = isRunning
        ? runningPhaseLabel || 'Выполняется'
        : isQueued
          ? fullIsActive && kind !== 'full'
            ? 'Ожидает полную'
            : (kindStatus.message || '').includes('Ожидает')
            ? 'Ожидает окно'
            : 'В очереди'
          : kindStatus.status === 'cancelled'
            ? 'Остановлена'
            : kindStatus.status === 'skipped'
              ? (kindStatus.message || '').includes('Уже выполняется')
                ? 'Ожидает другое обновление'
                : 'Не выполняется'
              : 'Не выполняется';

      const helperParts = [
        kindStatus.progress_percent && kindStatus.status === 'running'
          ? `Прогресс: ${Math.round(kindStatus.progress_percent)}%`
          : null,
        kindStatus.message &&
        kindStatus.message !== 'Синхронизация завершена' &&
        kindStatus.message !== 'Синхронизация выполняется' &&
        kindStatus.message !== 'Синхронизация в очереди' &&
        kindStatus.message !== 'Для магазина уже выполняется другая синхронизация'
          ? kindStatus.message
          : null,
      ].filter(Boolean);

      return {
        key: kind,
        label: SYNC_KIND_LABELS[kind],
        isRunning: isRunning || isQueued,
        canRunManually: true,
        cadenceLabel: cadenceLabels[kind],
        progressPercent: kindStatus.progress_percent,
        currentLabel:
          kind !== 'full' && fullIsActive && !isRunning && !isQueued
            ? 'Не запущена'
            : currentLabel,
        lastRunLabel: formatMoment(lastMeaningfulRunAt),
        resultLabel: result.label,
        resultTone: result.tone,
        helperText: helperParts.join(' · ') || undefined,
      };
    }

    if (kind === 'full') {
      const overallResult = getOverallResult(status);
      return {
        key: kind,
        label: SYNC_KIND_LABELS[kind],
        isRunning: false,
        canRunManually: true,
        currentLabel: 'Не выполняется',
        lastRunLabel: formatMoment(getOverallLastRun(status)),
        resultLabel: overallResult.label,
        resultTone: overallResult.tone,
        cadenceLabel: cadenceLabels[kind],
        helperText: cadenceLabels[kind],
      };
    }

    return {
      key: kind,
      label: SYNC_KIND_LABELS[kind],
      isRunning: false,
      canRunManually: true,
      currentLabel: 'Не выполняется',
      lastRunLabel: '—',
      resultLabel: 'Не запущено',
      resultTone: 'bg-slate-100 text-slate-600 ring-slate-200',
      cadenceLabel: cadenceLabels[kind],
      helperText: cadenceLabels[kind],
    };
  });
}

export default function SyncsPage() {
  const { refreshStores } = useStoreContext();
  const [stores, setStores] = useState<Store[]>([]);
  const [cadenceSettings, setCadenceSettings] = useState<SyncCadenceSettings | null>(null);
  const [syncStatuses, setSyncStatuses] = useState<Record<number, StoreSyncStatus>>({});
  const [loading, setLoading] = useState(true);
  const [syncingAction, setSyncingAction] = useState<{ storeId: number; kind: SyncKind } | null>(null);
  const [stoppingAction, setStoppingAction] = useState<{ storeId: number; kind: SyncKind } | null>(null);
  const [refreshing, setRefreshing] = useState(false);
  const [lastFetchedAt, setLastFetchedAt] = useState<string | null>(null);

  const fetchSyncStatuses = useCallback(async (storeItems: Store[]) => {
    if (storeItems.length === 0) {
      setSyncStatuses({});
      setLastFetchedAt(new Date().toISOString());
      return;
    }

    const statuses = await Promise.all(
      storeItems.map(async (store) => [store.id, await storesAPI.getSyncStatus(store.id)] as const)
    );
    setSyncStatuses(Object.fromEntries(statuses));
    setLastFetchedAt(new Date().toISOString());
  }, []);

  const fetchData = useCallback(async () => {
    try {
      setLoading(true);
      const [storeItems, settings] = await Promise.all([
        refreshStores(),
        settingsAPI.getSettings(),
      ]);
      setStores(storeItems);
      setCadenceSettings(settings);
      await fetchSyncStatuses(storeItems);
    } catch {
      toast.error('Не удалось загрузить синхронизации');
    } finally {
      setLoading(false);
    }
  }, [fetchSyncStatuses, refreshStores]);

  useEffect(() => {
    void fetchData();
  }, [fetchData]);

  const hasActiveSync = useMemo(
    () =>
      stores.some((store) => {
        const status = syncStatuses[store.id];
        const activeKinds = (status?.active_sync_kinds || []) as SyncKind[];
        return activeKinds.length > 0 || ['queued', 'running'].includes(status?.status || 'idle');
      }),
    [stores, syncStatuses]
  );

  useEffect(() => {
    if (!hasActiveSync || stores.length === 0) return;
    const timerId = window.setInterval(() => {
      if (typeof document !== 'undefined' && document.visibilityState !== 'visible') {
        return;
      }
      void fetchSyncStatuses(stores);
    }, 10000);
    return () => window.clearInterval(timerId);
  }, [fetchSyncStatuses, hasActiveSync, stores]);

  const summary = useMemo(
    () => ({
      total: stores.length,
      running: stores.filter((store) => {
        const status = syncStatuses[store.id];
        const activeKinds = (status?.active_sync_kinds || []) as SyncKind[];
        return (
          activeKinds.length > 0 ||
          ['queued', 'running'].includes(status?.status || 'idle') ||
          ['pending', 'running'].includes(status?.bootstrap_state || '')
        );
      }).length,
      bootstrap: stores.filter((store) => {
        const status = syncStatuses[store.id];
        return ['pending', 'running'].includes(status?.bootstrap_state || '');
      }).length,
      failed: stores.filter((store) => {
        const status = syncStatuses[store.id];
        return (
          status?.bootstrap_state === 'failed' ||
          status?.status === 'failed' ||
          Object.values(status?.sync_kinds || {}).some((kindStatus) => kindStatus?.status === 'failed')
        );
      }).length,
    }),
    [stores, syncStatuses]
  );
  const attentionStores = useMemo(
    () =>
      stores.filter((store) => {
        const status = syncStatuses[store.id];
        return (
          status?.bootstrap_state === 'failed' ||
          status?.status === 'failed' ||
          Object.values(status?.sync_kinds || {}).some((kindStatus) => kindStatus?.status === 'failed')
        );
      }),
    [stores, syncStatuses]
  );
  const bootstrapStores = useMemo(
    () =>
      stores.filter((store) => {
        const status = syncStatuses[store.id];
        return ['pending', 'running'].includes(status?.bootstrap_state || '');
      }),
    [stores, syncStatuses]
  );
  const cadenceLabels = useMemo(() => getCadenceLabels(cadenceSettings), [cadenceSettings]);

  const applyOptimisticManualStart = useCallback((storeId: number, kind: SyncKind) => {
    setSyncStatuses((prev) => {
      const current = prev[storeId];
      if (!current) return prev;

      const syncKinds = { ...(current.sync_kinds || {}) };
      const currentKind = { ...(syncKinds[kind] || {}) };
      currentKind.kind = kind;
      currentKind.status = 'running';
      currentKind.phase = 'running';
      currentKind.phase_label = 'Запускается';
      currentKind.message = 'Ручная синхронизация запускается';
      currentKind.progress_percent = currentKind.progress_percent ?? 5;
      currentKind.queued_at = currentKind.queued_at || new Date().toISOString();
      currentKind.started_at = new Date().toISOString();
      currentKind.finished_at = null;
      syncKinds[kind] = currentKind;

      const activeKinds = new Set([...(current.active_sync_kinds || []), kind]);

      return {
        ...prev,
        [storeId]: {
          ...current,
          status: 'running',
          message: 'Синхронизация запускается',
          active_sync_kinds: Array.from(activeKinds),
          active_sync: { kind, source: 'manual' },
          sync_kinds: syncKinds,
        },
      };
    });
  }, []);

  const handleSync = async (storeId: number, kind: SyncKind) => {
    setSyncingAction({ storeId, kind });
    try {
      const result =
        kind === 'products'
          ? await storesAPI.syncProducts(storeId)
          : kind === 'stocks'
            ? await storesAPI.syncStocks(storeId)
            : kind === 'supplies'
              ? await storesAPI.syncSupplies(storeId)
              : kind === 'reports'
                ? await storesAPI.syncReports(storeId)
                : kind === 'finance'
                  ? await storesAPI.syncFinance(storeId)
                  : kind === 'closed_months'
                    ? await closedMonthsAPI.sync(storeId, { monthsBack: 3 })
                  : await storesAPI.syncFull(storeId);
      const successMessage = 'message' in result && typeof result.message === 'string'
        ? result.message
        : 'Синхронизация поставлена в очередь';
      toast.success(successMessage);
      applyOptimisticManualStart(storeId, kind);
      window.setTimeout(() => {
        void fetchSyncStatuses(stores);
      }, 1200);
      window.setTimeout(() => {
        void fetchSyncStatuses(stores);
      }, 5000);
    } catch (error: any) {
      toast.error(error?.response?.data?.detail || 'Не удалось запустить синхронизацию');
      await fetchSyncStatuses(stores);
    } finally {
      setSyncingAction(null);
    }
  };

  const handleRefresh = async () => {
    try {
      setRefreshing(true);
      await fetchSyncStatuses(stores);
    } catch {
      toast.error('Не удалось обновить статусы синхронизаций');
    } finally {
      setRefreshing(false);
    }
  };

  const handleCancel = async (storeId: number, kind: SyncKind) => {
    setStoppingAction({ storeId, kind });
    try {
      const result =
        kind === 'closed_months'
          ? await closedMonthsAPI.cancel(storeId)
          : await storesAPI.cancelSync(storeId, kind);
      toast.success(result.message || 'Синхронизация остановлена');
      await fetchSyncStatuses(stores);
    } catch (error: any) {
      toast.error(error?.response?.data?.detail || 'Не удалось остановить синхронизацию');
      await fetchSyncStatuses(stores);
    } finally {
      setStoppingAction(null);
    }
  };

  if (loading) {
    return (
      <ProtectedRoute>
        <Layout>
          <div className="flex h-64 items-center justify-center">
            <div className="h-12 w-12 animate-spin rounded-full border-b-2 border-primary-600"></div>
          </div>
        </Layout>
      </ProtectedRoute>
    );
  }

  return (
    <ProtectedRoute>
      <Layout>
        <div className="space-y-6">
          <section className="rounded-[32px] border border-white/70 bg-[radial-gradient(circle_at_top_left,_rgba(255,255,255,0.96),_rgba(240,249,255,0.92)_36%,_rgba(255,247,237,0.90)_100%)] p-8 shadow-[0_24px_60px_rgba(15,23,42,0.08)]">
            <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
              <div className="max-w-3xl">
                <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Синхронизации магазинов</p>
                <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-950 sm:text-4xl">
                  Понятно, что обновляется и чем закончился последний запуск
                </h1>
                <p className="mt-3 text-sm leading-6 text-slate-600 sm:text-base">
                  Для каждого магазина видно, какие данные обновляются сейчас, когда был последний запуск и завершился он успешно или с ошибкой.
                </p>
                <p className="mt-2 text-xs text-slate-500">
                  Последнее обновление страницы: {formatMoment(lastFetchedAt)}
                </p>
                {hasActiveSync && (
                  <p className="mt-2 text-xs text-sky-700">
                    Пока идет синхронизация, страница сама обновляет статусы примерно каждые 10 секунд.
                  </p>
                )}
              </div>
              <div>
                <button
                  type="button"
                  onClick={() => void handleRefresh()}
                  disabled={refreshing}
                  className="inline-flex items-center gap-2 rounded-2xl border border-slate-200 bg-white/85 px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:opacity-60"
                >
                  <ArrowPathIcon className={`h-4 w-4 ${refreshing ? 'animate-spin' : ''}`} />
                  Проверить сейчас
                </button>
              </div>
            </div>
          </section>

          <section className="grid gap-4 md:grid-cols-4">
            <article className="card">
              <div className="text-sm font-medium text-slate-500">Магазинов всего</div>
              <div className="mt-3 text-3xl font-semibold text-slate-950">{summary.total}</div>
            </article>
            <article className="card">
              <div className="text-sm font-medium text-slate-500">Сейчас выполняются</div>
              <div className="mt-3 text-3xl font-semibold text-sky-900">{summary.running}</div>
            </article>
            <article className="card">
              <div className="text-sm font-medium text-slate-500">Первая загрузка</div>
              <div className="mt-3 text-3xl font-semibold text-amber-900">{summary.bootstrap}</div>
            </article>
            <article className="card">
              <div className="text-sm font-medium text-slate-500">Требуют внимания</div>
              <div className="mt-3 text-3xl font-semibold text-rose-900">{summary.failed}</div>
            </article>
          </section>

          {(attentionStores.length > 0 || bootstrapStores.length > 0) && (
            <section className="grid gap-4 xl:grid-cols-2">
              <article className="rounded-3xl border border-rose-200 bg-rose-50 p-5 text-rose-900 shadow-sm">
                <div className="text-sm font-semibold">Что требует внимания</div>
                <div className="mt-2 text-sm">
                  {attentionStores.length > 0
                    ? `Проверь ${attentionStores.length} ${attentionStores.length === 1 ? 'магазин' : 'магазина'}: ${attentionStores.slice(0, 3).map((store) => store.name).join(', ')}${attentionStores.length > 3 ? ' и другие.' : '.'}`
                    : 'Критичных ошибок сейчас не видно.'}
                </div>
              </article>
              <article className="rounded-3xl border border-amber-200 bg-amber-50 p-5 text-amber-900 shadow-sm">
                <div className="text-sm font-semibold">Первая синхронизация</div>
                <div className="mt-2 text-sm">
                  {bootstrapStores.length > 0
                    ? `Сейчас прогреваются ${bootstrapStores.length} ${bootstrapStores.length === 1 ? 'магазин' : 'магазина'}: ${bootstrapStores.slice(0, 3).map((store) => store.name).join(', ')}${bootstrapStores.length > 3 ? ' и другие.' : '.'}`
                    : 'Все подключенные магазины уже прошли первичную загрузку.'}
                </div>
              </article>
            </section>
          )}

          {stores.length === 0 ? (
            <div className="rounded-[28px] border border-dashed border-slate-300 bg-white px-6 py-12 text-center shadow-sm">
              <h3 className="text-lg font-semibold text-slate-900">Сначала подключи магазин</h3>
              <p className="mt-2 text-sm text-slate-500">Как только магазин появится, здесь начнётся мониторинг синхронизаций.</p>
            </div>
          ) : (
            <section className="grid gap-4">
              {stores.map((store) => {
                const syncStatus =
                  syncStatuses[store.id] ||
                  ({
                    store_id: store.id,
                    status: 'idle',
                    message: 'Синхронизация не запускалась',
                    active_sync_kinds: [],
                    sync_kinds: {},
                  } as StoreSyncStatus);

                const activeKinds = ((syncStatus.active_sync_kinds || []) as SyncKind[]).filter((kind) => kind !== 'full');
                const activeSync = syncStatus.active_sync;
                const activeSyncKind = (activeSync?.kind || null) as SyncKind | null;
                const activeSyncSource = activeSync?.source || null;
                const fullQueuedOrRunning = ['queued', 'running'].includes(
                  String((syncStatus.sync_kinds?.full?.status || 'idle'))
                );
                const closedMonthsBusy = ['queued', 'running'].includes(
                  String((syncStatus.sync_kinds?.closed_months?.status || 'idle'))
                );
                const manualOrdinaryBusy =
                  activeSyncSource === 'manual' &&
                  activeSyncKind !== null &&
                  activeSyncKind !== 'full';
                const rows = buildSyncRows(syncStatus, cadenceLabels);
                const fullIsBusy = fullQueuedOrRunning;
                const manualFullIsRunning = activeSyncKind === 'full' && activeSyncSource === 'manual';
                const bootstrapFullIsRunning =
                  activeSyncKind === 'full' &&
                  activeSyncSource !== 'manual' &&
                  ['pending', 'running'].includes(syncStatus.bootstrap_state || '');
                const overallResult = getOverallResult(syncStatus);
                const badge = getStoreBadge(syncStatus);
                const kindFailures = Object.entries(syncStatus.sync_kinds || {}).filter(([, value]) => value?.status === 'failed');
                const bootstrapMessage =
                  syncStatus.bootstrap_state === 'pending'
                    ? 'Первая полная синхронизация уже поставлена в очередь.'
                    : syncStatus.bootstrap_state === 'running'
                      ? syncStatus.sync_kinds?.full?.message || 'Первая полная синхронизация сейчас выполняется.'
                      : syncStatus.bootstrap_state === 'failed'
                        ? syncStatus.sync_kinds?.full?.message || 'Первая полная синхронизация завершилась ошибкой.'
                        : null;

                return (
                  <article key={store.id} className="rounded-[28px] border border-slate-200/80 bg-white p-6 shadow-[0_18px_44px_rgba(15,23,42,0.06)]">
                    <div className="flex flex-col gap-4 sm:flex-row sm:items-start sm:justify-between">
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <h2 className="truncate text-xl font-semibold text-slate-950">{store.name}</h2>
                          <span className={`rounded-full px-3 py-1 text-xs font-medium ${badge.tone}`}>
                            {badge.label}
                          </span>
                        </div>
                        <div className="mt-3 text-sm text-slate-700">
                          Сейчас обновляется:{' '}
                          <span className="font-medium text-slate-950">
                            {((syncStatus.active_sync_kinds || []) as SyncKind[]).length > 0
                              ? ((syncStatus.active_sync_kinds || []) as SyncKind[]).map((kind) => SYNC_KIND_LABELS[kind]).join(', ')
                              : 'ничего'}
                          </span>
                        </div>
                        <div className="mt-1 text-sm text-slate-500">
                          Последнее обновление статуса: {formatMoment(syncStatus.updated_at)}
                        </div>
                      </div>

                      <button
                        type="button"
                        onClick={() =>
                          manualFullIsRunning ? void handleCancel(store.id, 'full') : void handleSync(store.id, 'full')
                        }
                        disabled={
                          !!syncingAction ||
                          !!stoppingAction ||
                          (fullQueuedOrRunning && !manualFullIsRunning) ||
                          bootstrapFullIsRunning ||
                          closedMonthsBusy ||
                          manualOrdinaryBusy
                        }
                        className="inline-flex items-center gap-2 rounded-2xl border border-slate-200 px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:opacity-60"
                      >
                        <ArrowPathIcon
                          className={`h-4 w-4 ${
                            (syncingAction?.storeId === store.id && syncingAction.kind === 'full') ||
                            (stoppingAction?.storeId === store.id && stoppingAction.kind === 'full')
                              ? 'animate-spin'
                              : ''
                          }`}
                        />
                        {manualFullIsRunning ? 'Остановить полную синхронизацию' : 'Запустить полную синхронизацию'}
                      </button>
                    </div>

                    <div className="mt-5 rounded-[24px] border border-slate-200 bg-slate-50/70 p-4">
                      <div className="flex flex-wrap items-center justify-between gap-3 border-b border-slate-200 pb-3">
                        <div>
                          <h3 className="text-base font-semibold text-slate-950">Что происходит по синхронизациям</h3>
                          <p className="mt-1 text-sm text-slate-500">В текущем статусе видно, что происходит прямо сейчас. В результате — чем закончился последний запуск.</p>
                        </div>
                        <span className={`inline-flex rounded-full px-3 py-1 text-xs font-medium ring-1 ring-inset ${overallResult.tone}`}>
                          {overallResult.label}
                        </span>
                      </div>

                      <div className="mt-3 overflow-hidden rounded-2xl border border-slate-200 bg-white">
                        <div className="grid grid-cols-[1.25fr_0.9fr_1fr_0.9fr_0.7fr] gap-3 border-b border-slate-200 bg-slate-50 px-4 py-3 text-[11px] font-semibold uppercase tracking-[0.16em] text-slate-500">
                          <div>Синхронизация</div>
                          <div>Текущий статус</div>
                          <div>Последний запуск</div>
                          <div>Результат</div>
                          <div>Действие</div>
                        </div>

                        <div className="divide-y divide-slate-200">
                          {rows.map((row) => {
                            const Icon = SYNC_KIND_ICONS[row.key];
                            const rowIsStarting =
                              syncingAction?.storeId === store.id && syncingAction.kind === row.key;
                            const rowIsStopping =
                              stoppingAction?.storeId === store.id && stoppingAction.kind === row.key;
                            const rowCanStop =
                              ((activeSyncKind === row.key && activeSyncSource === 'manual') || row.key === 'closed_months') &&
                              row.isRunning &&
                              !(row.key === 'full' && ['pending', 'running'].includes(syncStatus.bootstrap_state || ''));
                            const rowRunDisabled =
                              row.key === 'full' ||
                              closedMonthsBusy ||
                              fullIsBusy ||
                              manualOrdinaryBusy ||
                              manualFullIsRunning ||
                              !!syncingAction ||
                              !!stoppingAction ||
                              !row.canRunManually;
                            return (
                              <div key={row.key} className={`grid grid-cols-[1.25fr_0.9fr_1fr_0.9fr_0.7fr] gap-3 px-4 py-3 ${row.isRunning ? 'bg-sky-50/60' : ''}`}>
                                <div className="min-w-0">
                                  <div className="flex items-center gap-2 text-sm font-medium text-slate-900">
                                    <Icon
                                      className={`h-4 w-4 shrink-0 ${
                                        row.isRunning && row.key === 'full'
                                          ? 'animate-spin text-sky-700'
                                          : row.isRunning
                                            ? 'text-sky-700'
                                            : 'text-slate-400'
                                      }`}
                                    />
                                    <span className="truncate">{row.label}</span>
                                  </div>
                                  <div className="mt-1 text-[11px] uppercase tracking-[0.12em] text-slate-400">{row.cadenceLabel}</div>
                                  {row.helperText && row.helperText !== row.cadenceLabel ? (
                                    <div className="mt-1 text-xs text-slate-400">{row.helperText}</div>
                                  ) : null}
                                  {row.progressPercent && row.isRunning ? (
                                    <div className="mt-2 h-1.5 overflow-hidden rounded-full bg-slate-100">
                                      <div
                                        className="h-full rounded-full bg-sky-500 transition-all"
                                        style={{ width: `${Math.max(6, Math.min(100, row.progressPercent))}%` }}
                                      />
                                    </div>
                                  ) : null}
                                </div>

                                <div className="text-sm">
                                  <span className={`inline-flex rounded-full px-2.5 py-1 text-xs font-medium ${row.isRunning ? 'bg-sky-100 text-sky-800' : 'bg-slate-100 text-slate-600'}`}>
                                    {row.currentLabel}
                                  </span>
                                </div>

                                <div className="text-sm text-slate-700">{row.lastRunLabel}</div>

                                <div className="text-sm">
                                  <span className={`inline-flex rounded-full px-2.5 py-1 text-xs font-medium ring-1 ring-inset ${row.resultTone}`}>
                                    {row.resultLabel}
                                  </span>
                                </div>

                                <div className="text-sm">
                                  {row.key === 'full' ? (
                                    <span className="text-xs text-slate-400">Кнопка сверху</span>
                                  ) : (
                                    rowCanStop ? (
                                      <button
                                        type="button"
                                        onClick={() => void handleCancel(store.id, row.key)}
                                        disabled={!!syncingAction || !!stoppingAction}
                                        className="inline-flex items-center gap-2 rounded-xl border border-rose-200 bg-rose-50 px-3 py-1.5 text-xs font-medium text-rose-700 transition hover:border-rose-300 hover:text-rose-800 disabled:cursor-not-allowed disabled:opacity-50"
                                      >
                                        <ArrowPathIcon className={`h-3.5 w-3.5 ${rowIsStopping ? 'animate-spin' : ''}`} />
                                        Остановить
                                      </button>
                                    ) : (
                                      <button
                                        type="button"
                                        onClick={() => void handleSync(store.id, row.key)}
                                        disabled={rowRunDisabled}
                                        className="inline-flex items-center gap-2 rounded-xl border border-slate-200 px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:border-slate-300 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-50"
                                      >
                                        <ArrowPathIcon className={`h-3.5 w-3.5 ${rowIsStarting ? 'animate-spin' : ''}`} />
                                        Запустить
                                      </button>
                                    )
                                  )}
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    </div>

                    {syncStatus.bootstrap_state === 'failed' && bootstrapMessage ? (
                      <div className="mt-4 flex items-start gap-2 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
                        <ExclamationTriangleIcon className="mt-0.5 h-5 w-5 shrink-0" />
                        <div>
                          <div className="font-medium">Первая полная синхронизация требует внимания</div>
                          <div className="mt-1 text-rose-600">{bootstrapMessage}</div>
                        </div>
                      </div>
                    ) : kindFailures.length > 0 ? (
                      <div className="mt-4 flex items-start gap-2 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
                        <ExclamationTriangleIcon className="mt-0.5 h-5 w-5 shrink-0" />
                        <div>
                          <div className="font-medium">Есть проблемы в фоновых синхронизациях</div>
                          <div className="mt-1 text-rose-600">
                            {kindFailures.map(([kind, value]) => `${SYNC_KIND_LABELS[kind as SyncKind]}: ${value?.message || 'ошибка'}`).join(' · ')}
                          </div>
                        </div>
                      </div>
                    ) : syncStatus.status === 'failed' && syncStatus.message ? (
                      <div className="mt-4 flex items-start gap-2 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm text-rose-700">
                        <ExclamationTriangleIcon className="mt-0.5 h-5 w-5 shrink-0" />
                        <div>
                          <div className="font-medium">Последний запуск завершился с ошибкой</div>
                          <div className="mt-1 text-rose-600">{syncStatus.message}</div>
                        </div>
                      </div>
                    ) : bootstrapMessage ? (
                      <div className="mt-4 flex items-start gap-2 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 text-sm text-sky-700">
                        <ClockIcon className="mt-0.5 h-5 w-5 shrink-0" />
                        <div>{bootstrapMessage}</div>
                      </div>
                    ) : syncStatus.status === 'success' ? (
                      <div className="mt-4 flex items-start gap-2 rounded-2xl border border-emerald-200 bg-emerald-50 px-4 py-3 text-sm text-emerald-700">
                        <CheckCircleIcon className="mt-0.5 h-5 w-5 shrink-0" />
                        <div>Последний известный запуск завершился успешно.</div>
                      </div>
                    ) : syncStatus.status === 'running' || syncStatus.status === 'queued' ? (
                      <div className="mt-4 flex items-start gap-2 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 text-sm text-sky-700">
                        <ClockIcon className="mt-0.5 h-5 w-5 shrink-0" />
                        <div>{syncStatus.message || 'Синхронизация выполняется'}</div>
                      </div>
                    ) : null}
                  </article>
                );
              })}
            </section>
          )}
        </div>
      </Layout>
    </ProtectedRoute>
  );
}
