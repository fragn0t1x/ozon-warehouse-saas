'use client';

import { useEffect, useMemo, useState } from 'react';
import { useRouter, useSearchParams } from 'next/navigation';
import {
  ArrowPathIcon,
  CheckBadgeIcon,
  ClockIcon,
  CubeTransparentIcon,
} from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { storesAPI, type StoreSyncStatus } from '@/lib/api/stores';
import { useAuth } from '@/lib/context/AuthContext';

const POLL_INTERVAL_MS = 8000;

const PHASES = [
  { key: 'prepare', label: 'Подготовка' },
  { key: 'products', label: 'Товары' },
  { key: 'stocks', label: 'Остатки' },
  { key: 'supplies', label: 'Поставки' },
  { key: 'reports', label: 'Отчеты' },
  { key: 'finance', label: 'Финансы' },
  { key: 'completed', label: 'Готово' },
] as const;

function formatTimestamp(value?: string | null) {
  if (!value) {
    return 'Еще не обновлялось';
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return 'Еще не обновлялось';
  }

  return new Intl.DateTimeFormat('ru-RU', {
    dateStyle: 'short',
    timeStyle: 'medium',
  }).format(parsed);
}

function getProgress(status: StoreSyncStatus | null) {
  const full = status?.sync_kinds?.full;
  const raw = full?.progress_percent;
  if (typeof raw === 'number' && Number.isFinite(raw)) {
    return Math.max(4, Math.min(100, Math.round(raw)));
  }
  if (status?.bootstrap_state === 'completed') {
    return 100;
  }
  if (status?.bootstrap_state === 'failed') {
    return 12;
  }
  return status?.status === 'running' ? 16 : 8;
}

export default function OnboardingSyncPage() {
  const router = useRouter();
  const searchParams = useSearchParams();
  const { bootstrapStoreId, refreshBootstrapStatus, user } = useAuth();

  const [status, setStatus] = useState<StoreSyncStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [retrying, setRetrying] = useState(false);

  const storeId = useMemo(() => {
    const fromQuery = searchParams.get('store');
    const parsed = fromQuery ? Number(fromQuery) : NaN;
    if (Number.isFinite(parsed) && parsed > 0) {
      return parsed;
    }
    return bootstrapStoreId;
  }, [bootstrapStoreId, searchParams]);

  useEffect(() => {
    if (!storeId) {
      return;
    }

    let cancelled = false;

    const loadStatus = async (showInitialLoader = false) => {
      if (!showInitialLoader && typeof document !== 'undefined' && document.visibilityState !== 'visible') {
        return;
      }
      if (showInitialLoader) {
        setLoading(true);
      }

      try {
        const nextStatus = await storesAPI.getSyncStatus(storeId);
        if (cancelled) {
          return;
        }

        setStatus(nextStatus);

        if (nextStatus.bootstrap_state === 'completed') {
          await refreshBootstrapStatus(user);
          router.replace('/dashboard');
          router.refresh();
        }
      } catch {
        if (!cancelled) {
          toast.error('Не удалось получить статус первой синхронизации');
        }
      } finally {
        if (!cancelled && showInitialLoader) {
          setLoading(false);
        }
      }
    };

    void loadStatus(true);
    const timer = window.setInterval(() => {
      void loadStatus(false);
    }, POLL_INTERVAL_MS);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [refreshBootstrapStatus, router, storeId, user]);

  const fullStatus = status?.sync_kinds?.full;
  const progress = getProgress(status);
  const activePhase = fullStatus?.phase ?? (status?.bootstrap_state === 'completed' ? 'completed' : 'prepare');
  const activePhaseIndex = Math.max(PHASES.findIndex((phase) => phase.key === activePhase), 0);

  const handleRetry = async () => {
    if (!storeId) {
      toast.error('Не удалось определить магазин для повторной синхронизации');
      return;
    }

    setRetrying(true);
    try {
      await storesAPI.syncFull(storeId);
      toast.success('Повторная полная синхронизация поставлена в очередь');
      const refreshed = await storesAPI.getSyncStatus(storeId);
      setStatus(refreshed);
    } catch {
      toast.error('Не удалось поставить повторную синхронизацию в очередь');
    } finally {
      setRetrying(false);
    }
  };

  if (!storeId && !loading) {
    return (
      <ProtectedRoute>
        <div className="min-h-screen bg-[linear-gradient(180deg,#f6fbf8_0%,#edf6ff_100%)] px-4 py-8 sm:px-6 lg:px-8">
          <div className="mx-auto max-w-3xl rounded-[32px] border border-white/80 bg-white/90 p-8 text-center shadow-[0_24px_60px_rgba(15,23,42,0.08)]">
            <h1 className="text-3xl font-semibold tracking-tight text-slate-950">Не удалось определить магазин для первой синхронизации.</h1>
            <p className="mt-4 text-sm leading-6 text-slate-500">
              Попробуй заново открыть кабинет. Если проблема повторится, мы отдельно доберем эту ветку в статусах синхронизации.
            </p>
          </div>
        </div>
      </ProtectedRoute>
    );
  }

  return (
    <ProtectedRoute>
      <div className="min-h-screen bg-[radial-gradient(circle_at_top_left,_rgba(24,196,145,0.16),_transparent_30%),radial-gradient(circle_at_bottom_right,_rgba(244,114,48,0.14),_transparent_28%),linear-gradient(180deg,#f6fbf8_0%,#edf6ff_100%)] px-4 py-8 sm:px-6 lg:px-8">
        <div className="mx-auto max-w-5xl space-y-6">
          <section className="overflow-hidden rounded-[32px] border border-white/70 bg-[linear-gradient(135deg,rgba(10,39,52,0.98),rgba(8,95,122,0.94))] px-6 py-8 text-white shadow-[0_24px_60px_rgba(15,23,42,0.18)] sm:px-8">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div className="max-w-2xl">
                <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/10 px-4 py-1 text-xs font-semibold uppercase tracking-[0.18em] text-emerald-100">
                  <CubeTransparentIcon className="h-4 w-4" />
                  Первая синхронизация магазина
                </div>
                <h1 className="mt-5 text-3xl font-semibold tracking-tight">Личный кабинет откроется сразу после полной загрузки данных из Ozon.</h1>
                <p className="mt-3 max-w-2xl text-sm leading-6 text-cyan-50/90">
                  Сейчас подтягиваем товары, остатки, поставки, отчеты и финансовые данные. Обычно это занимает несколько минут, в зависимости от размера магазина.
                </p>
              </div>
              <div className="rounded-3xl border border-white/10 bg-white/10 px-5 py-4 text-right">
                <div className="text-xs uppercase tracking-[0.18em] text-cyan-100">Прогресс</div>
                <div className="mt-2 text-4xl font-semibold">{progress}%</div>
                <div className="mt-2 text-xs text-cyan-50/80">Последнее обновление: {formatTimestamp(status?.updated_at)}</div>
              </div>
            </div>

            <div className="mt-8">
              <div className="h-4 overflow-hidden rounded-full bg-white/10">
                <div
                  className="h-full rounded-full bg-[linear-gradient(90deg,#34d399_0%,#fde047_55%,#fb923c_100%)] transition-all duration-700"
                  style={{ width: `${progress}%` }}
                />
              </div>
              <div className="mt-3 flex flex-wrap items-center justify-between gap-3 text-sm text-cyan-50/90">
                <span>{fullStatus?.message || status?.message || 'Подготавливаем синхронизацию магазина'}</span>
                <span>{status?.bootstrap_state === 'failed' ? 'Нужно повторить запуск' : 'Страница обновляется автоматически'}</span>
              </div>
            </div>
          </section>

          <section className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
            <div className="rounded-[28px] border border-white/80 bg-white/88 p-6 shadow-[0_18px_50px_rgba(15,23,42,0.08)]">
              <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Статус</div>
              <div className="mt-3 text-2xl font-semibold text-slate-950">
                {status?.bootstrap_state === 'completed'
                  ? 'Готово'
                  : status?.bootstrap_state === 'failed'
                    ? 'Нужна повторная синхронизация'
                    : 'Идет загрузка'}
              </div>
              <p className="mt-2 text-sm leading-6 text-slate-500">
                {status?.bootstrap_state === 'failed'
                  ? 'Синхронизация остановилась с ошибкой. Можно безопасно запустить ее повторно.'
                  : 'Кабинет откроется автоматически сразу после завершения первой полной синхронизации.'}
              </p>
            </div>

            <div className="rounded-[28px] border border-white/80 bg-white/88 p-6 shadow-[0_18px_50px_rgba(15,23,42,0.08)]">
              <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Текущий этап</div>
              <div className="mt-3 text-2xl font-semibold text-slate-950">{fullStatus?.phase_label || 'Подготовка'}</div>
              <p className="mt-2 text-sm leading-6 text-slate-500">{fullStatus?.message || 'Готовим первый импорт данных магазина.'}</p>
            </div>

            <div className="rounded-[28px] border border-white/80 bg-white/88 p-6 shadow-[0_18px_50px_rgba(15,23,42,0.08)]">
              <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Поставлена в очередь</div>
              <div className="mt-3 text-lg font-semibold text-slate-950">{formatTimestamp(status?.queued_at)}</div>
              <p className="mt-2 text-sm leading-6 text-slate-500">Если очередь свободна, синхронизация обычно стартует почти сразу.</p>
            </div>

            <div className="rounded-[28px] border border-white/80 bg-white/88 p-6 shadow-[0_18px_50px_rgba(15,23,42,0.08)]">
              <div className="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Запуск</div>
              <div className="mt-3 text-lg font-semibold text-slate-950">{formatTimestamp(status?.started_at)}</div>
              <p className="mt-2 text-sm leading-6 text-slate-500">Когда первый импорт завершится, мы сразу перекинем тебя в дашборд.</p>
            </div>
          </section>

          <section className="rounded-[32px] border border-white/80 bg-white/90 p-6 shadow-[0_24px_60px_rgba(15,23,42,0.08)] sm:p-8">
            <div className="flex items-center justify-between gap-4">
              <div>
                <h2 className="text-2xl font-semibold tracking-tight text-slate-950">Этапы загрузки</h2>
                <p className="mt-2 text-sm leading-6 text-slate-500">Так видно, где именно находится первая полная синхронизация магазина.</p>
              </div>
              {status?.bootstrap_state === 'failed' && (
                <button
                  type="button"
                  onClick={handleRetry}
                  disabled={retrying}
                  className="inline-flex items-center gap-2 rounded-full bg-slate-950 px-5 py-3 text-sm font-semibold text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  <ArrowPathIcon className={retrying ? 'h-5 w-5 animate-spin' : 'h-5 w-5'} />
                  Запустить повторно
                </button>
              )}
            </div>

            <div className="mt-8 grid gap-4 lg:grid-cols-2">
              {PHASES.map((phase, index) => {
                const isDone = status?.bootstrap_state === 'completed' ? true : index < activePhaseIndex;
                const isActive = index === activePhaseIndex && status?.bootstrap_state !== 'completed';
                const isFailed = status?.bootstrap_state === 'failed' && index === activePhaseIndex;

                return (
                  <div
                    key={phase.key}
                    className={[
                      'rounded-[28px] border p-5 transition',
                      isFailed
                        ? 'border-rose-300 bg-rose-50'
                        : isDone
                          ? 'border-emerald-300 bg-emerald-50'
                          : isActive
                            ? 'border-sky-300 bg-sky-50'
                            : 'border-slate-200 bg-slate-50/70',
                    ].join(' ')}
                  >
                    <div className="flex items-center gap-3">
                      <div
                        className={[
                          'flex h-11 w-11 items-center justify-center rounded-full border',
                          isFailed
                            ? 'border-rose-300 text-rose-600'
                            : isDone
                              ? 'border-emerald-300 text-emerald-600'
                              : isActive
                                ? 'border-sky-300 text-sky-600'
                                : 'border-slate-300 text-slate-500',
                        ].join(' ')}
                      >
                        {isDone ? <CheckBadgeIcon className="h-6 w-6" /> : <ClockIcon className="h-6 w-6" />}
                      </div>
                      <div>
                        <div className="text-sm font-semibold uppercase tracking-[0.16em] text-slate-500">Этап {index + 1}</div>
                        <div className="mt-1 text-lg font-semibold text-slate-950">{phase.label}</div>
                      </div>
                    </div>
                  </div>
                );
              })}
            </div>
          </section>
        </div>
      </div>
    </ProtectedRoute>
  );
}
