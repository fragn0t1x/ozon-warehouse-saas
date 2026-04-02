'use client';

import { useEffect, useMemo, useState } from 'react';
import {
  ArrowPathIcon,
  CheckCircleIcon,
  ClockIcon,
  ExclamationTriangleIcon,
} from '@heroicons/react/24/outline';

import { storesAPI, type StoreSyncKindStatus, type StoreSyncStatus } from '@/lib/api/stores';

type SyncKind = 'products' | 'stocks' | 'supplies' | 'reports' | 'finance' | 'full';

type SyncFreshnessPanelProps = {
  storeIds: number[];
  kinds: SyncKind[];
  title: string;
  description: string;
  periodLabel?: string;
};

const SYNC_FRESHNESS_POLL_MS = 30000;

const ACTIVE_STATUSES = new Set(['queued', 'running']);
const FAILURE_STATUSES = new Set(['failed']);
const KIND_LABELS: Record<SyncKind, string> = {
  products: 'Каталог',
  supplies: 'Поставки',
  stocks: 'Остатки',
  reports: 'Отчеты',
  finance: 'Финансы',
  full: 'Полная синхронизация',
};

function formatMoment(value?: string | null) {
  if (!value) {
    return 'Нет подтвержденного обновления';
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return 'Нет подтвержденного обновления';
  }

  return parsed.toLocaleString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function parseTimestamp(value?: string | null) {
  if (!value) {
    return null;
  }

  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }

  return parsed.getTime();
}

function getFullKind(status: StoreSyncStatus) {
  return status.sync_kinds?.full;
}

function pickSuccessfulTimestamp(kindStatus?: StoreSyncKindStatus, fullKindStatus?: StoreSyncKindStatus) {
  if (!kindStatus) {
    return pickSuccessfulTimestamp(fullKindStatus);
  }

  const directSuccess = kindStatus.last_success_at || (kindStatus.status === 'success' ? kindStatus.finished_at || null : null);
  if (directSuccess) {
    return directSuccess;
  }

  return fullKindStatus?.last_success_at || (fullKindStatus?.status === 'success' ? fullKindStatus.finished_at || null : null);
}

function getLatestTimestamp(status: StoreSyncStatus, kinds: SyncKind[]) {
  const fullKind = getFullKind(status);
  const timestamps = kinds
    .map((kind) => pickSuccessfulTimestamp(status.sync_kinds?.[kind], fullKind))
    .filter((value): value is string => Boolean(value))
    .sort((left, right) => (parseTimestamp(left) ?? 0) - (parseTimestamp(right) ?? 0));

  return timestamps.at(-1) || null;
}

function getActiveKind(status: StoreSyncStatus, kinds: SyncKind[]) {
  const requestedKind = kinds
    .map((kind) => [kind, status.sync_kinds?.[kind]] as const)
    .find(([, kindStatus]) => kindStatus && ACTIVE_STATUSES.has(kindStatus.status));

  if (requestedKind) {
    return requestedKind;
  }

  const fullKind = getFullKind(status);
  if (fullKind && ACTIVE_STATUSES.has(fullKind.status)) {
    return ['full', fullKind] as const;
  }

  return undefined;
}

function getFailedKinds(status: StoreSyncStatus, kinds: SyncKind[]) {
  const requestedFailures = kinds
    .map((kind) => [kind, status.sync_kinds?.[kind]] as const)
    .filter(([, kindStatus]) => kindStatus && FAILURE_STATUSES.has(kindStatus.status));

  if (requestedFailures.length > 0) {
    return requestedFailures;
  }

  const fullKind = getFullKind(status);
  if (fullKind && FAILURE_STATUSES.has(fullKind.status)) {
    return [['full', fullKind] as const];
  }

  return [];
}

function getKindStatus(status: StoreSyncStatus, kind: SyncKind) {
  const direct = status.sync_kinds?.[kind];
  if (direct) {
    return direct;
  }

  const fullKind = getFullKind(status);
  if (fullKind && ['success', 'running', 'queued', 'failed'].includes(fullKind.status)) {
    return fullKind;
  }

  return undefined;
}

export function SyncFreshnessPanel({ storeIds, kinds, title, description, periodLabel }: SyncFreshnessPanelProps) {
  const [statuses, setStatuses] = useState<Record<number, StoreSyncStatus>>({});

  useEffect(() => {
    if (storeIds.length === 0) {
      setStatuses({});
      return;
    }

    let cancelled = false;

    const loadStatuses = async () => {
      if (typeof document !== 'undefined' && document.visibilityState !== 'visible') {
        return;
      }
      try {
        const entries = await Promise.all(
          storeIds.map(async (storeId) => [storeId, await storesAPI.getSyncStatus(storeId)] as const),
        );

        if (!cancelled) {
          setStatuses(Object.fromEntries(entries));
        }
      } catch {
        if (!cancelled) {
          setStatuses({});
        }
      }
    };

    void loadStatuses();
    const timer = window.setInterval(() => {
      void loadStatuses();
    }, SYNC_FRESHNESS_POLL_MS);

    return () => {
      cancelled = true;
      window.clearInterval(timer);
    };
  }, [storeIds]);

  const summary = useMemo(() => {
    const relevantStatuses = storeIds
      .map((storeId) => statuses[storeId])
      .filter((status): status is StoreSyncStatus => Boolean(status));

    const timestamps = relevantStatuses
      .map((status) => getLatestTimestamp(status, kinds))
      .filter((value): value is string => Boolean(value))
      .sort();

    const activeStatuses = relevantStatuses
      .map((status) => [status, getActiveKind(status, kinds)] as const)
      .filter(([, activeKind]) => Boolean(activeKind));

    const failedStatuses = relevantStatuses
      .map((status) => [status, getFailedKinds(status, kinds)] as const)
      .filter(([, failedKinds]) => failedKinds.length > 0);

    return {
      latestAt: timestamps.at(-1) || null,
      coveredStores: timestamps.length,
      totalStores: storeIds.length,
      activeStatuses,
      failedStatuses,
      relevantStatuses,
    };
  }, [kinds, statuses, storeIds]);

  if (storeIds.length === 0) {
    return null;
  }

  const hasActive = summary.activeStatuses.length > 0;
  const hasFailures = summary.failedStatuses.length > 0;
  const statusTone = hasFailures ? 'problem' : hasActive ? 'active' : summary.coveredStores > 0 ? 'ok' : 'waiting';
  const statusLabel =
    hasFailures
      ? 'Есть ошибки'
      : hasActive
        ? 'Идет обновление'
        : summary.coveredStores > 0
          ? 'Данные подтверждены'
          : 'Ожидаем первую синхронизацию';

  const kindCards = kinds.map((kind) => {
    const kindStatuses = summary.relevantStatuses
      .map((status) => getKindStatus(status, kind))
      .filter((item): item is StoreSyncKindStatus => Boolean(item));

    const latestSuccess = kindStatuses
      .map((item) => item.last_success_at || (item.status === 'success' ? item.finished_at || null : null))
      .filter((value): value is string => Boolean(value))
      .sort((left, right) => (parseTimestamp(left) ?? 0) - (parseTimestamp(right) ?? 0))
      .at(-1) || null;

    const activeItem = kindStatuses.find((item) => ACTIVE_STATUSES.has(item.status));
    const failedItem = kindStatuses.find((item) => FAILURE_STATUSES.has(item.status));
    const covered = kindStatuses.filter((item) => Boolean(item.last_success_at || item.finished_at)).length;

    let badgeClass =
      'border-slate-200 bg-slate-100 text-slate-700';
    let badgeLabel = 'Нет данных';
    let detail = 'Еще не было успешной синхронизации';

    if (failedItem) {
      badgeClass = 'border-rose-200 bg-rose-50 text-rose-700';
      badgeLabel = 'Ошибка';
      detail = failedItem.phase_label || failedItem.message || 'Нужна повторная синхронизация';
    } else if (activeItem) {
      badgeClass = 'border-sky-200 bg-sky-50 text-sky-700';
      badgeLabel = activeItem.status === 'queued' ? 'В очереди' : 'Обновляется';
      detail = activeItem.phase_label || activeItem.message || 'Сервис обновляет данные';
    } else if (latestSuccess) {
      badgeClass = 'border-emerald-200 bg-emerald-50 text-emerald-700';
      badgeLabel = 'Обновлено';
      detail = `Последний успех: ${formatMoment(latestSuccess)}`;
    }

    return {
      kind,
      label: KIND_LABELS[kind],
      latestSuccess,
      covered,
      badgeClass,
      badgeLabel,
      detail,
    };
  });

  return (
    <div className="rounded-[24px] border border-slate-200 bg-white/88 p-5 shadow-[0_12px_30px_rgba(15,23,42,0.05)]">
      <div className="flex flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-slate-500">Свежесть данных</div>
          <div className="mt-2 text-base font-semibold text-slate-950">{title}</div>
          <p className="mt-1 text-sm leading-6 text-slate-500">{description}</p>
        </div>

        <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-right">
          <div className="text-xs uppercase tracking-[0.14em] text-slate-500">Общий статус</div>
          <div className="mt-2 flex items-center justify-end gap-2 text-sm font-semibold text-slate-950">
            {statusTone === 'active' ? <ArrowPathIcon className="h-4 w-4 animate-spin text-sky-600" /> : null}
            {statusTone === 'problem' ? <ExclamationTriangleIcon className="h-4 w-4 text-rose-600" /> : null}
            {statusTone === 'ok' ? <CheckCircleIcon className="h-4 w-4 text-emerald-600" /> : null}
            {statusTone === 'waiting' ? <ClockIcon className="h-4 w-4 text-amber-600" /> : null}
            {statusLabel}
          </div>
          <div className="mt-1 text-xs text-slate-500">Последний успех: {formatMoment(summary.latestAt)}</div>
          <div className="mt-1 text-xs text-slate-400">
            Магазинов с подтвержденным обновлением: {summary.coveredStores} из {summary.totalStores}
          </div>
          {periodLabel ? <div className="mt-1 text-xs text-slate-400">{periodLabel}</div> : null}
        </div>
      </div>

      <div className="mt-5 grid gap-3 md:grid-cols-2 2xl:grid-cols-4">
        {kindCards.map((card) => (
          <div key={card.kind} className="min-w-0 rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
            <div className="flex items-start justify-between gap-3">
              <div className="min-w-0 flex-1">
                <div className="text-sm font-semibold text-slate-950">{card.label}</div>
                <div className="mt-1 break-words text-xs text-slate-500">
                  {card.latestSuccess ? `Последнее обновление: ${formatMoment(card.latestSuccess)}` : 'Пока не обновлялось'}
                </div>
              </div>
              <span
                className={`inline-flex max-w-[9rem] shrink-0 items-center justify-center rounded-full border px-3 py-1 text-center text-xs font-medium leading-4 ${card.badgeClass}`}
              >
                {card.badgeLabel}
              </span>
            </div>

            <div className="mt-3 break-words text-sm leading-5 text-slate-600">{card.detail}</div>
          </div>
        ))}
      </div>
    </div>
  );
}
