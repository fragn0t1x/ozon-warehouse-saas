'use client';

import { ArrowDownTrayIcon, DocumentArrowDownIcon } from '@heroicons/react/24/outline';

import type { ExportJobStatus } from '@/lib/api/warehouse';

interface ExportJobCardProps {
  title: string;
  description: string;
  status: ExportJobStatus | null;
  onStart: () => void | Promise<void>;
  onDownload?: () => void | Promise<void>;
  onClear?: () => void | Promise<void>;
  startLabel?: string;
  isStarting?: boolean;
  isDownloading?: boolean;
  isClearing?: boolean;
}

function statusBadge(status: ExportJobStatus | null) {
  const value = status?.status ?? 'idle';
  switch (value) {
    case 'queued':
      return { label: 'В очереди', className: 'bg-amber-50 text-amber-700 border-amber-200' };
    case 'running':
      return { label: status?.phase_label || 'Выполняется', className: 'bg-sky-50 text-sky-700 border-sky-200' };
    case 'success':
      return { label: 'Готово', className: 'bg-emerald-50 text-emerald-700 border-emerald-200' };
    case 'stale':
      return { label: 'Устарело', className: 'bg-orange-50 text-orange-700 border-orange-200' };
    case 'error':
      return { label: 'Ошибка', className: 'bg-rose-50 text-rose-700 border-rose-200' };
    default:
      return { label: 'Не запускалось', className: 'bg-slate-100 text-slate-600 border-slate-200' };
  }
}

function formatRunTime(value?: string | null) {
  if (!value) return '—';
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) return '—';
  return parsed.toLocaleString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatWindowLabel(days?: number | null) {
  if (!days) return 'Период не указан';
  if (days >= 90) return 'За 3 месяца';
  if (days >= 30) return 'За месяц';
  return 'За неделю';
}

function formatRunLabel(run: NonNullable<ExportJobStatus['recent_runs']>[number]) {
  const custom = String(run.selection_label || '').trim();
  if (custom) {
    return custom;
  }
  return formatWindowLabel(run.order_window_days);
}

export function ExportJobCard({
  title,
  description,
  status,
  onStart,
  onDownload,
  onClear,
  startLabel = 'Сформировать Excel',
  isStarting = false,
  isDownloading = false,
  isClearing = false,
}: ExportJobCardProps) {
  const badge = statusBadge(status);
  const progress = Math.max(0, Math.min(status?.progress_percent ?? 0, 100));
  const isBusy = isStarting || status?.status === 'queued' || status?.status === 'running';
  const canDownload = status?.status === 'success' && !!onDownload;
  const canClear = !isBusy && !!onClear && (
    !!status?.file_name ||
    (status?.recent_runs?.length ?? 0) > 0 ||
    status?.status === 'error' ||
    status?.status === 'stale'
  );
  const recentRuns = status?.recent_runs ?? [];

  return (
    <section className="rounded-[24px] border border-slate-200 bg-white p-5 shadow-sm">
      <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div className="flex items-center gap-3">
            <h3 className="text-base font-semibold text-slate-950">{title}</h3>
            <span className={`inline-flex rounded-full border px-3 py-1 text-xs font-medium ${badge.className}`}>
              {badge.label}
            </span>
          </div>
          <p className="mt-2 max-w-3xl text-sm text-slate-500">{description}</p>
          {status?.message ? (
            <p className="mt-2 text-sm text-slate-700">{status.message}</p>
          ) : null}
        </div>

        <div className="flex flex-wrap gap-2">
          <button
            type="button"
            onClick={() => void onStart()}
            disabled={isBusy}
            className="inline-flex items-center gap-2 rounded-full bg-sky-600 px-4 py-2 text-sm font-medium text-white transition hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
          >
            <DocumentArrowDownIcon className="h-4 w-4" />
            {isBusy ? 'Уже формируется' : startLabel}
          </button>
          {canDownload ? (
            <button
              type="button"
              onClick={() => void onDownload?.()}
              disabled={isDownloading}
              className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-sky-200 hover:text-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
            >
              <ArrowDownTrayIcon className="h-4 w-4" />
              {isDownloading ? 'Скачиваем...' : 'Скачать'}
            </button>
          ) : null}
          {canClear ? (
            <button
              type="button"
              onClick={() => void onClear?.()}
              disabled={isClearing}
              className="inline-flex items-center gap-2 rounded-full border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-rose-200 hover:text-rose-700 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {isClearing ? 'Очищаем...' : 'Очистить отчеты'}
            </button>
          ) : null}
        </div>
      </div>

      {(status?.status === 'queued' || status?.status === 'running' || status?.status === 'success' || status?.status === 'error' || status?.status === 'stale') ? (
        <div className="mt-4">
          <div className="h-2.5 overflow-hidden rounded-full bg-slate-100">
            <div
              className={`h-full rounded-full transition-all ${
                status?.status === 'error'
                  ? 'bg-rose-500'
                  : status?.status === 'stale'
                    ? 'bg-orange-500'
                  : status?.status === 'success'
                    ? 'bg-emerald-500'
                    : 'bg-sky-500'
              }`}
              style={{ width: `${progress}%` }}
            />
          </div>
          <div className="mt-2 flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-slate-500">
            <span>{progress}%</span>
            {typeof status?.processed_items === 'number' && typeof status?.total_items === 'number' && status.total_items > 0 ? (
              <span>{status.processed_items} из {status.total_items}</span>
            ) : null}
            {status?.file_name ? <span>{status.file_name}</span> : null}
          </div>
        </div>
      ) : null}

      {recentRuns.length ? (
        <div className="mt-4 border-t border-slate-100 pt-4">
          <p className="text-xs font-semibold uppercase tracking-[0.12em] text-slate-400">Последние выгрузки</p>
          <div className="mt-3 space-y-2">
            {recentRuns.map((run, index) => {
              const runStatusClass = run.status === 'success'
                ? 'bg-emerald-50 text-emerald-700 border-emerald-200'
                : 'bg-rose-50 text-rose-700 border-rose-200';
              const runStatusLabel = run.status === 'success' ? 'Успешно' : 'Ошибка';
              return (
                <div
                  key={`${run.finished_at || 'run'}-${index}`}
                  className="flex flex-col gap-2 rounded-2xl border border-slate-100 bg-slate-50/70 px-3 py-3 md:flex-row md:items-center md:justify-between"
                >
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className={`inline-flex rounded-full border px-2.5 py-1 text-[11px] font-medium ${runStatusClass}`}>
                        {runStatusLabel}
                      </span>
                      <span className="text-xs text-slate-500">{formatRunLabel(run)}</span>
                      <span className="text-xs text-slate-400">{formatRunTime(run.finished_at)}</span>
                    </div>
                    <p className="mt-1 text-sm text-slate-700">{run.message}</p>
                    {run.error && run.status === 'error' ? (
                      <p className="mt-1 text-xs text-rose-600">{run.error}</p>
                    ) : null}
                  </div>
                  {run.file_name && run.status === 'success' ? (
                    <span className="text-xs text-slate-400">{run.file_name}</span>
                  ) : null}
                </div>
              );
            })}
          </div>
        </div>
      ) : null}
    </section>
  );
}
