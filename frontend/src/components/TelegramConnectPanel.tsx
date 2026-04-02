'use client';

import { useCallback, useEffect, useState } from 'react';
import toast from 'react-hot-toast';

import {
  settingsAPI,
  type TelegramConnectStatus,
} from '@/lib/api/settings';

function formatDateTime(value?: string | null) {
  if (!value) {
    return null;
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return null;
  }

  return new Intl.DateTimeFormat('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
}

function statusBadge(status: TelegramConnectStatus['status']) {
  if (status === 'connected') {
    return 'border-emerald-200 bg-emerald-50 text-emerald-800';
  }
  if (status === 'pending') {
    return 'border-amber-200 bg-amber-50 text-amber-800';
  }
  if (status === 'not_configured') {
    return 'border-rose-200 bg-rose-50 text-rose-800';
  }
  return 'border-slate-200 bg-slate-50 text-slate-700';
}

function statusLabel(status: TelegramConnectStatus['status']) {
  if (status === 'connected') {
    return 'Подключено';
  }
  if (status === 'pending') {
    return 'Ожидаем подтверждение';
  }
  if (status === 'not_configured') {
    return 'Недоступно';
  }
  return 'Не подключено';
}

function botBadgeClasses(isAvailable: boolean) {
  return isAvailable
    ? 'border-emerald-200 bg-emerald-50 text-emerald-800'
    : 'border-rose-200 bg-rose-50 text-rose-800';
}

function botStatusLabel(isAvailable: boolean) {
  return isAvailable ? 'Бот в сети' : 'Бот недоступен';
}

async function copyText(value: string, successMessage: string) {
  try {
    await navigator.clipboard.writeText(value);
    toast.success(successMessage);
  } catch {
    toast.error('Не удалось скопировать');
  }
}

type TelegramConnectPanelProps = {
  onStatusChange?: (status: TelegramConnectStatus | null) => void;
  compact?: boolean;
};

export function TelegramConnectPanel({
  onStatusChange,
  compact = false,
}: TelegramConnectPanelProps) {
  const [status, setStatus] = useState<TelegramConnectStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [creating, setCreating] = useState(false);
  const [disconnecting, setDisconnecting] = useState(false);

  const loadStatus = useCallback(
    async (silent = false) => {
      if (!silent) {
        setLoading(true);
      }
      try {
        const nextStatus = await settingsAPI.getTelegramConnectStatus();
        setStatus(nextStatus);
        onStatusChange?.(nextStatus);
      } catch {
        if (!silent) {
          toast.error('Не удалось проверить подключение Telegram');
        }
      } finally {
        if (!silent) {
          setLoading(false);
        }
      }
    },
    [onStatusChange],
  );

  useEffect(() => {
    void loadStatus();
  }, [loadStatus]);

  useEffect(() => {
    if (status?.status !== 'pending') {
      return;
    }

    const intervalId = window.setInterval(() => {
      void loadStatus(true);
    }, 3000);

    return () => window.clearInterval(intervalId);
  }, [loadStatus, status?.status]);

  const prepareTelegramConnect = useCallback(async () => {
    setCreating(true);
    try {
      const nextStatus = await settingsAPI.createTelegramConnectLink(false);
      setStatus(nextStatus);
      onStatusChange?.(nextStatus);

      if (nextStatus.status === 'connected') {
        toast.success('Telegram уже подключен.');
        return;
      }

      if (!nextStatus.connect_url && !nextStatus.manual_command) {
        toast.error(nextStatus.message || 'Не удалось подготовить ссылку для Telegram');
        return;
      }

      toast.success('Варианты подключения готовы. Можно открыть Telegram, скопировать ссылку или использовать код.');
    } catch {
      toast.error('Не удалось подготовить варианты подключения');
    } finally {
      setCreating(false);
    }
  }, [onStatusChange]);

  const openPreparedLink = useCallback(() => {
    if (!status?.connect_url) {
      toast.error('Ссылка еще не готова');
      return;
    }
    window.open(status.connect_url, '_blank', 'noopener,noreferrer');
  }, [status?.connect_url]);

  const copyLink = useCallback(async () => {
    if (!status?.connect_url) {
      toast.error('Ссылка еще не готова');
      return;
    }
    await copyText(status.connect_url, 'Ссылка скопирована');
  }, [status?.connect_url]);

  const copyCommand = useCallback(async () => {
    if (!status?.manual_command) {
      toast.error('Код еще не готов');
      return;
    }
    await copyText(status.manual_command, 'Команда скопирована');
  }, [status?.manual_command]);

  const disconnectTelegram = useCallback(async () => {
    setDisconnecting(true);
    try {
      const nextStatus = await settingsAPI.disconnectTelegram();
      setStatus(nextStatus);
      onStatusChange?.(nextStatus);
      toast.success('Telegram отключен');
    } catch {
      toast.error('Не удалось отключить Telegram');
    } finally {
      setDisconnecting(false);
    }
  }, [onStatusChange]);

  const expiresAt = formatDateTime(status?.expires_at);
  const connectedAt = formatDateTime(status?.connected_at);
  const botLastSeenAt = formatDateTime(status?.bot_last_seen_at);
  const showPendingTools = status?.status === 'pending';

  return (
    <div className={compact ? 'rounded-2xl border border-slate-200 bg-slate-50/70 p-4' : 'rounded-3xl border border-slate-200 bg-slate-50/70 p-6'}>
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-2">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="text-base font-semibold text-slate-950">Telegram-бот</h3>
            <span className={`inline-flex items-center rounded-full border px-3 py-1 text-xs font-semibold ${statusBadge(status?.status || 'not_connected')}`}>
              {statusLabel(status?.status || 'not_connected')}
            </span>
            <span className={`inline-flex items-center rounded-full border px-3 py-1 text-xs font-semibold ${botBadgeClasses(Boolean(status?.bot_available))}`}>
              {botStatusLabel(Boolean(status?.bot_available))}
            </span>
          </div>
          <p className="text-sm leading-6 text-slate-600">
            Подключение происходит автоматически: можно открыть бота по ссылке, отсканировать QR-код телефоном или вручную отправить короткий код.
          </p>
          {status?.message && (
            <p className="text-sm text-slate-600">{status.message}</p>
          )}
          {status?.bot_username && (
            <p className="text-xs text-slate-500">
              Бот: @{status.bot_username}
            </p>
          )}
          {status?.bot_status_message && (
            <p className={`text-xs ${status.bot_available ? 'text-emerald-700' : 'text-rose-700'}`}>
              {status.bot_status_message}
              {botLastSeenAt ? ` Последний сигнал: ${botLastSeenAt}.` : ''}
            </p>
          )}
          {expiresAt && status?.status === 'pending' && (
            <p className="text-xs text-amber-700">Ссылка активна до {expiresAt}</p>
          )}
          {connectedAt && status?.status === 'connected' && (
            <p className="text-xs text-emerald-700">Подключено {connectedAt}</p>
          )}
        </div>

        <div className="flex flex-wrap gap-3">
          <button
            type="button"
            onClick={() => void loadStatus()}
            disabled={loading || creating}
            className="inline-flex items-center rounded-2xl border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:text-slate-900 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {loading ? 'Проверяем...' : 'Обновить статус'}
          </button>
          <button
            type="button"
            onClick={() => void prepareTelegramConnect()}
            disabled={creating || loading || disconnecting || status?.status === 'not_configured'}
            className={`inline-flex items-center rounded-2xl px-4 py-2 text-sm font-semibold text-white transition disabled:cursor-not-allowed disabled:opacity-60 ${
              status?.status === 'connected'
                ? 'bg-emerald-600 hover:bg-emerald-600'
                : status?.status === 'pending'
                  ? 'bg-amber-600 hover:bg-amber-700'
                  : 'bg-sky-600 hover:bg-sky-700'
            }`}
          >
            {creating
              ? 'Готовим варианты...'
              : status?.status === 'connected'
                ? 'Подключено'
                : status?.status === 'pending'
                  ? 'Показать варианты снова'
                : 'Подключить Telegram'}
          </button>
          {status?.status === 'connected' && (
            <button
              type="button"
              onClick={() => void disconnectTelegram()}
              disabled={disconnecting || creating || loading}
              className="inline-flex items-center rounded-2xl border border-rose-200 bg-rose-50 px-4 py-2 text-sm font-medium text-rose-700 transition hover:border-rose-300 hover:bg-rose-100 disabled:cursor-not-allowed disabled:opacity-60"
            >
              {disconnecting ? 'Отключаем...' : 'Отключить Telegram'}
            </button>
          )}
        </div>
      </div>

      {showPendingTools && (
        <div className="mt-5 grid gap-4 lg:grid-cols-[240px_minmax(0,1fr)]">
          <div className="rounded-2xl border border-slate-200 bg-white p-4">
            {status.qr_code_url ? (
              // Внешний QR-сервис нужен только для генерации изображения по уже готовой ссылке.
              // eslint-disable-next-line @next/next/no-img-element
              <img
                src={status.qr_code_url}
                alt="QR-код для подключения Telegram"
                className="mx-auto h-52 w-52 rounded-xl border border-slate-200 bg-white object-contain"
              />
            ) : (
              <div className="flex h-52 items-center justify-center rounded-xl border border-dashed border-slate-300 text-sm text-slate-500">
                QR-код появится после подготовки ссылки
              </div>
            )}
            <p className="mt-3 text-center text-xs leading-5 text-slate-500">
              Если Telegram открыт только на телефоне, просто отсканируйте QR-код камерой.
            </p>
          </div>

          <div className="space-y-4">
            <div className="rounded-2xl border border-slate-200 bg-white p-4">
              <p className="text-sm font-semibold text-slate-950">Вариант 1. Открыть ссылку</p>
              <p className="mt-1 text-sm text-slate-600">
                Подойдет для Telegram Web, телефона или другого устройства.
              </p>
              <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-xs text-slate-700 break-all">
                {status.connect_url || 'Ссылка еще не готова'}
              </div>
              <div className="mt-3 flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={openPreparedLink}
                  disabled={!status.connect_url}
                  className="inline-flex items-center rounded-xl bg-sky-600 px-3 py-2 text-sm font-medium text-white transition hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  Открыть Telegram
                </button>
                <button
                  type="button"
                  onClick={() => void copyLink()}
                  disabled={!status.connect_url}
                  className="inline-flex items-center rounded-xl border border-slate-300 bg-white px-3 py-2 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:text-slate-900 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  Скопировать ссылку
                </button>
              </div>
            </div>

            <div className="rounded-2xl border border-amber-200 bg-amber-50/80 p-4">
              <p className="text-sm font-semibold text-slate-950">Вариант 2. Ввести код вручную</p>
              <p className="mt-1 text-sm text-slate-700">
                Если ссылка не открылась, зайдите к боту @{status.bot_username} и отправьте эту команду:
              </p>
              <div className="mt-3 rounded-xl border border-amber-200 bg-white px-3 py-3 text-sm font-semibold text-slate-900">
                {status.manual_command || 'Команда появится после подготовки ссылки'}
              </div>
              <p className="mt-2 text-xs text-slate-600">
                Короткий код: <span className="font-semibold text-slate-900">{status.manual_code || '—'}</span>
              </p>
              <div className="mt-3 flex flex-wrap gap-2">
                <button
                  type="button"
                  onClick={() => void copyCommand()}
                  disabled={!status.manual_command}
                  className="inline-flex items-center rounded-xl border border-amber-300 bg-white px-3 py-2 text-sm font-medium text-amber-900 transition hover:border-amber-400 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  Скопировать команду
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
