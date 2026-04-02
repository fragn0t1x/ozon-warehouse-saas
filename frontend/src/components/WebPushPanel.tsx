'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import toast from 'react-hot-toast';

import { settingsAPI, type WebPushStatus } from '@/lib/api/settings';

type WebPushPanelProps = {
  compact?: boolean;
};

function permissionLabel(permission: NotificationPermission | 'unsupported') {
  if (permission === 'granted') {
    return 'Разрешено в браузере';
  }
  if (permission === 'denied') {
    return 'Заблокировано в браузере';
  }
  if (permission === 'default') {
    return 'Еще не разрешено';
  }
  return 'Браузер не поддерживает';
}

function permissionBadgeClasses(permission: NotificationPermission | 'unsupported') {
  if (permission === 'granted') {
    return 'border-emerald-200 bg-emerald-50 text-emerald-800';
  }
  if (permission === 'denied') {
    return 'border-rose-200 bg-rose-50 text-rose-800';
  }
  return 'border-slate-200 bg-slate-50 text-slate-700';
}

function urlBase64ToUint8Array(base64String: string) {
  const padding = '='.repeat((4 - (base64String.length % 4)) % 4);
  const normalized = (base64String + padding).replace(/-/g, '+').replace(/_/g, '/');
  const rawData = window.atob(normalized);
  return Uint8Array.from(rawData, (char) => char.charCodeAt(0));
}

async function exportKey(subscription: PushSubscription, keyName: 'p256dh' | 'auth') {
  const key = subscription.getKey(keyName);
  if (!key) {
    throw new Error(`Ключ ${keyName} отсутствует`);
  }
  const bytes = new Uint8Array(key);
  let binary = '';
  bytes.forEach((byte) => {
    binary += String.fromCharCode(byte);
  });
  return window.btoa(binary);
}

export function WebPushPanel({ compact = false }: WebPushPanelProps) {
  const [status, setStatus] = useState<WebPushStatus | null>(null);
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [browserSupported, setBrowserSupported] = useState(false);
  const [permission, setPermission] = useState<NotificationPermission | 'unsupported'>('unsupported');
  const [hasBrowserSubscription, setHasBrowserSubscription] = useState(false);

  const cardClassName = compact
    ? 'rounded-2xl border border-slate-200 bg-slate-50/70 p-4'
    : 'rounded-3xl border border-slate-200 bg-slate-50/70 p-6';

  const detectBrowser = useCallback(async () => {
    const supported =
      typeof window !== 'undefined'
      && 'Notification' in window
      && 'serviceWorker' in navigator
      && 'PushManager' in window;
    setBrowserSupported(supported);
    if (!supported) {
      setPermission('unsupported');
      setHasBrowserSubscription(false);
      return;
    }

    setPermission(Notification.permission);
    try {
      const registration = await navigator.serviceWorker.register('/push-sw.js');
      const subscription = await registration.pushManager.getSubscription();
      setHasBrowserSubscription(Boolean(subscription));
    } catch {
      setHasBrowserSubscription(false);
    }
  }, []);

  const loadStatus = useCallback(async () => {
    setLoading(true);
    try {
      const nextStatus = await settingsAPI.getWebPushStatus();
      setStatus(nextStatus);
    } catch {
      toast.error('Не удалось загрузить статус web push');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void Promise.all([loadStatus(), detectBrowser()]);
  }, [detectBrowser, loadStatus]);

  const refreshState = useCallback(async () => {
    await Promise.all([loadStatus(), detectBrowser()]);
  }, [detectBrowser, loadStatus]);

  const connectPush = useCallback(async () => {
    if (!browserSupported) {
      toast.error('Этот браузер не поддерживает push-уведомления');
      return;
    }
    if (!status?.configured || !status.public_key) {
      toast.error(status?.message || 'Web push еще не настроен на сервере');
      return;
    }

    setBusy(true);
    try {
      const registration = await navigator.serviceWorker.register('/push-sw.js');
      let permissionResult = Notification.permission;
      if (permissionResult !== 'granted') {
        permissionResult = await Notification.requestPermission();
      }
      setPermission(permissionResult);
      if (permissionResult !== 'granted') {
        toast.error('Браузер не разрешил push-уведомления');
        return;
      }

      let subscription = await registration.pushManager.getSubscription();
      if (!subscription) {
        subscription = await registration.pushManager.subscribe({
          userVisibleOnly: true,
          applicationServerKey: urlBase64ToUint8Array(status.public_key),
        });
      }

      const p256dhKey = await exportKey(subscription, 'p256dh');
      const authKey = await exportKey(subscription, 'auth');
      const nextStatus = await settingsAPI.subscribeWebPush({
        endpoint: subscription.endpoint,
        p256dh_key: p256dhKey,
        auth_key: authKey,
        user_agent: navigator.userAgent,
      });

      setStatus(nextStatus);
      setHasBrowserSubscription(true);
      toast.success('Push-уведомления подключены в этом браузере');
    } catch {
      toast.error('Не удалось подключить web push');
    } finally {
      setBusy(false);
    }
  }, [browserSupported, status?.configured, status?.message, status?.public_key]);

  const disconnectPush = useCallback(async () => {
    if (!browserSupported) {
      return;
    }
    setBusy(true);
    try {
      const registration = await navigator.serviceWorker.register('/push-sw.js');
      const subscription = await registration.pushManager.getSubscription();
      const endpoint = subscription?.endpoint || null;

      const nextStatus = await settingsAPI.unsubscribeWebPush(endpoint);
      if (subscription) {
        await subscription.unsubscribe();
      }

      setStatus(nextStatus);
      setHasBrowserSubscription(false);
      toast.success('Push-уведомления в этом браузере отключены');
    } catch {
      toast.error('Не удалось отключить web push');
    } finally {
      setBusy(false);
    }
  }, [browserSupported]);

  const sendTestPush = useCallback(async () => {
    setBusy(true);
    try {
      const result = await settingsAPI.sendWebPushTest();
      if (result.sent_count > 0) {
        toast.success(result.message);
      } else {
        toast.error(result.message);
      }
    } catch {
      toast.error('Не удалось отправить тестовый push');
    } finally {
      setBusy(false);
    }
  }, []);

  const statusText = useMemo(() => {
    if (!status) {
      return 'Проверяем канал...';
    }
    if (!status.configured) {
      return status.message || 'Web push еще не настроен на сервере.';
    }
    if (!status.library_available) {
      return status.message || 'Backend еще не готов отправлять push-уведомления.';
    }
    if (!hasBrowserSubscription) {
      return 'Можно подключить этот браузер и получать короткие уведомления даже без Telegram.';
    }
    return 'Этот браузер уже подписан на push-уведомления.';
  }, [hasBrowserSubscription, status]);

  return (
    <div className={cardClassName}>
      <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
        <div className="space-y-2">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="text-base font-semibold text-slate-950">Web push</h3>
            <span className={`inline-flex items-center rounded-full border px-3 py-1 text-xs font-semibold ${permissionBadgeClasses(permission)}`}>
              {permissionLabel(permission)}
            </span>
            <span className={`inline-flex items-center rounded-full border px-3 py-1 text-xs font-semibold ${
              status?.enabled ? 'border-emerald-200 bg-emerald-50 text-emerald-800' : 'border-slate-200 bg-slate-50 text-slate-700'
            }`}>
              {status?.enabled ? 'Канал включен' : 'Канал не включен'}
            </span>
          </div>
          <p className="text-sm leading-6 text-slate-600">
            Короткие уведомления прямо в браузере. Хорошо подходят как резерв, если Telegram работает нестабильно.
          </p>
          <p className="text-sm text-slate-600">{statusText}</p>
          {status && (
            <p className="text-xs text-slate-500">
              Подключено браузеров/устройств: {status.subscription_count}
            </p>
          )}
        </div>

        <div className="flex flex-wrap gap-3">
          <button
            type="button"
            onClick={() => void refreshState()}
            disabled={loading || busy}
            className="inline-flex items-center rounded-2xl border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:text-slate-900 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {loading ? 'Проверяем...' : 'Обновить статус'}
          </button>
          <button
            type="button"
            onClick={() => void connectPush()}
            disabled={busy || loading || !browserSupported || !status?.configured || !status?.library_available}
            className="inline-flex items-center rounded-2xl bg-sky-600 px-4 py-2 text-sm font-semibold text-white transition hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
          >
            {busy ? 'Подключаем...' : hasBrowserSubscription ? 'Подключить заново' : 'Подключить браузер'}
          </button>
          <button
            type="button"
            onClick={() => void disconnectPush()}
            disabled={busy || loading || !hasBrowserSubscription}
            className="inline-flex items-center rounded-2xl border border-rose-300 bg-white px-4 py-2 text-sm font-medium text-rose-700 transition hover:border-rose-400 hover:text-rose-800 disabled:cursor-not-allowed disabled:opacity-60"
          >
            Отключить в этом браузере
          </button>
          <button
            type="button"
            onClick={() => void sendTestPush()}
            disabled={busy || loading || !hasBrowserSubscription || !status?.enabled}
            className="inline-flex items-center rounded-2xl border border-emerald-300 bg-white px-4 py-2 text-sm font-medium text-emerald-700 transition hover:border-emerald-400 hover:text-emerald-800 disabled:cursor-not-allowed disabled:opacity-60"
          >
            Отправить тест
          </button>
        </div>
      </div>
    </div>
  );
}
