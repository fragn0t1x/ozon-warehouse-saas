'use client';

import Link from 'next/link';
import { useCallback, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import toast from 'react-hot-toast';

import { notificationsAPI, type UserNotification } from '@/lib/api/notifications';

function formatDateTime(value: string) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(date);
}

function severityClasses(severity: string) {
  if (severity === 'warning') {
    return 'border-amber-200 bg-amber-50 text-amber-900';
  }
  if (severity === 'error') {
    return 'border-rose-200 bg-rose-50 text-rose-900';
  }
  return 'border-slate-200 bg-white text-slate-900';
}

export default function NotificationsPage() {
  const router = useRouter();
  const [items, setItems] = useState<UserNotification[]>([]);
  const [unreadCount, setUnreadCount] = useState(0);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);

  const load = useCallback(async () => {
    setLoading(true);
    try {
      const response = await notificationsAPI.list();
      setItems(response.items);
      setUnreadCount(response.unread_count);
    } catch {
      toast.error('Не удалось загрузить уведомления');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void load();
  }, [load]);

  const markRead = useCallback(async (notificationId: number) => {
    try {
      const nextUnread = await notificationsAPI.markRead(notificationId);
      setUnreadCount(nextUnread);
      setItems((prev) =>
        prev.map((item) => (item.id === notificationId ? { ...item, read_at: new Date().toISOString() } : item)),
      );
    } catch {
      toast.error('Не удалось отметить уведомление как прочитанное');
    }
  }, []);

  const markAllRead = useCallback(async () => {
    setSubmitting(true);
    try {
      await notificationsAPI.markAllRead();
      setUnreadCount(0);
      setItems((prev) => prev.map((item) => ({ ...item, read_at: item.read_at || new Date().toISOString() })));
      toast.success('Все уведомления отмечены как прочитанные');
    } catch {
      toast.error('Не удалось обновить уведомления');
    } finally {
      setSubmitting(false);
    }
  }, []);

  return (
    <div className="space-y-6">
      <div className="rounded-3xl border border-slate-200 bg-white p-6 shadow-sm">
        <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
          <div className="max-w-2xl">
            <h1 className="mt-2 text-2xl font-semibold text-slate-950">Уведомления</h1>
            <p className="mt-2 text-sm leading-6 text-slate-600">
              Здесь собраны события по поставкам, складу и отчетам.
            </p>
            <div className="mt-4 flex flex-wrap gap-3">
              <button
                type="button"
                onClick={() => router.back()}
                className="rounded-2xl bg-slate-950 px-4 py-2.5 text-sm font-medium text-white transition hover:bg-slate-800"
              >
                Вернуться назад
              </button>
              <Link
                href="/dashboard"
                className="rounded-2xl border border-slate-300 bg-white px-4 py-2.5 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:text-slate-950"
              >
                На дашборд
              </Link>
              <Link
                href="/supplies"
                className="rounded-2xl border border-slate-300 bg-white px-4 py-2.5 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:text-slate-950"
              >
                К поставкам
              </Link>
              <Link
                href="/shipments"
                className="rounded-2xl border border-slate-300 bg-white px-4 py-2.5 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:text-slate-950"
              >
                К отправкам
              </Link>
            </div>
          </div>

          <div className="rounded-3xl border border-slate-200 bg-slate-50 px-4 py-4 text-sm text-slate-700">
            <div>
              Непрочитанных: <span className="font-semibold text-slate-950">{unreadCount}</span>
            </div>
            <button
              type="button"
              onClick={() => void markAllRead()}
              disabled={submitting || unreadCount === 0}
              className="mt-3 w-full rounded-2xl border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:text-slate-950 disabled:cursor-not-allowed disabled:opacity-60"
            >
              Отметить все как прочитанные
            </button>
          </div>
        </div>
      </div>

      {loading ? (
        <div className="rounded-3xl border border-slate-200 bg-white p-6 text-sm text-slate-500 shadow-sm">
          Загружаем уведомления...
        </div>
      ) : items.length === 0 ? (
        <div className="rounded-3xl border border-dashed border-slate-300 bg-slate-50 p-8 text-center text-sm text-slate-500">
          Пока уведомлений нет.
        </div>
      ) : (
        <div className="space-y-3">
          {items.map((item) => (
            <div
              key={item.id}
              className={`rounded-3xl border p-5 shadow-sm transition ${severityClasses(item.severity)} ${
                item.read_at ? 'opacity-85' : ''
              }`}
            >
              <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                <div className="space-y-2">
                  <div className="flex flex-wrap items-center gap-2">
                    <h2 className="text-base font-semibold">{item.title}</h2>
                    {!item.read_at && (
                      <span className="rounded-full bg-slate-950 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-white">
                        Новое
                      </span>
                    )}
                    {item.is_important && (
                      <span className="rounded-full bg-amber-100 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.16em] text-amber-900">
                        Важно
                      </span>
                    )}
                  </div>
                  <p className="whitespace-pre-line text-sm leading-6 text-slate-700">{item.body}</p>
                  <p className="text-xs text-slate-500">{formatDateTime(item.created_at)}</p>
                </div>

                <div className="flex shrink-0 items-center gap-2">
                  {item.action_url && (
                    <Link
                      href={item.action_url}
                      className="rounded-2xl border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:text-slate-950"
                    >
                      Открыть
                    </Link>
                  )}
                  {!item.read_at && (
                    <button
                      type="button"
                      onClick={() => void markRead(item.id)}
                      className="rounded-2xl border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:text-slate-950"
                    >
                      Прочитано
                    </button>
                  )}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      <div className="rounded-3xl border border-slate-200 bg-slate-50 p-5 shadow-sm">
        <div className="flex flex-col gap-3 sm:flex-row sm:flex-wrap sm:items-center sm:justify-between">
          <div>
            <h2 className="text-base font-semibold text-slate-950">Вернуться в кабинет</h2>
            <p className="mt-1 text-sm text-slate-600">Быстрые переходы в основные разделы.</p>
          </div>
          <div className="flex flex-wrap gap-3">
            <button
              type="button"
              onClick={() => router.back()}
              className="rounded-2xl bg-slate-950 px-4 py-2.5 text-sm font-medium text-white transition hover:bg-slate-800"
            >
              Назад
            </button>
            <Link
              href="/dashboard"
              className="rounded-2xl border border-slate-300 bg-white px-4 py-2.5 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:text-slate-950"
            >
              Дашборд
            </Link>
            <Link
              href="/supplies"
              className="rounded-2xl border border-slate-300 bg-white px-4 py-2.5 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:text-slate-950"
            >
              Поставки
            </Link>
            <Link
              href="/shipments"
              className="rounded-2xl border border-slate-300 bg-white px-4 py-2.5 text-sm font-medium text-slate-700 transition hover:border-slate-400 hover:text-slate-950"
            >
              Отправки
            </Link>
          </div>
        </div>
      </div>
    </div>
  );
}
