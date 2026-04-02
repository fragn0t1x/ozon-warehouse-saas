'use client';

import Link from 'next/link';
import { Fragment, useEffect, useMemo, useState } from 'react';
import { usePathname } from 'next/navigation';
import { Menu, Transition } from '@headlessui/react';
import {
  Bars3Icon,
  BellIcon,
  BuildingStorefrontIcon,
  ChartBarIcon,
  ChatBubbleLeftRightIcon,
  CubeIcon,
  UserCircleIcon,
} from '@heroicons/react/24/outline';

import { useAuth } from '@/lib/context/AuthContext';
import { notificationsAPI } from '@/lib/api/notifications';
import { useStoreContext } from '@/lib/context/StoreContext';

interface HeaderProps {
  setSidebarOpen: (open: boolean) => void;
}

function classNames(...classes: string[]) {
  return classes.filter(Boolean).join(' ');
}

function resolveTelegramUrl(value?: string | null): string {
  const raw = (value || '').trim();
  if (!raw) return 'https://t.me/fragn0t1x21';
  if (raw.startsWith('http://') || raw.startsWith('https://')) return raw;
  return `https://t.me/${raw.replace(/^@/, '')}`;
}

export function Header({ setSidebarOpen }: HeaderProps) {
  const pathname = usePathname() || '';
  const { user, logout } = useAuth();
  const { stores, selectedStoreId, setSelectedStoreId, isLoading } = useStoreContext();
  const [unreadNotifications, setUnreadNotifications] = useState(0);

  const todayLabel = useMemo(
    () =>
      new Intl.DateTimeFormat('ru-RU', {
        weekday: 'long',
        day: 'numeric',
        month: 'long',
      }).format(new Date()),
    []
  );

  const supportUrl = resolveTelegramUrl(process.env.NEXT_PUBLIC_SUPPORT_TELEGRAM_URL);

  useEffect(() => {
    let cancelled = false;
    const load = async () => {
      if (typeof document !== 'undefined' && document.visibilityState !== 'visible') {
        return;
      }
      try {
        const count = await notificationsAPI.getUnreadCount();
        if (!cancelled) {
          setUnreadNotifications(count);
        }
      } catch {
        if (!cancelled) {
          setUnreadNotifications(0);
        }
      }
    };

    void load();
    const intervalId = window.setInterval(() => {
      void load();
    }, 30000);

    return () => {
      cancelled = true;
      window.clearInterval(intervalId);
    };
  }, []);

  const quickLinks = [
    { name: 'Товары', href: '/products', icon: CubeIcon },
    { name: 'Склад', href: '/warehouse', icon: BuildingStorefrontIcon },
    { name: 'Отправки', href: '/shipments', icon: ChartBarIcon },
  ];

  return (
    <header className="sticky top-0 z-30 px-4 pt-4 sm:px-6 lg:px-8">
      <div className="mx-auto flex max-w-7xl flex-col gap-3 rounded-[28px] border border-white/70 bg-white/85 px-4 py-3 shadow-[0_18px_48px_rgba(15,23,42,0.08)] backdrop-blur xl:flex-row xl:items-center xl:justify-between">
        <div className="flex items-center gap-3">
          <button
            type="button"
            className="inline-flex h-10 w-10 items-center justify-center rounded-2xl border border-slate-200 bg-white text-slate-600 transition hover:text-slate-900 md:hidden"
            onClick={() => setSidebarOpen(true)}
          >
            <span className="sr-only">Open sidebar</span>
            <Bars3Icon className="h-5 w-5" />
          </button>

          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-sky-700">Control Room</p>
            <p className="text-sm text-slate-500">{todayLabel}</p>
          </div>
        </div>

        <div className="flex flex-1 flex-col gap-2 xl:mx-4 xl:max-w-4xl">
          <div className="grid grid-cols-1 gap-2 lg:grid-cols-[minmax(168px,188px)_1fr]">
            <div className="flex h-14 min-w-0 items-center gap-2 rounded-2xl border border-sky-100 bg-gradient-to-br from-sky-50 to-slate-50 px-3 shadow-[inset_0_1px_0_rgba(255,255,255,0.85)]">
              <div className="flex h-9 w-9 shrink-0 items-center justify-center rounded-xl bg-white text-sky-700 shadow-sm">
                <BuildingStorefrontIcon className="h-5 w-5" />
              </div>
              <div className="min-w-0 flex-1">
                <div className="text-[10px] font-semibold uppercase tracking-[0.18em] text-sky-700">Активный магазин</div>
                {isLoading ? (
                  <div className="mt-1 h-4 animate-pulse rounded-xl bg-sky-100" />
                ) : stores.length > 0 ? (
                  <select
                    value={selectedStoreId ?? ''}
                    onChange={(event) => setSelectedStoreId(event.target.value ? Number(event.target.value) : null)}
                    className="mt-0.5 w-full truncate border-0 bg-transparent py-0 pl-0 pr-6 text-base font-semibold text-slate-900 outline-none focus:ring-0"
                  >
                    {stores.map((store) => (
                      <option key={store.id} value={store.id}>
                        {store.name}
                      </option>
                    ))}
                  </select>
                ) : (
                  <div className="mt-0.5 text-xs text-slate-500">Сначала добавь магазин OZON.</div>
                )}
              </div>
            </div>

            <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
              {quickLinks.map((item) => {
                const isActive = pathname === item.href || pathname.startsWith(`${item.href}/`);
                const disabled = stores.length === 0;
                return (
                  <Link
                    key={item.name}
                    href={disabled ? '/stores' : item.href}
                    className={classNames(
                      'inline-flex h-14 items-center justify-center gap-2 rounded-2xl px-3 text-sm font-medium transition',
                      isActive
                        ? 'bg-slate-950 text-white shadow-[0_14px_28px_rgba(15,23,42,0.16)]'
                        : 'border border-slate-200 bg-white text-slate-700 hover:border-slate-300 hover:text-slate-950',
                      disabled ? 'opacity-60' : ''
                    )}
                  >
                    <item.icon className="h-4 w-4 shrink-0" />
                    <span className="max-w-full truncate">{item.name}</span>
                  </Link>
                );
              })}
            </div>
          </div>
        </div>

        <div className="flex items-center gap-2 self-end xl:self-auto">
          <Link
            href="/notifications"
            className="relative inline-flex h-14 w-14 items-center justify-center rounded-2xl border border-slate-200 bg-white text-slate-600 shadow-sm transition hover:border-slate-300 hover:text-slate-950 hover:shadow"
            title="Уведомления"
          >
            <span className="sr-only">Уведомления</span>
            <BellIcon className="h-6 w-6" />
            {unreadNotifications > 0 && (
              <span className="absolute right-2 top-2 inline-flex min-w-[1.25rem] items-center justify-center rounded-full bg-rose-500 px-1.5 py-0.5 text-[11px] font-semibold text-white">
                {unreadNotifications > 99 ? '99+' : unreadNotifications}
              </span>
            )}
          </Link>

          <Link
            href={supportUrl}
            target="_blank"
            rel="noreferrer"
            className="inline-flex h-14 w-14 items-center justify-center rounded-2xl border border-slate-200 bg-white text-slate-600 shadow-sm transition hover:border-slate-300 hover:text-slate-950 hover:shadow"
            title="Поддержка в Telegram"
          >
            <span className="sr-only">Поддержка в Telegram</span>
            <ChatBubbleLeftRightIcon className="h-6 w-6" />
          </Link>

          <Menu as="div" className="relative">
            <Menu.Button className="flex h-14 w-14 items-center justify-center rounded-2xl border border-slate-200 bg-white text-slate-500 shadow-sm transition hover:border-slate-300 hover:text-slate-950 hover:shadow">
              <span className="sr-only">Открыть меню пользователя</span>
              <UserCircleIcon className="h-7 w-7" />
            </Menu.Button>

            <Transition
              as={Fragment}
              enter="transition ease-out duration-100"
              enterFrom="transform opacity-0 scale-95"
              enterTo="transform opacity-100 scale-100"
              leave="transition ease-in duration-75"
              leaveFrom="transform opacity-100 scale-100"
              leaveTo="transform opacity-0 scale-95"
            >
              <Menu.Items className="absolute right-0 mt-3 w-64 rounded-3xl border border-slate-200 bg-white p-2 shadow-[0_18px_48px_rgba(15,23,42,0.12)] focus:outline-none">
                <div className="rounded-2xl bg-slate-50 px-4 py-3">
                  <p className="truncate text-sm font-medium text-slate-900">{user?.email}</p>
                  <p className="mt-1 text-xs text-slate-500">
                    {user?.is_admin ? 'Администратор системы' : user?.role === 'owner' ? 'Owner кабинета' : 'Участник кабинета'}
                  </p>
                </div>

                <Menu.Item>
                  {({ active }) => (
                    <button
                      onClick={logout}
                      className={`mt-2 w-full rounded-2xl px-4 py-3 text-left text-sm font-medium transition ${
                        active ? 'bg-slate-100 text-slate-950' : 'text-slate-700'
                      }`}
                    >
                      Выйти из кабинета
                    </button>
                  )}
                </Menu.Item>
              </Menu.Items>
            </Transition>
          </Menu>
        </div>
      </div>
    </header>
  );
}
