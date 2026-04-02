'use client';

import Link from 'next/link';
import { usePathname } from 'next/navigation';
import {
  ArrowLeftOnRectangleIcon,
  ArrowsRightLeftIcon,
  ArrowPathIcon,
  ChevronDoubleLeftIcon,
  ChevronDoubleRightIcon,
  Cog6ToothIcon,
  CubeIcon,
  DocumentTextIcon,
  HomeIcon,
  QueueListIcon,
  ShieldCheckIcon,
  UserGroupIcon,
  TruckIcon,
} from '@heroicons/react/24/outline';

import { useAuth } from '@/lib/context/AuthContext';

function classNames(...classes: string[]) {
  return classes.filter(Boolean).join(' ');
}

interface SidebarProps {
  collapsed?: boolean;
  onNavigate?: () => void;
  onToggleCollapsed?: () => void;
}

export function Sidebar({ collapsed = false, onNavigate, onToggleCollapsed }: SidebarProps) {
  const pathname = usePathname() || "";
  const { logout, user } = useAuth();

  const primaryNavigation = [
    { name: 'Магазин сегодня', href: '/dashboard', icon: HomeIcon },
    { name: 'Налоги и НДС', href: '/tax-history', icon: DocumentTextIcon },
    { name: 'Себестоимость', href: '/cost-history', icon: CubeIcon },
    { name: 'Поставки', href: '/supplies', icon: TruckIcon },
    { name: 'Магазины', href: '/stores', icon: QueueListIcon },
    { name: 'Обновление данных', href: '/syncs', icon: ArrowPathIcon },
    { name: 'Связи товаров', href: '/product-links', icon: ArrowsRightLeftIcon },
    ...(!user?.is_admin && user?.role === 'owner' ? [{ name: 'Команда', href: '/team/users', icon: UserGroupIcon }] : []),
    { name: 'Настройки', href: '/settings', icon: Cog6ToothIcon },
    ...(user?.is_admin ? [{ name: 'Админка', href: '/admin/users', icon: ShieldCheckIcon }] : []),
  ];

  return (
    <div className={`flex h-full min-h-0 flex-col overflow-hidden border-r border-white/10 bg-[linear-gradient(180deg,#071424_0%,#0f2239_46%,#122d46_100%)] pb-5 pt-6 text-white shadow-[24px_0_60px_rgba(8,18,35,0.32)] ${collapsed ? 'px-3' : 'px-5'}`}>
      <div className={`mb-4 hidden items-center md:flex ${collapsed ? 'justify-center' : 'justify-between'}`}>
        {!collapsed ? (
          <div>
            <div className="text-[11px] font-semibold uppercase tracking-[0.18em] text-sky-300">Меню</div>
          </div>
        ) : null}
        <button
          type="button"
          onClick={onToggleCollapsed}
          className="inline-flex h-10 w-10 items-center justify-center rounded-2xl border border-white/10 bg-white/5 text-slate-200 transition hover:bg-white/10 hover:text-white"
          title={collapsed ? 'Развернуть меню' : 'Свернуть меню'}
        >
          {collapsed ? <ChevronDoubleRightIcon className="h-5 w-5" /> : <ChevronDoubleLeftIcon className="h-5 w-5" />}
        </button>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto pr-1">
        <nav className="mt-1 flex flex-col gap-2">
          {primaryNavigation.map((item) => {
            const isActive =
              pathname === item.href ||
              pathname.startsWith(`${item.href}/`);

            return (
              <Link
                key={item.name}
                href={item.href}
                onClick={onNavigate}
                title={collapsed ? item.name : undefined}
                className={classNames(
                  `group flex items-center rounded-2xl py-3 text-sm font-medium transition ${collapsed ? 'justify-center px-2' : 'gap-3 px-4'}`,
                  isActive
                    ? 'bg-white text-slate-950 shadow-[0_14px_30px_rgba(255,255,255,0.14)]'
                    : 'text-slate-200 hover:bg-white/10 hover:text-white'
                )}
              >
                <item.icon className={classNames('h-5 w-5 shrink-0', isActive ? 'text-sky-600' : 'text-slate-300')} />
                {!collapsed ? <span>{item.name}</span> : null}
              </Link>
            );
          })}
        </nav>
      </div>

      <button
        type="button"
        onClick={logout}
        title={collapsed ? 'Выйти' : undefined}
        className={`mt-6 inline-flex rounded-2xl border border-white/10 bg-white/5 py-3 text-sm font-medium text-slate-200 transition hover:bg-white/10 hover:text-white ${collapsed ? 'justify-center px-2' : 'items-center gap-3 px-4'}`}
      >
        <ArrowLeftOnRectangleIcon className="h-5 w-5" />
        {!collapsed ? 'Выйти' : null}
      </button>
    </div>
  );
}
