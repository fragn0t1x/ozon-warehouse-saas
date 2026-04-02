'use client';

import { useState } from 'react';
import { ArrowRightIcon, SparklesIcon } from '@heroicons/react/24/outline';

import { useAuth } from '@/lib/context/AuthContext';

export default function LoginPage() {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const { login } = useAuth();

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setIsLoading(true);
    try {
      const nextPath = typeof window !== 'undefined'
        ? new URLSearchParams(window.location.search).get('next') || '/dashboard'
        : '/dashboard';
      await login(email, password, nextPath);
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="relative min-h-screen overflow-hidden px-4 py-10 sm:px-6 lg:px-8">
      <div className="absolute inset-0 bg-[radial-gradient(circle_at_top_left,_rgba(48,214,255,0.2),_transparent_28%),radial-gradient(circle_at_bottom_right,_rgba(255,124,102,0.18),_transparent_24%)]" />

      <div className="relative mx-auto grid min-h-[calc(100vh-5rem)] max-w-6xl items-center gap-8 lg:grid-cols-[1.1fr_0.9fr]">
        <section className="rounded-[36px] border border-white/60 bg-[linear-gradient(160deg,rgba(8,18,35,0.96),rgba(15,37,64,0.92))] p-8 text-white shadow-[0_28px_80px_rgba(15,23,42,0.28)] sm:p-10">
          <div className="inline-flex items-center gap-2 rounded-full border border-white/15 bg-white/10 px-4 py-1 text-xs font-semibold uppercase tracking-[0.22em] text-cyan-200">
            <SparklesIcon className="h-4 w-4" />
            SaaS для OZON продавцов
          </div>

          <h1 className="mt-6 max-w-xl text-4xl font-semibold tracking-tight sm:text-5xl">
            Склад, поставки и приёмка без бесконечных таблиц.
          </h1>

          <p className="mt-5 max-w-xl text-base leading-7 text-slate-200">
            Кабинет собран под сценарии из PDF: общий или раздельный склад, простой или расширенный режим упаковки,
            резерв на этапе готовности к отгрузке, контроль ETA и аккуратная работа с расхождениями.
          </p>

          <div className="mt-8 grid gap-4 sm:grid-cols-3">
            <div className="flex min-h-[168px] flex-col rounded-3xl bg-white/10 p-4">
              <div className="text-sm text-slate-300">Резервы</div>
              <div className="mt-3 max-w-full overflow-hidden text-xl font-semibold leading-tight [overflow-wrap:anywhere] sm:text-2xl">
                Авто-резерв
              </div>
            </div>
            <div className="flex min-h-[168px] flex-col rounded-3xl bg-white/10 p-4">
              <div className="text-sm text-slate-300">Отправки</div>
              <div className="mt-3 max-w-full overflow-hidden text-xl font-semibold leading-tight [overflow-wrap:anywhere] sm:text-2xl">
                По правилам
              </div>
            </div>
            <div className="flex min-h-[168px] flex-col rounded-3xl bg-white/10 p-4">
              <div className="text-sm text-slate-300">Приёмка</div>
              <div className="mt-3 max-w-full overflow-hidden text-xl font-semibold leading-tight [overflow-wrap:anywhere] sm:text-2xl">
                С контролем потерь
              </div>
            </div>
          </div>
        </section>

        <section className="card p-8 sm:p-10">
          <div>
            <p className="text-xs font-semibold uppercase tracking-[0.2em] text-sky-700">Вход в кабинет</p>
            <h2 className="mt-3 text-3xl font-semibold text-slate-950">Рады видеть вас снова</h2>
            <p className="mt-2 text-sm leading-6 text-slate-500">
              После первого входа мы откроем пошаговый мастер, чтобы спокойно собрать базовую конфигурацию склада, уведомлений и первого магазина.
            </p>
          </div>

          <form className="mt-8 space-y-4" onSubmit={handleSubmit}>
            <div>
              <label htmlFor="email" className="mb-2 block text-sm font-medium text-slate-700">
                Email
              </label>
              <input
                id="email"
                name="email"
                type="email"
                autoComplete="email"
                required
                value={email}
                onChange={(event) => setEmail(event.target.value)}
                className="input-field"
                placeholder="you@example.com"
                disabled={isLoading}
              />
            </div>

            <div>
              <label htmlFor="password" className="mb-2 block text-sm font-medium text-slate-700">
                Пароль
              </label>
              <input
                id="password"
                name="password"
                type="password"
                autoComplete="current-password"
                required
                value={password}
                onChange={(event) => setPassword(event.target.value)}
                className="input-field"
                placeholder="Введите пароль"
                disabled={isLoading}
              />
            </div>

            <button
              type="submit"
              disabled={isLoading}
              className="btn-primary mt-2 w-full gap-2 disabled:cursor-not-allowed disabled:opacity-70"
            >
              {isLoading ? 'Входим...' : 'Войти в систему'}
              <ArrowRightIcon className="h-4 w-4" />
            </button>
          </form>
        </section>
      </div>
    </div>
  );
}
