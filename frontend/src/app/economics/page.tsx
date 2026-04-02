'use client';

import { useEffect } from 'react';
import { useRouter } from 'next/navigation';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { Layout } from '@/components/Layout';

export default function EconomicsRedirectPage() {
  const router = useRouter();

  useEffect(() => {
    router.replace('/dashboard');
  }, [router]);

  return (
    <ProtectedRoute>
      <Layout>
        <div className="rounded-3xl border border-white/70 bg-white/85 p-8 shadow-[0_16px_40px_rgba(15,23,42,0.06)] backdrop-blur">
          <h1 className="text-2xl font-semibold text-slate-950">Экономика теперь в Магазин сегодня</h1>
          <p className="mt-2 text-sm text-slate-500">
            Перенаправляем в Control Center. Экономика теперь показывается только по активному магазину.
          </p>
        </div>
      </Layout>
    </ProtectedRoute>
  );
}
