'use client';

import { useState } from 'react';
import { PencilIcon, PlusIcon, TrashIcon } from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { Layout } from '@/components/Layout';
import { StoreForm } from '@/components/Stores/StoreForm';
import { storesAPI, type Store } from '@/lib/api/stores';
import { useAuth } from '@/lib/context/AuthContext';
import { useStoreContext } from '@/lib/context/StoreContext';

export default function StoresPage() {
  const { user } = useAuth();
  const { stores, isLoading, refreshStores } = useStoreContext();
  const [showForm, setShowForm] = useState(false);
  const [editingStore, setEditingStore] = useState<Store | null>(null);
  const canManageStores = Boolean(user?.can_manage_business_settings);

  const handleDelete = async (id: number, name: string) => {
    if (!canManageStores) {
      toast.error('Только владелец кабинета может изменять магазины');
      return;
    }

    if (!confirm(`Вы уверены, что хотите удалить магазин "${name}"?`)) {
      return;
    }

    try {
      await storesAPI.delete(id);
      toast.success('Магазин удалён');
      await refreshStores();
    } catch {
      toast.error('Ошибка при удалении магазина');
    }
  };

  const handleFormSuccess = async (savedStore?: Store) => {
    setShowForm(false);
    setEditingStore(null);
    await refreshStores(savedStore?.id ?? null);
  };

  const formatDate = (dateString: string) => new Date(dateString).toLocaleDateString('ru-RU');

  if (isLoading) {
    return (
      <ProtectedRoute>
        <Layout>
          <div className="flex h-64 items-center justify-center">
            <div className="h-12 w-12 animate-spin rounded-full border-b-2 border-primary-600"></div>
          </div>
        </Layout>
      </ProtectedRoute>
    );
  }

  return (
    <ProtectedRoute>
      <Layout>
        <div className="space-y-8">
          <section className="rounded-[32px] border border-white/70 bg-[radial-gradient(circle_at_top_left,_rgba(255,255,255,0.96),_rgba(240,249,255,0.92)_36%,_rgba(255,247,237,0.90)_100%)] p-8 shadow-[0_24px_60px_rgba(15,23,42,0.08)]">
            <div className="flex flex-col gap-6 lg:flex-row lg:items-end lg:justify-between">
              <div className="max-w-3xl">
                <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Магазины OZON</p>
                <h1 className="mt-3 text-3xl font-semibold tracking-tight text-slate-950 sm:text-4xl">
                  Подключай и управляй магазинами без смешивания данных
                </h1>
                <p className="mt-3 text-sm leading-6 text-slate-600 sm:text-base">
                  Здесь только карточки магазинов: название, Client-ID, редактирование и удаление. Статусы синхронизаций вынесены на отдельную страницу.
                </p>
                {!canManageStores && (
                  <div className="mt-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
                    Управлять магазинами может только владелец кабинета. Участникам доступен просмотр списка и статусов.
                  </div>
                )}
              </div>
              {canManageStores && (
                <button
                  onClick={() => {
                    setEditingStore(null);
                    setShowForm(true);
                  }}
                  className="inline-flex items-center rounded-2xl bg-slate-950 px-5 py-3 text-sm font-medium text-white transition hover:bg-slate-800"
                >
                  <PlusIcon className="-ml-1 mr-2 h-5 w-5" />
                  Добавить магазин
                </button>
              )}
            </div>
          </section>

          {stores.length === 0 ? (
            <div className="rounded-[28px] border border-dashed border-slate-300 bg-white px-6 py-12 text-center shadow-sm">
              <h3 className="text-lg font-semibold text-slate-900">Пока нет подключённых магазинов</h3>
              <p className="mt-2 text-sm text-slate-500">Добавь первый магазин OZON, и после этого мы автоматически поставим полную синхронизацию в очередь.</p>
              {canManageStores && (
                <button
                  onClick={() => {
                    setEditingStore(null);
                    setShowForm(true);
                  }}
                  className="mt-6 inline-flex items-center rounded-2xl bg-slate-950 px-4 py-3 text-sm font-medium text-white transition hover:bg-slate-800"
                >
                  <PlusIcon className="-ml-1 mr-2 h-5 w-5" />
                  Добавить магазин
                </button>
              )}
            </div>
          ) : (
            <section className="grid gap-4 xl:grid-cols-2">
              {stores.map((store) => (
                <article key={store.id} className="rounded-[28px] border border-slate-200/80 bg-white p-6 shadow-[0_18px_44px_rgba(15,23,42,0.06)]">
                  <div className="flex items-start justify-between gap-4">
                    <div className="min-w-0">
                      <div className="flex flex-wrap items-center gap-2">
                        <h2 className="truncate text-xl font-semibold text-slate-950">{store.name}</h2>
                        <span className={`rounded-full px-3 py-1 text-xs font-medium ${store.is_active ? 'bg-emerald-100 text-emerald-800' : 'bg-slate-100 text-slate-600'}`}>
                          {store.is_active ? 'Активен' : 'Неактивен'}
                        </span>
                      </div>
                      <div className="mt-4 grid gap-3 sm:grid-cols-2">
                        <div className="rounded-2xl bg-slate-50 px-4 py-3">
                          <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Client-ID</div>
                          <div className="mt-1 text-base font-semibold text-slate-900">{store.client_id}</div>
                        </div>
                        <div className="rounded-2xl bg-slate-50 px-4 py-3">
                          <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Добавлен</div>
                          <div className="mt-1 text-base font-semibold text-slate-900">{formatDate(store.created_at)}</div>
                        </div>
                      </div>
                    </div>

                    {canManageStores && (
                      <div className="flex shrink-0 items-center gap-2">
                        <button
                          onClick={() => {
                            setEditingStore(store);
                            setShowForm(true);
                          }}
                          className="rounded-2xl border border-slate-200 p-2 text-slate-500 transition hover:border-sky-200 hover:text-sky-700"
                          title="Редактировать"
                        >
                          <PencilIcon className="h-5 w-5" />
                        </button>
                        <button
                          onClick={() => handleDelete(store.id, store.name)}
                          className="rounded-2xl border border-slate-200 p-2 text-slate-500 transition hover:border-rose-200 hover:text-rose-700"
                          title="Удалить"
                        >
                          <TrashIcon className="h-5 w-5" />
                        </button>
                      </div>
                    )}
                  </div>
                </article>
              ))}
            </section>
          )}
        </div>

        {showForm && canManageStores && (
          <StoreForm
            store={editingStore}
            onClose={() => {
              setShowForm(false);
              setEditingStore(null);
            }}
            onSuccess={handleFormSuccess}
          />
        )}
      </Layout>
    </ProtectedRoute>
  );
}
