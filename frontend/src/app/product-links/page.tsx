'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import { CheckIcon, PlusIcon, TrashIcon, XMarkIcon } from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { Layout } from '@/components/Layout';
import {
  warehouseProductsAPI,
  type WarehouseProductItem,
  type WarehouseProductVariant,
  type WarehouseProductsLinksResponse,
} from '@/lib/api/warehouseProducts';

function getPackLabel(variant: WarehouseProductVariant) {
  const raw =
    variant.attributes?.['Количество в упаковке'] ||
    variant.attributes?.['Кол-во в упаковке'] ||
    variant.attributes?.['Упаковка'];

  return raw || `${variant.pack_size} шт`;
}

function buildSuggestedProductName(variants: WarehouseProductVariant[]) {
  const candidates = variants
    .map((variant) => variant.source_base_name?.trim())
    .filter(Boolean) as string[];

  if (!candidates.length) {
    return 'Новый товар';
  }

  const counts = new Map<string, number>();
  candidates.forEach((candidate) => {
    counts.set(candidate, (counts.get(candidate) || 0) + 1);
  });

  return Array.from(counts.entries()).sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0], 'ru'))[0][0];
}

export default function ProductLinksPage() {
  const [data, setData] = useState<WarehouseProductsLinksResponse>({ warehouse_products: [], unlinked_variants: [] });
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [selectedVariantIds, setSelectedVariantIds] = useState<number[]>([]);
  const [createModalOpen, setCreateModalOpen] = useState(false);
  const [attachModalOpen, setAttachModalOpen] = useState(false);
  const [newProductName, setNewProductName] = useState('');
  const [selectedWarehouseProductId, setSelectedWarehouseProductId] = useState<number | null>(null);

  const loadLinks = useCallback(async () => {
    setLoading(true);
    setLoadError(null);
    try {
      const nextData = await warehouseProductsAPI.getLinks();
      setData(nextData);
    } catch {
      setData({ warehouse_products: [], unlinked_variants: [] });
      setLoadError('Не удалось загрузить связи товаров.');
      toast.error('Не удалось загрузить связи товаров');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadLinks();
  }, [loadLinks]);

  const selectedVariants = useMemo(
    () => data.unlinked_variants.filter((variant) => selectedVariantIds.includes(variant.id)),
    [data.unlinked_variants, selectedVariantIds],
  );

  const selectedCount = selectedVariantIds.length;

  useEffect(() => {
    const suggestedName = buildSuggestedProductName(selectedVariants);
    setNewProductName((current) => current || suggestedName);
  }, [selectedVariants]);

  const resetSelection = () => {
    setSelectedVariantIds([]);
    setSelectedWarehouseProductId(null);
    setNewProductName('');
  };

  const toggleVariant = (variantId: number) => {
    setSelectedVariantIds((current) =>
      current.includes(variantId) ? current.filter((id) => id !== variantId) : [...current, variantId]
    );
  };

  const selectAllVisible = () => {
    setSelectedVariantIds(data.unlinked_variants.map((variant) => variant.id));
  };

  const clearSelection = () => {
    setSelectedVariantIds([]);
  };

  const refreshAfterMutation = async (successMessage: string) => {
    await loadLinks();
    resetSelection();
    setCreateModalOpen(false);
    setAttachModalOpen(false);
    toast.success(successMessage);
  };

  const handleCreateProduct = async () => {
    if (!selectedVariantIds.length) {
      toast.error('Сначала выбери вариации');
      return;
    }
    if (!newProductName.trim()) {
      toast.error('Укажи название нового товара');
      return;
    }

    setSubmitting(true);
    try {
      await warehouseProductsAPI.attachVariants({
        variant_ids: selectedVariantIds,
        warehouse_product_name: newProductName.trim(),
      });
      await refreshAfterMutation('Товар создан, вариации привязаны');
    } catch {
      toast.error('Не удалось создать товар из выбранных вариаций');
    } finally {
      setSubmitting(false);
    }
  };

  const handleAttachToExisting = async (warehouseProductId?: number) => {
    const targetId = warehouseProductId ?? selectedWarehouseProductId;
    if (!selectedVariantIds.length) {
      toast.error('Сначала выбери вариации');
      return;
    }
    if (!targetId) {
      toast.error('Выбери товар, к которому нужно добавить вариации');
      return;
    }

    setSubmitting(true);
    try {
      await warehouseProductsAPI.attachVariants({
        variant_ids: selectedVariantIds,
        warehouse_product_id: targetId,
      });
      await refreshAfterMutation('Вариации добавлены к товару');
    } catch {
      toast.error('Не удалось добавить вариации к товару');
    } finally {
      setSubmitting(false);
    }
  };

  const handleDetachVariant = async (variantId: number) => {
    setSubmitting(true);
    try {
      await warehouseProductsAPI.detachVariants({ variant_ids: [variantId] });
      await loadLinks();
      toast.success('Вариация отвязана');
    } catch {
      toast.error('Не удалось отвязать вариацию');
    } finally {
      setSubmitting(false);
    }
  };

  const handleDeleteWarehouseProduct = async (item: WarehouseProductItem) => {
    if (
      !window.confirm(
        `Удалить товар «${item.name}»? Все связанные вариации останутся в системе и станут непривязанными.`
      )
    ) {
      return;
    }

    setSubmitting(true);
    try {
      await warehouseProductsAPI.deleteWarehouseProduct(item.id);
      await loadLinks();
      toast.success('Товар удален, вариации отвязаны');
    } catch {
      toast.error('Не удалось удалить товар');
    } finally {
      setSubmitting(false);
    }
  };

  const summary = useMemo(
    () => ({
      warehouseProducts: data.warehouse_products.length,
      linkedVariants: data.warehouse_products.reduce((acc, item) => acc + item.variants_count, 0),
      unlinkedVariants: data.unlinked_variants.length,
    }),
    [data],
  );

  if (loading) {
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
        <div className="mb-8">
          <h1 className="text-2xl font-semibold text-gray-900">Связи товаров</h1>
          <p className="mt-1 max-w-4xl text-sm text-gray-500">
            Здесь собираются одинаковые товары из разных магазинов в один складской товар. Это влияет и на общую
            себестоимость: после правильной связи один и тот же товар не придется заново заполнять для каждого
            магазина по отдельности.
          </p>
        </div>

        <div className="mb-8 grid gap-4 sm:grid-cols-3">
          <div className="rounded-3xl border border-slate-200 bg-white px-5 py-4 shadow-sm">
            <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Товары склада</div>
            <div className="mt-2 text-3xl font-semibold text-slate-950">{summary.warehouseProducts}</div>
          </div>
          <div className="rounded-3xl border border-slate-200 bg-white px-5 py-4 shadow-sm">
            <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Связанные вариации</div>
            <div className="mt-2 text-3xl font-semibold text-slate-950">{summary.linkedVariants}</div>
          </div>
          <div className="rounded-3xl border border-slate-200 bg-white px-5 py-4 shadow-sm">
            <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Непривязанные вариации</div>
            <div className="mt-2 text-3xl font-semibold text-slate-950">{summary.unlinkedVariants}</div>
          </div>
        </div>

        {loadError ? (
          <div className="rounded-3xl border border-amber-200 bg-amber-50 px-6 py-8 shadow-sm">
            <h2 className="text-lg font-semibold text-amber-950">Не удалось открыть связи товаров</h2>
            <p className="mt-2 text-sm text-amber-800">{loadError}</p>
            <button
              type="button"
              onClick={() => void loadLinks()}
              className="mt-4 inline-flex rounded-2xl bg-amber-600 px-4 py-3 text-sm font-medium text-white transition hover:bg-amber-700"
            >
              Повторить загрузку
            </button>
          </div>
        ) : (
          <div className="space-y-8">
            <section className="rounded-[32px] border border-slate-200 bg-white p-6 shadow-sm">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                <div>
                  <h2 className="text-xl font-semibold text-slate-950">Непривязанные вариации</h2>
                  <p className="mt-1 text-sm text-slate-500">
                    Выдели нужные артикулы и либо создай из них новый товар, либо добавь их к уже существующему.
                  </p>
                </div>

                <div className="flex flex-wrap gap-2">
                  <button
                    type="button"
                    onClick={selectAllVisible}
                    disabled={!data.unlinked_variants.length}
                    className="rounded-2xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:opacity-50"
                  >
                    Выбрать все
                  </button>
                  <button
                    type="button"
                    onClick={clearSelection}
                    disabled={!selectedCount}
                    className="rounded-2xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:opacity-50"
                  >
                    Снять выбор
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      if (!selectedCount) {
                        toast.error('Сначала выбери вариации');
                        return;
                      }
                      setNewProductName(buildSuggestedProductName(selectedVariants));
                      setCreateModalOpen(true);
                    }}
                    disabled={!selectedCount}
                    className="inline-flex items-center gap-2 rounded-2xl bg-slate-950 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:opacity-50"
                  >
                    <PlusIcon className="h-4 w-4" />
                    Создать товар
                  </button>
                  <button
                    type="button"
                    onClick={() => {
                      if (!selectedCount) {
                        toast.error('Сначала выбери вариации');
                        return;
                      }
                      setSelectedWarehouseProductId(data.warehouse_products[0]?.id ?? null);
                      setAttachModalOpen(true);
                    }}
                    disabled={!selectedCount || !data.warehouse_products.length}
                    className="inline-flex items-center gap-2 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-2 text-sm font-medium text-sky-700 transition hover:bg-sky-100 disabled:opacity-50"
                  >
                    <CheckIcon className="h-4 w-4" />
                    Добавить к товару
                  </button>
                </div>
              </div>

              <div className="mt-4 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                Выбрано вариаций: <span className="font-semibold text-slate-900">{selectedCount}</span>
              </div>

              {!data.unlinked_variants.length ? (
                <div className="mt-5 rounded-3xl border border-dashed border-slate-300 bg-slate-50 px-6 py-10 text-center text-sm text-slate-500">
                  Сейчас все вариации уже привязаны к товарам склада.
                </div>
              ) : (
                <div className="mt-5 overflow-hidden rounded-3xl border border-slate-200">
                  <div className="grid grid-cols-[56px_minmax(0,1.1fr)_minmax(0,1fr)_160px_140px_120px] gap-3 bg-slate-50 px-4 py-3 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                    <div></div>
                    <div>Артикул</div>
                    <div>Исходная группа</div>
                    <div>Магазин</div>
                    <div>Цвет / Размер</div>
                    <div>Упаковка</div>
                  </div>
                  <div className="divide-y divide-slate-200">
                    {data.unlinked_variants.map((variant) => (
                      <label
                        key={variant.id}
                        className="grid cursor-pointer grid-cols-[56px_minmax(0,1.1fr)_minmax(0,1fr)_160px_140px_120px] gap-3 px-4 py-3 text-sm text-slate-700 transition hover:bg-slate-50"
                      >
                        <div className="flex items-center">
                          <input
                            type="checkbox"
                            checked={selectedVariantIds.includes(variant.id)}
                            onChange={() => toggleVariant(variant.id)}
                            className="h-4 w-4 rounded border-slate-300 text-sky-600"
                          />
                        </div>
                        <div>
                          <div className="font-medium text-slate-950">{variant.offer_id}</div>
                          {variant.is_archived && (
                            <div className="mt-1 text-xs font-medium text-amber-700">Архив OZON</div>
                          )}
                        </div>
                        <div className="min-w-0">
                          <div className="truncate font-medium text-slate-900">{variant.source_base_name}</div>
                          <div className="truncate text-xs text-slate-500">{variant.source_product_name}</div>
                        </div>
                        <div className="truncate">{variant.store_name}</div>
                        <div className="truncate">{variant.color} / {variant.size}</div>
                        <div>{getPackLabel(variant)}</div>
                      </label>
                    ))}
                  </div>
                </div>
              )}
            </section>

            <section className="space-y-5">
              <div>
                <h2 className="text-xl font-semibold text-slate-950">Товары склада</h2>
                <p className="mt-1 text-sm text-slate-500">
                  Здесь видно, какие вариации входят в каждый товар. Можно добавить к товару выбранные непривязанные
                  вариации, отвязать конкретную вариацию или удалить сам товар.
                </p>
              </div>

              {!data.warehouse_products.length ? (
                <div className="rounded-3xl border border-dashed border-slate-300 bg-white px-6 py-12 text-center shadow-sm">
                  <h2 className="text-lg font-semibold text-slate-900">Пока нет товаров склада</h2>
                  <p className="mt-2 text-sm text-slate-500">
                    Выдели непривязанные вариации выше и создай первый товар.
                  </p>
                </div>
              ) : (
                data.warehouse_products.map((item) => (
                  <section
                    key={item.id}
                    className="overflow-hidden rounded-[32px] border border-slate-200 bg-white shadow-sm"
                  >
                    <div className="flex flex-col gap-4 border-b border-slate-200 bg-slate-50 px-6 py-5 lg:flex-row lg:items-center lg:justify-between">
                      <div>
                        <h3 className="text-xl font-semibold text-slate-950">{item.name}</h3>
                        <p className="mt-1 text-sm text-slate-500">
                          Вариаций: {item.variants_count} · Магазинов: {item.stores_count}
                        </p>
                      </div>

                      <div className="flex flex-wrap gap-2">
                        <button
                          type="button"
                          onClick={() => void handleAttachToExisting(item.id)}
                          disabled={!selectedCount}
                          className="rounded-2xl border border-sky-200 bg-sky-50 px-4 py-2 text-sm font-medium text-sky-700 transition hover:bg-sky-100 disabled:opacity-50"
                        >
                          Добавить выбранные вариации
                        </button>
                        <button
                          type="button"
                          onClick={() => void handleDeleteWarehouseProduct(item)}
                          disabled={submitting}
                          className="inline-flex items-center gap-2 rounded-2xl border border-rose-200 bg-rose-50 px-4 py-2 text-sm font-medium text-rose-700 transition hover:bg-rose-100 disabled:opacity-50"
                        >
                          <TrashIcon className="h-4 w-4" />
                          Удалить товар
                        </button>
                      </div>
                    </div>

                    <div className="overflow-hidden">
                      <div className="grid grid-cols-[160px_minmax(0,1fr)_120px_120px_120px_120px] gap-3 bg-white px-6 py-3 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                        <div>Магазин</div>
                        <div>Артикул</div>
                        <div>Цвет</div>
                        <div>Размер</div>
                        <div>Упаковка</div>
                        <div></div>
                      </div>
                      <div className="divide-y divide-slate-200">
                        {item.variants.map((variant) => (
                          <div
                            key={variant.id}
                            className="grid grid-cols-[160px_minmax(0,1fr)_120px_120px_120px_120px] gap-3 px-6 py-3 text-sm text-slate-700"
                          >
                            <div className="truncate">{variant.store_name}</div>
                            <div className="min-w-0">
                              <div className="truncate font-medium text-slate-950">{variant.offer_id}</div>
                              <div className="truncate text-xs text-slate-500">{variant.source_base_name}</div>
                            </div>
                            <div className="truncate">{variant.color}</div>
                            <div className="truncate">{variant.size}</div>
                            <div>{getPackLabel(variant)}</div>
                            <div className="flex justify-end">
                              <button
                                type="button"
                                onClick={() => void handleDetachVariant(variant.id)}
                                disabled={submitting}
                                className="rounded-2xl border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:bg-slate-50 disabled:opacity-50"
                              >
                                Отвязать
                              </button>
                            </div>
                          </div>
                        ))}
                      </div>
                    </div>
                  </section>
                ))
              )}
            </section>
          </div>
        )}

        {createModalOpen && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/50 p-4 backdrop-blur-sm">
            <div className="w-full max-w-xl rounded-[28px] bg-white shadow-[0_28px_80px_rgba(15,23,42,0.24)]">
              <div className="flex items-start justify-between border-b border-slate-200 px-6 py-5">
                <div>
                  <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Создание товара</div>
                  <h2 className="mt-1 text-xl font-semibold text-slate-950">Новый товар из выбранных вариаций</h2>
                  <p className="mt-1 text-sm text-slate-500">Выбрано вариаций: {selectedCount}</p>
                </div>
                <button
                  type="button"
                  onClick={() => setCreateModalOpen(false)}
                  className="rounded-2xl p-2 text-slate-400 transition hover:bg-slate-100 hover:text-slate-700"
                >
                  <XMarkIcon className="h-5 w-5" />
                </button>
              </div>

              <div className="space-y-4 px-6 py-5">
                <div>
                  <label className="block text-sm font-medium text-slate-700">Название нового товара</label>
                  <input
                    type="text"
                    value={newProductName}
                    onChange={(event) => setNewProductName(event.target.value)}
                    className="mt-2 block w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                    placeholder="Например, Носки базовые"
                  />
                </div>

                <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                  После создания приложение само сгруппирует вариации внутри товара по цвету, размеру и упаковке.
                </div>
              </div>

              <div className="flex justify-end gap-3 border-t border-slate-200 px-6 py-5">
                <button
                  type="button"
                  onClick={() => setCreateModalOpen(false)}
                  className="rounded-2xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
                >
                  Отмена
                </button>
                <button
                  type="button"
                  onClick={() => void handleCreateProduct()}
                  disabled={submitting}
                  className="rounded-2xl bg-slate-950 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:opacity-50"
                >
                  {submitting ? 'Сохраняем...' : 'Создать товар'}
                </button>
              </div>
            </div>
          </div>
        )}

        {attachModalOpen && (
          <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/50 p-4 backdrop-blur-sm">
            <div className="w-full max-w-xl rounded-[28px] bg-white shadow-[0_28px_80px_rgba(15,23,42,0.24)]">
              <div className="flex items-start justify-between border-b border-slate-200 px-6 py-5">
                <div>
                  <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Добавление вариаций</div>
                  <h2 className="mt-1 text-xl font-semibold text-slate-950">Добавить к существующему товару</h2>
                  <p className="mt-1 text-sm text-slate-500">Выбрано вариаций: {selectedCount}</p>
                </div>
                <button
                  type="button"
                  onClick={() => setAttachModalOpen(false)}
                  className="rounded-2xl p-2 text-slate-400 transition hover:bg-slate-100 hover:text-slate-700"
                >
                  <XMarkIcon className="h-5 w-5" />
                </button>
              </div>

              <div className="space-y-4 px-6 py-5">
                <div>
                  <label className="block text-sm font-medium text-slate-700">К какому товару добавить вариации</label>
                  <select
                    value={selectedWarehouseProductId ?? ''}
                    onChange={(event) => setSelectedWarehouseProductId(event.target.value ? Number(event.target.value) : null)}
                    className="mt-2 block w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                  >
                    <option value="">Выбери товар</option>
                    {data.warehouse_products.map((item) => (
                      <option key={item.id} value={item.id}>
                        {item.name}
                      </option>
                    ))}
                  </select>
                </div>
              </div>

              <div className="flex justify-end gap-3 border-t border-slate-200 px-6 py-5">
                <button
                  type="button"
                  onClick={() => setAttachModalOpen(false)}
                  className="rounded-2xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
                >
                  Отмена
                </button>
                <button
                  type="button"
                  onClick={() => void handleAttachToExisting()}
                  disabled={submitting}
                  className="rounded-2xl bg-slate-950 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:opacity-50"
                >
                  {submitting ? 'Сохраняем...' : 'Добавить'}
                </button>
              </div>
            </div>
          </div>
        )}
      </Layout>
    </ProtectedRoute>
  );
}
