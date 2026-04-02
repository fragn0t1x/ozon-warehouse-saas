'use client';

import Image from 'next/image';
import Link from 'next/link';
import { useCallback, useEffect, useState } from 'react';
import toast from 'react-hot-toast';
import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { Layout } from '@/components/Layout';
import { SyncFreshnessPanel } from '@/components/SyncFreshnessPanel';
import { useStoreContext } from '@/lib/context/StoreContext';
import { productsAPI, type GroupedProduct } from '@/lib/api/products';
import { getVariantCharacteristicText, getVariantDisplayTitle } from '@/lib/utils/variantPresentation';
import { MagnifyingGlassIcon } from '@heroicons/react/24/outline';

type CatalogView = 'active' | 'archived' | 'all';

const filterVariantsByCatalogView = (product: GroupedProduct, catalogView: CatalogView): GroupedProduct | null => {
  const colors = product.colors
    .map((colorGroup) => {
      const sizes = colorGroup.sizes
        .map((sizeGroup) => {
          const variants = sizeGroup.variants.filter((variant) => {
            if (catalogView === 'all') {
              return true;
            }
            return catalogView === 'archived' ? variant.is_archived : !variant.is_archived;
          });

          if (variants.length === 0) {
            return null;
          }

          return {
            ...sizeGroup,
            variants,
          };
        })
        .filter((sizeGroup): sizeGroup is NonNullable<typeof sizeGroup> => Boolean(sizeGroup));

      if (sizes.length === 0) {
        return null;
      }

      return {
        ...colorGroup,
        sizes,
      };
    })
    .filter((colorGroup): colorGroup is NonNullable<typeof colorGroup> => Boolean(colorGroup));

  if (colors.length === 0) {
    return null;
  }

  const visibleVariants = colors.reduce(
    (acc, colorGroup) => acc + colorGroup.sizes.reduce((sizeAcc, sizeGroup) => sizeAcc + sizeGroup.variants.length, 0),
    0
  );
  const archivedVariants = colors.reduce(
    (acc, colorGroup) => (
      acc + colorGroup.sizes.reduce(
        (sizeAcc, sizeGroup) => sizeAcc + sizeGroup.variants.filter((variant) => variant.is_archived).length,
        0
      )
    ),
    0
  );

  return {
    ...product,
    colors,
    total_variants: visibleVariants,
    archived_variants_count: archivedVariants,
    active_variants_count: visibleVariants - archivedVariants,
    is_archived: archivedVariants > 0 && archivedVariants === visibleVariants,
  };
};

export default function ProductsPage() {
  const { selectedStore, selectedStoreId, stores, isLoading: storesLoading } = useStoreContext();
  const [products, setProducts] = useState<GroupedProduct[]>([]);
  const [filteredProducts, setFilteredProducts] = useState<GroupedProduct[]>([]);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [searchQuery, setSearchQuery] = useState('');
  const [catalogView, setCatalogView] = useState<CatalogView>('active');
  const [expandedProduct, setExpandedProduct] = useState<number | null>(null);

  useEffect(() => {
    const query = searchQuery.trim().toLowerCase();
    const byStatus = products
      .map((product) => filterVariantsByCatalogView(product, catalogView))
      .filter((product): product is GroupedProduct => Boolean(product));

    if (!query) {
      setFilteredProducts(byStatus);
      return;
    }

    const filtered = byStatus.filter(product =>
      product.name.toLowerCase().includes(query) ||
      product.colors.some(colorGroup =>
        colorGroup.sizes.some(sizeGroup =>
          sizeGroup.variants.some(v =>
            v.offer_id.toLowerCase().includes(query)
          )
        )
      )
    );
    setFilteredProducts(filtered);
  }, [catalogView, searchQuery, products]);

  const fetchProducts = useCallback(async () => {
    if (!selectedStoreId) {
      setProducts([]);
      setFilteredProducts([]);
      setLoadError(null);
      setLoading(false);
      return;
    }

    setLoading(true);
    setLoadError(null);
    try {
      const data = await productsAPI.getGrouped(selectedStoreId);
      setProducts(data);
    } catch {
      setProducts([]);
      setFilteredProducts([]);
      setLoadError('Не удалось загрузить товары выбранного магазина.');
      toast.error('Не удалось загрузить товары выбранного магазина');
    } finally {
      setLoading(false);
    }
  }, [selectedStoreId]);

  useEffect(() => {
    void fetchProducts();
  }, [fetchProducts]);

  const totalVariants = filteredProducts.reduce((acc, p) => acc + p.total_variants, 0);
  const activeProductsCount = products.filter((product) => product.active_variants_count > 0).length;
  const archivedProductsCount = products.filter((product) => product.archived_variants_count > 0).length;
  const variantsWithCost = products.reduce((acc, product) => (
    acc + product.colors.reduce((colorAcc, colorGroup) => (
      colorAcc + colorGroup.sizes.reduce((sizeAcc, sizeGroup) => (
        sizeAcc + sizeGroup.variants.filter((variant) => typeof variant.unit_cost === 'number').length
      ), 0)
    ), 0)
  ), 0);

  const viewLabel = catalogView === 'archived'
    ? 'Показываем только архивные товары и вариации.'
    : catalogView === 'all'
      ? 'Показываем и активные, и архивные товары этого магазина.'
      : 'Показываем только активные товары магазина.';

  if (loading || storesLoading) {
    return (
      <ProtectedRoute>
        <Layout>
          <div className="flex items-center justify-center h-64">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600"></div>
          </div>
        </Layout>
      </ProtectedRoute>
    );
  }

  return (
    <ProtectedRoute>
      <Layout>
        <div className="mb-8">
          <h1 className="text-2xl font-semibold text-gray-900">Товары</h1>
          <p className="mt-1 text-sm text-gray-500">
            Управление товарами и характеристиками выбранного магазина
          </p>
        </div>

        {stores.length === 0 ? (
          <div className="rounded-3xl border border-dashed border-slate-300 bg-white px-6 py-12 text-center shadow-sm">
            <h2 className="text-lg font-semibold text-slate-900">Сначала нужен магазин</h2>
            <p className="mt-2 text-sm text-slate-500">Добавь магазин OZON, и здесь сразу появятся только его товары.</p>
            <Link href="/stores" className="mt-5 inline-flex rounded-2xl bg-slate-950 px-4 py-3 text-sm font-medium text-white transition hover:bg-slate-800">
              Открыть управление магазинами
            </Link>
          </div>
        ) : loadError ? (
          <div className="rounded-3xl border border-amber-200 bg-amber-50 px-6 py-8 shadow-sm">
            <h2 className="text-lg font-semibold text-amber-950">Не удалось открыть товары</h2>
            <p className="mt-2 text-sm text-amber-800">{loadError}</p>
            <button
              type="button"
              onClick={() => void fetchProducts()}
              className="mt-4 inline-flex rounded-2xl bg-amber-600 px-4 py-3 text-sm font-medium text-white transition hover:bg-amber-700"
            >
              Повторить загрузку
            </button>
          </div>
        ) : (
          <>
        <SyncFreshnessPanel
          storeIds={selectedStoreId ? [selectedStoreId] : []}
          kinds={['products']}
          title="Каталог товаров и вариаций"
          description="Показываем время последней успешной синхронизации товаров по выбранному магазину."
        />

        <div className="mb-6 grid grid-cols-1 gap-4 md:grid-cols-2">
          <div className="rounded-2xl border border-sky-100 bg-sky-50 px-4 py-3">
            <div className="text-xs font-semibold uppercase tracking-[0.18em] text-sky-700">Активный магазин</div>
            <div className="mt-1 text-lg font-semibold text-slate-950">{selectedStore?.name || 'Не выбран'}</div>
            <div className="mt-1 text-sm text-slate-500">{viewLabel}</div>
          </div>

          <div className="relative">
            <MagnifyingGlassIcon className="absolute left-3 top-9 transform -translate-y-1/2 h-5 w-5 text-gray-400" />
            <input
              type="text"
              value={searchQuery}
              onChange={(e) => setSearchQuery(e.target.value)}
              placeholder="Поиск по названию и артикулу"
              className="pl-10 pr-4 py-2 w-full border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-primary-500 text-gray-900 bg-white"
            />
          </div>
        </div>

        <div className="mb-6 rounded-2xl border border-emerald-100 bg-emerald-50 px-4 py-3">
          <div className="flex flex-col gap-3 md:flex-row md:items-center md:justify-between">
            <div className="space-y-1 text-sm">
              <div className="font-medium text-slate-900">Себестоимость теперь ведется в отдельном разделе</div>
              <div className="text-slate-600">
                История, редактирование задним числом и пересборка закрытых месяцев перенесены на отдельную страницу.
              </div>
            </div>
            <Link
              href="/cost-history"
              className="inline-flex items-center justify-center rounded-xl border border-emerald-200 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-emerald-300 hover:bg-emerald-100/40"
            >
              Открыть себестоимость
            </Link>
          </div>
        </div>

        <div className="mb-6 flex flex-wrap items-center gap-2">
          {[
            { key: 'active', label: 'Активные', count: activeProductsCount },
            { key: 'archived', label: 'Архив', count: archivedProductsCount },
            { key: 'all', label: 'Все', count: products.length },
          ].map((option) => {
            const isSelected = catalogView === option.key;
            return (
              <button
                key={option.key}
                type="button"
                onClick={() => setCatalogView(option.key as CatalogView)}
                className={`inline-flex items-center gap-2 rounded-2xl border px-4 py-2 text-sm font-medium transition ${
                  isSelected
                    ? 'border-slate-900 bg-slate-900 text-white'
                    : 'border-slate-200 bg-white text-slate-700 hover:border-slate-300 hover:bg-slate-50'
                }`}
              >
                <span>{option.label}</span>
                <span className={`rounded-full px-2 py-0.5 text-xs ${isSelected ? 'bg-white/15 text-white' : 'bg-slate-100 text-slate-600'}`}>
                  {option.count}
                </span>
              </button>
            );
          })}
        </div>

        <div className="mb-8 grid grid-cols-1 gap-5 sm:grid-cols-2 xl:grid-cols-4">
          <div className="bg-white overflow-hidden shadow rounded-lg p-5">
            <div className="text-sm font-medium text-gray-500">Товаров в текущем списке</div>
            <div className="mt-1 text-3xl font-semibold text-gray-900">{filteredProducts.length}</div>
          </div>
          <div className="bg-white overflow-hidden shadow rounded-lg p-5">
            <div className="text-sm font-medium text-gray-500">Всего вариаций</div>
            <div className="mt-1 text-3xl font-semibold text-gray-900">{totalVariants}</div>
          </div>
          <div className="bg-white overflow-hidden shadow rounded-lg p-5">
            <div className="text-sm font-medium text-gray-500">Активных / архивных</div>
            <div className="mt-1 text-3xl font-semibold text-gray-900">{activeProductsCount} / {archivedProductsCount}</div>
          </div>
          <div className="bg-white overflow-hidden shadow rounded-lg p-5">
            <div className="text-sm font-medium text-gray-500">Себестоимость заполнена</div>
            <div className="mt-1 text-3xl font-semibold text-gray-900">{variantsWithCost}</div>
          </div>
        </div>

        <div className="space-y-6">
          {filteredProducts.map((product) => (
            <div key={product.id} className="bg-white shadow rounded-lg overflow-hidden">
              <button
                onClick={() => setExpandedProduct(expandedProduct === product.id ? null : product.id)}
                className="w-full px-6 py-4 flex items-center justify-between hover:bg-gray-50 transition-colors"
              >
                <div className="flex items-center space-x-4">
                  {product.image_url && (
                    <Image
                      src={product.image_url}
                      alt={product.name}
                      width={64}
                      height={64}
                      unoptimized
                      className="w-16 h-16 object-cover rounded"
                    />
                  )}
                  <div className="text-left">
                    <h3 className="text-lg font-medium text-gray-900">{product.name}</h3>
                    <div className="mt-1 flex items-center space-x-2">
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                        Вариаций: {product.total_variants}
                      </span>
                      <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-purple-100 text-purple-800">
                        Цветов: {product.colors.length}
                      </span>
                      {product.active_variants_count > 0 && product.archived_variants_count > 0 && (
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-amber-100 text-amber-800">
                          Есть архивные вариации
                        </span>
                      )}
                      {product.is_archived && (
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-slate-200 text-slate-700">
                          Архив OZON
                        </span>
                      )}
                    </div>
                  </div>
                </div>
                <div className="text-gray-400 text-xl">
                  {expandedProduct === product.id ? '▼' : '▶'}
                </div>
              </button>

              {expandedProduct === product.id && (
                <div className="px-6 pb-4 border-t">
                  <div className="space-y-6 mt-4">
                    <div className="rounded-2xl border border-slate-200 bg-slate-50/80 p-4">
                      <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                        <div>
                          <div className="text-sm font-semibold text-slate-900">Себестоимость и история</div>
                          <div className="mt-1 text-xs text-slate-500">
                            Редактирование себестоимости и история изменений теперь живут на отдельной странице.
                          </div>
                        </div>
                        <Link
                          href="/cost-history"
                          className="inline-flex items-center justify-center rounded-xl border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-slate-300 hover:bg-slate-100"
                        >
                          Открыть историю себестоимости
                        </Link>
                      </div>
                    </div>

                    {product.colors.map((colorGroup, idx) => (
                      <div key={idx} className="border rounded-lg overflow-hidden">
                        <div className="bg-gray-50 px-4 py-2 border-b">
                          <h4 className="font-medium text-gray-700">
                            🎨 {colorGroup.color}
                          </h4>
                        </div>

                        {/* Размеры внутри цвета */}
                        <div className="divide-y">
                          {colorGroup.sizes.map((sizeGroup, sizeIdx) => (
                            <div key={sizeIdx} className="p-3">
                              <div className="flex items-start">
                                <div className="w-24 flex-shrink-0">
                                  <span className="inline-flex items-center px-2 py-1 rounded text-sm font-medium bg-indigo-100 text-indigo-800">
                                    📏 {sizeGroup.size}
                                  </span>
                                </div>
                                <div className="flex-1 space-y-2">
                                  {sizeGroup.variants.map((variant) => (
                                    <div key={variant.id} className="flex items-center justify-between border-b border-gray-100 pb-2 last:border-0">
                                      <div className="flex-1">
                                        <div className="flex items-center gap-2 flex-wrap">
                                          <span className="text-sm font-mono text-gray-700 bg-gray-100 px-2 py-1 rounded">
                                            {getVariantDisplayTitle(variant)}
                                          </span>
                                        </div>
                                        <div className="mt-2 text-sm text-slate-500">
                                          {getVariantCharacteristicText(variant)}
                                        </div>
                                        <div className="mt-3 flex flex-wrap items-center gap-2">
                                          <span className="inline-flex rounded-full bg-slate-100 px-3 py-1 text-xs font-medium text-slate-700">
                                            Себестоимость: {typeof variant.unit_cost === 'number' ? `${variant.unit_cost} ₽` : 'не задана'}
                                          </span>
                                        </div>
                                      </div>
                                    </div>
                                  ))}
                                </div>
                              </div>
                            </div>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          ))}

          {filteredProducts.length === 0 && (
            <div className="text-center py-12 bg-white rounded-lg shadow">
              <p className="text-gray-500">
                {catalogView === 'archived'
                  ? 'Архивных товаров не найдено'
                  : 'Товары не найдены'}
              </p>
            </div>
          )}
        </div>
          </>
        )}
      </Layout>
    </ProtectedRoute>
  );
}
