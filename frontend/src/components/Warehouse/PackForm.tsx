'use client';

import { useEffect, useMemo, useState } from 'react';
import { PlusIcon, XMarkIcon } from '@heroicons/react/24/outline';
import type { AxiosError } from 'axios';
import toast from 'react-hot-toast';

import { type GroupedProduct } from '@/lib/api/products';
import { productsAPI } from '@/lib/api/products';
import { warehouseAPI } from '@/lib/api/warehouse';

interface PackFormProps {
  storeId?: number;
  warehouseId?: number;
  onClose: () => void;
  onSuccess: () => void;
}

interface PackProductLine {
  key: string;
  variantId: number;
  color: string;
  size: string;
  offerId: string;
  packSize: number;
}

interface PackBlock {
  id: number;
  productId: number | null;
  boxes: Record<string, number>;
}

function getActiveGroupedProducts(products: GroupedProduct[]): GroupedProduct[] {
  return products
    .filter((product) => !product.is_archived)
    .map((product) => ({
      ...product,
      colors: product.colors
        .map((colorGroup) => ({
          ...colorGroup,
          sizes: colorGroup.sizes
            .map((sizeGroup) => ({
              ...sizeGroup,
              variants: sizeGroup.variants.filter((variant) => !variant.is_archived),
            }))
            .filter((sizeGroup) => sizeGroup.variants.length > 0),
        }))
        .filter((colorGroup) => colorGroup.sizes.length > 0),
    }))
    .filter((product) => product.colors.length > 0);
}

function getErrorMessage(error: unknown, fallback: string) {
  const typedError = error as AxiosError<{ detail?: string | Array<{ msg?: string }> }>;

  if (!typedError.response?.data?.detail) {
    return fallback;
  }

  if (typeof typedError.response.data.detail === 'string') {
    return typedError.response.data.detail;
  }

  if (Array.isArray(typedError.response.data.detail)) {
    return typedError.response.data.detail.map((item) => item.msg).filter(Boolean).join(', ') || fallback;
  }

  return fallback;
}

function createBlock(id: number): PackBlock {
  return {
    id,
    productId: null,
    boxes: {},
  };
}

function getProductPackLines(product: GroupedProduct | undefined): PackProductLine[] {
  if (!product) {
    return [];
  }

  return product.colors.flatMap((colorGroup) =>
    colorGroup.sizes.flatMap((sizeGroup) =>
      sizeGroup.variants.map((variant) => ({
        key: `${colorGroup.color}::${sizeGroup.size || 'Без размера'}::${variant.id}`,
        variantId: variant.id,
        color: colorGroup.color,
        size: sizeGroup.size || 'Без размера',
        offerId: variant.offer_id,
        packSize: variant.pack_size,
      }))
    )
  );
}

export function PackForm({ storeId, warehouseId, onClose, onSuccess }: PackFormProps) {
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [loadingProducts, setLoadingProducts] = useState(true);
  const [products, setProducts] = useState<GroupedProduct[]>([]);
  const [blocks, setBlocks] = useState<PackBlock[]>([createBlock(1)]);

  useEffect(() => {
    const loadProducts = async () => {
      if (!storeId) {
        setProducts([]);
        setLoadingProducts(false);
        return;
      }

      setLoadingProducts(true);
      try {
        const data = await productsAPI.getGrouped(storeId);
        setProducts(getActiveGroupedProducts(data));
      } catch {
        toast.error('Не удалось загрузить товары магазина');
      } finally {
        setLoadingProducts(false);
      }
    };

    void loadProducts();
  }, [storeId]);

  const canSubmit = useMemo(() => {
    return blocks.some((block) => Object.values(block.boxes).some((boxes) => boxes > 0));
  }, [blocks]);

  const updateBlockProduct = (blockId: number, productId: number | null) => {
    const product = products.find((item) => item.id === productId);
    const nextBoxes = Object.fromEntries(
      getProductPackLines(product).map((line) => [line.key, 0])
    );

    setBlocks((prev) => prev.map((block) => (
      block.id === blockId
        ? { ...block, productId, boxes: nextBoxes }
        : block
    )));
  };

  const updateBoxes = (blockId: number, key: string, boxes: number) => {
    setBlocks((prev) => prev.map((block) => (
      block.id === blockId
        ? { ...block, boxes: { ...block.boxes, [key]: Math.max(0, boxes) } }
        : block
    )));
  };

  const addBlock = () => {
    setBlocks((prev) => [...prev, createBlock(Date.now())]);
  };

  const removeBlock = (blockId: number) => {
    setBlocks((prev) => prev.filter((block) => block.id !== blockId));
  };

  const onSubmit = async () => {
    const items = blocks.flatMap((block) => {
      const product = products.find((item) => item.id === block.productId);
      const lines = getProductPackLines(product);

      return lines
        .map((line) => ({
          variant_id: line.variantId,
          boxes: block.boxes[line.key] || 0,
        }))
        .filter((line) => line.boxes > 0);
    });

    if (items.length === 0) {
      toast.error('Заполни хотя бы одну строку упаковки');
      return;
    }

    setIsSubmitting(true);
    try {
      await warehouseAPI.packBatch({
        store_id: storeId,
        warehouse_id: warehouseId,
        items,
      });

      toast.success('Упаковка выполнена успешно');
      onSuccess();
      onClose();
    } catch (error: unknown) {
      toast.error(getErrorMessage(error, 'Ошибка при упаковке'));
    } finally {
      setIsSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-slate-950/60 p-4 backdrop-blur-sm">
      <div className="max-h-[92vh] w-full max-w-5xl overflow-y-auto rounded-[30px] bg-white shadow-2xl">
        <div className="flex items-center justify-between border-b border-slate-200 px-6 py-5">
          <div>
            <h2 className="text-xl font-semibold text-slate-950">Упаковка товара</h2>
            <p className="mt-1 text-sm text-slate-500">Выбирай товар и пачкой заполняй нужные вариации. Упаковка ведётся уже по конкретным артикулам.</p>
          </div>
          <button onClick={onClose} className="text-slate-400 transition hover:text-slate-600">
            <XMarkIcon className="h-6 w-6" />
          </button>
        </div>

        <div className="space-y-4 p-6">
          {!storeId ? (
            <div className="rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-4 py-6 text-sm text-slate-500">
              Сначала выбери активный магазин в левом меню.
            </div>
          ) : loadingProducts ? (
            <div className="rounded-2xl bg-slate-100 px-4 py-6 text-sm text-slate-500">Загружаем каталог магазина...</div>
          ) : (
            <>
              {blocks.map((block, index) => {
                const selectedProduct = products.find((product) => product.id === block.productId);
                const lines = getProductPackLines(selectedProduct);

                return (
                  <div key={block.id} className="rounded-[26px] border border-slate-200 bg-slate-50/70 p-4">
                    <div className="flex items-start justify-between gap-3">
                      <div className="flex-1">
                        <label className="block text-sm font-medium text-slate-700">Товар</label>
                        <select
                          value={block.productId ?? ''}
                          onChange={(event) => updateBlockProduct(block.id, event.target.value ? Number(event.target.value) : null)}
                          className="mt-2 block w-full rounded-2xl border border-slate-300 bg-white px-4 py-3 text-sm text-slate-900 shadow-sm outline-none transition focus:border-sky-400"
                        >
                          <option value="">Выбери товар</option>
                          {products.map((product) => (
                            <option key={product.id} value={product.id}>
                              {product.name} · {product.total_variants} вариаций
                            </option>
                          ))}
                        </select>
                      </div>

                      {index > 0 && (
                        <button
                          type="button"
                          onClick={() => removeBlock(block.id)}
                          className="inline-flex h-11 w-11 items-center justify-center rounded-2xl border border-rose-200 bg-rose-50 text-rose-600 transition hover:bg-rose-100"
                          title="Удалить товар из упаковки"
                        >
                          <XMarkIcon className="h-5 w-5" />
                        </button>
                      )}
                    </div>

                    {selectedProduct && (
                      <div className="mt-4 overflow-hidden rounded-2xl border border-slate-200 bg-white">
                        <div className="overflow-hidden">
                          <table className="min-w-full table-fixed border-collapse">
                            <colgroup>
                              <col className="w-[18%]" />
                              <col className="w-[16%]" />
                              <col className="w-[22%]" />
                              <col className="w-[14%]" />
                              <col className="w-[30%]" />
                            </colgroup>
                            <thead>
                              <tr className="bg-slate-50 text-xs font-semibold uppercase tracking-[0.14em] text-slate-500 [&>th:not(:last-child)]:border-r [&>th:not(:last-child)]:border-slate-200">
                                <th className="border-b border-slate-200 px-4 py-3 text-left">Цвет</th>
                                <th className="border-b border-slate-200 px-4 py-3 text-left">Размер</th>
                                <th className="border-b border-slate-200 px-4 py-3 text-left">Артикул</th>
                                <th className="border-b border-slate-200 px-4 py-3 text-left">Упаковка</th>
                                <th className="border-b border-slate-200 px-4 py-3 text-right">Коробки</th>
                              </tr>
                            </thead>
                            <tbody>
                              {selectedProduct.colors.map((colorGroup) =>
                                colorGroup.sizes.flatMap((sizeGroup, sizeIndex) =>
                                  sizeGroup.variants.map((line, variantIndex) => {
                                    const key = `${colorGroup.color}::${sizeGroup.size || 'Без размера'}::${line.id}`;
                                    const colorRowSpan = colorGroup.sizes.reduce((sum, currentSizeGroup) => sum + currentSizeGroup.variants.length, 0);
                                    const sizeRowSpan = sizeGroup.variants.length;

                                    return (
                                      <tr key={key} className="text-sm text-slate-700 [&>td:not(:last-child)]:border-r [&>td:not(:last-child)]:border-slate-100">
                                        {sizeIndex === 0 && variantIndex === 0 && (
                                          <td rowSpan={colorRowSpan} className="align-middle border-b border-slate-100 px-4 py-3 font-medium text-slate-900">
                                            {colorGroup.color}
                                          </td>
                                        )}
                                        {variantIndex === 0 && (
                                          <td rowSpan={sizeRowSpan} className="align-middle border-b border-slate-100 px-4 py-3">
                                            {sizeGroup.size || 'Без размера'}
                                          </td>
                                        )}
                                        <td className="border-b border-slate-100 px-4 py-3 font-mono text-slate-900">{line.offer_id}</td>
                                        <td className="border-b border-slate-100 px-4 py-3 text-slate-500">{line.pack_size} шт</td>
                                        <td className="border-b border-slate-100 px-4 py-3">
                                          <input
                                            type="number"
                                            min={0}
                                            step={1}
                                            value={block.boxes[key] ?? 0}
                                            onChange={(event) => updateBoxes(block.id, key, Number(event.target.value) || 0)}
                                            className="w-full rounded-xl border border-slate-300 px-3 py-2 text-right text-sm text-slate-900 outline-none transition focus:border-sky-400"
                                          />
                                        </td>
                                      </tr>
                                    );
                                  })
                                )
                              )}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    )}
                  </div>
                );
              })}

              <button
                type="button"
                onClick={addBlock}
                className="inline-flex items-center gap-2 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm font-medium text-slate-700 transition hover:bg-slate-100"
              >
                <PlusIcon className="h-5 w-5" />
                Добавить ещё товар
              </button>
            </>
          )}
        </div>

        <div className="flex justify-end gap-3 border-t border-slate-200 px-6 py-5">
          <button
            type="button"
            onClick={onClose}
            className="rounded-2xl border border-slate-300 px-4 py-3 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
          >
            Отмена
          </button>
          <button
            type="button"
            onClick={onSubmit}
            disabled={isSubmitting || loadingProducts || !storeId || !canSubmit}
            className="rounded-2xl bg-sky-600 px-4 py-3 text-sm font-medium text-white transition hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {isSubmitting ? 'Упаковываем...' : 'Упаковать'}
          </button>
        </div>
      </div>
    </div>
  );
}
