'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { Layout } from '@/components/Layout';
import { closedMonthsAPI, type ClosedMonthFinance } from '@/lib/api/closedMonths';
import { economicsHistoryAPI, type VariantCostHistoryEntry } from '@/lib/api/economicsHistory';
import { productsAPI } from '@/lib/api/products';
import { useStoreContext } from '@/lib/context/StoreContext';

function formatDate(value: string) {
  if (!value) {
    return '—';
  }
  const parsed = new Date(`${value}T00:00:00`);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat('ru-RU').format(parsed);
}

function formatCurrency(value: number | null | undefined) {
  if (typeof value !== 'number') {
    return 'Не задана';
  }
  return new Intl.NumberFormat('ru-RU', {
    style: 'currency',
    currency: 'RUB',
    maximumFractionDigits: 0,
  }).format(value);
}

function variantLabel(item: VariantCostHistoryEntry) {
  const bits = [item.offer_id];
  if (item.color) bits.push(item.color);
  if (item.size) bits.push(item.size);
  if (item.pack_size) bits.push(`${item.pack_size} шт.`);
  return bits.join(' · ');
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function parseDraftCost(value: string | null | undefined) {
  const trimmed = (value || '').trim();
  if (!trimmed) {
    return { valid: true, value: null as number | null };
  }
  const parsed = Number(trimmed);
  if (!Number.isFinite(parsed) || parsed < 0) {
    return { valid: false, value: null as number | null };
  }
  return { valid: true, value: parsed };
}

function monthStartIsoDate(month: string | null | undefined) {
  if (!month || !/^\d{4}-\d{2}$/.test(month)) {
    return null;
  }
  return `${month}-01`;
}

function currentMonthStartIsoDate() {
  return `${todayIsoDate().slice(0, 7)}-01`;
}

function suggestedMissingCostEffectiveFrom(months: ClosedMonthFinance[]) {
  const monthsNeedingCost = months
    .filter((item) => Number(item.coverage_ratio || 0) < 0.9999)
    .sort((a, b) => a.month.localeCompare(b.month, 'ru'));
  return monthStartIsoDate(monthsNeedingCost[0]?.month) || currentMonthStartIsoDate();
}

const MISSING_VARIANTS_PAGE_SIZE = 6;

function normalizeForCompare(value: string | null | undefined) {
  return (value || '').trim().toLowerCase();
}

function sizeSortValue(value: string | null | undefined) {
  const text = normalizeForCompare(value);
  if (!text) {
    return { primary: Number.POSITIVE_INFINITY, secondary: Number.POSITIVE_INFINITY, raw: '' };
  }
  const matches = text.match(/\d+/g);
  if (!matches || matches.length === 0) {
    return { primary: Number.POSITIVE_INFINITY, secondary: Number.POSITIVE_INFINITY, raw: text };
  }
  return {
    primary: Number(matches[0]),
    secondary: matches[1] ? Number(matches[1]) : Number(matches[0]),
    raw: text,
  };
}

function compareOptionalText(a: string | null | undefined, b: string | null | undefined) {
  return normalizeForCompare(a).localeCompare(normalizeForCompare(b), 'ru');
}

function compareSizes(a: string | null | undefined, b: string | null | undefined) {
  const left = sizeSortValue(a);
  const right = sizeSortValue(b);
  if (left.primary !== right.primary) {
    return left.primary - right.primary;
  }
  if (left.secondary !== right.secondary) {
    return left.secondary - right.secondary;
  }
  return left.raw.localeCompare(right.raw, 'ru');
}

type VariantHistoryGroup = {
  key: string;
  product_name: string;
  variant_id: number;
  offer_id: string;
  color?: string | null;
  size?: string | null;
  pack_size: number;
  is_archived: boolean;
  latest: VariantCostHistoryEntry;
  items: VariantCostHistoryEntry[];
  activeItems: VariantCostHistoryEntry[];
  pendingDeleteCount: number;
  allPendingDelete: boolean;
};

type ProductHistoryGroup = {
  key: string;
  product_id: number;
  product_name: string;
  variants: VariantHistoryGroup[];
};

type MissingCostVariant = {
  variant_id: number;
  product_id: number;
  product_name: string;
  offer_id: string;
  color?: string | null;
  size?: string | null;
  pack_size: number;
  is_archived: boolean;
};

type MissingCostProductGroup = {
  key: string;
  product_id: number;
  product_name: string;
  showPackSize: boolean;
  items: MissingCostVariant[];
};

export default function CostHistoryPage() {
  const { selectedStore, selectedStoreId, stores, isLoading: storesLoading } = useStoreContext();
  const [history, setHistory] = useState<VariantCostHistoryEntry[]>([]);
  const [catalogMissingCostVariants, setCatalogMissingCostVariants] = useState<MissingCostVariant[]>([]);
  const [loading, setLoading] = useState(true);
  const [hasLoadedOnce, setHasLoadedOnce] = useState(false);
  const [search, setSearch] = useState('');
  const [expandedProductKey, setExpandedProductKey] = useState<string | null>(null);
  const [expandedVariantKey, setExpandedVariantKey] = useState<string | null>(null);
  const [costDrafts, setCostDrafts] = useState<Record<number, string>>({});
  const [effectiveDrafts, setEffectiveDrafts] = useState<Record<number, string>>({});
  const [savingGroupKey, setSavingGroupKey] = useState<string | null>(null);
  const [pendingDeletedHistoryIds, setPendingDeletedHistoryIds] = useState<Record<number, true>>({});
  const [missingPage, setMissingPage] = useState(1);
  const [missingCostDefaultDate, setMissingCostDefaultDate] = useState(currentMonthStartIsoDate());

  const loadHistory = useCallback(async () => {
    if (!selectedStoreId) {
      setHistory([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      const [data, groupedProducts] = await Promise.all([
        economicsHistoryAPI.getVariantCostHistory(selectedStoreId),
        productsAPI.getGrouped(selectedStoreId),
      ]);
      const months = await closedMonthsAPI.list(selectedStoreId, 24).catch(() => [] as ClosedMonthFinance[]);
      setHistory(data);
      setMissingCostDefaultDate(suggestedMissingCostEffectiveFrom(months));
      const missingVariants: MissingCostVariant[] = [];
      for (const product of groupedProducts) {
        for (const colorGroup of product.colors) {
          for (const sizeGroup of colorGroup.sizes) {
            for (const variant of sizeGroup.variants) {
              if (variant.is_archived || typeof variant.unit_cost === 'number') {
                continue;
              }
              missingVariants.push({
                variant_id: variant.id,
                product_id: product.id,
                product_name: product.name,
                offer_id: variant.offer_id,
                color: colorGroup.color,
                size: sizeGroup.size,
                pack_size: variant.pack_size,
                is_archived: variant.is_archived,
              });
            }
          }
        }
      }
      missingVariants.sort((a, b) => a.product_name.localeCompare(b.product_name, 'ru') || a.offer_id.localeCompare(b.offer_id, 'ru'));
      setCatalogMissingCostVariants(missingVariants);
    } catch {
      setHistory([]);
      setCatalogMissingCostVariants([]);
      toast.error('Не удалось загрузить историю себестоимости');
    } finally {
      setLoading(false);
      setHasLoadedOnce(true);
    }
  }, [selectedStoreId]);

  useEffect(() => {
    void loadHistory();
  }, [loadHistory]);

  const grouped = useMemo<ProductHistoryGroup[]>(() => {
    const map = new Map<string, VariantHistoryGroup>();
    for (const item of history) {
      const key = `${item.variant_id}`;
      if (!map.has(key)) {
        const isPendingDelete = Boolean(pendingDeletedHistoryIds[item.id]);
        map.set(key, {
          key,
          product_name: item.product_name,
          variant_id: item.variant_id,
          offer_id: item.offer_id,
          color: item.color,
          size: item.size,
          pack_size: item.pack_size,
          is_archived: item.is_archived,
          latest: item,
          items: [item],
          activeItems: isPendingDelete ? [] : [item],
          pendingDeleteCount: isPendingDelete ? 1 : 0,
          allPendingDelete: isPendingDelete,
        });
        continue;
      }
      const group = map.get(key)!;
      group.items.push(item);
      if (pendingDeletedHistoryIds[item.id]) {
        group.pendingDeleteCount += 1;
      } else {
        group.activeItems.push(item);
      }
    }
    const byProduct = new Map<string, ProductHistoryGroup>();
    for (const variantGroup of Array.from(map.values())) {
      variantGroup.allPendingDelete = variantGroup.activeItems.length === 0;
      variantGroup.latest = variantGroup.activeItems[0] ?? variantGroup.items[0];
      const productKey = String(variantGroup.latest.product_id);
      if (!byProduct.has(productKey)) {
        byProduct.set(productKey, {
          key: productKey,
          product_id: variantGroup.latest.product_id,
          product_name: variantGroup.product_name,
          variants: [],
        });
      }
      byProduct.get(productKey)!.variants.push(variantGroup);
    }
    return Array.from(byProduct.values()).map((productGroup) => ({
      ...productGroup,
      variants: productGroup.variants.sort((a, b) => a.offer_id.localeCompare(b.offer_id, 'ru')),
    }));
  }, [history, pendingDeletedHistoryIds]);

  const filtered = useMemo(() => {
    const query = search.trim().toLowerCase();
    if (!query) {
      return grouped;
    }
      return grouped
        .map((productGroup) => ({
          ...productGroup,
          variants: productGroup.variants.filter((item) => {
            const text = [
              item.product_name,
              item.offer_id,
              item.color || '',
              item.size || '',
              String(item.pack_size || ''),
            ]
              .join(' ')
              .toLowerCase();
            return text.includes(query);
          }),
        }))
        .filter((productGroup) => productGroup.variants.length > 0);
  }, [grouped, search]);

  const latestChangedAt = history[0]?.effective_from ?? null;
  const missingCostCount = catalogMissingCostVariants.length;
  const missingGroupedProducts = useMemo<MissingCostProductGroup[]>(() => {
    const groupedMap = new Map<number, MissingCostVariant[]>();
    for (const item of catalogMissingCostVariants) {
      const key = item.product_id;
      if (!groupedMap.has(key)) {
        groupedMap.set(key, []);
      }
      groupedMap.get(key)!.push(item);
    }

    return Array.from(groupedMap.entries())
      .map(([product_id, items]) => {
        const sortedItems = [...items].sort((a, b) => {
          const colorCompare = compareOptionalText(a.color, b.color);
          if (colorCompare !== 0) {
            return colorCompare;
          }
          const sizeCompare = compareSizes(a.size, b.size);
          if (sizeCompare !== 0) {
            return sizeCompare;
          }
          if (a.pack_size !== b.pack_size) {
            return a.pack_size - b.pack_size;
          }
          return a.offer_id.localeCompare(b.offer_id, 'ru');
        });

        return {
          key: String(product_id),
          product_id,
          product_name: sortedItems[0]?.product_name || '',
          showPackSize: sortedItems.some((item) => item.pack_size !== 1),
          items: sortedItems,
        };
      })
      .sort((a, b) => a.product_name.localeCompare(b.product_name, 'ru'));
  }, [catalogMissingCostVariants]);
  const missingPagesCount = Math.max(1, Math.ceil(missingGroupedProducts.length / MISSING_VARIANTS_PAGE_SIZE));
  const pagedMissingGroups = useMemo(() => {
    const startIndex = (missingPage - 1) * MISSING_VARIANTS_PAGE_SIZE;
    return missingGroupedProducts.slice(startIndex, startIndex + MISSING_VARIANTS_PAGE_SIZE);
  }, [missingGroupedProducts, missingPage]);
  const missingRangeStart = missingGroupedProducts.length === 0 ? 0 : (missingPage - 1) * MISSING_VARIANTS_PAGE_SIZE + 1;
  const missingRangeEnd = Math.min(missingPage * MISSING_VARIANTS_PAGE_SIZE, missingGroupedProducts.length);

  useEffect(() => {
    setMissingPage((current) => Math.min(current, missingPagesCount));
  }, [missingPagesCount]);

  useEffect(() => {
    setMissingPage(1);
  }, [selectedStoreId]);

  useEffect(() => {
    setCostDrafts((current) => {
      const next = { ...current };
      for (const productGroup of grouped) {
        for (const item of productGroup.variants) {
          next[item.variant_id] =
          current[item.variant_id] ??
          (typeof item.latest.unit_cost === 'number' ? String(item.latest.unit_cost) : '');
        }
      }
      return next;
    });
    setEffectiveDrafts((current) => {
      const next = { ...current };
      for (const productGroup of grouped) {
        for (const item of productGroup.variants) {
          next[item.variant_id] = current[item.variant_id] ?? item.latest.effective_from ?? todayIsoDate();
        }
      }
      return next;
    });
  }, [grouped]);

  useEffect(() => {
    setEffectiveDrafts((current) => {
      const next = { ...current };
      for (const item of catalogMissingCostVariants) {
        next[item.variant_id] = current[item.variant_id] ?? missingCostDefaultDate;
      }
      return next;
    });
  }, [catalogMissingCostVariants, missingCostDefaultDate]);

  useEffect(() => {
    setPendingDeletedHistoryIds({});
  }, [selectedStoreId]);

  const collectMissingGroupItems = useCallback((group: MissingCostProductGroup) => {
    const items = group.items.flatMap((item) => {
      const rawValue = costDrafts[item.variant_id];
      const parsed = parseDraftCost(rawValue);
      if (!parsed.valid) {
        throw new Error(`Проверь себестоимость у вариации ${item.offer_id}`);
      }
      if ((rawValue || '').trim() === '') {
        return [];
      }
      return [{
        variant_id: item.variant_id,
        unit_cost: parsed.value,
        effective_from: effectiveDrafts[item.variant_id] || missingCostDefaultDate,
      }];
    });
    return items;
  }, [costDrafts, effectiveDrafts, missingCostDefaultDate]);

  const collectHistoryProductItems = useCallback((productGroup: ProductHistoryGroup) => {
    const items = productGroup.variants.flatMap((group) => {
      const parsed = parseDraftCost(costDrafts[group.variant_id]);
      if (!parsed.valid) {
        throw new Error(`Проверь себестоимость у вариации ${group.offer_id}`);
      }
      const effectiveFrom = effectiveDrafts[group.variant_id] || group.latest.effective_from || todayIsoDate();
      const latestUnitCost = group.activeItems[0]?.unit_cost ?? null;
      const latestEffectiveFrom = group.activeItems[0]?.effective_from || effectiveFrom;
      if (parsed.value == null && latestUnitCost == null) {
        return [];
      }
      const hasChanges =
        parsed.value !== latestUnitCost ||
        (parsed.value !== null && effectiveFrom !== latestEffectiveFrom);
      if (!hasChanges) {
        return [];
      }
      return [{
        variant_id: group.variant_id,
        unit_cost: parsed.value,
        effective_from: effectiveFrom,
      }];
    });
    return items;
  }, [costDrafts, effectiveDrafts]);

  const collectHistoryDeleteIds = useCallback((productGroup: ProductHistoryGroup) => {
    return productGroup.variants.flatMap((group) =>
      group.items
        .filter((item) => pendingDeletedHistoryIds[item.id])
        .map((item) => item.id)
    );
  }, [pendingDeletedHistoryIds]);

  const saveBatch = useCallback(async (
    groupKey: string,
    items: Array<{ variant_id: number; unit_cost: number | null; effective_from?: string | null }>,
    deleteHistoryIds: number[],
    successMessage: string,
  ) => {
    if (items.length === 0 && deleteHistoryIds.length === 0) {
      toast.error('Сначала внеси изменения хотя бы в одну вариацию');
      return;
    }

    setSavingGroupKey(groupKey);
    try {
      await productsAPI.updateVariantCostsBatch(items, deleteHistoryIds);
      await loadHistory();
      setPendingDeletedHistoryIds((current) => {
        if (deleteHistoryIds.length === 0) {
          return current;
        }
        const next = { ...current };
        for (const historyId of deleteHistoryIds) {
          delete next[historyId];
        }
        return next;
      });
      toast.success(successMessage);
    } catch (error) {
      const message = error instanceof Error ? error.message : '';
      toast.error(message || 'Не удалось сохранить изменения себестоимости');
    } finally {
      setSavingGroupKey(null);
    }
  }, [loadHistory]);

  const saveMissingProductGroup = async (group: MissingCostProductGroup) => {
    try {
      const items = collectMissingGroupItems(group);
      await saveBatch(
        `missing-${group.key}`,
        items,
        [],
        'Себестоимость сохранена пачкой по товару. Закрытые месяцы пересчитаются автоматически одним общим запуском.',
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Не удалось подготовить изменения';
      toast.error(message);
    }
  };

  const saveHistoryProductGroup = async (productGroup: ProductHistoryGroup) => {
    try {
      const items = collectHistoryProductItems(productGroup);
      const deleteHistoryIds = collectHistoryDeleteIds(productGroup);
      await saveBatch(
        `history-${productGroup.key}`,
        items,
        deleteHistoryIds,
        deleteHistoryIds.length > 0 && items.length === 0
          ? 'Удаления сохранены. Закрытые месяцы пересчитаются автоматически одним общим запуском.'
          : 'Изменения по товару сохранены. Закрытые месяцы пересчитаются автоматически одним общим запуском.',
      );
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Не удалось подготовить изменения';
      toast.error(message);
    }
  };

  const deleteHistoryEntry = async (group: VariantHistoryGroup, historyId: number) => {
    const isLastEntry = group.items.length <= 1;
    const confirmMessage = isLastEntry
      ? 'Удалить последнюю запись себестоимости? У вариации больше не будет задана себестоимость, а затронутые месяцы пересчитаются автоматически.'
      : 'Убрать эту запись из истории? Изменение применится после кнопки сохранения по товару.';
    if (!window.confirm(confirmMessage)) {
      return;
    }
    const deletedEntry = group.items.find((item) => item.id === historyId) ?? null;
    const remainingActiveItems = group.items.filter(
      (item) => item.id !== historyId && !pendingDeletedHistoryIds[item.id]
    );
    const nextLatest = remainingActiveItems[0] ?? null;

    setPendingDeletedHistoryIds((current) => ({ ...current, [historyId]: true }));
    if (deletedEntry) {
      setCostDrafts((current) => {
        const currentDraft = current[group.variant_id] ?? '';
        const deletedValue = deletedEntry.unit_cost != null ? String(deletedEntry.unit_cost) : '';
        if (currentDraft !== deletedValue) {
          return current;
        }
        return {
          ...current,
          [group.variant_id]: nextLatest?.unit_cost != null ? String(nextLatest.unit_cost) : '',
        };
      });
      setEffectiveDrafts((current) => {
        const currentDraft = current[group.variant_id] ?? deletedEntry.effective_from ?? todayIsoDate();
        if (currentDraft !== deletedEntry.effective_from) {
          return current;
        }
        return {
          ...current,
          [group.variant_id]: nextLatest?.effective_from ?? todayIsoDate(),
        };
      });
    }
    toast('Запись помечена на удаление. Нажми «Сохранить изменения по товару».');
  };

  const restoreHistoryEntry = (group: VariantHistoryGroup, historyId: number) => {
    const restoredEntry = group.items.find((item) => item.id === historyId) ?? null;
    setPendingDeletedHistoryIds((current) => {
      const next = { ...current };
      delete next[historyId];
      return next;
    });
    if (restoredEntry) {
      setCostDrafts((current) => {
        const currentDraft = current[group.variant_id] ?? '';
        const activeValue = group.activeItems[0]?.unit_cost != null ? String(group.activeItems[0].unit_cost) : '';
        if (currentDraft !== activeValue) {
          return current;
        }
        return {
          ...current,
          [group.variant_id]: restoredEntry.unit_cost != null ? String(restoredEntry.unit_cost) : '',
        };
      });
      setEffectiveDrafts((current) => {
        const currentDraft = current[group.variant_id] ?? group.activeItems[0]?.effective_from ?? todayIsoDate();
        const activeDate = group.activeItems[0]?.effective_from ?? todayIsoDate();
        if (currentDraft !== activeDate) {
          return current;
        }
        return {
          ...current,
          [group.variant_id]: restoredEntry.effective_from ?? todayIsoDate(),
        };
      });
    }
  };

  if ((!hasLoadedOnce && loading) || storesLoading) {
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
        <div className="space-y-6">
          <div className="rounded-[24px] border border-slate-200 bg-white px-5 py-4 shadow-sm">
            <div className="flex flex-col gap-3 lg:flex-row lg:items-end lg:justify-between">
              <div>
                <div className="text-xs font-semibold uppercase tracking-[0.22em] text-emerald-700">Себестоимость</div>
                <h1 className="mt-2 text-2xl font-semibold text-slate-950">Себестоимость по активному магазину</h1>
                <p className="mt-1 max-w-3xl text-sm text-slate-600">История, быстрые правки и новые вариации без себестоимости.</p>
              </div>
              <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-2 text-sm text-slate-700">
                <div>Активный магазин: <span className="font-semibold text-slate-950">{selectedStore?.name || 'Не выбран'}</span></div>
              </div>
            </div>
          </div>

          {stores.length === 0 || !selectedStore ? (
            <div className="rounded-[28px] border border-dashed border-slate-300 bg-white px-6 py-12 text-center shadow-sm">
              <h2 className="text-lg font-semibold text-slate-900">Сначала подключите магазин</h2>
              <p className="mt-2 text-sm text-slate-500">После подключения магазина здесь появится история себестоимости вариаций.</p>
              <Link href="/stores" className="mt-5 inline-flex rounded-2xl bg-slate-950 px-4 py-3 text-sm font-medium text-white transition hover:bg-slate-800">
                Открыть магазины
              </Link>
            </div>
          ) : (
            <>
              <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                <div className="rounded-2xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
                  <div className="text-sm font-medium text-slate-500">Товаров с историей</div>
                  <div className="mt-1 text-2xl font-semibold text-slate-950">{grouped.length}</div>
                </div>
                <div className="rounded-2xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
                  <div className="text-sm font-medium text-slate-500">Всего записей истории</div>
                  <div className="mt-1 text-2xl font-semibold text-slate-950">{history.length}</div>
                </div>
                <div className="rounded-2xl border border-slate-200 bg-white px-4 py-3 shadow-sm">
                  <div className="text-sm font-medium text-slate-500">Последнее изменение</div>
                  <div className="mt-1 text-lg font-semibold text-slate-950">{latestChangedAt ? formatDate(latestChangedAt) : '—'}</div>
                </div>
                <div className={`rounded-2xl border px-4 py-3 shadow-sm ${missingCostCount > 0 ? 'border-amber-200 bg-amber-50' : 'border-slate-200 bg-white'}`}>
                  <div className="text-sm font-medium text-slate-500">Без себестоимости</div>
                  <div className="mt-1 text-2xl font-semibold text-slate-950">{missingCostCount}</div>
                  <div className="mt-0.5 text-sm text-slate-500">
                    {missingCostCount > 0
                      ? 'Нужно заполнить'
                      : 'Все заполнено'}
                  </div>
                </div>
              </div>

              {catalogMissingCostVariants.length > 0 ? (
                <div className="rounded-[24px] border border-amber-200 bg-amber-50/70 px-4 py-4 shadow-sm">
                  <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                    <div>
                      <h2 className="text-lg font-semibold text-slate-950">Без себестоимости</h2>
                      <p className="mt-1 text-sm text-slate-600">
                        Показываем только незаполненные вариации. После сохранения запись сразу исчезает из этого списка.
                      </p>
                      <p className="mt-1 text-xs text-slate-500">
                        Дата по умолчанию подставляется автоматически так, чтобы закрытые месяцы не оставались без покрытия.
                      </p>
                    </div>
                    <div className="flex flex-wrap items-center gap-2 text-sm text-slate-700">
                      <span className="rounded-2xl border border-white/80 bg-white px-3 py-2">
                        Всего: <span className="font-semibold text-slate-950">{missingCostCount}</span>
                      </span>
                      <span className="rounded-2xl border border-white/80 bg-white px-3 py-2">
                        Показаны: <span className="font-semibold text-slate-950">{missingRangeStart}-{missingRangeEnd}</span>
                      </span>
                    </div>
                  </div>

                  <div className="mt-4 space-y-3">
                    {pagedMissingGroups.map((group) => (
                      <div key={`missing-group-${group.key}`} className="overflow-hidden rounded-2xl border border-white/90 bg-white shadow-sm">
                        <div className="border-b border-slate-100 px-3.5 py-3">
                          <div className="flex flex-col gap-1.5 sm:flex-row sm:items-center sm:justify-between">
                            <div>
                              <div className="text-sm font-semibold text-slate-950">{group.product_name}</div>
                              <div className="mt-0.5 text-xs text-slate-500">
                                Вариаций без себестоимости: <span className="font-semibold text-slate-700">{group.items.length}</span>
                              </div>
                            </div>
                            <button
                              type="button"
                              onClick={() => void saveMissingProductGroup(group)}
                              disabled={savingGroupKey === `missing-${group.key}`}
                              className="inline-flex items-center justify-center rounded-xl bg-slate-950 px-3.5 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                            >
                              {savingGroupKey === `missing-${group.key}` ? 'Сохраняем...' : 'Сохранить товар'}
                            </button>
                          </div>
                        </div>

                        <div className="overflow-x-auto">
                          <table className="min-w-full text-sm">
                            <thead className="bg-slate-50">
                              <tr className="text-left text-[11px] uppercase tracking-[0.12em] text-slate-500">
                                <th className="px-3.5 py-2 font-medium">Артикул</th>
                                {group.items.some((item) => item.color) ? (
                                  <th className="px-3.5 py-2 font-medium">Цвет</th>
                                ) : null}
                                {group.items.some((item) => item.size) ? (
                                  <th className="px-3.5 py-2 font-medium">Размер</th>
                                ) : null}
                                {group.showPackSize ? (
                                  <th className="px-3.5 py-2 font-medium">Уп.</th>
                                ) : null}
                                <th className="px-3.5 py-2 font-medium">Себестоимость</th>
                                <th className="px-3.5 py-2 font-medium">Действует с</th>
                              </tr>
                            </thead>
                            <tbody>
                              {group.items.map((item, index) => (
                                <tr key={`missing-${item.variant_id}`} className={index === 0 ? '' : 'border-t border-slate-100'}>
                                  <td className="px-3.5 py-2.5 font-medium text-slate-900">{item.offer_id}</td>
                                  {group.items.some((row) => row.color) ? (
                                    <td className="px-3.5 py-2.5 text-slate-600">{item.color || '—'}</td>
                                  ) : null}
                                  {group.items.some((row) => row.size) ? (
                                    <td className="px-3.5 py-2.5 text-slate-600">{item.size || '—'}</td>
                                  ) : null}
                                  {group.showPackSize ? (
                                    <td className="px-3.5 py-2.5 text-slate-600">{item.pack_size}</td>
                                  ) : null}
                                  <td className="px-3.5 py-2.5">
                                    <input
                                      type="number"
                                      min="0"
                                      step="0.01"
                                      value={costDrafts[item.variant_id] ?? ''}
                                      onChange={(event) =>
                                        setCostDrafts((current) => ({
                                          ...current,
                                          [item.variant_id]: event.target.value,
                                        }))
                                      }
                                      placeholder="Сумма"
                                      className="block w-28 rounded-xl border border-slate-300 px-3 py-2 text-sm text-slate-900"
                                    />
                                  </td>
                                  <td className="px-3.5 py-2.5">
                                    <input
                                      type="date"
                                      value={effectiveDrafts[item.variant_id] ?? todayIsoDate()}
                                      onChange={(event) =>
                                        setEffectiveDrafts((current) => ({
                                          ...current,
                                          [item.variant_id]: event.target.value || todayIsoDate(),
                                        }))
                                      }
                                      className="block w-40 rounded-xl border border-slate-300 px-3 py-2 text-sm text-slate-900"
                                    />
                                  </td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        </div>
                      </div>
                    ))}
                  </div>

                  {missingPagesCount > 1 ? (
                    <div className="mt-4 flex flex-wrap items-center justify-between gap-3">
                      <div className="text-xs text-slate-500">
                        Страница {missingPage} из {missingPagesCount}
                      </div>
                      <div className="flex flex-wrap items-center gap-2">
                        <button
                          type="button"
                          onClick={() => setMissingPage((current) => Math.max(1, current - 1))}
                          disabled={missingPage === 1}
                          className="rounded-xl border border-slate-200 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
                        >
                          Назад
                        </button>
                        {Array.from({ length: missingPagesCount }, (_, index) => index + 1).map((page) => (
                          <button
                            key={page}
                            type="button"
                            onClick={() => setMissingPage(page)}
                            className={`rounded-xl px-3 py-1.5 text-sm font-medium transition ${
                              page === missingPage
                                ? 'bg-slate-950 text-white'
                                : 'border border-slate-200 bg-white text-slate-700 hover:bg-slate-50'
                            }`}
                          >
                            {page}
                          </button>
                        ))}
                        <button
                          type="button"
                          onClick={() => setMissingPage((current) => Math.min(missingPagesCount, current + 1))}
                          disabled={missingPage === missingPagesCount}
                          className="rounded-xl border border-slate-200 bg-white px-3 py-1.5 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-50"
                        >
                          Вперед
                        </button>
                      </div>
                    </div>
                  ) : null}
                </div>
              ) : null}

              <div className="rounded-[24px] border border-slate-200 bg-white px-5 py-4 shadow-sm">
                <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                  <div>
                    <h2 className="text-xl font-semibold text-slate-950">История по вариациям</h2>
                    <p className="mt-1 text-sm text-slate-500">Смотри историю и меняй себестоимость прямо здесь.</p>
                  </div>
                  <div className="flex flex-col gap-2 sm:flex-row">
                    <input
                      type="text"
                      value={search}
                      onChange={(event) => setSearch(event.target.value)}
                      placeholder="Поиск по товару, артикулу, размеру"
                      className="w-full rounded-2xl border border-slate-200 px-4 py-2 text-sm text-slate-900 shadow-sm sm:w-72"
                    />
                  </div>
                </div>

                {filtered.length === 0 ? (
                  <div className="mt-6 rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-4 py-8 text-sm text-slate-500">
                    По текущему фильтру история не найдена.
                  </div>
                ) : (
                  <div className="mt-5 space-y-3">
                    {filtered.map((productGroup) => {
                      const isOpen = expandedProductKey === productGroup.key;
                      return (
                        <div key={productGroup.key} className="rounded-2xl border border-slate-200 bg-slate-50/70">
                          <button
                            type="button"
                            onClick={() => setExpandedProductKey((current) => (current === productGroup.key ? null : productGroup.key))}
                            className="flex w-full flex-col gap-3 px-4 py-3.5 text-left md:flex-row md:items-center md:justify-between"
                          >
                            <div>
                              <div className="text-base font-semibold text-slate-950">{productGroup.product_name}</div>
                              <div className="mt-0.5 text-sm text-slate-500">
                                Вариаций с историей: {productGroup.variants.length}
                              </div>
                            </div>
                            <div className="grid gap-2 text-right sm:grid-cols-2 sm:gap-5">
                              <div>
                                <div className="text-xs uppercase tracking-[0.16em] text-slate-400">Последнее изменение</div>
                                <div className="mt-0.5 text-sm font-semibold text-slate-950">
                                  {formatDate(productGroup.variants[0]?.latest.effective_from || '')}
                                </div>
                              </div>
                              <div>
                                <div className="text-xs uppercase tracking-[0.16em] text-slate-400">Вариаций</div>
                                <div className="mt-0.5 text-sm font-semibold text-slate-950">{productGroup.variants.length}</div>
                              </div>
                            </div>
                          </button>

                          {isOpen && (
                            <div className="border-t border-slate-200 px-4 py-3">
                              <div className="mb-3 flex justify-end">
                                <button
                                  type="button"
                                  onClick={() => void saveHistoryProductGroup(productGroup)}
                                  disabled={savingGroupKey === `history-${productGroup.key}`}
                                  className="inline-flex items-center justify-center rounded-xl bg-slate-950 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                                >
                                  {savingGroupKey === `history-${productGroup.key}` ? 'Сохраняем...' : 'Сохранить изменения по товару'}
                                </button>
                              </div>
                              <div className="space-y-3">
                                {productGroup.variants.map((group) => {
                                  const isVariantOpen = expandedVariantKey === group.key;
                                  const hasPendingDeletes = group.pendingDeleteCount > 0;
                                  return (
                                    <div key={group.key} className="rounded-2xl border border-white bg-white shadow-sm">
                                      <button
                                        type="button"
                                        onClick={() => setExpandedVariantKey((current) => (current === group.key ? null : group.key))}
                                        className="flex w-full flex-col gap-3 px-4 py-3 text-left md:flex-row md:items-center md:justify-between"
                                      >
                                        <div>
                                          <div className="flex flex-wrap items-center gap-2">
                                            <div className="text-sm font-semibold text-slate-950">{variantLabel(group.latest)}</div>
                                            {group.is_archived && (
                                              <span className="inline-flex rounded-full bg-slate-200 px-2.5 py-1 text-[11px] font-medium text-slate-700">
                                                Архив
                                              </span>
                                            )}
                                            {hasPendingDeletes && (
                                              <span className="inline-flex rounded-full bg-amber-100 px-2.5 py-1 text-[11px] font-medium text-amber-800">
                                                Удаление не сохранено
                                              </span>
                                            )}
                                          </div>
                                          <div className="mt-0.5 text-sm text-slate-500">
                                            {group.allPendingDelete
                                              ? 'После сохранения у вариации не останется себестоимости'
                                              : `Себестоимость: ${formatCurrency(group.latest.unit_cost)}`}
                                          </div>
                                        </div>
                                        <div className="grid gap-2 text-right sm:grid-cols-2 sm:gap-4">
                                          <div>
                                            <div className="text-xs uppercase tracking-[0.16em] text-slate-400">Действует с</div>
                                            <div className="mt-0.5 text-sm font-semibold text-slate-950">
                                              {group.allPendingDelete ? 'Будет очищено' : formatDate(group.latest.effective_from)}
                                            </div>
                                          </div>
                                          <div>
                                            <div className="text-xs uppercase tracking-[0.16em] text-slate-400">Изменений</div>
                                            <div className="mt-0.5 text-sm font-semibold text-slate-950">{group.items.length}</div>
                                          </div>
                                        </div>
                                      </button>

                                      {isVariantOpen && (
                                        <div className="border-t border-slate-200 px-4 py-3">
                                          <div className="mb-3 rounded-2xl border border-emerald-100 bg-emerald-50 px-4 py-3">
                                            <div className="flex flex-col gap-2 lg:flex-row lg:items-end lg:justify-between">
                                              <div>
                                                <div className="text-sm font-semibold text-slate-950">Добавить новое значение</div>
                                                <div className="mt-1 text-xs text-slate-500">
                                                  Изменения сохраняются общей кнопкой по товару выше.
                                                </div>
                                              </div>
                                            </div>
                                            <div className="mt-3 grid gap-2.5 sm:grid-cols-[1fr_165px_auto] sm:items-end">
                                              <div>
                                                <label className="mb-1 block text-xs font-medium uppercase tracking-[0.12em] text-slate-500">Себестоимость</label>
                                                <input
                                                  type="number"
                                                  min="0"
                                                  step="0.01"
                                                  value={costDrafts[group.variant_id] ?? ''}
                                                  onChange={(event) =>
                                                    setCostDrafts((current) => ({
                                                      ...current,
                                                      [group.variant_id]: event.target.value,
                                                    }))
                                                  }
                                                  placeholder="Введите сумму"
                                                  className="block w-full rounded-xl border border-slate-300 px-3 py-2 text-sm text-slate-900"
                                                />
                                              </div>
                                              <div>
                                                <label className="mb-1 block text-xs font-medium uppercase tracking-[0.12em] text-slate-500">Действует с</label>
                                                <input
                                                  type="date"
                                                  value={effectiveDrafts[group.variant_id] ?? todayIsoDate()}
                                                  onChange={(event) =>
                                                    setEffectiveDrafts((current) => ({
                                                      ...current,
                                                      [group.variant_id]: event.target.value || todayIsoDate(),
                                                    }))
                                                  }
                                                  className="block w-full rounded-xl border border-slate-300 px-3 py-2 text-sm text-slate-900"
                                                />
                                              </div>
                                              <div className="text-[11px] text-slate-500 sm:pb-2">Пересчет с этой даты</div>
                                            </div>
                                          </div>
                                          {hasPendingDeletes ? (
                                            <div className="mb-3 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-xs text-amber-900">
                                              Удаление записей пока только подготовлено. Нажми «Сохранить изменения по товару», чтобы применить его вместе с остальными правками.
                                            </div>
                                          ) : null}

                                          <div className="space-y-2">
                                            {group.items.map((item, index) => (
                                              <div
                                                key={item.id}
                                                className={`rounded-2xl border px-3.5 py-3 ${
                                                  pendingDeletedHistoryIds[item.id]
                                                    ? 'border-amber-200 bg-amber-50 opacity-80'
                                                    : 'border-slate-200 bg-slate-50'
                                                }`}
                                              >
                                                <div className="grid gap-2 sm:grid-cols-[1fr_auto_auto] sm:items-center sm:gap-3">
                                                  <div>
                                                    <div className="flex flex-wrap items-center gap-2">
                                                      <div className="font-medium text-slate-950">{formatDate(item.effective_from)}</div>
                                                      <span className={`inline-flex rounded-full px-2.5 py-1 text-[11px] font-medium ${index === 0 ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-100 text-slate-700'}`}>
                                                        {index === 0 ? 'Текущее значение' : 'Историческая запись'}
                                                      </span>
                                                      {pendingDeletedHistoryIds[item.id] ? (
                                                        <span className="inline-flex rounded-full bg-amber-100 px-2.5 py-1 text-[11px] font-medium text-amber-800">
                                                          Будет удалено
                                                        </span>
                                                      ) : null}
                                                    </div>
                                                    <div className="mt-0.5 text-xs text-slate-500">{new Intl.DateTimeFormat('ru-RU', {
                                                      day: '2-digit',
                                                      month: '2-digit',
                                                      year: 'numeric',
                                                      hour: '2-digit',
                                                      minute: '2-digit',
                                                    }).format(new Date(item.created_at))}</div>
                                                  </div>
                                                  <div className="text-base font-semibold text-slate-950">{formatCurrency(item.unit_cost)}</div>
                                                  <div className="flex justify-end">
                                                    <button
                                                      type="button"
                                                      onClick={() => {
                                                        if (pendingDeletedHistoryIds[item.id]) {
                                                          restoreHistoryEntry(group, item.id);
                                                          return;
                                                        }
                                                        void deleteHistoryEntry(group, item.id);
                                                      }}
                                                      className={`inline-flex rounded-xl border bg-white px-3 py-1.5 text-xs font-medium transition ${
                                                        pendingDeletedHistoryIds[item.id]
                                                          ? 'border-slate-300 text-slate-700 hover:bg-slate-50'
                                                          : 'border-rose-200 text-rose-700 hover:bg-rose-50'
                                                      }`}
                                                    >
                                                      {pendingDeletedHistoryIds[item.id] ? 'Вернуть' : 'Удалить'}
                                                    </button>
                                                  </div>
                                                </div>
                                              </div>
                                            ))}
                                          </div>
                                        </div>
                                      )}
                                    </div>
                                  );
                                })}
                              </div>
                            </div>
                          )}
                        </div>
                      );
                    })}
                  </div>
                )}
              </div>
            </>
          )}
        </div>
      </Layout>
    </ProtectedRoute>
  );
}
