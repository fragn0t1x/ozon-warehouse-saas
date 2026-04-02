'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { Layout } from '@/components/Layout';
import { economicsHistoryAPI, type StoreEconomicsHistoryEntry } from '@/lib/api/economicsHistory';
import { storesAPI } from '@/lib/api/stores';
import { useStoreContext } from '@/lib/context/StoreContext';
import { getTaxRatePreset, TAX_OPTIONS, VAT_OPTIONS, type TaxMode, type VatMode } from '@/lib/unitEconomicsCalculator';

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

function formatDateTime(value: string) {
  if (!value) {
    return '—';
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  }).format(parsed);
}

function formatPercent(value: number) {
  return new Intl.NumberFormat('ru-RU', {
    maximumFractionDigits: 1,
  }).format(value);
}

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

function buildStoreTaxFormula(vatMode: VatMode, taxMode: TaxMode, taxRate: number) {
  const vatOption = VAT_OPTIONS.find((item) => item.value === vatMode);
  const taxOption = TAX_OPTIONS.find((item) => item.value === taxMode);
  const vatRate = vatOption?.rate ?? 0;
  const vatLabel = vatRate > 0 ? `${formatPercent(vatRate * 100)}% НДС` : 'без НДС';
  const taxLabel = `${formatPercent(taxRate)}%`;

  if (taxMode === 'before_tax') {
    return {
      title: 'Налог в расчет не включен',
      formula: 'Чистая прибыль = прибыль до налога',
      note: 'Режим нужен для управленческой оценки без учета налога.',
    };
  }

  if (taxMode === 'usn_income') {
    return {
      title: 'Налог считается с дохода',
      formula: `Налог = выручка ${vatLabel === 'без НДС' ? 'без НДС' : `без ${vatLabel}`} × ${taxLabel}`,
      note: 'Комиссия Ozon, себестоимость и другие расходы налоговую базу не уменьшают.',
    };
  }

  if (taxMode === 'usn_income_expenses') {
    return {
      title: 'Налог считается с прибыли',
      formula: `Налог = max((выручка ${vatLabel === 'без НДС' ? 'без НДС' : `без ${vatLabel}`} - себестоимость - расходы Ozon + корректировки Ozon) × ${taxLabel}, выручка без НДС × 1%)`,
      note: 'Для УСН доходы минус расходы учитываем минимальный налог 1% от дохода.',
    };
  }

  return {
    title: taxOption?.label || 'Кастомная модель',
    formula: `Налог = max(прибыль до налога, 0) × ${taxLabel}`,
    note: 'Подходит для управленческой модели, если используете свою ставку налога на прибыль.',
  };
}

export default function TaxHistoryPage() {
  const { selectedStore, selectedStoreId, stores, isLoading: storesLoading, refreshStores } = useStoreContext();
  const [history, setHistory] = useState<StoreEconomicsHistoryEntry[]>([]);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [deletingId, setDeletingId] = useState<number | null>(null);
  const [draftVatMode, setDraftVatMode] = useState<VatMode>('none');
  const [draftTaxMode, setDraftTaxMode] = useState<TaxMode>('usn_income_expenses');
  const [draftTaxRate, setDraftTaxRate] = useState<number>(15);
  const [draftEffectiveFrom, setDraftEffectiveFrom] = useState<string>(todayIsoDate());

  const loadHistory = useCallback(async () => {
    if (!selectedStoreId) {
      setHistory([]);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      const data = await economicsHistoryAPI.getStoreEconomicsHistory(selectedStoreId);
      setHistory(data);
    } catch {
      setHistory([]);
      toast.error('Не удалось загрузить историю налоговой схемы');
    } finally {
      setLoading(false);
    }
  }, [selectedStoreId]);

  useEffect(() => {
    void loadHistory();
  }, [loadHistory]);

  useEffect(() => {
    if (!selectedStore) {
      return;
    }
    setDraftVatMode(selectedStore.economics_vat_mode);
    setDraftTaxMode(selectedStore.economics_tax_mode);
    setDraftTaxRate(selectedStore.economics_tax_rate);
    setDraftEffectiveFrom(selectedStore.economics_effective_from || todayIsoDate());
  }, [selectedStore]);

  const currentVat = VAT_OPTIONS.find((item) => item.value === selectedStore?.economics_vat_mode);
  const currentTax = TAX_OPTIONS.find((item) => item.value === selectedStore?.economics_tax_mode);
  const firstEntry = history[history.length - 1] ?? null;
  const latestEntry = history[0] ?? null;
  const changesCount = Math.max(history.length - 1, 0);

  const timeline = useMemo(
    () =>
      history.map((item, index) => ({
        ...item,
        vatLabel: VAT_OPTIONS.find((option) => option.value === item.vat_mode)?.label ?? item.vat_mode,
        taxLabel: TAX_OPTIONS.find((option) => option.value === item.tax_mode)?.label ?? item.tax_mode,
        isCurrent: index === 0,
      })),
    [history]
  );
  const taxFormula = buildStoreTaxFormula(draftVatMode, draftTaxMode, draftTaxRate);

  const saveTaxSettings = async () => {
    if (!selectedStoreId) {
      return;
    }
    setSaving(true);
    try {
      await storesAPI.patch(selectedStoreId, {
        economics_vat_mode: draftVatMode,
        economics_tax_mode: draftTaxMode,
        economics_tax_rate: draftTaxRate,
        economics_effective_from: draftEffectiveFrom || null,
      });
      await refreshStores();
      await loadHistory();
      toast.success('Налоговая схема сохранена. История и закрытые месяцы обновятся автоматически.');
    } catch {
      toast.error('Не удалось сохранить налоговую схему');
    } finally {
      setSaving(false);
    }
  };

  const deleteHistoryEntry = async (historyId: number) => {
    if (!selectedStoreId) {
      return;
    }
    if (history.length <= 1) {
      toast.error('Нельзя удалить последнюю налоговую схему магазина');
      return;
    }
    if (!window.confirm('Удалить эту запись из истории налоговой схемы? Затронутые закрытые месяцы пересчитаются автоматически.')) {
      return;
    }
    setDeletingId(historyId);
    try {
      await economicsHistoryAPI.deleteStoreEconomicsHistory(selectedStoreId, historyId);
      await refreshStores();
      await loadHistory();
      toast.success('Запись истории удалена');
    } catch (error) {
      const message = error instanceof Error ? error.message : '';
      toast.error(message || 'Не удалось удалить запись истории');
    } finally {
      setDeletingId(null);
    }
  };

  if (loading || storesLoading) {
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
          <div className="rounded-[28px] border border-slate-200 bg-white px-6 py-6 shadow-sm">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
              <div>
                <div className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Налоги и НДС</div>
                <h1 className="mt-2 text-3xl font-semibold text-slate-950">История налоговой схемы магазина</h1>
                <p className="mt-2 max-w-3xl text-sm text-slate-600">
                  Здесь видно, какая схема налога и НДС действовала у активного магазина в разные периоды.
                  Именно эта история используется при расчете закрытых месяцев.
                </p>
              </div>
              <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-700">
                <div>Активный магазин: <span className="font-semibold text-slate-950">{selectedStore?.name || 'Не выбран'}</span></div>
                <div className="mt-1 text-slate-500">Переключить магазин можно в шапке портала.</div>
              </div>
            </div>
          </div>

          {stores.length === 0 || !selectedStore ? (
            <div className="rounded-[28px] border border-dashed border-slate-300 bg-white px-6 py-12 text-center shadow-sm">
              <h2 className="text-lg font-semibold text-slate-900">Сначала подключите магазин</h2>
              <p className="mt-2 text-sm text-slate-500">После подключения здесь появится история налоговой схемы и НДС.</p>
              <Link href="/stores" className="mt-5 inline-flex rounded-2xl bg-slate-950 px-4 py-3 text-sm font-medium text-white transition hover:bg-slate-800">
                Открыть магазины
              </Link>
            </div>
          ) : (
            <>
              <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
                <div className="rounded-3xl border border-slate-200 bg-white px-5 py-5 shadow-sm">
                  <div className="text-sm font-medium text-slate-500">Текущий режим НДС</div>
                  <div className="mt-2 text-xl font-semibold text-slate-950">{currentVat?.label || 'Не задан'}</div>
                  <div className="mt-2 text-sm text-slate-500">{currentVat?.note || 'Берем это значение в новых расчетах и калькуляторе.'}</div>
                </div>
                <div className="rounded-3xl border border-slate-200 bg-white px-5 py-5 shadow-sm">
                  <div className="text-sm font-medium text-slate-500">Текущая налоговая модель</div>
                  <div className="mt-2 text-xl font-semibold text-slate-950">{currentTax?.label || 'Не задана'}</div>
                  <div className="mt-2 text-sm text-slate-500">Ставка: {formatPercent(selectedStore.economics_tax_rate)}%</div>
                </div>
                <div className="rounded-3xl border border-slate-200 bg-white px-5 py-5 shadow-sm">
                  <div className="text-sm font-medium text-slate-500">Изменений в истории</div>
                  <div className="mt-2 text-3xl font-semibold text-slate-950">{changesCount}</div>
                  <div className="mt-2 text-sm text-slate-500">Без учета текущей стартовой записи.</div>
                </div>
                <div className="rounded-3xl border border-slate-200 bg-white px-5 py-5 shadow-sm">
                  <div className="text-sm font-medium text-slate-500">История действует</div>
                  <div className="mt-2 text-xl font-semibold text-slate-950">{firstEntry ? formatDate(firstEntry.effective_from) : '—'}</div>
                  <div className="mt-2 text-sm text-slate-500">Последнее обновление: {latestEntry ? formatDate(latestEntry.effective_from) : '—'}</div>
                </div>
              </div>

              <div className="rounded-[28px] border border-slate-200 bg-white px-6 py-6 shadow-sm">
                <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                  <div>
                    <h2 className="text-xl font-semibold text-slate-950">Изменить схему прямо здесь</h2>
                    <p className="mt-1 text-sm text-slate-500">
                      Можно сохранить новую или более раннюю дату действия. Затронутые закрытые месяцы пересчитаются автоматически.
                    </p>
                  </div>
                  <button
                    type="button"
                    onClick={() => void saveTaxSettings()}
                    disabled={saving}
                    className="inline-flex items-center justify-center rounded-2xl bg-slate-950 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {saving ? 'Сохраняем...' : 'Сохранить схему'}
                  </button>
                </div>

                <div className="mt-5 grid gap-4 lg:grid-cols-2">
                  <div>
                    <label className="mb-1 block text-sm font-medium text-gray-700">Режим НДС</label>
                    <select
                      value={draftVatMode}
                      onChange={(event) => setDraftVatMode(event.target.value as VatMode)}
                      className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-primary-500 focus:ring-primary-500"
                    >
                      {VAT_OPTIONS.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                    <p className="mt-1 text-xs text-gray-500">
                      {VAT_OPTIONS.find((item) => item.value === draftVatMode)?.note}
                    </p>
                  </div>

                  <div>
                    <label className="mb-1 block text-sm font-medium text-gray-700">Налоговая модель</label>
                    <select
                      value={draftTaxMode}
                      onChange={(event) => {
                        const nextMode = event.target.value as TaxMode;
                        setDraftTaxMode(nextMode);
                        setDraftTaxRate(getTaxRatePreset(nextMode));
                      }}
                      className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-primary-500 focus:ring-primary-500"
                    >
                      {TAX_OPTIONS.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                    <p className="mt-1 text-xs text-gray-500">
                      {TAX_OPTIONS.find((item) => item.value === draftTaxMode)?.note}
                    </p>
                  </div>

                  <div>
                    <label className="mb-1 block text-sm font-medium text-gray-700">Налоговая ставка</label>
                    <input
                      type="number"
                      min="0"
                      step="0.1"
                      value={draftTaxRate}
                      onChange={(event) => setDraftTaxRate(Number(event.target.value || 0))}
                      className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-primary-500 focus:ring-primary-500"
                    />
                  </div>

                  <div>
                    <label className="mb-1 block text-sm font-medium text-gray-700">Действует с</label>
                    <input
                      type="date"
                      value={draftEffectiveFrom}
                      onChange={(event) => setDraftEffectiveFrom(event.target.value || todayIsoDate())}
                      className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-primary-500 focus:ring-primary-500"
                    />
                  </div>
                </div>

                <div className="mt-4 rounded-2xl border border-sky-100 bg-sky-50 px-4 py-3">
                  <div className="text-xs uppercase tracking-[0.14em] text-sky-700">Как сейчас считаем налог</div>
                  <div className="mt-2 text-sm font-semibold text-slate-950">{taxFormula.title}</div>
                  <div className="mt-2 text-sm text-slate-700">{taxFormula.formula}</div>
                  <div className="mt-2 text-xs text-slate-500">{taxFormula.note}</div>
                </div>
              </div>

              <div className="rounded-[28px] border border-slate-200 bg-white px-6 py-6 shadow-sm">
                <div className="flex flex-col gap-3 lg:flex-row lg:items-center lg:justify-between">
                  <div>
                    <h2 className="text-xl font-semibold text-slate-950">История по датам</h2>
                    <p className="mt-1 text-sm text-slate-500">
                      Если добавить запись задним числом, закрытые месяцы с этой даты и дальше пересчитаются автоматически.
                    </p>
                  </div>
                  <div className="text-sm text-slate-500">Ниже видно, как менялась схема во времени.</div>
                </div>

                {timeline.length === 0 ? (
                  <div className="mt-6 rounded-2xl border border-dashed border-slate-300 bg-slate-50 px-4 py-8 text-sm text-slate-500">
                    История пока пустая.
                  </div>
                ) : (
                  <div className="mt-6 space-y-4">
                    {timeline.map((item) => (
                      <div key={item.id} className="rounded-3xl border border-slate-200 bg-slate-50/70 px-5 py-5">
                        <div className="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
                          <div>
                            <div className="flex flex-wrap items-center gap-2">
                              <div className="text-lg font-semibold text-slate-950">{formatDate(item.effective_from)}</div>
                              <span className={`inline-flex rounded-full px-3 py-1 text-xs font-medium ${item.isCurrent ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-200 text-slate-700'}`}>
                                {item.isCurrent ? 'Текущая схема' : 'Историческая запись'}
                              </span>
                            </div>
                            <div className="mt-3 flex flex-wrap gap-2">
                              <span className="inline-flex rounded-full bg-white px-3 py-1 text-sm font-medium text-slate-700">{item.vatLabel}</span>
                              <span className="inline-flex rounded-full bg-white px-3 py-1 text-sm font-medium text-slate-700">{item.taxLabel}</span>
                              <span className="inline-flex rounded-full bg-white px-3 py-1 text-sm font-medium text-slate-700">{formatPercent(item.tax_rate)}%</span>
                            </div>
                          </div>
                          <div className="flex flex-col items-start gap-3 text-sm text-slate-500 lg:items-end">
                            <div>Добавлено: {formatDateTime(item.created_at)}</div>
                            <button
                              type="button"
                              onClick={() => void deleteHistoryEntry(item.id)}
                              disabled={deletingId === item.id || history.length <= 1}
                              className="inline-flex rounded-xl border border-rose-200 bg-white px-3 py-2 text-xs font-medium text-rose-700 transition hover:bg-rose-50 disabled:cursor-not-allowed disabled:opacity-50"
                            >
                              {deletingId === item.id ? 'Удаляем...' : 'Удалить запись'}
                            </button>
                          </div>
                        </div>
                      </div>
                    ))}
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
