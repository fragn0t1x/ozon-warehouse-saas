'use client';

import Link from 'next/link';
import { useEffect, useMemo, useState } from 'react';
import {
  ArrowPathIcon,
  CalculatorIcon,
  ChartBarSquareIcon,
  CubeIcon,
  ScaleIcon,
} from '@heroicons/react/24/outline';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { Layout } from '@/components/Layout';
import { dashboardAPI, type DashboardSummary, type UnitEconomicsRow } from '@/lib/api/dashboard';
import { useStoreContext } from '@/lib/context/StoreContext';
import {
  calculateUnitEconomics,
  DEFAULT_UNIT_ECONOMICS_INPUTS,
  getTaxRatePreset,
  TAX_OPTIONS,
  type TaxMode,
  type CalculatorInputs,
  UNIT_ECONOMICS_CALCULATOR_STORAGE_KEY,
  VAT_OPTIONS,
  type VatMode,
} from '@/lib/unitEconomicsCalculator';

type CurrentFilter = 'all' | 'profit' | 'low_margin' | 'break_even' | 'loss' | 'critical_loss' | 'no_cost';

interface CostReferenceInputs {
  purchaseCost: number;
  deliveryToUs: number;
  packaging: number;
  consumables: number;
  defectsRate: number;
  monthlyFixedCosts: number;
  plannedUnitsPerMonth: number;
}

function formatCurrency(value: number) {
  return new Intl.NumberFormat('ru-RU', {
    style: 'currency',
    currency: 'RUB',
    maximumFractionDigits: 0,
  }).format(value);
}

function formatPercent(value: number) {
  return `${Math.round(value * 100)}%`;
}

function formatNumber(value: number) {
  return new Intl.NumberFormat('ru-RU', {
    maximumFractionDigits: 2,
  }).format(value);
}

function round2(value: number) {
  return Math.round(value * 100) / 100;
}

function getDisplayedPrice(row: UnitEconomicsRow) {
  return typeof row.current_price_gross === 'number' && row.current_price_gross > 0
    ? row.current_price_gross
    : row.average_sale_price_gross;
}

function getDisplayedNetProfit(row: UnitEconomicsRow) {
  return typeof row.current_estimated_net_profit === 'number'
    ? row.current_estimated_net_profit
    : Number(row.estimated_net_profit || 0);
}

function getDisplayedMargin(row: UnitEconomicsRow) {
  return typeof row.current_margin_ratio === 'number'
    ? row.current_margin_ratio
    : Number(row.margin_ratio || 0);
}

function getDisplayedRevenueNet(row: UnitEconomicsRow) {
  return typeof row.current_revenue_net_of_vat === 'number'
    ? row.current_revenue_net_of_vat
    : Number(row.revenue_net_of_vat || 0);
}

function getDisplayedGrossProfit(row: UnitEconomicsRow) {
  return typeof row.current_gross_profit_before_ozon === 'number'
    ? row.current_gross_profit_before_ozon
    : Number(row.gross_profit_before_ozon || 0);
}

function getDisplayedProfitBeforeTax(row: UnitEconomicsRow) {
  return typeof row.current_profit_before_tax === 'number'
    ? row.current_profit_before_tax
    : Number(row.profit_before_tax || 0);
}

function getDisplayedTaxAmount(row: UnitEconomicsRow) {
  return typeof row.current_tax_amount === 'number'
    ? row.current_tax_amount
    : Number(row.tax_amount || 0);
}

function getDisplayedCommission(row: UnitEconomicsRow) {
  return typeof row.current_allocated_commission === 'number'
    ? row.current_allocated_commission
    : Number(row.allocated_commission || 0);
}

function getDisplayedLogistics(row: UnitEconomicsRow) {
  return typeof row.current_allocated_logistics === 'number'
    ? row.current_allocated_logistics
    : Number(row.allocated_logistics || 0);
}

function getDisplayedAcquiring(row: UnitEconomicsRow) {
  return typeof row.current_allocated_compensation === 'number'
    ? row.current_allocated_compensation
    : 0;
}

function getDisplayedMarketing(row: UnitEconomicsRow) {
  return typeof row.current_allocated_marketing === 'number'
    ? row.current_allocated_marketing
    : Number(row.allocated_marketing || 0);
}

function getDisplayedServices(row: UnitEconomicsRow) {
  return typeof row.current_allocated_services === 'number'
    ? row.current_allocated_services
    : Number(row.allocated_services || 0);
}

function getDisplayedOther(row: UnitEconomicsRow) {
  return typeof row.current_allocated_other === 'number'
    ? row.current_allocated_other
    : Number(row.allocated_other || 0);
}

function getDisplayedReturnReserve(row: UnitEconomicsRow) {
  return typeof row.current_return_reserve === 'number' ? row.current_return_reserve : 0;
}

function getProfitabilityState(row: UnitEconomicsRow) {
  const netProfit = getDisplayedNetProfit(row);
  const margin = getDisplayedMargin(row);
  const unitCost = Number(row.unit_cost || 0);

  if (unitCost <= 0) {
    return {
      key: 'no_cost' as const,
      label: 'Нет себестоимости',
      className: 'bg-amber-50 text-amber-700 border-amber-200',
      sortOrder: 0,
    };
  }
  if (netProfit < -50 || margin <= -0.05) {
    return {
      key: 'critical_loss' as const,
      label: 'Сильный минус',
      className: 'bg-rose-100 text-rose-800 border-rose-300',
      sortOrder: 1,
    };
  }
  if (netProfit < 0) {
    return {
      key: 'loss' as const,
      label: 'В минус',
      className: 'bg-rose-50 text-rose-700 border-rose-200',
      sortOrder: 2,
    };
  }
  if (netProfit <= 10) {
    return {
      key: 'break_even' as const,
      label: 'Почти в ноль',
      className: 'bg-amber-50 text-amber-700 border-amber-200',
      sortOrder: 3,
    };
  }
  if (margin < 0.05) {
    return {
      key: 'low_margin' as const,
      label: 'Очень низкая маржа',
      className: 'bg-sky-50 text-sky-700 border-sky-200',
      sortOrder: 4,
    };
  }
  return {
    key: 'profit' as const,
    label: 'Прибыльно',
    className: 'bg-emerald-50 text-emerald-700 border-emerald-200',
    sortOrder: 5,
  };
}

function getDataQuality(row: UnitEconomicsRow) {
  const months = Number(row.historical_profile_months || 0);
  const salesUnits = Number(row.historical_profile_sales_units || 0);

  if (months >= 1 && salesUnits >= 20) {
    return {
      label: 'Данных достаточно',
      className: 'bg-emerald-50 text-emerald-700 border-emerald-200',
    };
  }
  if (months >= 1 && salesUnits >= 8) {
    return {
      label: 'Данных пока немного',
      className: 'bg-amber-50 text-amber-700 border-amber-200',
    };
  }
  return {
    label: 'Данных мало',
    className: 'bg-slate-100 text-slate-600 border-slate-200',
  };
}

function Field({
  label,
  hint,
  value,
  onChange,
  step = '1',
  min = '0',
  suffix,
}: {
  label: string;
  hint?: string;
  value: number;
  onChange: (value: number) => void;
  step?: string;
  min?: string;
  suffix?: string;
}) {
  return (
    <label className="block rounded-2xl border border-slate-200 bg-white px-4 py-3">
      <div className="text-sm font-medium text-slate-900">{label}</div>
      {hint ? <div className="mt-1 text-xs leading-5 text-slate-500">{hint}</div> : null}
      <div className="mt-3 flex items-center gap-3">
        <input
          type="number"
          inputMode="decimal"
          min={min}
          step={step}
          value={Number.isFinite(value) ? value : 0}
          onChange={(event) => onChange(Number(event.target.value || 0))}
          className="w-full rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-900 outline-none transition focus:border-sky-300"
        />
        {suffix ? <span className="shrink-0 text-sm font-medium text-slate-500">{suffix}</span> : null}
      </div>
    </label>
  );
}

export default function UnitEconomicsCalculatorPage() {
  const { stores, selectedStoreId } = useStoreContext();
  const [inputs, setInputs] = useState<CalculatorInputs>(DEFAULT_UNIT_ECONOMICS_INPUTS);
  const [currentStoreId, setCurrentStoreId] = useState<number | null>(selectedStoreId ?? null);
  const [currentOfferId, setCurrentOfferId] = useState<string>('');
  const [currentRows, setCurrentRows] = useState<UnitEconomicsRow[]>([]);
  const [currentSummary, setCurrentSummary] = useState<DashboardSummary['unit_economics'] | null>(null);
  const [rowsLoading, setRowsLoading] = useState(false);
  const [searchQuery, setSearchQuery] = useState('');
  const [currentFilter, setCurrentFilter] = useState<CurrentFilter>('all');
  const [costReferenceInputs, setCostReferenceInputs] = useState<CostReferenceInputs>({
    purchaseCost: 620,
    deliveryToUs: 45,
    packaging: 18,
    consumables: 12,
    defectsRate: 3,
    monthlyFixedCosts: 60000,
    plannedUnitsPerMonth: 250,
  });

  useEffect(() => {
    try {
      const raw = window.localStorage.getItem(UNIT_ECONOMICS_CALCULATOR_STORAGE_KEY);
      if (!raw) {
        return;
      }
      const parsed = JSON.parse(raw) as Partial<CalculatorInputs>;
      setInputs((current) => ({ ...current, ...parsed }));
    } catch {
      setInputs(DEFAULT_UNIT_ECONOMICS_INPUTS);
    }
  }, []);

  useEffect(() => {
    window.localStorage.setItem(UNIT_ECONOMICS_CALCULATOR_STORAGE_KEY, JSON.stringify(inputs));
  }, [inputs]);

  useEffect(() => {
    if (typeof selectedStoreId === 'number') {
      setCurrentStoreId(selectedStoreId);
    }
  }, [selectedStoreId]);

  const selectedStore = useMemo(
    () => stores.find((store) => store.id === currentStoreId) ?? null,
    [currentStoreId, stores],
  );

  useEffect(() => {
    if (!selectedStore) {
      return;
    }
    setInputs((current) => ({
      ...current,
      vatMode: selectedStore.economics_vat_mode,
      taxMode: selectedStore.economics_tax_mode,
      taxRate: selectedStore.economics_tax_rate,
    }));
  }, [selectedStore]);

  useEffect(() => {
    let cancelled = false;

    const loadCurrentRows = async () => {
      try {
        setRowsLoading(true);
        const report = await dashboardAPI.getUnitEconomicsReport({
          storeId: currentStoreId,
          query: searchQuery.trim(),
          limit: 500,
        });
        if (cancelled) {
          return;
        }
        setCurrentRows(report.rows);
        setCurrentSummary(report.summary);
        setCurrentOfferId((current) => (
          report.rows.some((row) => row.offer_id === current)
            ? current
            : (report.rows[0]?.offer_id ?? '')
        ));
      } finally {
        if (!cancelled) {
          setRowsLoading(false);
        }
      }
    };

    void loadCurrentRows();

    return () => {
      cancelled = true;
    };
  }, [currentStoreId, searchQuery]);

  const taxOption = TAX_OPTIONS.find((item) => item.value === inputs.taxMode) ?? TAX_OPTIONS[0];
  const vatOption = VAT_OPTIONS.find((item) => item.value === inputs.vatMode) ?? VAT_OPTIONS[0];
  const calculatorResult = useMemo(() => calculateUnitEconomics(inputs), [inputs]);

  const filteredRows = useMemo(() => {
    const rows = currentRows.filter((row) => {
      const state = getProfitabilityState(row);
      if (currentFilter !== 'all' && state.key !== currentFilter) {
        return false;
      }
      return true;
    });

    return [...rows].sort((left, right) => {
      const leftState = getProfitabilityState(left);
      const rightState = getProfitabilityState(right);
      if (leftState.sortOrder !== rightState.sortOrder) {
        return leftState.sortOrder - rightState.sortOrder;
      }
      return getDisplayedNetProfit(left) - getDisplayedNetProfit(right);
    });
  }, [currentFilter, currentRows]);

  const selectedRow = useMemo(
    () => currentRows.find((row) => row.offer_id === currentOfferId) ?? filteredRows[0] ?? null,
    [currentOfferId, currentRows, filteredRows],
  );

  const countsByState = useMemo(() => {
    return currentRows.reduce<Record<CurrentFilter, number>>(
      (accumulator, row) => {
        const state = getProfitabilityState(row);
        accumulator[state.key] += 1;
        accumulator.all += 1;
        return accumulator;
      },
      { all: 0, profit: 0, low_margin: 0, break_even: 0, loss: 0, critical_loss: 0, no_cost: 0 },
    );
  }, [currentRows]);

  const filteredTotals = useMemo(() => {
    return filteredRows.reduce(
      (accumulator, row) => ({
        units: accumulator.units + Number(row.units || 0),
        revenue: accumulator.revenue + Number(getDisplayedPrice(row) || 0),
        netProfit: accumulator.netProfit + Number(getDisplayedNetProfit(row) || 0),
      }),
      { units: 0, revenue: 0, netProfit: 0 },
    );
  }, [filteredRows]);

  const riskOverview = useMemo(() => {
    const summarize = (filter: CurrentFilter) => {
      const rows = currentRows.filter((row) => getProfitabilityState(row).key === filter);
      return {
        count: rows.length,
        netProfit: round2(rows.reduce((total, row) => total + Number(getDisplayedNetProfit(row) || 0), 0)),
      };
    };

    return {
      critical_loss: summarize('critical_loss'),
      loss: summarize('loss'),
      break_even: summarize('break_even'),
      low_margin: summarize('low_margin'),
    };
  }, [currentRows]);

  const baseCostPerUnit = useMemo(() => {
    const variablePart =
      costReferenceInputs.purchaseCost +
      costReferenceInputs.deliveryToUs +
      costReferenceInputs.packaging +
      costReferenceInputs.consumables;
    const defectReserve = variablePart * (costReferenceInputs.defectsRate / 100);
    return round2(variablePart + defectReserve);
  }, [costReferenceInputs]);

  const fullCostPerUnit = useMemo(() => {
    const fixedPartPerUnit =
      costReferenceInputs.plannedUnitsPerMonth > 0
        ? costReferenceInputs.monthlyFixedCosts / costReferenceInputs.plannedUnitsPerMonth
        : 0;
    return round2(baseCostPerUnit + fixedPartPerUnit);
  }, [baseCostPerUnit, costReferenceInputs]);

  const currentFilterOptions: Array<{ value: CurrentFilter; label: string }> = [
    { value: 'all', label: `Все (${countsByState.all})` },
    { value: 'profit', label: `Прибыльно (${countsByState.profit})` },
    { value: 'low_margin', label: `Низкая маржа (${countsByState.low_margin})` },
    { value: 'break_even', label: `Почти в ноль (${countsByState.break_even})` },
    { value: 'loss', label: `В минус (${countsByState.loss})` },
    { value: 'critical_loss', label: `Сильный минус (${countsByState.critical_loss})` },
    { value: 'no_cost', label: `Без себестоимости (${countsByState.no_cost})` },
  ];

  return (
    <ProtectedRoute>
      <Layout>
        <div className="space-y-6">
          <section className="rounded-[32px] border border-white/70 bg-[radial-gradient(circle_at_top_left,_rgba(255,255,255,0.95),_rgba(240,249,255,0.92)_40%,_rgba(255,247,237,0.92)_100%)] p-7 shadow-[0_24px_60px_rgba(15,23,42,0.08)]">
            <div className="flex flex-col gap-5 lg:flex-row lg:items-end lg:justify-between">
              <div className="max-w-4xl">
                <div className="inline-flex items-center gap-2 rounded-full border border-white/70 bg-white/85 px-4 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-sky-700">
                  <CalculatorIcon className="h-4 w-4" />
                  Юнит-экономика
                </div>
                <h1 className="mt-4 text-3xl font-semibold tracking-tight text-slate-950 sm:text-4xl">
                  Смотри, насколько выгодно продаются текущие артикулы, и заранее считай новый товар.
                </h1>
                <p className="mt-3 max-w-3xl text-sm leading-6 text-slate-600 sm:text-base">
                  Здесь можно быстро проверить текущие товары, посчитать новый товар и понять, как правильно считать себестоимость.
                </p>
              </div>

              <button
                type="button"
                onClick={() => setInputs(DEFAULT_UNIT_ECONOMICS_INPUTS)}
                className="inline-flex items-center justify-center gap-2 rounded-2xl border border-slate-200 bg-white px-5 py-4 text-sm font-medium text-slate-700 shadow-sm transition hover:-translate-y-0.5 hover:border-slate-300 hover:text-slate-950"
              >
                <ArrowPathIcon className="h-4 w-4" />
                Сбросить расчет нового товара
              </button>
            </div>
          </section>

          <section className="rounded-3xl border border-white/70 bg-white/85 p-5 shadow-[0_16px_40px_rgba(15,23,42,0.06)] backdrop-blur">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
              <div className="max-w-3xl">
                <div className="flex items-center gap-2">
                  <ChartBarSquareIcon className="h-5 w-5 text-sky-700" />
                  <h2 className="text-lg font-semibold text-slate-950">Текущие товары</h2>
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-600">
                  Здесь видно, насколько выгодно товар продается сейчас при текущей цене на Ozon.
                </p>
              </div>
                <div className="rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 text-sm text-sky-900">
                  {currentSummary?.basis_label || 'Ждем экономику по товарам'}
                  <div className="mt-1 text-xs text-sky-700">
                  Цена, комиссия и логистика берутся из текущей карточки товара. Остальные расходы считаем по последним собранным данным.
                  </div>
                </div>
            </div>

            <div className="mt-5 grid gap-4 xl:grid-cols-[1.15fr_0.85fr]">
              <div className="space-y-4">
                <div className="grid gap-3 lg:grid-cols-[220px_minmax(0,1fr)]">
                  <label className="block rounded-2xl border border-slate-200 bg-white px-4 py-3">
                    <div className="text-sm font-medium text-slate-900">Магазин</div>
                    <select
                      value={currentStoreId ?? ''}
                      onChange={(event) => setCurrentStoreId(event.target.value ? Number(event.target.value) : null)}
                      className="mt-3 w-full rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-900 outline-none transition focus:border-sky-300"
                    >
                      {stores.map((store) => (
                        <option key={store.id} value={store.id}>
                          {store.name}
                        </option>
                      ))}
                    </select>
                  </label>

                  <label className="block rounded-2xl border border-slate-200 bg-white px-4 py-3">
                    <div className="text-sm font-medium text-slate-900">Поиск по товару или артикулу</div>
                    <input
                      type="text"
                      value={searchQuery}
                      onChange={(event) => setSearchQuery(event.target.value)}
                      placeholder="Например, Носки или FUT1чернM"
                      className="mt-3 w-full rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-900 outline-none transition focus:border-sky-300"
                    />
                  </label>
                </div>

                <div className="flex flex-wrap gap-2">
                  {currentFilterOptions.map((option) => (
                    <button
                      key={option.value}
                      type="button"
                      onClick={() => setCurrentFilter(option.value)}
                      className={`rounded-full px-3 py-2 text-sm font-medium transition ${
                        currentFilter === option.value
                          ? 'bg-slate-950 text-white'
                          : 'border border-slate-200 bg-white text-slate-700 hover:border-slate-300'
                      }`}
                    >
                      {option.label}
                    </button>
                  ))}
                </div>

                <div className="grid gap-3 md:grid-cols-3">
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                    <div className="text-xs uppercase tracking-[0.14em] text-slate-500">SKU в выборке</div>
                    <div className="mt-2 text-2xl font-semibold text-slate-950">{formatNumber(filteredRows.length)}</div>
                  </div>
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                    <div className="text-xs uppercase tracking-[0.14em] text-slate-500">Сумма текущих цен по SKU</div>
                    <div className="mt-2 text-2xl font-semibold text-slate-950">{formatCurrency(filteredTotals.revenue)}</div>
                  </div>
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                    <div className="text-xs uppercase tracking-[0.14em] text-slate-500">Суммарная чистая прибыль на 1 ед.</div>
                    <div className={`mt-2 text-2xl font-semibold ${filteredTotals.netProfit >= 0 ? 'text-emerald-700' : 'text-rose-700'}`}>
                      {formatCurrency(filteredTotals.netProfit)}
                    </div>
                  </div>
                </div>

                <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-4">
                  {[
                    {
                      key: 'critical_loss' as const,
                      title: 'Сильный минус',
                      hint: 'Товары, которые уже сильно убыточны.',
                      className: 'border-rose-300 bg-rose-50 text-rose-900',
                    },
                    {
                      key: 'loss' as const,
                      title: 'В минус',
                      hint: 'Товары с отрицательной прибылью.',
                      className: 'border-rose-200 bg-rose-50/70 text-rose-800',
                    },
                    {
                      key: 'break_even' as const,
                      title: 'Почти в ноль',
                      hint: 'Прибыль почти исчезла.',
                      className: 'border-amber-200 bg-amber-50 text-amber-900',
                    },
                    {
                      key: 'low_margin' as const,
                      title: 'Очень низкая маржа',
                      hint: 'Товар еще в плюсе, но запас маленький.',
                      className: 'border-sky-200 bg-sky-50 text-sky-900',
                    },
                  ].map((item) => {
                    const summary = riskOverview[item.key];
                    const isActive = currentFilter === item.key;
                    return (
                      <button
                        key={item.key}
                        type="button"
                        onClick={() => setCurrentFilter(item.key)}
                        className={`rounded-2xl border px-4 py-4 text-left transition hover:-translate-y-0.5 ${item.className} ${
                          isActive ? 'ring-2 ring-slate-950/10' : ''
                        }`}
                      >
                        <div className="text-xs font-semibold uppercase tracking-[0.16em] opacity-70">{item.title}</div>
                        <div className="mt-3 text-3xl font-semibold">{formatNumber(summary.count)}</div>
                        <div className="mt-2 text-sm font-medium">
                          {summary.count > 0 ? formatCurrency(summary.netProfit) : 'Пока нет SKU'}
                        </div>
                        <div className="mt-2 text-xs leading-5 opacity-75">{item.hint}</div>
                      </button>
                    );
                  })}
                </div>

                <div className="overflow-hidden rounded-3xl border border-slate-200 bg-white">
                  <div className="grid grid-cols-[minmax(220px,1.5fr)_minmax(120px,0.8fr)_minmax(120px,0.8fr)_minmax(140px,0.9fr)_150px] gap-0 border-b border-slate-200 bg-slate-50 text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">
                    <div className="px-4 py-3">Товар / артикул</div>
                    <div className="px-4 py-3">Текущая цена</div>
                    <div className="px-4 py-3">Себестоимость</div>
                    <div className="px-4 py-3">Чистая прибыль / 1 ед.</div>
                    <div className="px-4 py-3">Статус</div>
                  </div>

                  {rowsLoading ? (
                    <div className="px-4 py-8 text-sm text-slate-500">Подтягиваем экономику товаров...</div>
                  ) : filteredRows.length === 0 ? (
                    <div className="px-4 py-8 text-sm text-slate-500">По текущему фильтру товары не найдены.</div>
                  ) : (
                    <div className="max-h-[640px] overflow-y-auto">
                      {filteredRows.map((row) => {
                        const state = getProfitabilityState(row);
                        const isActive = selectedRow?.offer_id === row.offer_id;
                        return (
                          <button
                            key={`${row.store_id}-${row.offer_id}`}
                            type="button"
                            onClick={() => setCurrentOfferId(row.offer_id)}
                            className={`grid w-full grid-cols-[minmax(220px,1.5fr)_minmax(120px,0.8fr)_minmax(120px,0.8fr)_minmax(140px,0.9fr)_150px] gap-0 border-b border-slate-100 text-left transition hover:bg-slate-50 ${
                              isActive ? 'bg-sky-50/60' : 'bg-white'
                            }`}
                          >
                            <div className="px-4 py-3">
                              <div className="text-sm font-medium text-slate-950">{row.title || row.offer_id}</div>
                              <div className="mt-1 text-xs text-slate-500">{row.offer_id}</div>
                            </div>
                            <div className="px-4 py-3 text-sm text-slate-900">
                              {getDisplayedPrice(row) ? formatCurrency(getDisplayedPrice(row) || 0) : '—'}
                            </div>
                            <div className="px-4 py-3 text-sm text-slate-900">
                              {row.unit_cost > 0 ? formatCurrency(row.unit_cost) : 'Не задана'}
                            </div>
                            <div className={`px-4 py-3 text-sm font-medium ${getDisplayedNetProfit(row) >= 0 ? 'text-emerald-700' : 'text-rose-700'}`}>
                              {formatCurrency(getDisplayedNetProfit(row))}
                            </div>
                            <div className="px-4 py-3">
                              <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${state.className}`}>
                                {state.label}
                              </span>
                            </div>
                          </button>
                        );
                      })}
                    </div>
                  )}
                </div>
              </div>

              <div className="space-y-4">
                <div className="rounded-3xl border border-slate-950 bg-slate-950 p-5 text-white shadow-[0_24px_60px_rgba(15,23,42,0.2)]">
                  <div className="text-sm font-medium text-slate-300">Выбранный артикул</div>
                  {selectedRow ? (
                    <>
                      <div className="mt-3 text-2xl font-semibold text-white">{selectedRow.title || selectedRow.offer_id}</div>
                      <div className="mt-1 text-sm text-slate-300">{selectedRow.offer_id}</div>
                      <div className="mt-4 flex flex-wrap gap-2">
                        <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${getProfitabilityState(selectedRow).className}`}>
                          {getProfitabilityState(selectedRow).label}
                        </span>
                        <span className={`inline-flex rounded-full border px-2.5 py-1 text-xs font-medium ${getDataQuality(selectedRow).className}`}>
                          {getDataQuality(selectedRow).label}
                        </span>
                      </div>
                      <div className="mt-5 grid gap-3 sm:grid-cols-2">
                        <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
                          <div className="text-xs uppercase tracking-[0.14em] text-slate-400">Чистая прибыль</div>
                          <div className={`mt-2 text-xl font-semibold ${getDisplayedNetProfit(selectedRow) >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                            {formatCurrency(getDisplayedNetProfit(selectedRow))}
                          </div>
                        </div>
                        <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
                          <div className="text-xs uppercase tracking-[0.14em] text-slate-400">Маржа</div>
                          <div className="mt-2 text-xl font-semibold text-white">{formatPercent(getDisplayedMargin(selectedRow) || 0)}</div>
                        </div>
                      </div>
                    </>
                  ) : (
                    <div className="mt-3 text-sm text-slate-300">Выбери артикул слева, чтобы увидеть подробный разбор.</div>
                  )}
                </div>

                <div className="rounded-3xl border border-white/70 bg-white/85 p-5 shadow-[0_16px_40px_rgba(15,23,42,0.06)] backdrop-blur">
                  <div className="text-sm font-semibold text-slate-950">Разбор по выбранному артикулу</div>
                  {selectedRow ? (
                    <div className="mt-4 space-y-3 text-sm">
                      {[
                        ['Текущая цена для покупателя', getDisplayedPrice(selectedRow) ? formatCurrency(getDisplayedPrice(selectedRow) || 0) : '—'],
                        ['Выручка без НДС', formatCurrency(getDisplayedRevenueNet(selectedRow))],
                        ['Себестоимость', selectedRow.unit_cost > 0 ? formatCurrency(selectedRow.unit_cost) : 'Не задана'],
                        ['Валовая прибыль до Ozon', formatCurrency(getDisplayedGrossProfit(selectedRow))],
                        ['Комиссия Ozon', formatCurrency(Math.abs(getDisplayedCommission(selectedRow)))],
                        ['Логистика Ozon', formatCurrency(Math.abs(getDisplayedLogistics(selectedRow)))],
                        ['Сервисы Ozon', formatCurrency(Math.abs(getDisplayedServices(selectedRow)))],
                        ['Эквайринг', formatCurrency(Math.abs(getDisplayedAcquiring(selectedRow)))],
                        ['Прочие расходы', formatCurrency(Math.abs(getDisplayedOther(selectedRow)))],
                        ['Маркетинг', formatCurrency(Math.abs(getDisplayedMarketing(selectedRow)))],
                        ['Резерв на возвраты', formatCurrency(Math.abs(getDisplayedReturnReserve(selectedRow)))],
                        ['Прибыль до налога', formatCurrency(getDisplayedProfitBeforeTax(selectedRow))],
                        ['Налог', formatCurrency(getDisplayedTaxAmount(selectedRow))],
                        ['Комиссия, %', typeof selectedRow.current_commission_percent === 'number' ? `${formatNumber(selectedRow.current_commission_percent)}%` : '—'],
                        ['Потеря при возврате', typeof selectedRow.current_return_amount === 'number' ? formatCurrency(Math.abs(selectedRow.current_return_amount)) : '—'],
                        ['Частота возвратов SKU', typeof selectedRow.current_return_rate === 'number' ? `${formatNumber(selectedRow.current_return_rate * 100)}%` : '—'],
                      ].map(([label, value]) => (
                        <div key={label} className="flex items-center justify-between gap-4 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                          <div className="text-slate-600">{label}</div>
                          <div className="font-semibold text-slate-950">{value}</div>
                        </div>
                      ))}
                      <div className="rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 text-sm text-sky-900">
                        Если себестоимость неверная, поменяй ее в разделе{' '}
                        <Link href="/cost-history" className="font-medium underline decoration-dotted underline-offset-2">
                          «Себестоимость»
                        </Link>
                        . Здесь показываем только итог и понятный разбор.
                      </div>
                    </div>
                  ) : (
                    <div className="mt-4 text-sm text-slate-500">Пока нечего разбирать: сначала выбери артикул из списка слева.</div>
                  )}
                </div>
              </div>
            </div>
          </section>

          <section className="grid gap-6 xl:grid-cols-[1.05fr_0.95fr]">
            <div className="space-y-6">
              <section className="rounded-3xl border border-white/70 bg-white/85 p-5 shadow-[0_16px_40px_rgba(15,23,42,0.06)] backdrop-blur">
                <div className="flex items-center gap-2">
                  <ScaleIcon className="h-5 w-5 text-sky-700" />
                  <h2 className="text-lg font-semibold text-slate-950">Новый товар</h2>
                </div>
                <p className="mt-2 text-sm leading-6 text-slate-600">
                  Здесь ничего не сохраняется в товарах. Это чистый расчет: вводишь свою себестоимость, цену и расходы, чтобы понять, стоит ли запускать SKU.
                </p>

                <div className="mt-5 grid gap-4 lg:grid-cols-2">
                  <label className="block rounded-2xl border border-slate-200 bg-white px-4 py-3">
                    <div className="text-sm font-medium text-slate-900">Налоговые настройки магазина</div>
                    <select
                      value={currentStoreId ?? ''}
                      onChange={(event) => setCurrentStoreId(event.target.value ? Number(event.target.value) : null)}
                      className="mt-3 w-full rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-900 outline-none transition focus:border-sky-300"
                    >
                      {stores.map((store) => (
                        <option key={store.id} value={store.id}>
                          {store.name}
                        </option>
                      ))}
                    </select>
                  </label>

                  <label className="block rounded-2xl border border-slate-200 bg-white px-4 py-3">
                    <div className="text-sm font-medium text-slate-900">Режим НДС</div>
                    <select
                      value={inputs.vatMode}
                      onChange={(event) => setInputs((current) => ({ ...current, vatMode: event.target.value as VatMode }))}
                      className="mt-3 w-full rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-900 outline-none transition focus:border-sky-300"
                    >
                      {VAT_OPTIONS.map((item) => (
                        <option key={item.value} value={item.value}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                    <div className="mt-2 text-xs text-slate-500">{vatOption.note}</div>
                  </label>

                  <label className="block rounded-2xl border border-slate-200 bg-white px-4 py-3">
                    <div className="text-sm font-medium text-slate-900">Налоговая модель</div>
                    <select
                      value={inputs.taxMode}
                      onChange={(event) => {
                        const nextMode = event.target.value as TaxMode;
                        setInputs((current) => ({
                          ...current,
                          taxMode: nextMode,
                          taxRate: getTaxRatePreset(nextMode),
                        }));
                      }}
                      className="mt-3 w-full rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-900 outline-none transition focus:border-sky-300"
                    >
                      {TAX_OPTIONS.map((item) => (
                        <option key={item.value} value={item.value}>
                          {item.label}
                        </option>
                      ))}
                    </select>
                    <div className="mt-2 text-xs text-slate-500">{taxOption.note}</div>
                  </label>

                  <Field
                    label="Налоговая ставка"
                    hint="Если нужно, можно вручную переопределить ставку."
                    value={inputs.taxRate}
                    onChange={(value) => setInputs((current) => ({ ...current, taxRate: value }))}
                    step="0.1"
                    suffix="%"
                  />
                </div>

                <div className="mt-5 grid gap-4 lg:grid-cols-2">
                  <Field
                    label="Цена для покупателя"
                    hint="Финальная цена карточки на витрине."
                    value={inputs.salePriceGross}
                    onChange={(value) => setInputs((current) => ({ ...current, salePriceGross: value }))}
                    suffix="₽"
                  />
                  <Field
                    label="Себестоимость товара"
                    hint="Готовая себестоимость, которую ты уже определил для этого товара."
                    value={inputs.cogs}
                    onChange={(value) => setInputs((current) => ({ ...current, cogs: value }))}
                    suffix="₽"
                  />
                  <Field label="Логистика до Ozon" hint="Доставка единицы до Ozon или точки отгрузки." value={inputs.inboundLogistics} onChange={(value) => setInputs((current) => ({ ...current, inboundLogistics: value }))} suffix="₽" />
                  <Field label="Упаковка и подготовка" hint="Пакет, стикер, маркировка и материалы." value={inputs.packaging} onChange={(value) => setInputs((current) => ({ ...current, packaging: value }))} suffix="₽" />
                  <Field label="Комиссия Ozon" hint="Укажи комиссию как процент от цены продажи." value={inputs.ozonCommissionRate} onChange={(value) => setInputs((current) => ({ ...current, ozonCommissionRate: value }))} step="0.1" suffix="%" />
                  <Field label="Фулфилмент / обработка" hint="Средний расход на один заказ." value={inputs.fulfillmentPerOrder} onChange={(value) => setInputs((current) => ({ ...current, fulfillmentPerOrder: value }))} suffix="₽" />
                  <Field label="Последняя миля" hint="Средняя доставка покупателю." value={inputs.lastMilePerOrder} onChange={(value) => setInputs((current) => ({ ...current, lastMilePerOrder: value }))} suffix="₽" />
                  <Field label="Прочие расходы маркетплейса" hint="Хранение, эквайринг и другие расходы на одну продажу." value={inputs.otherMarketplaceCosts} onChange={(value) => setInputs((current) => ({ ...current, otherMarketplaceCosts: value }))} suffix="₽" />
                  <Field label="Реклама как % от цены" hint="Если удобнее считать ДРР как процент." value={inputs.marketingRate} onChange={(value) => setInputs((current) => ({ ...current, marketingRate: value }))} step="0.1" suffix="%" />
                  <Field label="Реклама в рублях" hint="Или фиксированный расход на один заказ." value={inputs.marketingFixed} onChange={(value) => setInputs((current) => ({ ...current, marketingFixed: value }))} suffix="₽" />
                  <Field label="Доля возвратов" hint="Сколько продаж в среднем возвращается." value={inputs.returnRate} onChange={(value) => setInputs((current) => ({ ...current, returnRate: value }))} step="0.1" suffix="%" />
                  <Field label="Обратная логистика" hint="Средний расход на один возврат." value={inputs.reverseLogistics} onChange={(value) => setInputs((current) => ({ ...current, reverseLogistics: value }))} suffix="₽" />
                  <Field label="Обработка возврата" hint="Переупаковка, сортировка, приемка." value={inputs.returnHandling} onChange={(value) => setInputs((current) => ({ ...current, returnHandling: value }))} suffix="₽" />
                  <Field label="Потеря себестоимости на возврате" hint="Если часть возвратов уходит в уценку или брак." value={inputs.returnWriteoffRate} onChange={(value) => setInputs((current) => ({ ...current, returnWriteoffRate: value }))} step="0.1" suffix="%" />
                  <Field label="Доп. потери на возврате" hint="Химчистка, пересорт, уценка." value={inputs.returnAdditionalLoss} onChange={(value) => setInputs((current) => ({ ...current, returnAdditionalLoss: value }))} suffix="₽" />
                  <Field label="Постоянные расходы в месяц" hint="Команда, склад, сервисы и прочие общие расходы." value={inputs.monthlyFixedCosts} onChange={(value) => setInputs((current) => ({ ...current, monthlyFixedCosts: value }))} suffix="₽" />
                  <Field label="План продаж в месяц" hint="Нужен для прогноза по месяцу и точки безубыточности." value={inputs.plannedUnitsPerMonth} onChange={(value) => setInputs((current) => ({ ...current, plannedUnitsPerMonth: value }))} suffix="шт." />
                </div>
              </section>
            </div>

            <div className="space-y-6">
              <section className="rounded-3xl border border-slate-950 bg-slate-950 p-5 text-white shadow-[0_24px_60px_rgba(15,23,42,0.2)]">
                <div className="text-sm font-medium text-slate-300">Итог по новому товару</div>
                <div className={`mt-3 text-4xl font-semibold ${calculatorResult.netProfit >= 0 ? 'text-emerald-300' : 'text-rose-300'}`}>
                  {formatCurrency(calculatorResult.netProfit)}
                </div>
                <div className="mt-2 text-sm text-slate-300">
                  Чистая прибыль на единицу • {taxOption.label} • {vatOption.label}
                </div>
                <div className="mt-4 grid gap-3 sm:grid-cols-2">
                  <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
                    <div className="text-xs uppercase tracking-[0.14em] text-slate-400">Маржа</div>
                    <div className="mt-2 text-xl font-semibold text-white">{formatPercent(calculatorResult.margin)}</div>
                  </div>
                  <div className="rounded-2xl border border-white/10 bg-white/5 px-4 py-3">
                    <div className="text-xs uppercase tracking-[0.14em] text-slate-400">ROI на себестоимость</div>
                    <div className="mt-2 text-xl font-semibold text-white">{formatPercent(calculatorResult.roi)}</div>
                  </div>
                </div>
              </section>

              <section className="rounded-3xl border border-white/70 bg-white/85 p-5 shadow-[0_16px_40px_rgba(15,23,42,0.06)] backdrop-blur">
                <h2 className="text-lg font-semibold text-slate-950">Разбор расчета нового товара</h2>
                <div className="mt-5 space-y-3 text-sm">
                  {[
                    ['Цена покупателя', formatCurrency(inputs.salePriceGross)],
                    ['Выручка без НДС', formatCurrency(calculatorResult.revenueNetOfVat)],
                    ['Комиссия Ozon', formatCurrency(calculatorResult.commissionAmount)],
                    ['Реклама', formatCurrency(calculatorResult.marketingAmount)],
                    ['Резерв на возвраты', formatCurrency(calculatorResult.returnReserve)],
                    ['Все переменные расходы', formatCurrency(calculatorResult.variableCosts)],
                    ['Прибыль до налога', formatCurrency(calculatorResult.contributionBeforeTax)],
                    ['Налог', formatCurrency(calculatorResult.taxAmount)],
                  ].map(([label, value]) => (
                    <div key={label} className="flex items-center justify-between gap-4 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                      <div className="text-slate-600">{label}</div>
                      <div className="font-semibold text-slate-950">{value}</div>
                    </div>
                  ))}
                </div>
              </section>

              <section className="rounded-3xl border border-white/70 bg-white/85 p-5 shadow-[0_16px_40px_rgba(15,23,42,0.06)] backdrop-blur">
                <h2 className="text-lg font-semibold text-slate-950">Управленческие выводы</h2>
                <div className="mt-5 grid gap-3">
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                    <div className="text-xs uppercase tracking-[0.14em] text-slate-500">Цена безубыточности</div>
                    <div className="mt-2 text-xl font-semibold text-slate-950">
                      {calculatorResult.breakEvenPriceGross === null ? 'Не находится' : formatCurrency(calculatorResult.breakEvenPriceGross)}
                    </div>
                    <div className="mt-1 text-xs text-slate-500">Минимальная цена карточки при текущих расходах.</div>
                  </div>
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                    <div className="text-xs uppercase tracking-[0.14em] text-slate-500">Допустимая доп. реклама</div>
                    <div className="mt-2 text-xl font-semibold text-slate-950">{formatCurrency(calculatorResult.maxExtraAdSpend)}</div>
                    <div className="mt-1 text-xs text-slate-500">Сколько еще можно потратить на один заказ до нуля по чистой прибыли.</div>
                  </div>
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                    <div className="text-xs uppercase tracking-[0.14em] text-slate-500">Месяц при плане продаж</div>
                    <div className={`mt-2 text-xl font-semibold ${calculatorResult.monthlyProfit >= 0 ? 'text-emerald-700' : 'text-rose-700'}`}>
                      {formatCurrency(calculatorResult.monthlyProfit)}
                    </div>
                    <div className="mt-1 text-xs text-slate-500">После постоянных расходов и плана {formatNumber(inputs.plannedUnitsPerMonth)} шт.</div>
                  </div>
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3">
                    <div className="text-xs uppercase tracking-[0.14em] text-slate-500">Точка безубыточности по штукам</div>
                    <div className="mt-2 text-xl font-semibold text-slate-950">
                      {calculatorResult.breakEvenUnits === null ? 'Нет' : `${formatNumber(calculatorResult.breakEvenUnits)} шт.`}
                    </div>
                    <div className="mt-1 text-xs text-slate-500">Сколько нужно продать в месяц, чтобы закрыть постоянные расходы.</div>
                  </div>
                </div>
              </section>
            </div>
          </section>

          <section className="rounded-3xl border border-white/70 bg-white/85 p-5 shadow-[0_16px_40px_rgba(15,23,42,0.06)] backdrop-blur">
            <div className="flex items-center gap-2">
              <CubeIcon className="h-5 w-5 text-sky-700" />
              <h2 className="text-lg font-semibold text-slate-950">Как считать себестоимость</h2>
            </div>
            <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-600">
              Это справочный блок. Он помогает понять, из чего складывается себестоимость и чем базовая себестоимость отличается от полной.
            </p>

            <div className="mt-5 grid gap-4 xl:grid-cols-[0.95fr_1.05fr]">
              <div className="space-y-4">
                <div className="grid gap-4 md:grid-cols-2">
                  <Field label="Закупка / производство" hint="Сколько стоит сама единица товара." value={costReferenceInputs.purchaseCost} onChange={(value) => setCostReferenceInputs((current) => ({ ...current, purchaseCost: value }))} suffix="₽" />
                  <Field label="Доставка до тебя" hint="Логистика до твоего склада или офиса." value={costReferenceInputs.deliveryToUs} onChange={(value) => setCostReferenceInputs((current) => ({ ...current, deliveryToUs: value }))} suffix="₽" />
                  <Field label="Упаковка" hint="Пакеты, коробки, маркировка, стикеры." value={costReferenceInputs.packaging} onChange={(value) => setCostReferenceInputs((current) => ({ ...current, packaging: value }))} suffix="₽" />
                  <Field label="Прочие расходники" hint="Вкладыши, скотч, доп. материалы." value={costReferenceInputs.consumables} onChange={(value) => setCostReferenceInputs((current) => ({ ...current, consumables: value }))} suffix="₽" />
                  <Field label="Резерв на брак и потери" hint="Если часть товара теряется, портится или уходит в уценку." value={costReferenceInputs.defectsRate} onChange={(value) => setCostReferenceInputs((current) => ({ ...current, defectsRate: value }))} step="0.1" suffix="%" />
                  <Field label="Постоянные расходы в месяц" hint="Зарплата, аренда, склад, сервисы." value={costReferenceInputs.monthlyFixedCosts} onChange={(value) => setCostReferenceInputs((current) => ({ ...current, monthlyFixedCosts: value }))} suffix="₽" />
                  <Field label="План продаж в месяц" hint="Нужен только для распределения постоянных расходов на единицу." value={costReferenceInputs.plannedUnitsPerMonth} onChange={(value) => setCostReferenceInputs((current) => ({ ...current, plannedUnitsPerMonth: value }))} suffix="шт." />
                </div>

                <div className="rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 text-sm text-sky-900">
                  Если после такого расчета ты понимаешь, что себестоимость товара в системе неверная, меняй ее в разделе{' '}
                  <Link href="/cost-history" className="font-medium underline decoration-dotted underline-offset-2">
                    «Себестоимость»
                  </Link>
                  . Этот блок нужен только как справка.
                </div>
              </div>

              <div className="space-y-4">
                <div className="grid gap-4 md:grid-cols-2">
                  <div className="rounded-3xl border border-emerald-200 bg-emerald-50/90 p-5">
                    <div className="text-sm font-semibold text-slate-950">Себестоимость без постоянных расходов</div>
                    <div className="mt-3 text-3xl font-semibold text-emerald-700">{formatCurrency(baseCostPerUnit)}</div>
                    <div className="mt-3 text-sm leading-6 text-slate-700">
                      Закупка, доставка до тебя, упаковка, расходники и резерв на брак.
                    </div>
                  </div>
                  <div className="rounded-3xl border border-amber-200 bg-amber-50/90 p-5">
                    <div className="text-sm font-semibold text-slate-950">Полная себестоимость с постоянными расходами</div>
                    <div className="mt-3 text-3xl font-semibold text-amber-700">{formatCurrency(fullCostPerUnit)}</div>
                    <div className="mt-3 text-sm leading-6 text-slate-700">
                      Базовая себестоимость плюс доля аренды, зарплат, склада и других постоянных расходов.
                    </div>
                  </div>
                </div>

                <div className="rounded-3xl border border-slate-200 bg-slate-50 p-5">
                  <div className="text-sm font-semibold text-slate-950">Как на это смотреть</div>
                  <div className="mt-3 space-y-3 text-sm leading-6 text-slate-700">
                    <p>
                      <span className="font-medium text-slate-950">Базовая себестоимость</span> подходит для оперативной оценки товара: выгодно ли продавать его прямо сейчас и не ушел ли он в минус на уровне одной продажи.
                    </p>
                    <p>
                      <span className="font-medium text-slate-950">Полная себестоимость</span> нужна, если хочешь понимать реальную прибыль бизнеса с учетом команды, склада и остальных общих расходов.
                    </p>
                    <p>
                      В самой системе фактическую себестоимость вариации ты меняешь отдельно в разделе <Link href="/cost-history" className="font-medium text-sky-700 underline decoration-dotted underline-offset-2">«Себестоимость»</Link>.
                    </p>
                  </div>
                </div>
              </div>
            </div>
          </section>
        </div>
      </Layout>
    </ProtectedRoute>
  );
}
