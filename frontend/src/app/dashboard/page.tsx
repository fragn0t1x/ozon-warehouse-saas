'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import {
  BuildingStorefrontIcon,
  CubeIcon,
  SparklesIcon,
  Squares2X2Icon,
  TruckIcon,
} from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { ClosedMonthMetricCard } from '@/components/ClosedMonthMetricCard';
import { Layout } from '@/components/Layout';
import { SyncFreshnessPanel } from '@/components/SyncFreshnessPanel';
import {
  closedMonthsAPI,
  type ClosedMonthFinanceDetail,
  type ClosedMonthOfferFinance,
} from '@/lib/api/closedMonths';
import { dashboardAPI, normalizeDashboardSummary, type DashboardSummary } from '@/lib/api/dashboard';
import { getSupplyStatusLabel, getSupplyStatusStyle } from '@/lib/constants/supplyStatus';
import { useAuth } from '@/lib/context/AuthContext';
import { useStoreContext } from '@/lib/context/StoreContext';
import { TAX_OPTIONS, VAT_OPTIONS } from '@/lib/unitEconomicsCalculator';

function formatDate(value: string | null) {
  if (!value) {
    return '—';
  }

  if (/^\d{4}-\d{2}-\d{2}$/.test(value)) {
    return new Date(`${value}T00:00:00`).toLocaleDateString('ru-RU', {
      day: '2-digit',
      month: '2-digit',
      year: 'numeric',
    });
  }

  return new Date(value).toLocaleString('ru-RU', {
    day: '2-digit',
    month: '2-digit',
    year: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  });
}

function formatCurrency(value: number) {
  return new Intl.NumberFormat('ru-RU', {
    style: 'currency',
    currency: 'RUB',
    maximumFractionDigits: 0,
  }).format(value);
}

function formatPercent(value: number) {
  if (!Number.isFinite(value)) {
    return '—';
  }

  return new Intl.NumberFormat('ru-RU', {
    style: 'percent',
    maximumFractionDigits: 1,
  }).format(value);
}

function formatNumber(value: number) {
  return new Intl.NumberFormat('ru-RU').format(value);
}

function formatSignedCurrency(value: number) {
  const abs = formatCurrency(Math.abs(value));
  if (value > 0) {
    return `+${abs}`;
  }
  if (value < 0) {
    return `-${abs}`;
  }
  return abs;
}

function formatSignedUnits(value: number) {
  const abs = formatNumber(Math.abs(value));
  if (value > 0) {
    return `+${abs} шт.`;
  }
  if (value < 0) {
    return `-${abs} шт.`;
  }
  return `${abs} шт.`;
}

function formatMultiplier(value: number) {
  return value.toFixed(2).replace(/\.00$/, '');
}

function formatDateOnlyRange(from: string | null, to: string | null) {
  if (!from || !to) {
    return 'последние 30 дней';
  }

  const fromDate = new Date(from);
  const toDate = new Date(to);
  if (Number.isNaN(fromDate.getTime()) || Number.isNaN(toDate.getTime())) {
    return 'последние 30 дней';
  }

  return `${fromDate.toLocaleDateString('ru-RU')} - ${toDate.toLocaleDateString('ru-RU')}`;
}

function formatPeriodLabel(value: string | null | undefined) {
  if (!value) {
    return '—';
  }

  const [from, to] = value.split(' - ');
  if (!from || !to) {
    return value;
  }

  return formatDateOnlyRange(from, to);
}

function formatMonthPeriod(value: string | null) {
  if (!value || !/^\d{4}-\d{2}$/.test(value)) {
    return 'закрытый месяц Ozon';
  }
  const [year, month] = value.split('-').map(Number);
  return new Date(year, (month || 1) - 1, 1).toLocaleDateString('ru-RU', {
    month: 'long',
    year: 'numeric',
  });
}

function metricInfoAlign(index: number, total: number): 'start' | 'end' {
  const startCount = total <= 3 ? 1 : 2;
  return index < startCount ? 'start' : 'end';
}

export default function DashboardPage() {
  const { isInitialized, user } = useAuth();
  const { stores, selectedStore, selectedStoreId } = useStoreContext();
  const [summary, setSummary] = useState<DashboardSummary | null>(null);
  const [selectedStoreSummary, setSelectedStoreSummary] = useState<DashboardSummary | null>(null);
  const [closedMonthDetail, setClosedMonthDetail] = useState<ClosedMonthFinanceDetail | null>(null);
  const [closedMonthDetailLoading, setClosedMonthDetailLoading] = useState(false);
  const [latestClosedMonthFromHistory, setLatestClosedMonthFromHistory] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const selectedClosedMonthPeriod =
    latestClosedMonthFromHistory ??
    (user?.is_admin ? (selectedStoreSummary?.finance?.realization_closed_month?.period ?? null) : null);

  const loadSummary = useCallback(async () => {
    if (!user) {
      setSummary(null);
      setSelectedStoreSummary(null);
      setLoadError(null);
      setLoading(false);
      return;
    }

    setLoading(true);
    setLoadError(null);
    try {
      const hasSingleAccessibleStore =
        typeof selectedStoreId === 'number' &&
        stores.length === 1 &&
        stores[0]?.id === selectedStoreId;

      if (hasSingleAccessibleStore) {
        const singleStoreSummary = await dashboardAPI.getSummary(selectedStoreId);
        const normalized = normalizeDashboardSummary(singleStoreSummary);
        setSummary(normalized);
        setSelectedStoreSummary(normalized);
      } else {
        const [summaryData, selectedSummaryData] = await Promise.all([
          dashboardAPI.getSummary(),
          typeof selectedStoreId === 'number' ? dashboardAPI.getSummary(selectedStoreId) : Promise.resolve(null),
        ]);
        setSummary(summaryData);
        setSelectedStoreSummary(selectedSummaryData ? normalizeDashboardSummary(selectedSummaryData) : null);
      }
    } catch {
      setSummary(null);
      setSelectedStoreSummary(null);
      setLoadError('Не удалось загрузить сводку дашборда.');
      toast.error('Не удалось загрузить сводку дашборда');
    } finally {
      setLoading(false);
    }
  }, [selectedStoreId, stores, user]);

  useEffect(() => {
    if (!isInitialized) {
      return;
    }

    if (!user) {
      setLoading(false);
      return;
    }

    void loadSummary();
  }, [isInitialized, loadSummary, user]);

  useEffect(() => {
    let cancelled = false;

    const loadLatestClosedMonthFromHistory = async () => {
      if (!selectedStoreId) {
        setLatestClosedMonthFromHistory(null);
        return;
      }

      try {
        const months = await closedMonthsAPI.list(selectedStoreId, 1);
        if (!cancelled) {
          setLatestClosedMonthFromHistory(months[0]?.month ?? null);
        }
      } catch {
        if (!cancelled) {
          setLatestClosedMonthFromHistory(null);
        }
      }
    };

    void loadLatestClosedMonthFromHistory();

    return () => {
      cancelled = true;
    };
  }, [selectedStoreId]);

  useEffect(() => {
    let cancelled = false;

    const loadClosedMonthDetail = async () => {
      if (!selectedStoreId || !selectedClosedMonthPeriod) {
        setClosedMonthDetail(null);
        setClosedMonthDetailLoading(false);
        return;
      }

      setClosedMonthDetailLoading(true);
      try {
        const data = await closedMonthsAPI.get(selectedStoreId, selectedClosedMonthPeriod);
        if (!cancelled) {
          setClosedMonthDetail(data);
        }
      } catch {
        if (!cancelled) {
          setClosedMonthDetail(null);
        }
      } finally {
        if (!cancelled) {
          setClosedMonthDetailLoading(false);
        }
      }
    };

    void loadClosedMonthDetail();

    return () => {
      cancelled = true;
    };
  }, [selectedClosedMonthPeriod, selectedStoreId]);

  const heroText = useMemo(() => {
    if (!selectedStoreSummary) {
      return 'Собираем картину по выбранному магазину';
    }

    if (selectedStoreSummary.active_supplies > 0) {
      return `По магазину сейчас в работе ${selectedStoreSummary.active_supplies} поставок — видно, где нужен приход, а где уже пора смотреть отправки и приемку.`;
    }

    return 'Раздел «Магазин сегодня» теперь показывает только активный магазин: поставки, склад и самые активные артикулы именно по нему.';
  }, [selectedStoreSummary]);

  if (loading && !summary) {
    return (
      <ProtectedRoute>
        <Layout>
          <div className="flex h-64 items-center justify-center">
            <div className="h-12 w-12 animate-spin rounded-full border-b-2 border-primary-600" />
          </div>
        </Layout>
      </ProtectedRoute>
    );
  }

  if (loadError || !summary) {
    return (
      <ProtectedRoute>
        <Layout>
          <div className="rounded-3xl border border-white/60 bg-white/80 p-8 shadow-lg shadow-slate-200/60 backdrop-blur">
            <h1 className="text-2xl font-semibold text-slate-900">Дашборд</h1>
            <p className="mt-2 text-sm text-slate-500">
              {loadError || 'Сводка пока недоступна. Попробуем ещё раз чуть позже.'}
            </p>
            <button
              type="button"
              onClick={() => void loadSummary()}
              className="mt-4 inline-flex items-center rounded-2xl bg-sky-600 px-4 py-2 text-sm font-medium text-white transition hover:bg-sky-700"
            >
              Повторить загрузку
            </button>
          </div>
        </Layout>
      </ProtectedRoute>
    );
  }

  const controlSummary = selectedStoreSummary ?? summary;
  const statusEntries = Object.entries(controlSummary.status_counts ?? {}).sort((a, b) => b[1] - a[1]);
  const stockCards = controlSummary.stock_by_store;
  const activeStoreStock = stockCards[0] ?? null;
  const activeStoreTopOffers =
    controlSummary.sales.top_offers_by_store.find((group) => group.store_id === selectedStoreId)
      ?? controlSummary.sales.top_offers_by_store[0]
      ?? null;
  const financeSummary = controlSummary.finance;
  const realization = financeSummary.realization_closed_month;
  const unitEconomicsSummary = controlSummary.unit_economics;
  const closedMonthSummary = closedMonthDetail?.month ?? null;
  const selectedVatMode = closedMonthSummary?.vat_mode_used ?? selectedStore?.economics_vat_mode;
  const selectedTaxMode = closedMonthSummary?.tax_mode_used ?? selectedStore?.economics_tax_mode;
  const selectedTaxRate = closedMonthSummary?.tax_rate_used ?? selectedStore?.economics_tax_rate ?? 0;
  const selectedVatOption = VAT_OPTIONS.find((item) => item.value === selectedVatMode);
  const selectedTaxOption = TAX_OPTIONS.find((item) => item.value === selectedTaxMode);
  const selectedVatRate = selectedVatOption?.rate ?? 0;
  const closedMonthCoverageRatio = closedMonthSummary?.coverage_ratio ?? unitEconomicsSummary.cost_coverage_ratio;
  const closedMonthSoldUnits = closedMonthSummary?.sold_units ?? realization.sold_units;
  const closedMonthSoldAmount = closedMonthSummary?.sold_amount ?? realization.sold_amount;
  const closedMonthReturnedUnits = closedMonthSummary?.returned_units ?? realization.returned_units;
  const closedMonthReturnedAmount = closedMonthSummary?.returned_amount ?? realization.returned_amount;
  const closedMonthRevenueAmount = closedMonthSummary?.revenue_amount ?? realization.net_amount;
  const closedMonthRevenueNetOfVat = closedMonthSummary?.revenue_net_of_vat ?? unitEconomicsSummary.tracked_revenue_net_of_vat;
  const closedMonthCogs = closedMonthSummary?.cogs ?? unitEconomicsSummary.tracked_cogs;
  const closedMonthGrossProfit = closedMonthSummary?.gross_profit ?? unitEconomicsSummary.gross_profit_before_ozon;
  const closedMonthCashflow = financeSummary.closed_month_cashflow;
  const closedMonthOzonCommission = closedMonthSummary?.ozon_commission ?? Math.abs(realization.net_fee ?? 0);
  const closedMonthOzonServices = closedMonthSummary?.ozon_services ?? Math.abs(closedMonthCashflow.services_amount ?? 0);
  const closedMonthOzonLogistics = closedMonthSummary?.ozon_logistics ?? Math.abs(closedMonthCashflow.logistics_amount ?? 0);
  const closedMonthOzonAcquiring = closedMonthSummary?.ozon_acquiring ?? 0;
  const closedMonthOzonOtherExpenses = closedMonthSummary?.ozon_other_expenses ?? 0;
  const closedMonthOzonIncentives = closedMonthSummary?.ozon_incentives ?? realization.net_incentives ?? 0;
  const closedMonthOzonExpenses =
    closedMonthOzonCommission +
    closedMonthOzonServices +
    closedMonthOzonLogistics +
    closedMonthOzonAcquiring +
    closedMonthOzonOtherExpenses;
  const closedMonthCompensation = closedMonthSummary?.ozon_compensation ?? realization.compensation_amount ?? 0;
  const closedMonthDecompensation = closedMonthSummary?.ozon_decompensation ?? realization.decompensation_amount ?? 0;
  const closedMonthCorrections = closedMonthSummary?.ozon_adjustments_net ?? realization.net_adjustment ?? 0;
  const closedMonthProfitBeforeTax = closedMonthSummary?.profit_before_tax ?? (
    closedMonthGrossProfit +
      closedMonthOzonIncentives -
      closedMonthOzonCommission -
      closedMonthOzonServices -
      closedMonthOzonLogistics -
      closedMonthOzonAcquiring -
      closedMonthOzonOtherExpenses +
      closedMonthCorrections
  );
  const closedMonthTaxAmount = closedMonthSummary?.tax_amount ?? unitEconomicsSummary.tax_amount;
  const closedMonthNetProfit = closedMonthSummary?.net_profit ?? (closedMonthProfitBeforeTax - closedMonthTaxAmount);
  const hasFullCostCoverage = closedMonthCoverageRatio >= 0.9999;
  const revenueExplanation = `${formatCurrency(closedMonthSoldAmount)} - ${formatCurrency(closedMonthReturnedAmount)} = ${formatCurrency(closedMonthRevenueAmount)}`;
  const grossProfitExplanation = `${formatCurrency(closedMonthRevenueNetOfVat)} - ${formatCurrency(closedMonthCogs)} = ${formatCurrency(closedMonthGrossProfit)}`;
  const commissionExplanation = `${formatCurrency(realization.sold_fee)} - ${formatCurrency(realization.returned_fee)} = ${formatCurrency(closedMonthOzonCommission)}`;
  const incentivesExplanation = `${formatCurrency(realization.sold_incentives)} - ${formatCurrency(realization.returned_incentives)} = ${formatSignedCurrency(closedMonthOzonIncentives)}`;
  const incentivesExpression = closedMonthOzonIncentives >= 0
    ? `+ ${formatCurrency(closedMonthOzonIncentives)}`
    : `- ${formatCurrency(Math.abs(closedMonthOzonIncentives))}`;
  const correctionsExpression = closedMonthCorrections >= 0
    ? `+ ${formatCurrency(closedMonthCorrections)}`
    : `- ${formatCurrency(Math.abs(closedMonthCorrections))}`;
  const acquiringExpression = `- ${formatCurrency(closedMonthOzonAcquiring)}`;
  const otherExpensesExpression = `- ${formatCurrency(closedMonthOzonOtherExpenses)}`;
  const profitBeforeTaxExplanation = `${formatCurrency(closedMonthGrossProfit)} ${incentivesExpression} - ${formatCurrency(closedMonthOzonCommission)} - ${formatCurrency(closedMonthOzonServices)} - ${formatCurrency(closedMonthOzonLogistics)} ${acquiringExpression} ${otherExpensesExpression} ${correctionsExpression} = ${formatCurrency(closedMonthProfitBeforeTax)}`;
  const netProfitExplanation = `${formatCurrency(closedMonthProfitBeforeTax)} - ${formatCurrency(closedMonthTaxAmount)} = ${formatCurrency(closedMonthNetProfit)}`;
  const taxExplanation = (() => {
    if (!selectedStore) {
      return 'Налог считается по выбранной модели магазина.';
    }

    if (selectedTaxMode === 'before_tax') {
      return 'В этом режиме налог в расчет не включается.';
    }

    if (selectedTaxMode === 'usn_income') {
      if (selectedVatRate > 0) {
        return `${formatCurrency(closedMonthRevenueNetOfVat)} × ${formatNumber(selectedTaxRate)}% = ${formatCurrency(closedMonthTaxAmount)}`;
      }
      return `${formatCurrency(closedMonthRevenueNetOfVat)} × ${formatNumber(selectedTaxRate)}% = ${formatCurrency(closedMonthTaxAmount)}`;
    }

    if (selectedTaxMode === 'usn_income_expenses') {
      const taxFromProfit = Math.max(closedMonthProfitBeforeTax, 0) * (selectedTaxRate / 100);
      const minimalTax = closedMonthRevenueNetOfVat * 0.01;
      if (minimalTax >= taxFromProfit) {
        return `max(${formatCurrency(closedMonthProfitBeforeTax)} × ${formatNumber(selectedTaxRate)}%, ${formatCurrency(closedMonthRevenueNetOfVat)} × 1%) = ${formatCurrency(closedMonthTaxAmount)}`;
      }
      return `${formatCurrency(closedMonthProfitBeforeTax)} × ${formatNumber(selectedTaxRate)}% = ${formatCurrency(closedMonthTaxAmount)}`;
    }

    return `${formatCurrency(Math.max(closedMonthProfitBeforeTax, 0))} × ${formatNumber(selectedTaxRate)}% = ${formatCurrency(closedMonthTaxAmount)}`;
  })();
  const closedMonthOfferPeriodLabel = closedMonthSummary ? formatMonthPeriod(closedMonthSummary.month) : unitEconomicsSummary.period_label;
  const closedMonthOfferRows = closedMonthDetail?.offers ?? [];
  const currentMonthOrdersAmount =
    Number.isFinite(financeSummary.current_month_to_date.orders_amount)
      ? financeSummary.current_month_to_date.orders_amount
      : controlSummary.sales.month_to_date.ordered_revenue;
  const currentMonthDeltaOrdersAmount =
    Number.isFinite(financeSummary.current_month_to_date.delta_orders_amount)
      ? financeSummary.current_month_to_date.delta_orders_amount
      : controlSummary.sales.month_to_date.delta_ordered_revenue;
  const currentMonthPeriodLabel = formatPeriodLabel(controlSummary.sales.month_to_date.period_label);
  const currentMonthCompareLabel = formatPeriodLabel(controlSummary.sales.month_to_date.compare_period_label);
  const closedMonthOfferCards = closedMonthOfferRows.length > 0
    ? (() => {
        const normalized = closedMonthOfferRows
      .filter((item) => item.net_profit != null)
      .map((item: ClosedMonthOfferFinance) => ({
        offer_id: item.offer_id,
        title: item.title || 'Без названия',
        estimated_net_profit: Number(item.net_profit || 0),
        revenue: Number(item.revenue_amount || 0),
        units: Number(item.net_units || 0),
      }));

        return {
          profitable: [...normalized]
            .sort((a, b) => b.estimated_net_profit - a.estimated_net_profit)
            .filter((item) => item.estimated_net_profit > 0)
            .slice(0, 3),
          loss: [...normalized]
            .sort((a, b) => a.estimated_net_profit - b.estimated_net_profit)
            .filter((item) => item.estimated_net_profit < 0)
            .slice(0, 3),
        };
      })()
    : null;
  const profitableOffers = closedMonthOfferCards?.profitable ?? unitEconomicsSummary.top_profitable_offers.slice(0, 3);
  const lossOffers = closedMonthOfferCards?.loss ?? unitEconomicsSummary.top_loss_offers.slice(0, 3);
  const closedMonthPrimaryCards = [
    {
      label: 'Продано',
      value: `${formatNumber(closedMonthSoldUnits)} шт.`,
      description: 'Штук, которые Ozon закрыл как продажи в этом месяце.',
      info: formatCurrency(closedMonthSoldAmount),
      size: 'primary' as const,
    },
    {
      label: 'Возвраты',
      value: `${formatNumber(closedMonthReturnedUnits)} шт.`,
      description: 'Штук, которые Ozon закрыл как возвраты за тот же месяц.',
      info: formatCurrency(closedMonthReturnedAmount),
      tone: 'negative' as const,
      size: 'primary' as const,
    },
    {
      label: 'Выручка закрытого месяца',
      value: formatCurrency(closedMonthRevenueAmount),
      description: 'Реализация минус возвраты по закрытому месяцу.',
      info: revenueExplanation,
      size: 'primary' as const,
    },
    ...(hasFullCostCoverage
      ? [
          {
            label: 'Валовая прибыль',
            value: formatCurrency(closedMonthGrossProfit),
            description: 'Выручка без НДС минус себестоимость.',
            info: grossProfitExplanation,
            tone: closedMonthGrossProfit >= 0 ? ('default' as const) : ('negative' as const),
            size: 'primary' as const,
          },
          {
            label: 'Чистая прибыль',
            value: formatCurrency(closedMonthNetProfit),
            description: 'После всех расходов Ozon, корректировок и налога.',
            info: netProfitExplanation,
            tone: closedMonthNetProfit >= 0 ? ('positive' as const) : ('negative' as const),
            size: 'primary' as const,
          },
        ]
      : []),
  ];
  const closedMonthSecondaryCards = [
    ...(hasFullCostCoverage
      ? [
          {
            label: 'Себестоимость',
            value: formatCurrency(closedMonthCogs),
            description: 'Историческая себестоимость, которая действовала для этого месяца.',
          },
        ]
      : []),
    {
      label: 'Комиссия Ozon',
      value: formatCurrency(closedMonthOzonCommission),
      description: 'Начислено по продажам минус возврат комиссии по отчету реализации.',
      info: commissionExplanation,
    },
    {
      label: 'Логистика Ozon',
      value: formatCurrency(closedMonthOzonLogistics),
      description: 'Доставка и возвратная логистика Ozon за закрытый месяц.',
    },
    {
      label: 'Услуги Ozon',
      value: formatCurrency(closedMonthOzonServices),
      description: 'Сервисы Ozon за месяц, кроме логистики и эквайринга.',
    },
    {
      label: 'Эквайринг Ozon',
      value: formatCurrency(closedMonthOzonAcquiring),
      description: 'Комиссия эквайринга по monthly transactions Ozon.',
    },
    {
      label: 'Прочие расходы Ozon',
      value: formatCurrency(closedMonthOzonOtherExpenses),
      description: 'Редкие расходы Ozon вне комиссии, логистики, сервисов и эквайринга.',
    },
    {
      label: 'Бонусы и софинансирование',
      value: formatCurrency(closedMonthOzonIncentives),
      description: 'Доплаты Ozon и партнерское софинансирование за месяц.',
      info: incentivesExplanation,
      tone: closedMonthOzonIncentives >= 0 ? ('positive' as const) : ('negative' as const),
    },
    {
      label: 'Корректировки Ozon',
      value: formatSignedCurrency(closedMonthCorrections),
      description: 'Компенсации минус декомпенсации Ozon за закрытый месяц.',
      info: `${formatCurrency(closedMonthCompensation)} компенсаций - ${formatCurrency(closedMonthDecompensation)} декомпенсаций`,
      tone: closedMonthCorrections >= 0 ? ('positive' as const) : ('negative' as const),
    },
    ...(hasFullCostCoverage
      ? [
          {
            label: 'Прибыль до налога',
            value: formatCurrency(closedMonthProfitBeforeTax),
            description: 'После расходов и доплат Ozon, но до налога.',
            info: profitBeforeTaxExplanation,
            tone: closedMonthProfitBeforeTax >= 0 ? ('default' as const) : ('negative' as const),
          },
          {
            label: 'Налог',
            value: formatCurrency(closedMonthTaxAmount),
            description: 'Считаем по исторической налоговой схеме этого месяца.',
            info: taxExplanation,
          },
        ]
      : []),
  ];

  const cards = [
    {
      label: 'Поставок на сегодня',
      value: controlSummary.today_supplies,
      note:
        controlSummary.active_supplies > 0
          ? `Всего в работе: ${controlSummary.active_supplies}`
          : 'Сегодня новых поставок нет',
      icon: TruckIcon,
    },
    {
      label: 'Ждут приход',
      value: controlSummary.waiting_for_stock_supplies,
      note:
        controlSummary.waiting_for_stock_supplies > 0
          ? 'Этим поставкам не хватает прихода для резерва'
          : 'Все поставки можно резервировать без ожидания',
      icon: CubeIcon,
    },
    {
      label: 'Доступно на складе',
      value: controlSummary.stock.available_units,
      note: `В резерве: ${controlSummary.stock.reserved_units} шт`,
      icon: Squares2X2Icon,
    },
    {
      label: 'Заказано за 30 дней',
      value: activeStoreStock?.ordered_30d_units ?? controlSummary.stock.ordered_30d_units,
      note:
        controlSummary.sales.updated_at
          ? `Отчеты Ozon обновлены: ${formatDate(controlSummary.sales.updated_at)}`
          : 'Спрос по последним отчетам Ozon',
      icon: BuildingStorefrontIcon,
    },
  ];

  return (
      <ProtectedRoute>
        <Layout>
        <div className="space-y-6">
          <section className="overflow-hidden rounded-[32px] border border-sky-100 bg-[radial-gradient(circle_at_top_left,_rgba(255,255,255,0.95),_rgba(237,246,255,0.92)_35%,_rgba(254,242,242,0.92)_100%)] p-8 shadow-[0_24px_60px_rgba(15,23,42,0.08)]">
            <div className="flex flex-col gap-8 lg:flex-row lg:items-end lg:justify-between">
              <div className="max-w-3xl">
                <div className="inline-flex items-center gap-2 rounded-full border border-white/70 bg-white/80 px-4 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-sky-700">
                  <SparklesIcon className="h-4 w-4" />
                  Главное за сегодня
                </div>
                <div className="mt-4 inline-flex rounded-full border border-slate-200 bg-white/80 px-4 py-2 text-sm font-medium text-slate-700">
                  Магазин: {selectedStore?.name || 'Не выбран'}
                </div>
                <h1 className="mt-4 text-3xl font-semibold tracking-tight text-slate-950 sm:text-4xl">
                  Что важно по складу и поставкам.
                </h1>
                <p className="mt-3 max-w-2xl text-sm leading-6 text-slate-600 sm:text-base">
                  {heroText}
                </p>
              </div>

              <div className="grid gap-3 sm:grid-cols-2">
                <Link
                  href="/supplies"
                  className="rounded-2xl border border-slate-200 bg-white/85 px-5 py-4 text-sm font-medium text-slate-900 shadow-sm transition hover:-translate-y-0.5 hover:shadow-md"
                >
                  Открыть поставки
                  <div className="mt-1 text-xs text-slate-500">Статусы и детали поставок</div>
                </Link>
                <Link
                  href="/warehouse"
                  className="rounded-2xl border border-sky-200 bg-sky-600 px-5 py-4 text-sm font-medium text-white shadow-sm transition hover:-translate-y-0.5 hover:bg-sky-700"
                >
                  Открыть склад
                  <div className="mt-1 text-xs text-sky-100">Приходы, упаковка и остатки</div>
                </Link>
              </div>
            </div>
          </section>

          <section className="grid auto-rows-fr gap-4 md:grid-cols-2 xl:grid-cols-4">
            {cards.map((card) => (
              <article
                key={card.label}
                className="flex h-full flex-col rounded-3xl border border-white/70 bg-white/85 p-5 shadow-[0_16px_40px_rgba(15,23,42,0.06)] backdrop-blur"
              >
                <div className="flex items-start justify-between">
                  <div>
                    <p className="text-sm font-medium text-slate-500">{card.label}</p>
                    <p className="mt-3 text-3xl font-semibold tracking-tight text-slate-950">{card.value}</p>
                    <p className="mt-2 text-sm text-slate-500">{card.note}</p>
                  </div>
                  <div className="rounded-2xl bg-slate-950 p-3 text-white">
                    <card.icon className="h-6 w-6" />
                  </div>
                </div>
              </article>
            ))}
          </section>

          <section>
            <SyncFreshnessPanel
              storeIds={selectedStoreId ? [selectedStoreId] : []}
              kinds={['products', 'supplies', 'stocks', 'reports']}
              title="Когда обновлялись данные"
              description="Когда в последний раз обновлялись товары, поставки, остатки и отчеты."
            />
          </section>

          <section className="grid items-stretch gap-6 xl:grid-cols-2">
            <div className="flex h-full flex-col rounded-3xl border border-white/70 bg-white/85 p-6 shadow-[0_16px_40px_rgba(15,23,42,0.06)] backdrop-blur">
              <div className="flex flex-wrap items-center justify-between gap-4">
                <div>
                  <h2 className="text-lg font-semibold text-slate-950">Топ артикулов магазина</h2>
                  <p className="mt-1 text-sm text-slate-500">
                    Заказы за последние {controlSummary.sales.period_days} дней по активному магазину.
                  </p>
                </div>
                <div className="text-right text-xs text-slate-500">
                  Обновлено: {formatDate(activeStoreTopOffers?.updated_at || controlSummary.sales.updated_at)}
                </div>
              </div>

              <div className="mt-5 grid flex-1 gap-4">
                {activeStoreTopOffers?.items && activeStoreTopOffers.items.length > 0 ? (
                  activeStoreTopOffers.items.slice(0, 5).map((item, index) => (
                    <article key={`${item.offer_id}-${index}`} className="rounded-2xl border border-slate-200 bg-slate-50/70 p-5">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="text-xs uppercase tracking-[0.14em] text-slate-400">
                            #{index + 1} по заказам
                          </div>
                          <div className="mt-1 text-sm font-semibold text-slate-950">{item.offer_id}</div>
                          <div className="mt-1 text-sm text-slate-500">{item.title || 'Без названия'}</div>
                        </div>
                        <div className="text-right">
                          <div className="text-sm font-semibold text-slate-950">{formatNumber(item.units)} шт.</div>
                          <div className="mt-1 text-xs text-slate-500">{formatCurrency(item.revenue)}</div>
                        </div>
                      </div>
                    </article>
                  ))
                ) : (
                  <div className="rounded-2xl border border-dashed border-slate-200 p-6 text-sm text-slate-500">
                    Когда данные загрузятся, здесь появятся топ-артикулы по заказам.
                  </div>
                )}
              </div>
            </div>

            <div className="flex h-full flex-col rounded-3xl border border-white/70 bg-white/85 p-6 shadow-[0_16px_40px_rgba(15,23,42,0.06)] backdrop-blur">
              <div className="flex flex-wrap items-start justify-between gap-4">
                <div>
                  <h2 className="text-lg font-semibold text-slate-950">Контекст магазина</h2>
                  <p className="mt-1 text-sm text-slate-500">Какой склад закреплен за магазином и что видно по спросу.</p>
                </div>
                <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-right">
                  <div className="text-xs uppercase tracking-[0.14em] text-slate-500">Склад</div>
                  <div className="mt-2 text-sm font-medium text-slate-950">
                    {activeStoreStock?.warehouse_scope === 'shared' ? 'Общий' : 'Отдельный'}
                  </div>
                </div>
              </div>

              <div className="mt-5 grid flex-1 gap-4">
                <div className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
                  <div className="text-sm text-slate-500">Склад магазина</div>
                  <div className="mt-1 text-base font-semibold text-slate-950">
                    {activeStoreStock?.warehouse_name || 'Склад не найден'}
                  </div>
                  <div className="mt-1 text-xs text-slate-500">
                    {activeStoreStock?.warehouse_scope === 'shared'
                      ? 'Магазин работает через общий склад кабинета'
                      : 'Склад закреплен только за этим магазином'}
                  </div>
                  <div className="mt-2 text-xs text-slate-400">
                    Обновлено: {formatDate(activeStoreStock?.warehouse_updated_at || controlSummary.stock.updated_at)}
                  </div>
                </div>

                <div className="rounded-2xl border border-slate-200 bg-slate-50/70 p-4">
                  <div className="text-sm text-slate-500">Спрос по Ozon</div>
                  <div className="mt-3 grid gap-3 sm:grid-cols-3">
                    <div className="rounded-2xl bg-white p-3">
                      <div className="text-xs text-slate-400">Заказано за 30 дней</div>
                      <div className="mt-1 text-lg font-semibold text-slate-950">
                        {activeStoreStock?.ordered_30d_units ?? controlSummary.stock.ordered_30d_units}
                      </div>
                    </div>
                    <div className="rounded-2xl bg-white p-3">
                      <div className="text-xs text-slate-400">Доступно к продаже</div>
                      <div className="mt-1 text-lg font-semibold text-slate-950">{activeStoreStock?.ozon_available_units ?? 0}</div>
                    </div>
                    <div className="rounded-2xl bg-white p-3">
                      <div className="text-xs text-slate-400">Сейчас в пути</div>
                      <div className="mt-1 text-lg font-semibold text-slate-950">{activeStoreStock?.ozon_in_transit_units ?? 0}</div>
                    </div>
                  </div>
                  <div className="mt-3 text-xs text-slate-400">
                    Ozon: {formatDateOnlyRange(controlSummary.stock.orders_period_from, controlSummary.stock.orders_period_to)}
                    {controlSummary.stock.orders_updated_at ? ` · обновлено ${formatDate(controlSummary.stock.orders_updated_at)}` : ''}
                  </div>
                </div>

              </div>
            </div>
          </section>

          <section className="rounded-3xl border border-white/70 bg-white/85 p-6 shadow-[0_16px_40px_rgba(15,23,42,0.06)] backdrop-blur">
            <div className="flex flex-col gap-4 lg:flex-row lg:items-end lg:justify-between">
              <div>
                <h2 className="text-lg font-semibold text-slate-950">Деньги по активному магазину</h2>
                <p className="mt-1 text-sm text-slate-500">
                  Здесь только активный магазин. Сначала смотри, как идет текущий месяц, а ниже уже закрытый месяц по деньгам и артикулы за тот же период. Внутри блока больше нет смешения разных окон данных.
                </p>
                {selectedStore ? (
                  <div className="mt-3 flex flex-wrap gap-2">
                    <div className="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700">
                      НДС: {selectedVatOption?.label || 'Не задан'}
                    </div>
                    <div className="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700">
                      Налог: {selectedTaxOption?.label || 'Не задан'}
                    </div>
                    <div className="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700">
                      Ставка: {selectedTaxRate}%
                    </div>
                    {closedMonthSummary?.tax_effective_from_used ? (
                      <div className="inline-flex items-center rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700">
                        Схема действует с: {new Date(closedMonthSummary.tax_effective_from_used).toLocaleDateString('ru-RU')}
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </div>
              <Link
                href="/unit-economics-calculator"
                className="inline-flex items-center justify-center rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-medium text-slate-900 shadow-sm transition hover:-translate-y-0.5 hover:shadow-md"
              >
                Открыть калькулятор экономики
              </Link>
            </div>

            <div className="mt-5 space-y-5">
              <div>
                <div className="text-sm font-semibold text-slate-900">Текущий месяц: с 1 числа по сегодня</div>
                <div className="mt-3 grid gap-4 xl:grid-cols-2">
                  <article className="rounded-3xl border border-slate-200 bg-slate-50 p-4">
                    <div className="text-sm font-medium text-slate-500">Заказано</div>
                    {controlSummary.sales.month_to_date.available ? (
                      <>
                        <div className="mt-3 text-3xl font-semibold text-slate-950">{formatNumber(controlSummary.sales.month_to_date.ordered_units)} шт.</div>
                        <div className="mt-2 text-sm text-slate-500">{formatCurrency(currentMonthOrdersAmount)}</div>
                        <div className="mt-3 flex flex-wrap gap-2 text-xs">
                          <span className={`inline-flex rounded-full px-2.5 py-1 font-medium ${controlSummary.sales.month_to_date.delta_ordered_units >= 0 ? 'bg-emerald-50 text-emerald-700' : 'bg-rose-50 text-rose-700'}`}>
                            {formatSignedUnits(controlSummary.sales.month_to_date.delta_ordered_units)}
                          </span>
                          <span className={`inline-flex rounded-full px-2.5 py-1 font-medium ${currentMonthDeltaOrdersAmount >= 0 ? 'bg-emerald-50 text-emerald-700' : 'bg-rose-50 text-rose-700'}`}>
                            {formatSignedCurrency(currentMonthDeltaOrdersAmount)}
                          </span>
                        </div>
                      </>
                    ) : (
                  <>
                        <div className="mt-3 text-3xl font-semibold text-slate-950">—</div>
                        <div className="mt-2 text-sm text-slate-500">Сравнение появится, когда данные загрузятся полностью</div>
                      </>
                    )}
                    <div className="mt-2 text-xs text-slate-400">
                      {currentMonthPeriodLabel} · сравнение с {currentMonthCompareLabel}
                    </div>
                  </article>
                  <article className="rounded-3xl border border-slate-200 bg-slate-50 p-4">
                    <div className="text-sm font-medium text-slate-500">Возвращено</div>
                    {financeSummary.current_month_to_date.available && financeSummary.current_month_to_date.returned_units_available ? (
                      <>
                        <div className="mt-3 text-3xl font-semibold text-rose-700">
                          {formatNumber(financeSummary.current_month_to_date.returned_units ?? 0)} шт.
                        </div>
                        <div className="mt-2 text-sm text-slate-500">{formatCurrency(financeSummary.current_month_to_date.returns_amount)}</div>
                      </>
                    ) : financeSummary.current_month_to_date.available ? (
                      <>
                        <div className="mt-3 text-3xl font-semibold text-rose-700">{formatCurrency(financeSummary.current_month_to_date.returns_amount)}</div>
                        <div className="mt-2 text-sm text-slate-500">По финансовым данным за текущий месяц</div>
                      </>
                    ) : (
                      <>
                        <div className="mt-3 text-3xl font-semibold text-rose-700">—</div>
                        <div className="mt-2 text-sm text-slate-500">Сравнение появится, когда данные загрузятся полностью</div>
                      </>
                    )}
                    {financeSummary.current_month_to_date.available ? (
                      <div className="mt-3 flex flex-wrap gap-2 text-xs">
                        {financeSummary.current_month_to_date.returned_units_delta_available ? (
                          <span className={`inline-flex rounded-full px-2.5 py-1 font-medium ${(financeSummary.current_month_to_date.delta_returned_units ?? 0) <= 0 ? 'bg-emerald-50 text-emerald-700' : 'bg-rose-50 text-rose-700'}`}>
                            {formatSignedUnits(financeSummary.current_month_to_date.delta_returned_units ?? 0)}
                          </span>
                        ) : null}
                        <span className={`inline-flex rounded-full px-2.5 py-1 font-medium ${financeSummary.current_month_to_date.delta_returns_amount <= 0 ? 'bg-emerald-50 text-emerald-700' : 'bg-rose-50 text-rose-700'}`}>
                          {formatSignedCurrency(financeSummary.current_month_to_date.delta_returns_amount)}
                        </span>
                      </div>
                    ) : null}
                    <div className="mt-2 text-xs text-slate-400">
                      {currentMonthPeriodLabel} · сравнение с {currentMonthCompareLabel}
                    </div>
                  </article>
                </div>
              </div>

              <div>
                <div className="flex flex-wrap items-center justify-between gap-3">
                  <div className="text-sm font-semibold text-slate-900">
                    Закрытый месяц Ozon: {closedMonthOfferPeriodLabel}
                  </div>
                  <div className="flex flex-wrap items-center justify-end gap-2">
                    <Link
                      href="/closed-months"
                      className="inline-flex items-center rounded-full bg-white px-3 py-1 text-xs font-medium text-sky-700 transition hover:bg-sky-50 hover:text-sky-800"
                    >
                      История месяцев
                    </Link>
                    <div
                      className={`inline-flex items-center rounded-full px-3 py-1 text-xs font-medium ${
                        hasFullCostCoverage
                          ? 'bg-emerald-50 text-emerald-700'
                          : 'bg-amber-50 text-amber-800'
                      }`}
                    >
                      {hasFullCostCoverage
                          ? 'Покрытие себестоимости 100%'
                          : `Покрытие себестоимости ${formatPercent(closedMonthCoverageRatio)}`}
                    </div>
                  </div>
                </div>
                {!hasFullCostCoverage ? (
                  <div className="mt-3 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-950">
                    Пока не показываем прибыль, потому что себестоимость заполнена не полностью.
                    Сейчас заполнено: {formatPercent(closedMonthCoverageRatio)}.
                    <Link href="/cost-history" className="ml-1 font-medium text-amber-900 underline decoration-dotted underline-offset-2">
                      Открыть себестоимость
                    </Link>
                  </div>
                ) : null}
                <div className={`mt-3 grid gap-4 ${hasFullCostCoverage ? 'xl:grid-cols-5' : 'xl:grid-cols-3'}`}>
                  {closedMonthPrimaryCards.map((card, index) => (
                    <ClosedMonthMetricCard
                      key={card.label}
                      {...card}
                      infoAlign={metricInfoAlign(index, closedMonthPrimaryCards.length)}
                    />
                  ))}
                </div>
                <div className="mt-4 grid gap-3 xl:grid-cols-5">
                  {closedMonthSecondaryCards.map((card, index) => (
                    <ClosedMonthMetricCard
                      key={card.label}
                      {...card}
                      infoAlign={metricInfoAlign(index, closedMonthSecondaryCards.length)}
                    />
                  ))}
                </div>
              </div>
            </div>

            {hasFullCostCoverage ? (
            <div className="mt-5 grid gap-4 xl:grid-cols-2">
              <article className="rounded-3xl border border-emerald-200 bg-emerald-50/70 p-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <h3 className="text-sm font-semibold text-slate-950">Что продается выгодно</h3>
                    <p className="mt-1 text-xs text-slate-500">
                      Лучшие артикулы за период: {closedMonthOfferPeriodLabel}.
                    </p>
                  </div>
                  <div className="flex flex-wrap items-center justify-end gap-2">
                    <div className="inline-flex items-center rounded-full bg-white/80 px-3 py-1 text-xs font-medium text-slate-600">
                      {closedMonthOfferPeriodLabel}
                    </div>
                    <div
                      className={`inline-flex items-center rounded-full px-3 py-1 text-xs font-medium ${
                        hasFullCostCoverage
                          ? 'bg-emerald-100 text-emerald-700'
                          : 'bg-amber-100 text-amber-800'
                      }`}
                    >
                      {hasFullCostCoverage
                        ? 'Себестоимость заполнена'
                        : `Покрытие себестоимости ${formatPercent(closedMonthCoverageRatio)}`}
                    </div>
                  </div>
                </div>

                <div className="mt-4 space-y-3">
                  {closedMonthDetailLoading && closedMonthSummary ? (
                    <div className="rounded-2xl border border-dashed border-emerald-200 bg-white px-4 py-5 text-sm text-slate-500">
                      Загружаем артикулы закрытого месяца.
                    </div>
                  ) : profitableOffers.length > 0 ? profitableOffers.map((item) => (
                    <div key={`${item.offer_id}-profit`} className="rounded-2xl border border-emerald-200 bg-white px-4 py-3">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="truncate text-sm font-semibold text-slate-950">{item.offer_id}</div>
                          <div className="mt-1 truncate text-xs text-slate-500">{item.title || 'Без названия'}</div>
                        </div>
                        <div className="text-right">
                          <div className="text-sm font-semibold text-emerald-700">{formatCurrency(item.estimated_net_profit)}</div>
                          <div className="mt-1 text-xs text-slate-500">{item.units} шт. • {formatCurrency(item.revenue)}</div>
                        </div>
                      </div>
                    </div>
                  )) : (
                    <div className="rounded-2xl border border-dashed border-emerald-200 bg-white px-4 py-5 text-sm text-slate-500">
                      Пока нет артикулов, по которым можно уверенно показать прибыль.
                      {!hasFullCostCoverage ? ' Сначала стоит заполнить себестоимость по оставшимся вариациям.' : ''}
                    </div>
                  )}
                </div>
              </article>

              <article className="rounded-3xl border border-rose-200 bg-rose-50/70 p-4">
                <div className="flex items-center justify-between gap-3">
                  <div>
                    <h3 className="text-sm font-semibold text-slate-950">Что сейчас тянет вниз</h3>
                    <p className="mt-1 text-xs text-slate-500">
                      Слабые или убыточные артикулы за период: {closedMonthOfferPeriodLabel}.
                    </p>
                  </div>
                  <div className="flex flex-wrap items-center justify-end gap-2">
                    <div className="inline-flex items-center rounded-full bg-white/80 px-3 py-1 text-xs font-medium text-slate-600">
                      {closedMonthOfferPeriodLabel}
                    </div>
                    <div
                      className={`inline-flex items-center rounded-full px-3 py-1 text-xs font-medium ${
                        hasFullCostCoverage
                          ? 'bg-rose-100 text-rose-700'
                          : 'bg-amber-100 text-amber-800'
                      }`}
                    >
                      {hasFullCostCoverage
                        ? 'Себестоимость заполнена'
                        : `Покрытие себестоимости ${formatPercent(closedMonthCoverageRatio)}`}
                    </div>
                    <div className="text-xs text-slate-500">
                      Возвраты: {closedMonthSoldAmount > 0 ? formatPercent(closedMonthReturnedAmount / closedMonthSoldAmount) : '—'}
                    </div>
                  </div>
                </div>

                <div className="mt-4 space-y-3">
                  {closedMonthDetailLoading && closedMonthSummary ? (
                    <div className="rounded-2xl border border-dashed border-rose-200 bg-white px-4 py-5 text-sm text-slate-500">
                      Загружаем артикулы закрытого месяца.
                    </div>
                  ) : lossOffers.length > 0 ? lossOffers.map((item) => (
                    <div key={`${item.offer_id}-loss`} className="rounded-2xl border border-rose-200 bg-white px-4 py-3">
                      <div className="flex items-start justify-between gap-3">
                        <div className="min-w-0">
                          <div className="truncate text-sm font-semibold text-slate-950">{item.offer_id}</div>
                          <div className="mt-1 truncate text-xs text-slate-500">{item.title || 'Без названия'}</div>
                        </div>
                        <div className="text-right">
                          <div className="text-sm font-semibold text-rose-700">{formatCurrency(item.estimated_net_profit)}</div>
                          <div className="mt-1 text-xs text-slate-500">{item.units} шт. • {formatCurrency(item.revenue)}</div>
                        </div>
                      </div>
                    </div>
                  )) : (
                    <div className="rounded-2xl border border-dashed border-rose-200 bg-white px-4 py-5 text-sm text-slate-500">
                      Явно убыточных артикулов по текущим данным не видно.
                    </div>
                  )}
                </div>
              </article>
            </div>
            ) : (
              <div className="mt-5 rounded-3xl border border-slate-200 bg-white px-5 py-6 text-sm text-slate-600">
                Пока себестоимость закрыта не полностью, не показываем блоки с выгодными и убыточными артикулами.
                Они появятся автоматически после заполнения себестоимости по всем проданным вариациям месяца.
                <Link href="/cost-history" className="ml-1 font-medium text-sky-700 underline decoration-dotted underline-offset-2">
                  Заполнить себестоимость
                </Link>
              </div>
            )}
          </section>

          <section>
            <div className="flex h-full flex-col rounded-3xl border border-white/70 bg-white/85 p-6 shadow-[0_16px_40px_rgba(15,23,42,0.06)] backdrop-blur">
              <div className="flex items-center justify-between">
                <div>
                  <h2 className="text-lg font-semibold text-slate-950">Где сейчас поставки</h2>
                  <p className="mt-1 text-sm text-slate-500">Сколько поставок сейчас находится в каждом статусе.</p>
                </div>
                <Link href="/supplies" className="text-sm font-medium text-sky-700 hover:text-sky-800">
                  Смотреть все поставки
                </Link>
              </div>

              <div className="mt-5 grid gap-3 md:grid-cols-2 2xl:grid-cols-4">
                {statusEntries.length > 0 ? (
                  statusEntries.map(([status, value]) => (
                    <div key={status} className="min-w-0 rounded-2xl border border-slate-100 bg-slate-50/90 p-4">
                      <div
                        className="inline-flex max-w-full rounded-full border px-3 py-1 text-xs font-medium"
                        style={getSupplyStatusStyle(status)}
                      >
                        {getSupplyStatusLabel(status)}
                      </div>
                      <div className="mt-4 text-3xl font-semibold text-slate-950">{value}</div>
                    </div>
                  ))
                ) : (
                  <div className="rounded-2xl border border-dashed border-slate-200 p-6 text-sm text-slate-500">
                    Пока нет поставок.
                  </div>
                )}
              </div>

              {controlSummary.waiting_for_stock_supplies > 0 && (
                <div className="mt-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
                  <span className="font-medium">Ожидают внесения прихода:</span>{' '}
                  {controlSummary.waiting_for_stock_supplies}. Резервирование возобновится автоматически после прихода.
                </div>
              )}
            </div>
          </section>


        </div>
      </Layout>
    </ProtectedRoute>
  );
}
