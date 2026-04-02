'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import axios from 'axios';
import Link from 'next/link';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { ClosedMonthMetricCard } from '@/components/ClosedMonthMetricCard';
import { ExportJobCard } from '@/components/ExportJobCard';
import { Layout } from '@/components/Layout';
import {
  closedMonthsAPI,
  type ClosedMonthFinance,
  type ClosedMonthFinanceDetail,
  type ClosedMonthOfferFinance,
} from '@/lib/api/closedMonths';
import { storesAPI, type StoreSyncKindStatus } from '@/lib/api/stores';
import type { ExportJobStatus } from '@/lib/api/warehouse';
import { useAuth } from '@/lib/context/AuthContext';
import { useStoreContext } from '@/lib/context/StoreContext';

type MetricKey = 'sold_units' | 'revenue_amount' | 'gross_profit' | 'net_profit';
type ChartRangeMonths = 1 | 3 | 6 | 12;
type ChartDisplayMode = 'lines' | 'bars';
type SkuFilter = 'all' | 'grew' | 'declined' | 'loss' | 'no_cost';
type MetricOption = {
  key: MetricKey;
  label: string;
  format: 'units' | 'currency';
  color: string;
  softClass: string;
  activeClass: string;
  requiresFullCoverage?: boolean;
};

const METRIC_OPTIONS: MetricOption[] = [
  {
    key: 'sold_units',
    label: 'Продано',
    format: 'units',
    color: '#0284c7',
    softClass: 'bg-sky-50 text-sky-700',
    activeClass: 'border-sky-300 bg-sky-600 text-white',
  },
  {
    key: 'revenue_amount',
    label: 'Выручка',
    format: 'currency',
    color: '#0f766e',
    softClass: 'bg-teal-50 text-teal-700',
    activeClass: 'border-teal-300 bg-teal-600 text-white',
  },
  {
    key: 'gross_profit',
    label: 'Валовая прибыль',
    format: 'currency',
    color: '#7c3aed',
    softClass: 'bg-violet-50 text-violet-700',
    activeClass: 'border-violet-300 bg-violet-600 text-white',
    requiresFullCoverage: true,
  },
  {
    key: 'net_profit',
    label: 'Чистая прибыль',
    format: 'currency',
    color: '#dc2626',
    softClass: 'bg-rose-50 text-rose-700',
    activeClass: 'border-rose-300 bg-rose-600 text-white',
    requiresFullCoverage: true,
  },
];

const CHART_RANGE_OPTIONS: Array<{ value: ChartRangeMonths; label: string }> = [
  { value: 1, label: '1 месяц' },
  { value: 3, label: '3 месяца' },
  { value: 6, label: '6 месяцев' },
  { value: 12, label: '1 год' },
];

function formatCurrency(value: number) {
  return new Intl.NumberFormat('ru-RU', {
    style: 'currency',
    currency: 'RUB',
    maximumFractionDigits: 0,
  }).format(value);
}

function formatNumber(value: number) {
  return new Intl.NumberFormat('ru-RU').format(value);
}

function formatPercent(value: number) {
  return new Intl.NumberFormat('ru-RU', {
    style: 'percent',
    maximumFractionDigits: 1,
  }).format(value);
}

function formatMetricValue(value: number, format: 'units' | 'currency') {
  return format === 'units' ? `${formatNumber(value)} шт.` : formatCurrency(value);
}

function formatSignedCurrency(value: number) {
  const abs = formatCurrency(Math.abs(value));
  if (value > 0) return `+${abs}`;
  if (value < 0) return `-${abs}`;
  return abs;
}

function monthLabel(month: string) {
  const [year, monthNumber] = month.split('-').map(Number);
  const date = new Date(year, (monthNumber || 1) - 1, 1);
  return date.toLocaleDateString('ru-RU', {
    month: 'long',
    year: 'numeric',
  });
}

function statusLabel(status: string) {
  if (status === 'ready') return 'Готов';
  if (status === 'needs_cost') return 'Нужна себестоимость';
  if (status === 'ozon_warning') return 'Ограничения Ozon';
  if (status === 'partial') return 'Требует внимания';
  if (status === 'cancelled') return 'Остановлено';
  if (status === 'pending') return 'Ждем отчет';
  if (status === 'error') return 'Ошибка';
  return status;
}

function statusClass(status: string) {
  if (status === 'ready') return 'bg-emerald-50 text-emerald-700';
  if (status === 'needs_cost') return 'bg-amber-50 text-amber-800';
  if (status === 'ozon_warning') return 'bg-sky-50 text-sky-700';
  if (status === 'partial') return 'bg-amber-50 text-amber-800';
  if (status === 'cancelled') return 'bg-slate-100 text-slate-700';
  if (status === 'pending') return 'bg-sky-50 text-sky-700';
  return 'bg-rose-50 text-rose-700';
}

function monthWarningsSummary(item: ClosedMonthFinance | null | undefined) {
  const payload = (item?.source_payload ?? null) as Record<string, unknown> | null;
  const summary = (payload?.warnings_summary ?? null) as Record<string, unknown> | null;

  if (summary) {
    return {
      critical: Array.isArray(summary.critical) ? (summary.critical as Array<Record<string, unknown>>) : [],
      informational: Array.isArray(summary.informational)
        ? (summary.informational as Array<Record<string, unknown>>)
        : [],
    };
  }

  const adjustments = (payload?.adjustments ?? null) as Record<string, unknown> | null;
  const transactions = (payload?.transactions ?? null) as Record<string, unknown> | null;
  const rawAdjustmentWarnings = Array.isArray(adjustments?.warnings)
    ? (adjustments?.warnings as Array<Record<string, unknown>>)
    : [];
  const rawTransactionWarnings = Array.isArray(transactions?.warnings)
    ? (transactions?.warnings as Array<Record<string, unknown>>)
    : [];

  const informational = rawAdjustmentWarnings.filter((warning) => {
    const kind = String(warning.kind || '').trim().toLowerCase();
    return Boolean(warning.missing_document) && (kind === 'compensation' || kind === 'decompensation');
  });
  const critical = [
    ...rawAdjustmentWarnings.filter((warning) => !informational.includes(warning)),
    ...rawTransactionWarnings,
  ];

  return { critical, informational };
}

function effectiveMonthStatus(item: ClosedMonthFinance | null | undefined) {
  if (!item) return 'pending';
  if (item.status === 'error') return 'error';
  if (item.status === 'pending') return 'pending';
  if (Number(item.coverage_ratio || 0) < 0.9999) return 'needs_cost';
  const warnings = monthWarningsSummary(item);
  if (warnings.critical.length > 0) return 'ozon_warning';
  return 'ready';
}

function hasCriticalOzonWarnings(item: ClosedMonthFinance | null | undefined) {
  return monthWarningsSummary(item).critical.length > 0;
}

function humanizeMonthWarning(warning: Record<string, unknown>) {
  const kind = String(warning.kind || '').trim().toLowerCase();
  const error = humanizeClosedMonthsMessage(String(warning.error || '').trim());
  if (kind === 'decompensation' || kind === 'compensation') {
    return error;
  }
  return error;
}

function metricInfoAlign(index: number, total: number): 'start' | 'end' {
  const startCount = total <= 3 ? 1 : 2;
  return index < startCount ? 'start' : 'end';
}

function createScale(values: number[], top: number, bottom: number, height: number) {
  const maxPositive = Math.max(...values, 0);
  const minNegative = Math.min(...values, 0);
  const range = Math.max(maxPositive - minNegative, 1);
  const plotHeight = height - top - bottom;
  const baselineY = top + (maxPositive / range) * plotHeight;

  return {
    baselineY,
    maxPositive,
    minNegative,
    map(value: number) {
      return top + ((maxPositive - value) / range) * plotHeight;
    },
  };
}

function humanizeClosedMonthsMessage(message: string | null | undefined) {
  const text = String(message || '').trim();
  if (!text) {
    return 'Ждем статус выгрузки истории закрытых месяцев.';
  }
  if (text.includes('/v2/finance/realization') && text.includes('404')) {
    return 'Ozon не отдал отчет реализации за один из месяцев. Остальные месяцы продолжаем загружать.';
  }
  if (text.includes('/v2/finance/realization') && text.includes('403')) {
    return 'Ozon не дал доступ к отчету реализации за один из месяцев.';
  }
  if (text.includes('/v2/finance/realization') && text.includes('500')) {
    return 'Ozon временно не отдал отчет реализации. Можно попробовать выгрузку чуть позже.';
  }
  if (text.includes('История закрытых месяцев загружена частично')) {
    return 'Часть месяцев загрузилась, но по одному или нескольким месяцам Ozon не отдал отчет.';
  }
  return text;
}

function vatModeLabel(value: string | null) {
  if (!value) {
    return 'Не задан';
  }
  if (value === 'none') return 'Без НДС';
  if (value === 'usn_5' || value === 'usn_vat_5') return 'УСН + НДС 5%';
  if (value === 'usn_7' || value === 'usn_vat_7') return 'УСН + НДС 7%';
  if (value === 'osno_22' || value === 'vat_20') return 'НДС 20%';
  if (value === 'osno_10' || value === 'vat_10') return 'НДС 10%';
  return value;
}

function taxModeLabel(value: string | null) {
  if (!value) {
    return 'Не задана';
  }
  if (value === 'before_tax') return 'До налога';
  if (value === 'usn_income') return 'УСН доходы';
  if (value === 'usn_income_expenses') return 'УСН доходы минус расходы';
  return value;
}

function closedMonthErrorReason(item: ClosedMonthFinance | null | undefined) {
  const payload = (item?.source_payload ?? null) as Record<string, unknown> | null;
  const rawError = typeof payload?.error === 'string' ? payload.error : '';
  const rawLastSyncError = typeof payload?.last_sync_error === 'string' ? payload.last_sync_error : '';
  const message = rawError || rawLastSyncError;
  if (!message) {
    return null;
  }
  return humanizeClosedMonthsMessage(message);
}

function deltaClass(value: number) {
  if (value > 0) return 'text-emerald-700';
  if (value < 0) return 'text-rose-700';
  return 'text-slate-500';
}

function formatDelta(value: number, format: 'units' | 'currency') {
  if (!Number.isFinite(value) || value === 0) {
    return format === 'units' ? '0 шт.' : '0 ₽';
  }
  const sign = value > 0 ? '+' : '−';
  const absValue = Math.abs(value);
  return format === 'units'
    ? `${sign}${formatNumber(absValue)} шт.`
    : `${sign}${formatCurrency(absValue).replace('-', '')}`;
}

function MetricChart({
  months,
  metrics,
  displayMode,
}: {
  months: ClosedMonthFinance[];
  metrics: MetricOption[];
  displayMode: ChartDisplayMode;
}) {
  const width = 960;
  const height = 300;
  const paddingLeft = 74;
  const paddingRight = 74;
  const top = 24;
  const bottom = 48;
  const step =
    months.length > 1 ? (width - paddingLeft - paddingRight) / (months.length - 1) : 0;
  const currencyMetrics = metrics.filter((item) => item.format === 'currency');
  const unitMetrics = metrics.filter((item) => item.format === 'units');
  const currencyScale =
    currencyMetrics.length > 0
      ? createScale(
          currencyMetrics.flatMap((metric) => months.map((item) => Number(item[metric.key] || 0))),
          top,
          bottom,
          height
        )
      : null;
  const unitScale =
    unitMetrics.length > 0
      ? createScale(
          unitMetrics.flatMap((metric) => months.map((item) => Number(item[metric.key] || 0))),
          top,
          bottom,
          height
        )
      : null;
  const [hoveredMonth, setHoveredMonth] = useState<string | null>(months[months.length - 1]?.month ?? null);

  const series = metrics.map((metric) => {
    const scale = metric.format === 'units' ? unitScale : currencyScale;
    const points = months.map((item, index) => {
      const value = Number(item[metric.key] || 0);
      return {
        month: item.month,
        value,
        x: paddingLeft + step * index,
        y: scale ? scale.map(value) : height / 2,
      };
    });
    const path = points
      .map((point, index) => `${index === 0 ? 'M' : 'L'} ${point.x.toFixed(1)} ${point.y.toFixed(1)}`)
      .join(' ');
    return { metric, points, path };
  });
  const seriesByKey = new Map(series.map((item) => [item.metric.key, item]));

  const axisTicks = [0, 0.5, 1];
  const hoveredMonthValue = hoveredMonth && months.some((item) => item.month === hoveredMonth) ? hoveredMonth : months[months.length - 1]?.month ?? null;
  const hoveredIndex = hoveredMonthValue ? months.findIndex((item) => item.month === hoveredMonthValue) : -1;
  const hoveredX = hoveredIndex >= 0 ? paddingLeft + step * hoveredIndex : null;
  const tooltipMonth = hoveredIndex >= 0 ? months[hoveredIndex] : null;
  const tooltipMetrics = tooltipMonth
    ? metrics.map((metric) => ({
        metric,
        value: Number(tooltipMonth[metric.key] || 0),
      }))
    : [];
  const tooltipWidth = 220;
  const tooltipHeight = 48 + tooltipMetrics.length * 22;
  const tooltipX =
    hoveredX == null
      ? paddingLeft
      : Math.min(
          Math.max(hoveredX - tooltipWidth / 2, paddingLeft),
          width - paddingRight - tooltipWidth
        );
  const tooltipY = top + 10;
  const renderAxisLabel = (ratio: number, scale: ReturnType<typeof createScale> | null, format: 'units' | 'currency') => {
    if (!scale) {
      return '';
    }
    const value = scale.maxPositive - (scale.maxPositive - scale.minNegative) * ratio;
    return formatMetricValue(value, format);
  };

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap gap-2">
        {metrics.map((metric) => (
          <div
            key={metric.key}
            className={`inline-flex items-center gap-2 rounded-full px-3 py-1 text-xs font-medium ${metric.softClass}`}
          >
            <span className="h-2.5 w-2.5 rounded-full" style={{ backgroundColor: metric.color }} />
            {metric.label}
          </div>
        ))}
      </div>
      <svg viewBox={`0 0 ${width} ${height}`} className="h-80 w-full">
        {axisTicks.map((ratio) => {
          const y = top + ratio * (height - top - bottom);
          return (
            <line
              key={`grid-${ratio}`}
              x1={paddingLeft}
              y1={y}
              x2={width - paddingRight}
              y2={y}
              stroke="#e2e8f0"
              strokeWidth="1"
            />
          );
        })}

        {currencyScale ? (
          <>
            <line
              x1={paddingLeft}
              y1={currencyScale.baselineY}
              x2={width - paddingRight}
              y2={currencyScale.baselineY}
              stroke="#cbd5e1"
              strokeWidth="1.5"
              strokeDasharray="6 4"
            />
            {axisTicks.map((ratio) => {
              const y = top + ratio * (height - top - bottom);
              return (
                <text
                  key={`currency-axis-${ratio}`}
                  x={width - paddingRight + 8}
                  y={y + 4}
                  fontSize="11"
                  fill="#64748b"
                >
                  {renderAxisLabel(ratio, currencyScale, 'currency')}
                </text>
              );
            })}
          </>
        ) : null}

        {unitScale ? (
          <>
            <line
              x1={paddingLeft}
              y1={unitScale.baselineY}
              x2={width - paddingRight}
              y2={unitScale.baselineY}
              stroke="#bae6fd"
              strokeWidth="1"
              strokeDasharray="4 4"
            />
            {axisTicks.map((ratio) => {
              const y = top + ratio * (height - top - bottom);
              return (
                <text
                  key={`units-axis-${ratio}`}
                  x={8}
                  y={y + 4}
                  fontSize="11"
                  fill="#64748b"
                >
                  {renderAxisLabel(ratio, unitScale, 'units')}
                </text>
              );
            })}
          </>
        ) : null}

        {displayMode === 'lines'
          ? series.map(({ metric, points, path }) => (
              <g key={metric.key}>
                <path
                  d={path}
                  fill="none"
                  stroke={metric.color}
                  strokeWidth="3"
                  strokeLinecap="round"
                  strokeLinejoin="round"
                />
                {points.map((point) => (
                  <circle
                    key={`${metric.key}-${point.month}`}
                    cx={point.x}
                    cy={point.y}
                    r="4"
                    fill={metric.color}
                    stroke="#ffffff"
                    strokeWidth="2"
                  />
                ))}
              </g>
            ))
          : months.map((month, monthIndex) => {
              const monthX = paddingLeft + step * monthIndex;
              const groupWidth = Math.max(Math.min(step * 0.72, 56), 24);
              const barWidth = Math.max(groupWidth / Math.max(metrics.length, 1) - 4, 8);
              const groupStartX = monthX - groupWidth / 2;

              return (
                <g key={`bars-${month.month}`}>
                  {metrics.map((metric, metricIndex) => {
                    const point = seriesByKey.get(metric.key)?.points[monthIndex];
                    const scale = metric.format === 'units' ? unitScale : currencyScale;
                    const baselineY = scale?.baselineY ?? height - bottom;
                    const y = point?.y ?? baselineY;
                    const barX = groupStartX + metricIndex * (barWidth + 4);
                    const barHeight = Math.max(Math.abs(baselineY - y), 2);
                    const barY = Math.min(y, baselineY);
                    return (
                      <rect
                        key={`${metric.key}-${month.month}`}
                        x={barX}
                        y={barY}
                        width={barWidth}
                        height={barHeight}
                        rx="8"
                        fill={metric.color}
                        opacity="0.92"
                      />
                    );
                  })}
                </g>
              );
            })}

        {months.map((item, index) => {
          const x = paddingLeft + step * index;
          const nextX =
            index < months.length - 1 ? paddingLeft + step * (index + 1) : width - paddingRight;
          const zoneWidth = months.length === 1 ? width - paddingLeft - paddingRight : Math.max(nextX - x, 36);
          return (
            <rect
              key={`hover-zone-${item.month}`}
              x={months.length === 1 ? paddingLeft : x - zoneWidth / 2}
              y={top}
              width={months.length === 1 ? width - paddingLeft - paddingRight : zoneWidth}
              height={height - top - bottom}
              fill="transparent"
              onMouseEnter={() => setHoveredMonth(item.month)}
            />
          );
        })}

        {hoveredX != null ? (
          <line
            x1={hoveredX}
            y1={top}
            x2={hoveredX}
            y2={height - bottom}
            stroke="#94a3b8"
            strokeWidth="1.5"
            strokeDasharray="4 4"
          />
        ) : null}

        {tooltipMonth ? (
          <g>
            <rect
              x={tooltipX}
              y={tooltipY}
              width={tooltipWidth}
              height={tooltipHeight}
              rx="16"
              fill="#0f172a"
              opacity="0.96"
            />
            <text x={tooltipX + 14} y={tooltipY + 22} fontSize="12" fontWeight="600" fill="#ffffff">
              {monthLabel(tooltipMonth.month)}
            </text>
            {tooltipMetrics.map((item, index) => (
              <g key={`tooltip-${item.metric.key}`}>
                <circle
                  cx={tooltipX + 18}
                  cy={tooltipY + 40 + index * 22}
                  r="4"
                  fill={item.metric.color}
                />
                <text x={tooltipX + 30} y={tooltipY + 44 + index * 22} fontSize="11" fill="#e2e8f0">
                  {item.metric.label}: {formatMetricValue(item.value, item.metric.format)}
                </text>
              </g>
            ))}
          </g>
        ) : null}

        {months.map((item, index) => {
          const x = paddingLeft + step * index;
          return (
            <text
              key={`month-label-${item.month}`}
              x={x}
              y={height - 14}
              textAnchor="middle"
              fontSize="12"
              fill="#64748b"
            >
              {item.month.slice(2)}
            </text>
          );
        })}
      </svg>
    </div>
  );
}

export default function ClosedMonthsPage() {
  const { user } = useAuth();
  const { selectedStore, selectedStoreId } = useStoreContext();
  const [months, setMonths] = useState<ClosedMonthFinance[]>([]);
  const [selectedMonth, setSelectedMonth] = useState<string | null>(null);
  const [selectedYear, setSelectedYear] = useState<string>('all');
  const [detail, setDetail] = useState<ClosedMonthFinanceDetail | null>(null);
  const [previousMonthOffers, setPreviousMonthOffers] = useState<ClosedMonthOfferFinance[]>([]);
  const [selectedMetricKeys, setSelectedMetricKeys] = useState<MetricKey[]>(['revenue_amount']);
  const [chartRangeMonths, setChartRangeMonths] = useState<ChartRangeMonths>(6);
  const [chartDisplayMode, setChartDisplayMode] = useState<ChartDisplayMode>('lines');
  const [skuFilter, setSkuFilter] = useState<SkuFilter>('all');
  const [loading, setLoading] = useState(true);
  const [detailLoading, setDetailLoading] = useState(false);
  const [syncing, setSyncing] = useState<number | null>(null);
  const [closedMonthsSync, setClosedMonthsSync] = useState<StoreSyncKindStatus | null>(null);
  const [exportStatus, setExportStatus] = useState<ExportJobStatus | null>(null);
  const [exportYear, setExportYear] = useState<number | null>(null);
  const [isStartingExport, setIsStartingExport] = useState(false);
  const [isDownloadingExport, setIsDownloadingExport] = useState(false);
  const [isClearingExport, setIsClearingExport] = useState(false);

  const selectedMetrics = METRIC_OPTIONS.filter((item) => selectedMetricKeys.includes(item.key));
  const canSeeTechnicalWarnings = Boolean(user?.is_admin);
  const chartMonths = useMemo(() => {
    const limit = Math.max(1, chartRangeMonths);
    return [...months.slice(0, limit)].reverse();
  }, [chartRangeMonths, months]);
  const availableYears = useMemo(() => {
    return Array.from(new Set(months.map((item) => item.month.slice(0, 4)))).sort((a, b) => Number(b) - Number(a));
  }, [months]);
  const visibleMonths = useMemo(() => {
    if (selectedYear === 'all') {
      return months;
    }
    return months.filter((item) => item.month.startsWith(`${selectedYear}-`));
  }, [months, selectedYear]);

  const loadMonths = useCallback(async () => {
    if (!selectedStoreId) {
      setMonths([]);
      setSelectedMonth(null);
      setDetail(null);
      setLoading(false);
      return;
    }

    setLoading(true);
    try {
      const data = await closedMonthsAPI.list(selectedStoreId, 24);
      setMonths(data);
      const initialMonth = data[0]?.month ?? null;
      setSelectedMonth((current) => current && data.some((item) => item.month === current) ? current : initialMonth);
    } catch {
      setMonths([]);
      setSelectedMonth(null);
      toast.error('Не удалось загрузить историю закрытых месяцев');
    } finally {
      setLoading(false);
    }
  }, [selectedStoreId]);

  useEffect(() => {
    void loadMonths();
  }, [loadMonths]);

  useEffect(() => {
    setSelectedYear('all');
  }, [selectedStoreId]);

  useEffect(() => {
    const nextYear = availableYears[0] ? Number(availableYears[0]) : null;
    setExportYear(nextYear);
  }, [availableYears, selectedStoreId]);

  useEffect(() => {
    if (selectedYear !== 'all' && !availableYears.includes(selectedYear)) {
      setSelectedYear('all');
    }
  }, [availableYears, selectedYear]);

  useEffect(() => {
    let cancelled = false;
    if (!selectedStoreId) {
      setExportStatus(null);
      return;
    }
    setExportStatus(null);
    void closedMonthsAPI.getExportStatus(selectedStoreId)
      .then((status) => {
        if (!cancelled) {
          setExportStatus(status);
        }
      })
      .catch(() => {});
    return () => {
      cancelled = true;
    };
  }, [selectedStoreId]);

  useEffect(() => {
    if (!selectedStoreId || !exportStatus || !['queued', 'running'].includes(exportStatus.status)) {
      return;
    }
    const timer = window.setInterval(async () => {
      try {
        const status = await closedMonthsAPI.getExportStatus(selectedStoreId);
        setExportStatus(status);
      } catch {}
    }, 2000);
    return () => window.clearInterval(timer);
  }, [exportStatus, selectedStoreId]);

  useEffect(() => {
    if (visibleMonths.length === 0) {
      return;
    }
    if (!selectedMonth || !visibleMonths.some((item) => item.month === selectedMonth)) {
      setSelectedMonth(visibleMonths[0].month);
    }
  }, [selectedMonth, visibleMonths]);

  useEffect(() => {
    const loadDetail = async () => {
      if (!selectedStoreId || !selectedMonth) {
        setDetail(null);
        return;
      }

      setDetailLoading(true);
      try {
        const data = await closedMonthsAPI.get(selectedStoreId, selectedMonth);
        setDetail(data);
      } catch {
        setDetail(null);
        toast.error('Не удалось загрузить детали месяца');
      } finally {
        setDetailLoading(false);
      }
    };

    void loadDetail();
  }, [selectedMonth, selectedStoreId]);

  useEffect(() => {
    const loadPreviousMonthOffers = async () => {
      if (!selectedStoreId || !selectedMonth) {
        setPreviousMonthOffers([]);
        return;
      }

      const currentIndex = months.findIndex((item) => item.month === selectedMonth);
      const previousMonth = currentIndex >= 0 ? months[currentIndex + 1]?.month ?? null : null;
      if (!previousMonth) {
        setPreviousMonthOffers([]);
        return;
      }

      try {
        const offers = await closedMonthsAPI.getOffers(selectedStoreId, previousMonth);
        setPreviousMonthOffers(offers);
      } catch {
        setPreviousMonthOffers([]);
      }
    };

    void loadPreviousMonthOffers();
  }, [months, selectedMonth, selectedStoreId]);

  const loadClosedMonthsSyncStatus = useCallback(async (force = false) => {
    if (!selectedStoreId) {
      setClosedMonthsSync(null);
      return null;
    }
    try {
      const status = await storesAPI.getSyncStatus(selectedStoreId, { force });
      const next = (status.sync_kinds?.closed_months as StoreSyncKindStatus | undefined) ?? null;
      setClosedMonthsSync(next);
      return next;
    } catch {
      return null;
    }
  }, [selectedStoreId]);

  useEffect(() => {
    void loadClosedMonthsSyncStatus(true);
  }, [loadClosedMonthsSyncStatus]);

  useEffect(() => {
    if (!selectedStoreId) {
      return;
    }
    const isActive = closedMonthsSync?.status === 'queued' || closedMonthsSync?.status === 'running';
    if (!isActive) {
      return;
    }
    const timer = window.setInterval(() => {
      void loadClosedMonthsSyncStatus(true);
    }, 3000);
    return () => window.clearInterval(timer);
  }, [closedMonthsSync?.status, loadClosedMonthsSyncStatus, selectedStoreId]);

  useEffect(() => {
    if (closedMonthsSync?.status === 'success') {
      void loadMonths();
    }
  }, [closedMonthsSync?.finished_at, closedMonthsSync?.status, loadMonths]);

  const requestSync = useCallback(async (options: { monthsBack?: number }) => {
    if (!selectedStoreId) {
      return;
    }
    if (closedMonthsSync?.status === 'queued' || closedMonthsSync?.status === 'running') {
      toast('Сначала дождись завершения текущей выгрузки истории');
      return;
    }
    const monthsBack = options.monthsBack ?? 3;
    setSyncing(monthsBack);
    try {
      await closedMonthsAPI.sync(selectedStoreId, { monthsBack });
      await loadClosedMonthsSyncStatus(true);
      toast.success(
        monthsBack <= 3
          ? 'Поставил в очередь загрузку последних 3 месяцев'
          : monthsBack <= 6
            ? 'Поставил в очередь загрузку последних 6 месяцев'
            : monthsBack <= 12
              ? 'Поставил в очередь догрузку истории за год'
              : 'Поставил в очередь догрузку истории за 2 года'
      );
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const detail = String(error.response?.data?.detail || '').trim();
        toast.error(detail || 'Не удалось поставить загрузку истории в очередь');
      } else {
        toast.error('Не удалось поставить загрузку истории в очередь');
      }
    } finally {
      setSyncing(null);
    }
  }, [closedMonthsSync?.status, loadClosedMonthsSyncStatus, selectedStoreId]);

  const cancelSync = useCallback(async () => {
    if (!selectedStoreId) {
      return;
    }
    try {
      await closedMonthsAPI.cancel(selectedStoreId);
      await loadClosedMonthsSyncStatus(true);
      toast.success('Остановил выгрузку закрытых месяцев');
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const detail = String(error.response?.data?.detail || '').trim();
        toast.error(detail || 'Не удалось остановить выгрузку');
      } else {
        toast.error('Не удалось остановить выгрузку');
      }
    }
  }, [loadClosedMonthsSyncStatus, selectedStoreId]);

  const handleStartExport = useCallback(async () => {
    if (!selectedStoreId || !exportYear) {
      toast.error('Сначала выбери магазин и год');
      return;
    }
    setIsStartingExport(true);
    try {
      const status = await closedMonthsAPI.startExport(selectedStoreId, exportYear);
      setExportStatus(status);
      if (status.duplicate_request) {
        toast('Excel по закрытым месяцам уже формируется');
      } else if (status.status === 'queued' || status.status === 'running') {
        toast.success(`Excel по закрытым месяцам за ${exportYear} год поставлен в очередь`);
      }
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const detail = String(error.response?.data?.detail || '').trim();
        toast.error(detail || 'Не удалось запустить экспорт закрытых месяцев');
      } else {
        toast.error('Не удалось запустить экспорт закрытых месяцев');
      }
    } finally {
      setIsStartingExport(false);
    }
  }, [exportYear, selectedStoreId]);

  const handleDownloadExport = useCallback(async () => {
    if (!selectedStoreId || !exportStatus?.file_name) {
      return;
    }
    setIsDownloadingExport(true);
    try {
      await closedMonthsAPI.downloadExport(selectedStoreId, exportStatus.file_name);
    } catch {
      toast.error('Не удалось скачать Excel по закрытым месяцам');
    } finally {
      setIsDownloadingExport(false);
    }
  }, [exportStatus?.file_name, selectedStoreId]);

  const handleClearExport = useCallback(async () => {
    if (!selectedStoreId) {
      return;
    }
    setIsClearingExport(true);
    try {
      const status = await closedMonthsAPI.clearExport(selectedStoreId);
      setExportStatus(status);
      toast.success('Выгрузки по закрытым месяцам очищены');
    } catch {
      toast.error('Не удалось очистить выгрузки по закрытым месяцам');
    } finally {
      setIsClearingExport(false);
    }
  }, [selectedStoreId]);

  const selectedMonthSummary = useMemo(
    () => months.find((item) => item.month === selectedMonth) ?? null,
    [months, selectedMonth]
  );
  const selectedMonthPreviousSummary = useMemo(() => {
    if (!selectedMonth) {
      return null;
    }
    const currentIndex = months.findIndex((item) => item.month === selectedMonth);
    if (currentIndex < 0) {
      return null;
    }
    return months[currentIndex + 1] ?? null;
  }, [months, selectedMonth]);
  const closedMonthsSyncActive = closedMonthsSync?.status === 'queued' || closedMonthsSync?.status === 'running';
  const closedMonthsSyncProgress = Math.max(0, Math.min(Number(closedMonthsSync?.progress_percent ?? 0), 100));
  const closedMonthsSyncMonthsRequested = Number(closedMonthsSync?.months_requested ?? 0);
  const closedMonthsSyncMonthsCompleted = Number(closedMonthsSync?.months_completed ?? 0);
  const closedMonthsSyncMessage = humanizeClosedMonthsMessage(closedMonthsSync?.message);
  const selectedMonthErrorReason = closedMonthErrorReason(selectedMonthSummary);
  const previousMonthOffersByOfferId = useMemo(
    () => new Map(previousMonthOffers.map((item) => [item.offer_id, item])),
    [previousMonthOffers]
  );
  const filteredOffers = useMemo(() => {
    const offers = detail?.offers ?? [];
    return offers.filter((item) => {
      const previousOffer = previousMonthOffersByOfferId.get(item.offer_id) ?? null;
      const profitDelta =
        previousOffer && item.net_profit != null && previousOffer.net_profit != null
          ? Number(item.net_profit || 0) - Number(previousOffer.net_profit || 0)
          : null;

      if (skuFilter === 'grew') {
        return profitDelta != null && profitDelta > 0;
      }
      if (skuFilter === 'declined') {
        return profitDelta != null && profitDelta < 0;
      }
      if (skuFilter === 'loss') {
        return item.net_profit != null && item.net_profit < 0;
      }
      if (skuFilter === 'no_cost') {
        return !item.has_cost;
      }
      return true;
    });
  }, [detail?.offers, previousMonthOffersByOfferId, skuFilter]);
  const selectedMonthHasFullCoverage = Number(selectedMonthSummary?.coverage_ratio || 0) >= 0.9999;
  const historyProfitMetricsReady = months.length > 0 && months.every((item) => Number(item.coverage_ratio || 0) >= 0.9999);
  const missingCostOffers = useMemo(() => {
    return (detail?.offers ?? [])
      .filter((item) => !item.has_cost)
      .sort((a, b) => Number(b.revenue_amount || 0) - Number(a.revenue_amount || 0));
  }, [detail?.offers]);
  const missingCostSummary = useMemo(() => {
    return missingCostOffers.reduce(
      (acc, item) => ({
        count: acc.count + 1,
        units: acc.units + Number(item.net_units || 0),
        revenue: acc.revenue + Number(item.revenue_amount || 0),
      }),
      { count: 0, units: 0, revenue: 0 }
    );
  }, [missingCostOffers]);
  const selectedMonthRevenueExplanation = selectedMonthSummary
    ? `${formatCurrency(selectedMonthSummary.sold_amount)} - ${formatCurrency(selectedMonthSummary.returned_amount)} = ${formatCurrency(selectedMonthSummary.revenue_amount)}`
    : '';
  const selectedMonthGrossProfitExplanation = selectedMonthSummary
    ? `${formatCurrency(selectedMonthSummary.revenue_net_of_vat)} - ${formatCurrency(selectedMonthSummary.cogs)} = ${formatCurrency(selectedMonthSummary.gross_profit)}`
    : '';
  const selectedMonthCorrectionsExplanation = selectedMonthSummary
    ? `${formatCurrency(selectedMonthSummary.ozon_compensation)} компенсаций - ${formatCurrency(selectedMonthSummary.ozon_decompensation)} декомпенсаций`
    : '';
  const selectedMonthProfitBeforeTaxExplanation = selectedMonthSummary
    ? `${formatCurrency(selectedMonthSummary.gross_profit)} ${selectedMonthSummary.ozon_incentives >= 0 ? '+' : '-'} ${formatCurrency(Math.abs(selectedMonthSummary.ozon_incentives))} - ${formatCurrency(selectedMonthSummary.ozon_commission)} - ${formatCurrency(selectedMonthSummary.ozon_services)} - ${formatCurrency(selectedMonthSummary.ozon_logistics)} - ${formatCurrency(selectedMonthSummary.ozon_acquiring)} - ${formatCurrency(selectedMonthSummary.ozon_other_expenses)} ${selectedMonthSummary.ozon_adjustments_net >= 0 ? '+' : '-'} ${formatCurrency(Math.abs(selectedMonthSummary.ozon_adjustments_net))} = ${formatCurrency(selectedMonthSummary.profit_before_tax)}`
    : '';
  const selectedMonthNetProfitExplanation = selectedMonthSummary
    ? `${formatCurrency(selectedMonthSummary.profit_before_tax)} - ${formatCurrency(selectedMonthSummary.tax_amount)} = ${formatCurrency(selectedMonthSummary.net_profit)}`
    : '';
  const selectedMonthTaxExplanation = selectedMonthSummary
    ? selectedMonthSummary.tax_mode_used === 'before_tax'
      ? 'В этом режиме налог в расчет не включается.'
      : selectedMonthSummary.tax_mode_used === 'usn_income'
        ? `${formatCurrency(selectedMonthSummary.revenue_net_of_vat)} × ${formatNumber(selectedMonthSummary.tax_rate_used ?? 0)}% = ${formatCurrency(selectedMonthSummary.tax_amount)}`
        : selectedMonthSummary.tax_mode_used === 'usn_income_expenses'
          ? `${formatCurrency(Math.max(selectedMonthSummary.profit_before_tax, 0))} × ${formatNumber(selectedMonthSummary.tax_rate_used ?? 0)}% = ${formatCurrency(selectedMonthSummary.tax_amount)}`
          : `${formatCurrency(selectedMonthSummary.tax_amount)}`
    : '';
  const selectedMonthPrimaryCards = selectedMonthSummary
    ? [
        {
          label: 'Продано',
          value: `${formatNumber(selectedMonthSummary.sold_units)} шт.`,
          description: 'Штук, которые Ozon закрыл как продажи в этом месяце.',
          info: formatCurrency(selectedMonthSummary.sold_amount),
          size: 'primary' as const,
        },
        {
          label: 'Возвраты',
          value: `${formatNumber(selectedMonthSummary.returned_units)} шт.`,
          description: 'Штук, которые Ozon закрыл как возвраты за тот же месяц.',
          info: formatCurrency(selectedMonthSummary.returned_amount),
          tone: 'negative' as const,
          size: 'primary' as const,
        },
        {
          label: 'Выручка закрытого месяца',
          value: formatCurrency(selectedMonthSummary.revenue_amount),
          description: 'Реализация минус возвраты по закрытому месяцу.',
          info: selectedMonthRevenueExplanation,
          size: 'primary' as const,
        },
        ...(selectedMonthHasFullCoverage
          ? [
              {
                label: 'Валовая прибыль',
                value: formatCurrency(selectedMonthSummary.gross_profit),
                description: 'Выручка без НДС минус себестоимость.',
                info: selectedMonthGrossProfitExplanation,
                tone: selectedMonthSummary.gross_profit >= 0 ? ('default' as const) : ('negative' as const),
                size: 'primary' as const,
              },
              {
                label: 'Чистая прибыль',
                value: formatCurrency(selectedMonthSummary.net_profit),
                description: 'После всех расходов Ozon, корректировок и налога.',
                info: selectedMonthNetProfitExplanation,
                tone: selectedMonthSummary.net_profit >= 0 ? ('positive' as const) : ('negative' as const),
                size: 'primary' as const,
              },
            ]
          : []),
      ]
    : [];
  const selectedMonthSecondaryCards = selectedMonthSummary
    ? [
        ...(selectedMonthHasFullCoverage
          ? [
              {
                label: 'Себестоимость',
                value: formatCurrency(selectedMonthSummary.cogs),
                description: 'Историческая себестоимость, которая действовала для этого месяца.',
              },
            ]
          : []),
        {
          label: 'Комиссия Ozon',
          value: formatCurrency(selectedMonthSummary.ozon_commission),
          description: 'По отчету реализации закрытого месяца.',
        },
        {
          label: 'Логистика Ozon',
          value: formatCurrency(selectedMonthSummary.ozon_logistics),
          description: 'Доставка и возвратная логистика Ozon за закрытый месяц.',
        },
        {
          label: 'Услуги Ozon',
          value: formatCurrency(selectedMonthSummary.ozon_services),
          description: 'Маркетинг, хранение и сервисы Ozon за месяц.',
        },
        {
          label: 'Эквайринг Ozon',
          value: formatCurrency(selectedMonthSummary.ozon_acquiring),
          description: 'Платежный эквайринг по monthly transactions Ozon.',
        },
        {
          label: 'Прочие расходы Ozon',
          value: formatCurrency(selectedMonthSummary.ozon_other_expenses),
          description: 'Прочие monthly-расходы, не попавшие в услуги.',
        },
        {
          label: 'Бонусы и софинансирование',
          value: formatCurrency(selectedMonthSummary.ozon_incentives),
          description: 'Партнерские выплаты и бонусы Ozon за месяц.',
          tone: selectedMonthSummary.ozon_incentives >= 0 ? ('positive' as const) : ('negative' as const),
        },
        {
          label: 'Корректировки Ozon',
          value: formatSignedCurrency(selectedMonthSummary.ozon_adjustments_net),
          description: 'Компенсации минус декомпенсации Ozon за месяц.',
          info: selectedMonthCorrectionsExplanation,
          tone: selectedMonthSummary.ozon_adjustments_net >= 0 ? ('positive' as const) : ('negative' as const),
        },
        ...(selectedMonthHasFullCoverage
          ? [
              {
                label: 'Прибыль до налога',
                value: formatCurrency(selectedMonthSummary.profit_before_tax),
                description: 'После расходов и доплат Ozon, но до налога.',
                info: selectedMonthProfitBeforeTaxExplanation,
                tone: selectedMonthSummary.profit_before_tax >= 0 ? ('default' as const) : ('negative' as const),
              },
              {
                label: 'Налог',
                value: formatCurrency(selectedMonthSummary.tax_amount),
                description: 'По исторической налоговой схеме, которая действовала в этом месяце.',
                info: selectedMonthTaxExplanation,
              },
            ]
          : []),
      ]
    : [];

  const toggleMetric = useCallback((metricKey: MetricKey) => {
    setSelectedMetricKeys((current) => {
      if (current.includes(metricKey)) {
        return current.length > 1 ? current.filter((item) => item !== metricKey) : current;
      }
      return [...current, metricKey];
    });
  }, []);

  useEffect(() => {
    if (historyProfitMetricsReady) {
      return;
    }
    setSelectedMetricKeys((current) => {
      const next = current.filter((key) => {
        const option = METRIC_OPTIONS.find((item) => item.key === key);
        return !option?.requiresFullCoverage;
      });
      return next.length > 0 ? next : ['revenue_amount'];
    });
  }, [historyProfitMetricsReady]);

  useEffect(() => {
    if (selectedMonthHasFullCoverage) {
      return;
    }
    if (skuFilter === 'grew' || skuFilter === 'declined' || skuFilter === 'loss') {
      setSkuFilter('all');
    }
  }, [selectedMonthHasFullCoverage, skuFilter]);

  return (
    <ProtectedRoute>
      <Layout>
        <div className="space-y-6">
          <section className="rounded-[2rem] border border-white/70 bg-[radial-gradient(circle_at_top_left,rgba(14,165,233,0.14),transparent_36%),linear-gradient(180deg,#ffffff_0%,#f8fbff_100%)] p-6 shadow-[0_20px_60px_rgba(15,23,42,0.08)]">
            <div className="flex flex-wrap items-start justify-between gap-4">
              <div>
                <Link href="/dashboard" className="text-sm font-medium text-sky-700 hover:text-sky-800">
                  ← Назад в Магазин сегодня
                </Link>
                <h1 className="mt-3 text-3xl font-semibold text-slate-950">Закрытые месяцы</h1>
                <p className="mt-2 max-w-3xl text-sm leading-6 text-slate-500">
                  История уже закрытых месяцев Ozon по активному магазину. Здесь видно, как менялись продажи,
                  выручка и прибыль, а ниже можно открыть любой месяц и увидеть таблицу всех артикулов.
                </p>
              </div>
              <div className="flex flex-wrap items-center gap-3">
                <div className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm text-slate-600">
                  Активный магазин: <span className="font-semibold text-slate-950">{selectedStore?.name || 'не выбран'}</span>
                </div>
                <button
                  type="button"
                  onClick={() => void requestSync({ monthsBack: 3 })}
                  disabled={!selectedStoreId || syncing !== null || closedMonthsSyncActive}
                  className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-medium text-slate-700 transition hover:border-sky-200 hover:text-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {syncing === 3 ? 'Ставлю в очередь...' : 'Загрузить 3 месяца'}
                </button>
                <button
                  type="button"
                  onClick={() => void requestSync({ monthsBack: 6 })}
                  disabled={!selectedStoreId || syncing !== null || closedMonthsSyncActive}
                  className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-medium text-slate-700 transition hover:border-sky-200 hover:text-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {syncing === 6 ? 'Ставлю в очередь...' : 'Загрузить 6 месяцев'}
                </button>
                <button
                  type="button"
                  onClick={() => void requestSync({ monthsBack: 12 })}
                  disabled={!selectedStoreId || syncing !== null || closedMonthsSyncActive}
                  className="rounded-2xl bg-sky-600 px-4 py-3 text-sm font-medium text-white transition hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {syncing === 12 ? 'Ставлю в очередь...' : 'Догрузить год'}
                </button>
                <button
                  type="button"
                  onClick={() => void requestSync({ monthsBack: 24 })}
                  disabled={!selectedStoreId || syncing !== null || closedMonthsSyncActive}
                  className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-medium text-slate-700 transition hover:border-sky-200 hover:text-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {syncing === 24 ? 'Ставлю в очередь...' : 'Догрузить 2 года'}
                </button>
                {closedMonthsSyncActive ? (
                  <button
                    type="button"
                    onClick={() => void cancelSync()}
                    className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-3 text-sm font-medium text-rose-700 transition hover:border-rose-300 hover:bg-rose-100"
                  >
                    Остановить выгрузку
                  </button>
                ) : null}
              </div>
            </div>
            <div className="mt-4 flex flex-col gap-3 xl:flex-row xl:items-start">
              <div className="w-full rounded-3xl border border-slate-200 bg-white/85 p-4 xl:max-w-xs">
                <div className="text-sm font-semibold text-slate-950">Excel по закрытым месяцам</div>
                <p className="mt-1 text-sm text-slate-500">
                  Выбери год, и мы соберем один Excel: сводка за год + отдельный лист на каждый закрытый месяц.
                </p>
                <div className="mt-3">
                  <label className="mb-2 block text-xs font-semibold uppercase tracking-[0.14em] text-slate-400">
                    Год для Excel
                  </label>
                  <select
                    value={exportYear ?? ''}
                    onChange={(event) => setExportYear(event.target.value ? Number(event.target.value) : null)}
                    disabled={!availableYears.length || isStartingExport || (exportStatus?.status === 'queued' || exportStatus?.status === 'running')}
                    className="w-full rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm text-slate-700 outline-none transition focus:border-sky-300 focus:ring-2 focus:ring-sky-100 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    {availableYears.length === 0 ? (
                      <option value="">Нет закрытых месяцев</option>
                    ) : (
                      availableYears.map((year) => (
                        <option key={year} value={year}>
                          {year} г.
                        </option>
                      ))
                    )}
                  </select>
                </div>
              </div>
              <div className="min-w-0 flex-1">
                <ExportJobCard
                  title="Excel по закрытым месяцам"
                  description="Первым листом идет сводка за выбранный год, а дальше каждый месяц оформлен отдельным листом с метриками и таблицей артикулов."
                  status={exportStatus}
                  onStart={handleStartExport}
                  onDownload={handleDownloadExport}
                  onClear={handleClearExport}
                  startLabel={exportYear ? `Сформировать Excel за ${exportYear} г.` : 'Сформировать Excel'}
                  isStarting={isStartingExport}
                  isDownloading={isDownloadingExport}
                  isClearing={isClearingExport}
                />
              </div>
            </div>
            {closedMonthsSync ? (
              <div className="mt-4 rounded-3xl border border-slate-200 bg-white/85 p-4">
                <div className="flex flex-wrap items-start justify-between gap-3">
                  <div>
                    <div className="text-sm font-semibold text-slate-950">Статус выгрузки истории</div>
                    <div className="mt-1 text-sm text-slate-500">{closedMonthsSyncMessage}</div>
                  </div>
                  <div
                    className={`inline-flex items-center rounded-full px-3 py-1 text-xs font-medium ${
                      closedMonthsSync.status === 'running'
                        ? 'bg-sky-50 text-sky-700'
                        : closedMonthsSync.status === 'queued'
                          ? 'bg-amber-50 text-amber-800'
                          : closedMonthsSync.status === 'success'
                            ? 'bg-emerald-50 text-emerald-700'
                            : closedMonthsSync.status === 'failed'
                              ? 'bg-rose-50 text-rose-700'
                              : 'bg-slate-100 text-slate-600'
                    }`}
                  >
                    {closedMonthsSync.phase_label || closedMonthsSync.status}
                  </div>
                </div>
                <div className="mt-4 h-3 overflow-hidden rounded-full bg-slate-100">
                  <div
                    className={`h-full rounded-full transition-all ${
                      closedMonthsSync.status === 'failed' ? 'bg-rose-500' : 'bg-sky-500'
                    }`}
                    style={{ width: `${closedMonthsSyncProgress}%` }}
                  />
                </div>
                <div className="mt-3 flex flex-wrap gap-3 text-xs text-slate-500">
                  <span>{closedMonthsSyncProgress}%</span>
                  {closedMonthsSyncMonthsRequested > 0 ? (
                    <span>
                      {closedMonthsSyncMonthsCompleted} из {closedMonthsSyncMonthsRequested} месяцев
                    </span>
                  ) : null}
                  {closedMonthsSync.current_month ? (
                    <span>Сейчас: {monthLabel(closedMonthsSync.current_month)}</span>
                  ) : null}
                  {closedMonthsSync.updated_at ? (
                    <span>Обновлено: {new Date(closedMonthsSync.updated_at).toLocaleString('ru-RU')}</span>
                  ) : null}
                </div>
                {closedMonthsSyncMonthsRequested > 0 ? (
                  <div className="mt-3 text-xs text-slate-500">
                    {closedMonthsSyncMonthsRequested <= 3
                      ? 'Быстрая загрузка: подтягиваем последние 3 закрытых месяца магазина.'
                      : closedMonthsSyncMonthsRequested <= 6
                        ? 'Расширенная загрузка: подтягиваем последние 6 закрытых месяцев магазина.'
                        : closedMonthsSyncMonthsRequested <= 12
                          ? 'Полная загрузка: догружаем историю закрытых месяцев магазина до года.'
                          : 'Расширенная история: догружаем закрытые месяцы магазина до 2 лет.'}
                  </div>
                ) : null}
              </div>
            ) : null}
          </section>

          {!selectedStoreId ? (
            <div className="rounded-3xl border border-white/70 bg-white/85 p-8 text-sm text-slate-500 shadow-[0_16px_40px_rgba(15,23,42,0.06)]">
              Сначала выбери активный магазин в шапке, и тогда можно будет открыть историю закрытых месяцев именно по нему.
            </div>
          ) : loading ? (
            <div className="flex h-48 items-center justify-center rounded-3xl border border-white/70 bg-white/85 shadow-[0_16px_40px_rgba(15,23,42,0.06)]">
              <div className="h-12 w-12 animate-spin rounded-full border-b-2 border-sky-600" />
            </div>
          ) : months.length === 0 ? (
            <div className="rounded-3xl border border-white/70 bg-white/85 p-8 shadow-[0_16px_40px_rgba(15,23,42,0.06)]">
              <h2 className="text-lg font-semibold text-slate-950">История еще не загружена</h2>
              <p className="mt-2 max-w-2xl text-sm leading-6 text-slate-500">
                Быстрый старт — подтянуть последние 3 закрытых месяца. После этого можно догрузить до года и получить
                наглядный график и подробные таблицы по артикулам.
              </p>
              <div className="mt-5 flex flex-wrap gap-3">
                <button
                  type="button"
                  onClick={() => void requestSync({ monthsBack: 3 })}
                  disabled={syncing !== null || closedMonthsSyncActive}
                  className="rounded-2xl bg-sky-600 px-4 py-3 text-sm font-medium text-white transition hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {syncing === 3 ? 'Ставлю в очередь...' : 'Загрузить 3 месяца'}
                </button>
                <button
                  type="button"
                  onClick={() => void requestSync({ monthsBack: 6 })}
                  disabled={syncing !== null || closedMonthsSyncActive}
                  className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-medium text-slate-700 transition hover:border-sky-200 hover:text-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {syncing === 6 ? 'Ставлю в очередь...' : 'Загрузить 6 месяцев'}
                </button>
                <button
                  type="button"
                  onClick={() => void requestSync({ monthsBack: 12 })}
                  disabled={syncing !== null || closedMonthsSyncActive}
                  className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-medium text-slate-700 transition hover:border-sky-200 hover:text-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {syncing === 12 ? 'Ставлю в очередь...' : 'Сразу догрузить год'}
                </button>
                <button
                  type="button"
                  onClick={() => void requestSync({ monthsBack: 24 })}
                  disabled={syncing !== null || closedMonthsSyncActive}
                  className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-sm font-medium text-slate-700 transition hover:border-sky-200 hover:text-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
                >
                  {syncing === 24 ? 'Ставлю в очередь...' : 'Догрузить 2 года'}
                </button>
              </div>
            </div>
          ) : (
            <>
              <section className="rounded-3xl border border-white/70 bg-white/90 p-6 shadow-[0_16px_40px_rgba(15,23,42,0.06)]">
                <div className="flex flex-wrap items-start justify-between gap-4">
                  <div>
                    <h2 className="text-lg font-semibold text-slate-950">Динамика закрытых месяцев</h2>
                    <p className="mt-1 text-sm text-slate-500">
                      Выбери метрику и посмотри, как менялись результаты магазина от месяца к месяцу.
                    </p>
                  </div>
                  <div className="flex flex-col items-start gap-3">
                    <div className="flex flex-wrap gap-2">
                      {METRIC_OPTIONS.map((option) => (
                        <button
                          key={option.key}
                          type="button"
                          onClick={() => toggleMetric(option.key)}
                          disabled={Boolean(option.requiresFullCoverage && !historyProfitMetricsReady)}
                          className={`rounded-full border px-3 py-2 text-sm font-medium transition disabled:cursor-not-allowed disabled:opacity-50 ${
                            selectedMetricKeys.includes(option.key)
                              ? option.activeClass
                              : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:bg-slate-50'
                          }`}
                        >
                          {option.label}
                        </button>
                      ))}
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {CHART_RANGE_OPTIONS.map((option) => (
                        <button
                          key={option.value}
                          type="button"
                          onClick={() => setChartRangeMonths(option.value)}
                          className={`rounded-full border px-3 py-2 text-xs font-medium transition ${
                            chartRangeMonths === option.value
                              ? 'border-slate-900 bg-slate-900 text-white'
                              : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:bg-slate-50'
                          }`}
                        >
                          {option.label}
                        </button>
                      ))}
                    </div>
                    <div className="flex flex-wrap gap-2">
                      {[
                        { value: 'lines' as const, label: 'Линии' },
                        { value: 'bars' as const, label: 'Столбцы' },
                      ].map((option) => (
                        <button
                          key={option.value}
                          type="button"
                          onClick={() => setChartDisplayMode(option.value)}
                          className={`rounded-full border px-3 py-2 text-xs font-medium transition ${
                            chartDisplayMode === option.value
                              ? 'border-slate-900 bg-slate-900 text-white'
                              : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:bg-slate-50'
                          }`}
                        >
                          {option.label}
                        </button>
                      ))}
                    </div>
                  </div>
                </div>
                <div className="mt-6 rounded-3xl border border-slate-100 bg-slate-50/80 p-4">
                  <MetricChart months={chartMonths} metrics={selectedMetrics} displayMode={chartDisplayMode} />
                  <div className="mt-3 flex flex-wrap gap-3 text-xs text-slate-500">
                    {!historyProfitMetricsReady ? (
                      <span>Валовая и чистая прибыль появятся на графике после заполнения себестоимости.</span>
                    ) : null}
                    <span>
                      Период графика: последние {Math.min(chartRangeMonths, Math.max(months.length, 1))}{' '}
                      {Math.min(chartRangeMonths, Math.max(months.length, 1)) === 1 ? 'месяц' : 'месяца'}
                    </span>
                  </div>
                </div>
              </section>

              <section className="rounded-3xl border border-white/70 bg-white/90 p-6 shadow-[0_16px_40px_rgba(15,23,42,0.06)]">
                <div className="flex flex-wrap items-center justify-between gap-4">
                  <div>
                    <h2 className="text-lg font-semibold text-slate-950">История месяцев</h2>
                    <p className="mt-1 text-sm text-slate-500">
                      Открой нужный месяц и посмотри, из каких артикулов и метрик сложился итог.
                    </p>
                  </div>
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-xs font-medium uppercase tracking-[0.14em] text-slate-400">Год</span>
                    <button
                      type="button"
                      onClick={() => setSelectedYear('all')}
                      className={`rounded-full border px-3 py-2 text-xs font-medium transition ${
                        selectedYear === 'all'
                          ? 'border-slate-900 bg-slate-900 text-white'
                          : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:bg-slate-50'
                      }`}
                    >
                      Все
                    </button>
                    {availableYears.map((year) => (
                      <button
                        key={year}
                        type="button"
                        onClick={() => setSelectedYear(year)}
                        className={`rounded-full border px-3 py-2 text-xs font-medium transition ${
                          selectedYear === year
                            ? 'border-slate-900 bg-slate-900 text-white'
                            : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:bg-slate-50'
                        }`}
                      >
                        {year}
                      </button>
                    ))}
                    <button
                      type="button"
                      onClick={() => void loadMonths()}
                      className="rounded-2xl border border-slate-200 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:border-sky-200 hover:text-sky-700"
                    >
                      Обновить список
                    </button>
                  </div>
                </div>

                <div className="mt-5 grid gap-4 xl:grid-cols-3">
                  {visibleMonths.map((item) => {
                    const isActive = item.month === selectedMonth;
                    const currentIndex = months.findIndex((month) => month.month === item.month);
                    const previousMonth = currentIndex >= 0 ? months[currentIndex + 1] ?? null : null;
                    const monthHasFullCoverage = Number(item.coverage_ratio || 0) >= 0.9999;
                    const previousMonthHasFullCoverage = previousMonth ? Number(previousMonth.coverage_ratio || 0) >= 0.9999 : false;
                    const displayStatus = effectiveMonthStatus(item);
                    const visibleStatus =
                      !canSeeTechnicalWarnings && displayStatus === 'ozon_warning' ? 'ready' : displayStatus;
                    const hasOzonWarnings = canSeeTechnicalWarnings && hasCriticalOzonWarnings(item);
                    const soldDelta = previousMonth ? Number(item.sold_units || 0) - Number(previousMonth.sold_units || 0) : 0;
                    const revenueDelta = previousMonth ? Number(item.revenue_amount || 0) - Number(previousMonth.revenue_amount || 0) : 0;
                    const profitDelta =
                      previousMonth && monthHasFullCoverage && previousMonthHasFullCoverage
                        ? Number(item.net_profit || 0) - Number(previousMonth.net_profit || 0)
                        : null;
                    const errorReason = closedMonthErrorReason(item);
                    return (
                      <button
                        key={item.month}
                        type="button"
                        onClick={() => setSelectedMonth(item.month)}
                        className={`rounded-3xl border px-5 py-4 text-left transition ${
                          isActive
                            ? 'border-sky-300 bg-sky-50 shadow-[0_12px_30px_rgba(14,165,233,0.12)]'
                            : 'border-slate-200 bg-white hover:border-sky-200'
                        }`}
                      >
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div>
                            <div className="text-base font-semibold text-slate-950">{monthLabel(item.month)}</div>
                            <div className="mt-1 text-xs text-slate-500">{item.month}</div>
                          </div>
                          <span className={`rounded-full px-3 py-1 text-xs font-medium ${statusClass(visibleStatus)}`}>
                            {statusLabel(visibleStatus)}
                          </span>
                        </div>
                        <div className="mt-4 grid gap-3 sm:grid-cols-2">
                          <div>
                            <div className="text-xs uppercase tracking-[0.14em] text-slate-400">Выручка</div>
                            <div className="mt-1 text-lg font-semibold text-slate-950">{formatCurrency(item.revenue_amount)}</div>
                            {previousMonth ? (
                              <div className={`mt-1 text-xs font-medium ${deltaClass(revenueDelta)}`}>
                                {formatDelta(revenueDelta, 'currency')} к {monthLabel(previousMonth.month)}
                              </div>
                            ) : (
                              <div className="mt-1 text-xs text-slate-400">Это самый старый месяц в выборке</div>
                            )}
                          </div>
                          <div>
                            <div className="text-xs uppercase tracking-[0.14em] text-slate-400">Чистая прибыль</div>
                            {monthHasFullCoverage ? (
                              <>
                                <div className={`mt-1 text-lg font-semibold ${item.net_profit >= 0 ? 'text-emerald-700' : 'text-rose-700'}`}>
                                  {formatCurrency(item.net_profit)}
                                </div>
                                {previousMonth && profitDelta != null ? (
                                  <div className={`mt-1 text-xs font-medium ${deltaClass(profitDelta)}`}>
                                    {formatDelta(profitDelta, 'currency')} к {monthLabel(previousMonth.month)}
                                  </div>
                                ) : (
                                  <div className="mt-1 text-xs text-slate-400">Нет более раннего полного месяца для сравнения</div>
                                )}
                              </>
                            ) : (
                              <>
                                <div className="mt-1 text-lg font-semibold text-slate-400">Скрыто</div>
                                <div className="mt-1 text-xs text-amber-700">Заполни себестоимость, чтобы увидеть прибыль</div>
                              </>
                            )}
                          </div>
                        </div>
                        <div className="mt-4 flex flex-wrap gap-2 text-xs">
                          <span className="rounded-full bg-slate-100 px-3 py-1 text-slate-600">
                            Продано: {formatNumber(item.sold_units)} шт.
                          </span>
                          {previousMonth ? (
                            <span className={`rounded-full px-3 py-1 ${soldDelta > 0 ? 'bg-emerald-50 text-emerald-700' : soldDelta < 0 ? 'bg-rose-50 text-rose-700' : 'bg-slate-100 text-slate-600'}`}>
                              {formatDelta(soldDelta, 'units')}
                            </span>
                          ) : null}
                          <span className="rounded-full bg-slate-100 px-3 py-1 text-slate-600">
                            Себестоимость заполнена: {formatPercent(item.coverage_ratio)}
                          </span>
                          {hasOzonWarnings ? (
                            <span className="rounded-full bg-sky-50 px-3 py-1 text-sky-700">
                              Есть предупреждения Ozon
                            </span>
                          ) : null}
                        </div>
                        {item.status === 'error' && errorReason ? (
                          <div className="mt-3 rounded-2xl bg-rose-50 px-3 py-2 text-xs leading-5 text-rose-700">
                            {errorReason}
                          </div>
                        ) : null}
                      </button>
                    );
                  })}
                </div>
              </section>

              <section className="rounded-3xl border border-white/70 bg-white/90 p-6 shadow-[0_16px_40px_rgba(15,23,42,0.06)]">
                {detailLoading || !selectedMonthSummary ? (
                  <div className="flex h-48 items-center justify-center">
                    <div className="h-12 w-12 animate-spin rounded-full border-b-2 border-sky-600" />
                  </div>
                ) : (
                  <>
                    {(() => {
                      const selectedMonthDisplayStatus = effectiveMonthStatus(selectedMonthSummary);
                      const visibleSelectedMonthStatus =
                        !canSeeTechnicalWarnings && selectedMonthDisplayStatus === 'ozon_warning'
                          ? 'ready'
                          : selectedMonthDisplayStatus;
                      const selectedMonthCriticalWarnings = monthWarningsSummary(selectedMonthSummary).critical;
                      return (
                        <>
                    <div className="flex flex-wrap items-start justify-between gap-4">
                      <div>
                        <h2 className="text-lg font-semibold text-slate-950">
                          {monthLabel(selectedMonthSummary.month)}
                        </h2>
                        <p className="mt-1 text-sm text-slate-500">
                          Подробный итог месяца по магазину и таблица всех артикулов, которые попали в реализацию.
                        </p>
                        <div className="mt-3 flex flex-wrap gap-2">
                          <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700">
                            НДС: {vatModeLabel(selectedMonthSummary.vat_mode_used)}
                          </span>
                          <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700">
                            Налог: {taxModeLabel(selectedMonthSummary.tax_mode_used)}
                          </span>
                          <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700">
                            Ставка: {selectedMonthSummary.tax_rate_used ?? 0}%
                          </span>
                          {selectedMonthSummary.tax_effective_from_used ? (
                            <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700">
                              Схема действует с {new Date(selectedMonthSummary.tax_effective_from_used).toLocaleDateString('ru-RU')}
                            </span>
                          ) : null}
                          {selectedMonthSummary.cost_snapshot_date ? (
                            <span className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs font-medium text-slate-700">
                              Себестоимость по состоянию на {new Date(selectedMonthSummary.cost_snapshot_date).toLocaleDateString('ru-RU')}
                            </span>
                          ) : null}
                        </div>
                      </div>
                      <div className="flex flex-wrap gap-2">
                        <span className={`rounded-full px-3 py-1 text-xs font-medium ${statusClass(visibleSelectedMonthStatus)}`}>
                          {statusLabel(visibleSelectedMonthStatus)}
                        </span>
                        <span className={`rounded-full px-3 py-1 text-xs font-medium ${
                          selectedMonthSummary.coverage_ratio >= 0.9999
                            ? 'bg-emerald-50 text-emerald-700'
                            : 'bg-amber-50 text-amber-800'
                        }`}>
                          Покрытие себестоимости {formatPercent(selectedMonthSummary.coverage_ratio)}
                        </span>
                      </div>
                    </div>

                    {canSeeTechnicalWarnings && selectedMonthCriticalWarnings.length > 0 ? (
                      <div className="mt-4 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-3 text-sm text-sky-950">
                        Ozon отдал этот месяц с техническими ограничениями. Основные цифры месяца собраны,
                        но часть технических данных пришла неидеально.
                        <div className="mt-2 flex flex-col gap-1 text-xs text-sky-700">
                          {selectedMonthCriticalWarnings.slice(0, 3).map((warning, index) => (
                            <span key={`selected-warning-${index}`}>• {humanizeMonthWarning(warning)}</span>
                          ))}
                        </div>
                      </div>
                    ) : null}

                    <div className={`mt-5 grid gap-3 ${selectedMonthHasFullCoverage ? 'xl:grid-cols-5' : 'xl:grid-cols-3'}`}>
                      {selectedMonthPrimaryCards.map((card, index) => (
                        <ClosedMonthMetricCard
                          key={card.label}
                          {...card}
                          infoAlign={metricInfoAlign(index, selectedMonthPrimaryCards.length)}
                        />
                      ))}
                    </div>
                    <div className="mt-3 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-xs text-slate-500">
                      Проверено: {selectedMonthSummary.generated_at
                        ? new Date(selectedMonthSummary.generated_at).toLocaleString('ru-RU', {
                            day: '2-digit',
                            month: '2-digit',
                            year: 'numeric',
                            hour: '2-digit',
                            minute: '2-digit',
                          })
                        : '—'}
                    </div>

                    {!selectedMonthHasFullCoverage ? (
                      <div className="mt-4 rounded-2xl border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-950">
                        В этом месяце себестоимость заполнена не по всему ассортименту, поэтому прибыль пока скрыта.
                        <Link href="/cost-history" className="ml-1 font-medium underline decoration-dotted underline-offset-2">
                          Заполнить себестоимость
                        </Link>
                      </div>
                    ) : null}

                    {missingCostOffers.length > 0 ? (
                      <div className="mt-4 rounded-3xl border border-amber-200 bg-amber-50/70 p-4">
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div>
                            <div className="text-sm font-semibold text-slate-950">Что не хватает до полной прибыли</div>
                            <div className="mt-1 text-xs text-slate-600">
                              Ниже артикулы без себестоимости. Пока они не заполнены, прибыль считается не по всему ассортименту.
                            </div>
                          </div>
                          <div className="flex flex-wrap gap-2 text-xs">
                            <span className="rounded-full bg-white px-3 py-1 font-medium text-slate-700">
                              Без себестоимости: {formatNumber(missingCostSummary.count)} SKU
                            </span>
                            <span className="rounded-full bg-white px-3 py-1 font-medium text-slate-700">
                              Нетто: {formatNumber(missingCostSummary.units)} шт.
                            </span>
                            <span className="rounded-full bg-white px-3 py-1 font-medium text-slate-700">
                              Выручка: {formatCurrency(missingCostSummary.revenue)}
                            </span>
                          </div>
                        </div>
                        <div className="mt-4 grid gap-3 xl:grid-cols-2">
                          {missingCostOffers.slice(0, 10).map((item) => (
                            <div key={`missing-cost-${item.offer_id}`} className="rounded-2xl border border-white bg-white px-4 py-3">
                              <div className="flex items-start justify-between gap-3">
                                <div className="min-w-0">
                                  <div className="truncate text-sm font-semibold text-slate-950">{item.offer_id}</div>
                                  <div className="mt-1 truncate text-xs text-slate-500">{item.title || 'Без названия'}</div>
                                </div>
                                <div className="text-right">
                                  <div className="text-sm font-semibold text-slate-950">{formatCurrency(item.revenue_amount)}</div>
                                  <div className="mt-1 text-xs text-slate-500">{formatNumber(item.net_units)} шт.</div>
                                </div>
                              </div>
                            </div>
                          ))}
                        </div>
                        {missingCostOffers.length > 10 ? (
                          <div className="mt-3 text-xs text-slate-500">
                            Показаны первые 10 артикулов без себестоимости по выручке.
                          </div>
                        ) : null}
                      </div>
                    ) : null}

                    <div className="mt-3 grid gap-3 md:grid-cols-2 xl:grid-cols-5">
                      {selectedMonthSecondaryCards.map((card, index) => (
                        <ClosedMonthMetricCard
                          key={card.label}
                          {...card}
                          infoAlign={metricInfoAlign(index, selectedMonthSecondaryCards.length)}
                        />
                      ))}
                    </div>

                    {selectedMonthPreviousSummary ? (
                      <div className="mt-5 rounded-3xl border border-slate-200 bg-slate-50/90 p-4">
                        <div className="flex flex-wrap items-start justify-between gap-3">
                          <div>
                            <div className="text-sm font-semibold text-slate-950">Месяц к месяцу</div>
                            <div className="mt-1 text-xs text-slate-500">
                              Сравнение с {monthLabel(selectedMonthPreviousSummary.month)}
                            </div>
                          </div>
                        </div>
                        <div className={`mt-4 grid gap-3 md:grid-cols-2 ${selectedMonthHasFullCoverage && Number(selectedMonthPreviousSummary.coverage_ratio || 0) >= 0.9999 ? 'xl:grid-cols-5' : 'xl:grid-cols-3'}`}>
                          {[
                            {
                              label: 'Продано',
                              current: Number(selectedMonthSummary.sold_units || 0),
                              previous: Number(selectedMonthPreviousSummary.sold_units || 0),
                              format: 'units' as const,
                            },
                            {
                              label: 'Выручка',
                              current: Number(selectedMonthSummary.revenue_amount || 0),
                              previous: Number(selectedMonthPreviousSummary.revenue_amount || 0),
                              format: 'currency' as const,
                            },
                            {
                              label: 'Покрытие себестоимости',
                              current: Number(selectedMonthSummary.coverage_ratio || 0),
                              previous: Number(selectedMonthPreviousSummary.coverage_ratio || 0),
                              format: 'percent' as const,
                            },
                            ...(selectedMonthHasFullCoverage && Number(selectedMonthPreviousSummary.coverage_ratio || 0) >= 0.9999
                              ? [
                                  {
                                    label: 'Валовая прибыль',
                                    current: Number(selectedMonthSummary.gross_profit || 0),
                                    previous: Number(selectedMonthPreviousSummary.gross_profit || 0),
                                    format: 'currency' as const,
                                  },
                                  {
                                    label: 'Чистая прибыль',
                                    current: Number(selectedMonthSummary.net_profit || 0),
                                    previous: Number(selectedMonthPreviousSummary.net_profit || 0),
                                    format: 'currency' as const,
                                  },
                                ]
                              : []),
                          ].map((item) => {
                            const delta = item.current - item.previous;
                            const isPercent = item.format === 'percent';
                            return (
                              <div key={item.label} className="rounded-2xl border border-white bg-white px-4 py-3">
                                <div className="text-xs uppercase tracking-[0.14em] text-slate-400">{item.label}</div>
                                <div className="mt-2 text-base font-semibold text-slate-950">
                                  {item.format === 'units'
                                    ? `${formatNumber(item.current)} шт.`
                                    : item.format === 'currency'
                                      ? formatCurrency(item.current)
                                      : formatPercent(item.current)}
                                </div>
                                <div className={`mt-1 text-xs font-medium ${deltaClass(delta)}`}>
                                  {isPercent
                                    ? `${delta > 0 ? '+' : delta < 0 ? '−' : ''}${formatPercent(Math.abs(delta))} к ${monthLabel(selectedMonthPreviousSummary.month)}`
                                    : `${formatDelta(delta, item.format)} к ${monthLabel(selectedMonthPreviousSummary.month)}`}
                                </div>
                              </div>
                            );
                          })}
                        </div>
                      </div>
                    ) : null}

                    {selectedMonthSummary.status === 'error' && selectedMonthErrorReason ? (
                      <div className="mt-4 rounded-2xl border border-rose-100 bg-rose-50 px-4 py-3 text-sm text-rose-700">
                        {selectedMonthErrorReason}
                      </div>
                    ) : null}

                    <div className="mt-6 space-y-4">
                      <div className="flex flex-wrap items-center justify-between gap-3 rounded-3xl border border-slate-200 bg-slate-50/90 px-4 py-3">
                        <div>
                          <div className="text-sm font-semibold text-slate-950">Фильтр по артикулам</div>
                          <div className="mt-1 text-xs text-slate-500">
                            Показано {formatNumber(filteredOffers.length)} из {formatNumber(detail?.offers?.length ?? 0)} артикулов
                          </div>
                        </div>
                        <div className="flex flex-wrap gap-2">
                          {[
                            { value: 'all' as const, label: 'Все' },
                            { value: 'grew' as const, label: 'Выросли' },
                            { value: 'declined' as const, label: 'Просели' },
                            { value: 'loss' as const, label: 'Убыточные' },
                            { value: 'no_cost' as const, label: 'Без себестоимости' },
                          ].map((option) => (
                            <button
                              key={option.value}
                              type="button"
                              onClick={() => setSkuFilter(option.value)}
                              disabled={
                                !selectedMonthHasFullCoverage &&
                                (option.value === 'grew' || option.value === 'declined' || option.value === 'loss')
                              }
                              className={`rounded-full border px-3 py-2 text-xs font-medium transition ${
                                skuFilter === option.value
                                  ? 'border-slate-900 bg-slate-900 text-white'
                                  : 'border-slate-200 bg-white text-slate-600 hover:border-slate-300 hover:bg-slate-50'
                              }`}
                            >
                              {option.label}
                            </button>
                          ))}
                        </div>
                      </div>

                      <div className="overflow-hidden rounded-3xl border border-slate-200">
                      <div className="overflow-x-auto">
                        <table className="min-w-full divide-y divide-slate-200 text-sm">
                          <thead className="bg-slate-50">
                            <tr className="text-left text-slate-500">
                              <th className="px-4 py-3 font-medium">Артикул</th>
                              <th className="px-4 py-3 font-medium">Продано</th>
                              <th className="px-4 py-3 font-medium">Возвраты</th>
                              <th className="px-4 py-3 font-medium">Выручка</th>
                              {selectedMonthHasFullCoverage ? (
                                <>
                                  <th className="px-4 py-3 font-medium">Себестоимость</th>
                                  <th className="px-4 py-3 font-medium">Валовая</th>
                                </>
                              ) : null}
                              <th className="px-4 py-3 font-medium">Комиссия</th>
                              <th className="px-4 py-3 font-medium">Логистика</th>
                              <th className="px-4 py-3 font-medium">Услуги</th>
                              <th className="px-4 py-3 font-medium">Эквайринг</th>
                              <th className="px-4 py-3 font-medium">Прочие</th>
                              <th className="px-4 py-3 font-medium">Бонусы</th>
                              <th className="px-4 py-3 font-medium">Корректировки</th>
                              {selectedMonthHasFullCoverage ? (
                                <>
                                  <th className="px-4 py-3 font-medium">Налог</th>
                                  <th className="px-4 py-3 font-medium">Чистая</th>
                                </>
                              ) : null}
                            </tr>
                          </thead>
                          <tbody className="divide-y divide-slate-100 bg-white">
                            {filteredOffers.map((item) => {
                              const previousOffer = previousMonthOffersByOfferId.get(item.offer_id) ?? null;
                              const soldDelta = previousOffer ? Number(item.sold_units || 0) - Number(previousOffer.sold_units || 0) : null;
                              const revenueDelta = previousOffer ? Number(item.revenue_amount || 0) - Number(previousOffer.revenue_amount || 0) : null;
                              const profitDelta =
                                previousOffer && item.net_profit != null && previousOffer.net_profit != null
                                  ? Number(item.net_profit || 0) - Number(previousOffer.net_profit || 0)
                                  : null;

                              return (
                                <tr key={`${item.month}-${item.offer_id}`} className="align-top">
                                  <td className="px-4 py-3">
                                    <div className="font-medium text-slate-950">{item.offer_id}</div>
                                    <div className="mt-1 max-w-[240px] truncate text-xs text-slate-500">{item.title || 'Без названия'}</div>
                                    {!item.has_cost ? (
                                      <div className="mt-2 inline-flex rounded-full bg-amber-50 px-2.5 py-1 text-[11px] font-medium text-amber-800">
                                        Нет себестоимости
                                      </div>
                                    ) : null}
                                  </td>
                                  <td className="px-4 py-3 text-slate-700">
                                    <div>{formatNumber(item.sold_units)} шт.</div>
                                    {soldDelta != null ? (
                                      <div className={`mt-1 text-[11px] font-medium ${deltaClass(soldDelta)}`}>
                                        {formatDelta(soldDelta, 'units')}
                                      </div>
                                    ) : (
                                      <div className="mt-1 text-[11px] text-slate-400">—</div>
                                    )}
                                  </td>
                                  <td className="px-4 py-3 text-slate-700">{formatNumber(item.returned_units)} шт.</td>
                                  <td className="px-4 py-3 font-medium text-slate-950">
                                    <div>{formatCurrency(item.revenue_amount)}</div>
                                    {revenueDelta != null ? (
                                      <div className={`mt-1 text-[11px] font-medium ${deltaClass(revenueDelta)}`}>
                                        {formatDelta(revenueDelta, 'currency')}
                                      </div>
                                    ) : (
                                      <div className="mt-1 text-[11px] text-slate-400">—</div>
                                    )}
                                  </td>
                                  {selectedMonthHasFullCoverage ? (
                                    <>
                                      <td className="px-4 py-3 text-slate-700">{item.cogs == null ? '—' : formatCurrency(item.cogs)}</td>
                                      <td className="px-4 py-3 text-slate-700">{item.gross_profit == null ? '—' : formatCurrency(item.gross_profit)}</td>
                                    </>
                                  ) : null}
                                  <td className="px-4 py-3 text-slate-700">{formatCurrency(item.ozon_commission)}</td>
                                  <td className="px-4 py-3 text-slate-700">{formatCurrency(item.ozon_logistics)}</td>
                                  <td className="px-4 py-3 text-slate-700">{formatCurrency(item.ozon_services)}</td>
                                  <td className="px-4 py-3 text-slate-700">{formatCurrency(item.ozon_acquiring)}</td>
                                  <td className="px-4 py-3 text-slate-700">{formatCurrency(item.ozon_other_expenses)}</td>
                                  <td className={`px-4 py-3 ${item.ozon_incentives >= 0 ? 'text-emerald-700' : 'text-rose-700'}`}>{formatCurrency(item.ozon_incentives)}</td>
                                  <td className="px-4 py-3 text-slate-700">{formatCurrency(item.ozon_adjustments_net)}</td>
                                  {selectedMonthHasFullCoverage ? (
                                    <>
                                      <td className="px-4 py-3 text-slate-700">{item.tax_amount == null ? '—' : formatCurrency(item.tax_amount)}</td>
                                      <td className={`px-4 py-3 font-semibold ${item.net_profit == null ? 'text-slate-400' : item.net_profit >= 0 ? 'text-emerald-700' : 'text-rose-700'}`}>
                                        <div>{item.net_profit == null ? '—' : formatCurrency(item.net_profit)}</div>
                                        {profitDelta != null ? (
                                          <div className={`mt-1 text-[11px] font-medium ${deltaClass(profitDelta)}`}>
                                            {formatDelta(profitDelta, 'currency')}
                                          </div>
                                        ) : (
                                          <div className="mt-1 text-[11px] text-slate-400">—</div>
                                        )}
                                      </td>
                                    </>
                                  ) : null}
                                </tr>
                              );
                            })}
                            {filteredOffers.length === 0 ? (
                              <tr>
                                <td colSpan={15} className="px-4 py-8 text-center text-sm text-slate-500">
                                  По этому фильтру ничего не нашлось.
                                </td>
                              </tr>
                            ) : null}
                          </tbody>
                        </table>
                      </div>
                    </div>
                    </div>
                        </>
                      );
                    })()}
                  </>
                )}
              </section>
            </>
          )}
        </div>
      </Layout>
    </ProtectedRoute>
  );
}
