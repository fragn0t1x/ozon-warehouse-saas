import { apiClient, ensureArray, ensureObject } from './client';
import type { ExportJobStatus } from './warehouse';

export interface ClosedMonthFinance {
  id: number;
  store_id: number;
  month: string;
  status: 'pending' | 'partial' | 'ready' | 'error' | string;
  is_final: boolean;
  is_locked: boolean;
  realization_available: boolean;
  coverage_ratio: number;
  sold_units: number;
  sold_amount: number;
  returned_units: number;
  returned_amount: number;
  revenue_amount: number;
  revenue_net_of_vat: number;
  cogs: number;
  gross_profit: number;
  ozon_commission: number;
  ozon_logistics: number;
  ozon_services: number;
  ozon_acquiring: number;
  ozon_other_expenses: number;
  ozon_incentives: number;
  ozon_compensation: number;
  ozon_decompensation: number;
  ozon_adjustments_net: number;
  profit_before_tax: number;
  tax_amount: number;
  net_profit: number;
  vat_mode_used: string | null;
  tax_mode_used: string | null;
  tax_rate_used: number | null;
  tax_effective_from_used: string | null;
  cost_basis: string | null;
  cost_snapshot_date: string | null;
  generated_at: string | null;
  checked_at: string | null;
  source_payload?: Record<string, unknown> | null;
}

export interface ClosedMonthOfferFinance {
  id: number;
  store_month_finance_id: number;
  store_id: number;
  month: string;
  offer_id: string;
  title: string | null;
  basis: string;
  sold_units: number;
  sold_amount: number;
  returned_units: number;
  returned_amount: number;
  net_units: number;
  revenue_amount: number;
  revenue_net_of_vat: number;
  ozon_commission: number;
  ozon_logistics: number;
  ozon_services: number;
  ozon_acquiring: number;
  ozon_other_expenses: number;
  ozon_incentives: number;
  ozon_adjustments_net: number;
  unit_cost: number | null;
  cogs: number | null;
  gross_profit: number | null;
  profit_before_tax: number | null;
  tax_amount: number | null;
  net_profit: number | null;
  margin_ratio: number | null;
  vat_mode_used: string | null;
  tax_mode_used: string | null;
  tax_rate_used: number | null;
  tax_effective_from_used: string | null;
  cost_effective_from_used: string | null;
  has_cost: boolean;
}

export interface ClosedMonthFinanceDetail {
  month: ClosedMonthFinance;
  offers: ClosedMonthOfferFinance[];
}

export interface ClosedMonthSyncResponse {
  status: string;
  store_id: number;
  months_requested: number;
  start_month: string | null;
  end_month: string | null;
  task_queued: boolean;
}

export interface ClosedMonthCancelResponse {
  status: string;
  store_id: number;
  task_id?: string | null;
  message: string;
}

const normalizeExportJobStatus = (value: unknown, defaults: Partial<ExportJobStatus> = {}): ExportJobStatus => {
  const data = ensureObject(value, {
    kind: defaults.kind ?? '',
    store_id: defaults.store_id ?? 0,
    status: 'idle' as ExportJobStatus['status'],
    message: '',
    phase: null as string | null,
    phase_label: null as string | null,
    progress_percent: 0,
    queued_at: null as string | null,
    started_at: null as string | null,
    finished_at: null as string | null,
    task_id: null as string | null,
    file_name: null as string | null,
    download_url: null as string | null,
    order_window_days: null as number | null,
    selection_label: null as string | null,
    processed_items: 0,
    total_items: 0,
    error: null as string | null,
    duplicate_request: false,
    recent_runs: [] as ExportJobStatus['recent_runs'],
  });

  return {
    ...data,
    recent_runs: ensureArray(data.recent_runs).map((run) =>
      ensureObject(run, {
        status: 'success' as 'success' | 'error',
        message: '',
        order_window_days: null as number | null,
        selection_label: null as string | null,
        file_name: null as string | null,
        error: null as string | null,
        finished_at: null as string | null,
      })
    ),
  };
};

export const closedMonthsAPI = {
  async list(storeId: number, limit = 24): Promise<ClosedMonthFinance[]> {
    const response = await apiClient.get(`/stores/${storeId}/closed-months`, {
      params: { limit },
    });
    return ensureArray<ClosedMonthFinance>(response.data);
  },

  async get(storeId: number, month: string): Promise<ClosedMonthFinanceDetail> {
    const response = await apiClient.get(`/stores/${storeId}/closed-months/${month}`);
    return response.data as ClosedMonthFinanceDetail;
  },

  async getOffers(storeId: number, month: string): Promise<ClosedMonthOfferFinance[]> {
    const response = await apiClient.get(`/stores/${storeId}/closed-months/${month}/offers`);
    return ensureArray<ClosedMonthOfferFinance>(response.data);
  },

  async sync(
    storeId: number,
    options: { monthsBack?: number; startMonth?: string | null } = {}
  ): Promise<ClosedMonthSyncResponse> {
    const { monthsBack = 3, startMonth = null } = options;
    const response = await apiClient.post(`/stores/${storeId}/closed-months/actions/sync`, null, {
      params: startMonth ? { months_back: monthsBack, start_month: startMonth } : { months_back: monthsBack },
    });
    return response.data as ClosedMonthSyncResponse;
  },

  async cancel(storeId: number): Promise<ClosedMonthCancelResponse> {
    const response = await apiClient.post(`/stores/${storeId}/closed-months/actions/cancel`);
    return response.data as ClosedMonthCancelResponse;
  },

  async startExport(storeId: number, year: number): Promise<ExportJobStatus> {
    const response = await apiClient.post(`/stores/${storeId}/closed-months/actions/export`, null, {
      params: { year },
    });
    return normalizeExportJobStatus(response.data, {
      kind: 'closed_months',
      store_id: storeId,
      selection_label: `${year} г.`,
    });
  },

  async getExportStatus(storeId: number): Promise<ExportJobStatus> {
    const response = await apiClient.get(`/stores/${storeId}/closed-months/actions/export/status`);
    return normalizeExportJobStatus(response.data, {
      kind: 'closed_months',
      store_id: storeId,
    });
  },

  async clearExport(storeId: number): Promise<ExportJobStatus> {
    const response = await apiClient.delete(`/stores/${storeId}/closed-months/actions/export`);
    return normalizeExportJobStatus(response.data, {
      kind: 'closed_months',
      store_id: storeId,
    });
  },

  async downloadExport(storeId: number, fileName?: string | null): Promise<void> {
    const response = await apiClient.get(`/stores/${storeId}/closed-months/actions/export/download`, {
      responseType: 'blob',
    });
    const blobUrl = window.URL.createObjectURL(response.data as Blob);
    const link = document.createElement('a');
    link.href = blobUrl;
    link.download = fileName || 'closed_months.xlsx';
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(blobUrl);
  },
};
