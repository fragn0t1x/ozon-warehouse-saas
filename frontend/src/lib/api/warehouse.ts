import { apiClient, ensureArray, ensureObject } from './client';

export interface StockItem {
  variant_id: number;
  offer_id: string;
  product_name: string;
  pack_size: number;
  color?: string;
  size?: string;
  unpacked: number;
  packed: number;
  reserved: number;
  available: number;
  group_unpacked?: number;
  group_reserved?: number;
  group_available?: number;
  ordered_30d?: number;
  group_ordered_30d?: number;
  warehouse_id: number;
  warehouse_name: string;
  store_id?: number;
  store_name?: string | null;
  product_store_id?: number;
  product_store_name?: string | null;
  attributes?: Record<string, string>;
}

export interface WarehouseOverviewVariant {
  variant_id: number;
  offer_id: string;
  sku: string;
  pack_size: number;
  color: string;
  size: string;
  attributes?: Record<string, string>;
  warehouse_unpacked: number;
  warehouse_reserved: number;
  warehouse_available: number;
  ozon_available: number;
  ozon_ready_for_sale: number;
  ozon_requested_to_supply: number;
  ozon_in_transit: number;
  ozon_returning: number;
  ordered_units: number;
  current_price?: number | null;
  total_units: number;
}

export interface WarehouseOverviewColorGroup {
  color: string;
  variants: WarehouseOverviewVariant[];
}

export interface WarehouseOverviewProduct {
  product_id: number;
  product_name: string;
  image_url?: string | null;
  warehouse_unpacked: number;
  warehouse_reserved: number;
  warehouse_available: number;
  ozon_available: number;
  ozon_ready_for_sale: number;
  ozon_requested_to_supply: number;
  ozon_in_transit: number;
  ozon_returning: number;
  ordered_units: number;
  total_units: number;
  colors: WarehouseOverviewColorGroup[];
}

export interface WarehouseOverview {
  store_id: number;
  store_name: string;
  warehouse_mode: 'shared' | 'per_store';
  packing_mode?: 'simple' | 'advanced';
  warehouse_name: string;
  warehouse_scope: 'shared' | 'per_store';
  order_window_days: number;
  available_order_windows: number[];
  orders_period_from?: string | null;
  orders_period_to?: string | null;
  orders_updated_at?: string | null;
  products: WarehouseOverviewProduct[];
}

export interface ExportJobStatus {
  kind: string;
  store_id: number;
  status: 'idle' | 'queued' | 'running' | 'success' | 'error' | 'stale';
  message: string;
  phase?: string | null;
  phase_label?: string | null;
  progress_percent: number;
  queued_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  task_id?: string | null;
  file_name?: string | null;
  download_url?: string | null;
  order_window_days?: number | null;
  selection_label?: string | null;
  processed_items?: number;
  total_items?: number;
  error?: string | null;
  duplicate_request?: boolean;
  recent_runs?: Array<{
    status: 'success' | 'error';
    message: string;
    order_window_days?: number | null;
    selection_label?: string | null;
    file_name?: string | null;
    error?: string | null;
    finished_at?: string | null;
  }>;
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
    order_window_days: defaults.order_window_days ?? null,
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

export interface IncomeRequest {
  store_id?: number;
  warehouse_id?: number;
  variant_id: number;
  quantity: number;
}

export interface IncomeBatchRequest {
  store_id?: number;
  warehouse_id?: number;
  items: Array<{
    variant_id: number;
    quantity: number;
  }>;
}

export interface PackRequest {
  store_id?: number;
  warehouse_id?: number;
  variant_id: number;
  boxes: number;
}

export interface PackBatchRequest {
  store_id?: number;
  warehouse_id?: number;
  items: Array<{
    variant_id: number;
    boxes: number;
  }>;
}

export interface ReserveRequest {
  store_id?: number;
  warehouse_id?: number;
  variant_id: number;
  quantity: number;
  supply_id: number;
}

export interface Transaction {
  id: number;
  type: string;
  quantity: number;
  variant_id: number;
  offer_id: string;
  pack_size: number;
  product_name: string;
  product_store_id?: number;
  product_store_name?: string | null;
  color?: string;
  size?: string;
  attributes?: Record<string, string>;
  created_at: string;
  reference_type?: string;
  reference_id?: number;
  batch_key: string;
  can_delete: boolean;
}

export interface WarehouseInfo {
  mode: 'shared' | 'per_store';
  packing_mode?: 'simple' | 'advanced';
  orders_period_from?: string | null;
  orders_period_to?: string | null;
  orders_updated_at?: string | null;
  orders_stores_covered?: number;
  orders_stores_missing?: number;
  warehouses: Array<{
    id: number;
    name: string;
    store_id: number | null;
    store_name: string | null;
  }>;
}

const normalizeStockItem = (value: unknown): StockItem => {
  const data = ensureObject(value, {
    variant_id: 0,
    offer_id: '',
    product_name: '',
    pack_size: 1,
    color: '',
    size: '',
    unpacked: 0,
    packed: 0,
    reserved: 0,
    available: 0,
    group_unpacked: 0,
    group_reserved: 0,
    group_available: 0,
    ordered_30d: 0,
    group_ordered_30d: 0,
    warehouse_id: 0,
    warehouse_name: '',
    store_id: undefined,
    store_name: null,
    product_store_id: undefined,
    product_store_name: null,
    attributes: {} as Record<string, string>,
  });
  return {
    ...data,
    attributes: ensureObject<Record<string, string>>(data.attributes, {}),
  };
};

const normalizeTransaction = (value: unknown): Transaction => {
  const data = ensureObject(value, {
    id: 0,
    type: '',
    quantity: 0,
    variant_id: 0,
    offer_id: '',
    pack_size: 1,
    product_name: '',
    product_store_id: undefined,
    product_store_name: null,
    color: '',
    size: '',
    attributes: {} as Record<string, string>,
    created_at: '',
    reference_type: undefined,
    reference_id: undefined,
    batch_key: '',
    can_delete: false,
  });
  return {
    ...data,
    attributes: ensureObject<Record<string, string>>(data.attributes, {}),
  };
};

const normalizeWarehouseInfo = (value: unknown): WarehouseInfo => {
  const data = ensureObject(value, {
    mode: 'shared' as WarehouseInfo['mode'],
    packing_mode: 'simple' as WarehouseInfo['packing_mode'],
    orders_period_from: null as string | null,
    orders_period_to: null as string | null,
    orders_updated_at: null as string | null,
    orders_stores_covered: 0,
    orders_stores_missing: 0,
    warehouses: [] as WarehouseInfo['warehouses'],
  });
  return {
    ...data,
    mode: ['shared', 'per_store'].includes(data.mode) ? data.mode : 'shared',
    packing_mode: ['simple', 'advanced', undefined, null].includes(data.packing_mode as never)
      ? data.packing_mode ?? 'simple'
      : 'simple',
    warehouses: ensureArray(data.warehouses).map((item) =>
      ensureObject(item, {
        id: 0,
        name: '',
        store_id: null,
        store_name: null,
      })
    ),
  };
};

const normalizeWarehouseOverview = (value: unknown): WarehouseOverview => {
  const data = ensureObject(value, {
    store_id: 0,
    store_name: '',
    warehouse_mode: 'shared' as WarehouseOverview['warehouse_mode'],
    packing_mode: 'simple' as WarehouseOverview['packing_mode'],
    warehouse_name: '',
    warehouse_scope: 'shared' as WarehouseOverview['warehouse_scope'],
    order_window_days: 7,
    available_order_windows: [7, 30] as number[],
    orders_period_from: null as string | null,
    orders_period_to: null as string | null,
    orders_updated_at: null as string | null,
    products: [] as WarehouseOverviewProduct[],
  });

  return {
    ...data,
    available_order_windows: ensureArray(data.available_order_windows).map((item) => Number(item) || 0).filter((item) => item > 0),
    products: ensureArray(data.products).map((product) => {
      const productData = ensureObject(product, {
        product_id: 0,
        product_name: '',
        image_url: null,
        warehouse_unpacked: 0,
        warehouse_reserved: 0,
        warehouse_available: 0,
        ozon_available: 0,
        ozon_ready_for_sale: 0,
        ozon_requested_to_supply: 0,
        ozon_in_transit: 0,
        ozon_returning: 0,
        ordered_units: 0,
        total_units: 0,
        colors: [] as WarehouseOverviewColorGroup[],
      });

      return {
        ...productData,
        colors: ensureArray(productData.colors).map((color) => {
          const colorData = ensureObject(color, {
            color: '',
            variants: [] as WarehouseOverviewVariant[],
          });
          return {
            ...colorData,
            variants: ensureArray(colorData.variants).map((variant) =>
              ensureObject(variant, {
                variant_id: 0,
                offer_id: '',
                sku: '',
                pack_size: 1,
                color: '',
                size: '',
                attributes: {} as Record<string, string>,
                warehouse_unpacked: 0,
                warehouse_reserved: 0,
                warehouse_available: 0,
                ozon_available: 0,
                ozon_ready_for_sale: 0,
                ozon_requested_to_supply: 0,
                ozon_in_transit: 0,
                ozon_returning: 0,
                ordered_units: 0,
                current_price: null as number | null,
                total_units: 0,
              })
            ),
          };
        }),
      };
    }),
  };
};

export const warehouseAPI = {
  getOverview: async (storeId: number, orderWindowDays: number = 7): Promise<WarehouseOverview> => {
    const params = new URLSearchParams({
      store_id: String(storeId),
      order_window_days: String(orderWindowDays),
    });
    const response = await apiClient.get(`/warehouse/overview?${params.toString()}`);
    return normalizeWarehouseOverview(response.data);
  },

  startExport: async (storeId: number, orderWindowDays: number): Promise<ExportJobStatus> => {
    const params = new URLSearchParams({
      store_id: String(storeId),
      order_window_days: String(orderWindowDays),
    });
    const response = await apiClient.post(`/warehouse/export?${params.toString()}`);
    return normalizeExportJobStatus(response.data, {
      kind: 'warehouse',
      store_id: storeId,
      order_window_days: orderWindowDays,
    });
  },

  getExportStatus: async (storeId: number): Promise<ExportJobStatus> => {
    const response = await apiClient.get(`/warehouse/export/status?store_id=${storeId}`);
    return normalizeExportJobStatus(response.data, {
      kind: 'warehouse',
      store_id: storeId,
    });
  },

  clearExport: async (storeId: number): Promise<ExportJobStatus> => {
    const response = await apiClient.delete(`/warehouse/export?store_id=${storeId}`);
    return normalizeExportJobStatus(response.data, {
      kind: 'warehouse',
      store_id: storeId,
    });
  },

  downloadExport: async (storeId: number, fileName?: string | null): Promise<void> => {
    const response = await apiClient.get(`/warehouse/export/download?store_id=${storeId}`, {
      responseType: 'blob',
    });
    const blobUrl = window.URL.createObjectURL(response.data as Blob);
    const link = document.createElement('a');
    link.href = blobUrl;
    link.download = fileName || 'warehouse.xlsx';
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(blobUrl);
  },

  getStocks: async (storeId?: number, warehouseId?: number): Promise<StockItem[]> => {
    let url = '/warehouse/stocks';
    const params = new URLSearchParams();
    if (storeId) params.append('store_id', storeId.toString());
    if (warehouseId) params.append('warehouse_id', warehouseId.toString());
    if (params.toString()) url += `?${params.toString()}`;

    const response = await apiClient.get(url);
    return ensureArray(response.data).map(normalizeStockItem);
  },

  income: async (data: IncomeRequest): Promise<Record<string, unknown>> => {
    const response = await apiClient.post('/warehouse/income', data);
    return ensureObject(response.data, {});
  },

  incomeBatch: async (data: IncomeBatchRequest): Promise<Record<string, unknown>> => {
    const response = await apiClient.post('/warehouse/income-batch', data);
    return ensureObject(response.data, {});
  },

  pack: async (data: PackRequest): Promise<Record<string, unknown>> => {
    const response = await apiClient.post('/warehouse/pack', data);
    return ensureObject(response.data, {});
  },

  packBatch: async (data: PackBatchRequest): Promise<Record<string, unknown>> => {
    const response = await apiClient.post('/warehouse/pack-batch', data);
    return ensureObject(response.data, {});
  },

  reserve: async (data: ReserveRequest): Promise<Record<string, unknown>> => {
    const response = await apiClient.post('/warehouse/reserve', data);
    return ensureObject(response.data, {});
  },

  getTransactions: async (storeId?: number, warehouseId?: number, limit: number = 20): Promise<Transaction[]> => {
    let url = '/warehouse/transactions';
    const params = new URLSearchParams();
    if (storeId) params.append('store_id', storeId.toString());
    if (warehouseId) params.append('warehouse_id', warehouseId.toString());
    if (limit) params.append('limit', limit.toString());
    if (params.toString()) url += `?${params.toString()}`;

    const response = await apiClient.get(url);
    return ensureArray(response.data).map(normalizeTransaction);
  },

  deleteTransaction: async (transactionId: number): Promise<Record<string, unknown>> => {
    const response = await apiClient.delete(`/warehouse/transactions/${transactionId}`);
    return ensureObject(response.data, {});
  },

  getWarehouseInfo: async (): Promise<WarehouseInfo> => {
    const response = await apiClient.get('/warehouse/info');
    return normalizeWarehouseInfo(response.data);
  },
};
