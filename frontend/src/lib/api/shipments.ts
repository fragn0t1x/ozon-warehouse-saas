import { apiClient, ensureArray, ensureObject } from './client';
import type { ExportJobStatus } from './warehouse';

export interface ShipmentVariant {
  variant_id: number;
  offer_id: string;
  pack_size: number;
  attributes: Record<string, string>;
  our_unpacked: number;
  our_packed: number;
  our_reserved: number;
  our_available: number;
  ozon_available: number;
  ozon_requested_to_supply: number;
  ozon_in_pipeline: number;
  ozon_returning: number;
  ordered_30d: number;
}

export interface ShipmentProduct {
  product_id: number;
  product_name: string;
  variants: ShipmentVariant[];
}

export interface ShipmentSupplySummary {
  id: number;
  order_number: string;
  status: string;
  timeslot_from: string | null;
  timeslot_to: string | null;
  eta_date: string | null;
}

export interface ShipmentWarehouse {
  id: number;
  name: string;
  ozon_id: string;
  avg_delivery_days: number;
  total_ozon_available: number;
  total_in_pipeline: number;
  total_ordered_30d: number;
  pending_departure_supplies: ShipmentSupplySummary[];
  in_transit_supplies: ShipmentSupplySummary[];
  products: ShipmentProduct[];
}

export interface ShipmentCluster {
  key: string;
  id: number | null;
  name: string;
  total_ozon_available: number;
  total_in_pipeline: number;
  total_ordered_30d: number;
  warehouses: ShipmentWarehouse[];
}

export interface ShipmentsResponse {
  clusters: ShipmentCluster[];
  warehouse_mode: 'shared' | 'per_store' | null;
  packing_mode: 'simple' | 'advanced' | null;
  our_warehouse_name: string | null;
  order_window_days: number;
  available_order_windows: number[];
  data_source?: 'ozon_stocks' | 'pipeline_only' | string | null;
  data_note?: string | null;
  orders_data_note?: string | null;
  orders_period_from?: string | null;
  orders_period_to?: string | null;
  orders_updated_at?: string | null;
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

const normalizeShipmentVariant = (value: unknown): ShipmentVariant => {
  const data = ensureObject(value, {
    variant_id: 0,
    offer_id: '',
    pack_size: 1,
    attributes: {} as Record<string, string>,
    our_unpacked: 0,
    our_packed: 0,
    our_reserved: 0,
    our_available: 0,
    ozon_available: 0,
    ozon_requested_to_supply: 0,
    ozon_in_pipeline: 0,
    ozon_returning: 0,
    ordered_30d: 0,
  });
  return {
    ...data,
    attributes: ensureObject<Record<string, string>>(data.attributes, {}),
  };
};

const normalizeShipmentProduct = (value: unknown): ShipmentProduct => {
  const data = ensureObject(value, {
    product_id: 0,
    product_name: '',
    variants: [] as ShipmentVariant[],
  });
  return {
    ...data,
    variants: ensureArray(data.variants).map(normalizeShipmentVariant),
  };
};

const normalizeShipmentWarehouse = (value: unknown): ShipmentWarehouse => {
  const data = ensureObject(value, {
    id: 0,
    name: '',
    ozon_id: '',
    avg_delivery_days: 0,
    total_ozon_available: 0,
    total_in_pipeline: 0,
    total_ordered_30d: 0,
    pending_departure_supplies: [] as ShipmentSupplySummary[],
    in_transit_supplies: [] as ShipmentSupplySummary[],
    products: [] as ShipmentProduct[],
  });
  return {
    ...data,
    pending_departure_supplies: ensureArray(data.pending_departure_supplies).map((item) => ensureObject(item, {
      id: 0,
      order_number: '',
      status: '',
      timeslot_from: null as string | null,
      timeslot_to: null as string | null,
      eta_date: null as string | null,
    })),
    in_transit_supplies: ensureArray(data.in_transit_supplies).map((item) => ensureObject(item, {
      id: 0,
      order_number: '',
      status: '',
      timeslot_from: null as string | null,
      timeslot_to: null as string | null,
      eta_date: null as string | null,
    })),
    products: ensureArray(data.products).map(normalizeShipmentProduct),
  };
};

const normalizeShipmentCluster = (value: unknown): ShipmentCluster => {
  const data = ensureObject(value, {
    key: '',
    id: null,
    name: '',
    total_ozon_available: 0,
    total_in_pipeline: 0,
    total_ordered_30d: 0,
    warehouses: [] as ShipmentWarehouse[],
  });
  return {
    ...data,
    warehouses: ensureArray(data.warehouses).map(normalizeShipmentWarehouse),
  };
};

export const shipmentsAPI = {
  getShipments: async (storeId?: number, orderWindowDays: number = 30): Promise<ShipmentsResponse> => {
    const query = new URLSearchParams();
    if (storeId) query.append('store_id', storeId.toString());
    query.append('order_window_days', String(orderWindowDays));
    const url = query.toString() ? `/shipments?${query.toString()}` : '/shipments';
    const response = await apiClient.get(url);
    const data = ensureObject(response.data, {
      clusters: [] as ShipmentCluster[],
      warehouse_mode: null as ShipmentsResponse['warehouse_mode'],
      packing_mode: null as ShipmentsResponse['packing_mode'],
      our_warehouse_name: null as string | null,
      order_window_days: 30,
      available_order_windows: [7, 30] as number[],
      data_source: null as ShipmentsResponse['data_source'],
      data_note: null as string | null,
      orders_data_note: null as string | null,
      orders_period_from: null as string | null,
      orders_period_to: null as string | null,
      orders_updated_at: null as string | null,
    });
    return {
      ...data,
      warehouse_mode: ['shared', 'per_store', null].includes(data.warehouse_mode as never)
        ? data.warehouse_mode
        : null,
      packing_mode: ['simple', 'advanced', null].includes(data.packing_mode as never)
        ? data.packing_mode
        : null,
      order_window_days: Number(data.order_window_days) || 30,
      available_order_windows: ensureArray(data.available_order_windows).map((item) => Number(item) || 0).filter((item) => item > 0),
      clusters: ensureArray(data.clusters).map(normalizeShipmentCluster),
    };
  },
  startExport: async (
    storeId: number,
    orderWindowDays: number,
    options?: { productFilter?: string | null; selectedProductNames?: string[] }
  ): Promise<ExportJobStatus> => {
    const query = new URLSearchParams({
      store_id: String(storeId),
      order_window_days: String(orderWindowDays),
    });
    const productFilter = String(options?.productFilter || '').trim();
    if (productFilter) {
      query.append('product_filter', productFilter);
    }
    for (const name of options?.selectedProductNames || []) {
      const normalizedName = String(name || '').trim();
      if (normalizedName) {
        query.append('selected_product_names', normalizedName);
      }
    }
    const response = await apiClient.post(`/shipments/export?${query.toString()}`);
    return normalizeExportJobStatus(response.data, {
      kind: 'shipments',
      store_id: storeId,
      order_window_days: orderWindowDays,
    });
  },
  getExportStatus: async (storeId: number): Promise<ExportJobStatus> => {
    const response = await apiClient.get(`/shipments/export/status?store_id=${storeId}`);
    return normalizeExportJobStatus(response.data, {
      kind: 'shipments',
      store_id: storeId,
    });
  },
  clearExport: async (storeId: number): Promise<ExportJobStatus> => {
    const response = await apiClient.delete(`/shipments/export?store_id=${storeId}`);
    return normalizeExportJobStatus(response.data, {
      kind: 'shipments',
      store_id: storeId,
    });
  },
  downloadExport: async (storeId: number, fileName?: string | null): Promise<void> => {
    const response = await apiClient.get(`/shipments/export/download?store_id=${storeId}`, {
      responseType: 'blob',
    });
    const blobUrl = window.URL.createObjectURL(response.data as Blob);
    const link = document.createElement('a');
    link.href = blobUrl;
    link.download = fileName || 'shipments.xlsx';
    document.body.appendChild(link);
    link.click();
    link.remove();
    window.URL.revokeObjectURL(blobUrl);
  },
};
