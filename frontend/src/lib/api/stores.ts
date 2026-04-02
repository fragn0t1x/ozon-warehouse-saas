import { apiClient, ensureArray, ensureObject } from './client';

export interface Store {
  id: number;
  user_id: number;
  name: string;
  client_id: string;
  is_active: boolean;
  created_at: string;
  warehouse_id?: number;
  bootstrap_state?: string | null;
  economics_vat_mode: 'none' | 'usn_5' | 'usn_7' | 'osno_10' | 'osno_22';
  economics_tax_mode: 'before_tax' | 'usn_income' | 'usn_income_expenses' | 'custom_profit';
  economics_tax_rate: number;
  economics_default_sale_price_gross?: number | null;
  economics_effective_from?: string | null;
}

export interface StoreCreate {
  name: string;
  client_id: string;
  api_key: string;
  user_id?: number;
  product_links?: StoreProductLinkDecision[];
}

export interface StoreValidate {
  name?: string;
  client_id: string;
  api_key: string;
}

export interface ValidationResult {
  valid: boolean;
  message: string;
  data?: {
    products_count: number;
    has_products: boolean;
  };
  store_info?: {
    name: string;
    client_id: string;
    is_valid: boolean;
  };
}

export interface StorePreviewVariant {
  offer_id: string;
  pack_size: number;
  color: string;
  size: string;
}

export interface StorePreviewSizeGroup {
  size: string;
  variants: StorePreviewVariant[];
}

export interface StorePreviewColorGroup {
  color: string;
  sizes: StorePreviewSizeGroup[];
}

export interface StorePreviewGroup {
  group_key: string;
  base_name: string;
  product_name: string;
  image_url?: string | null;
  total_variants: number;
  colors: StorePreviewColorGroup[];
  match_status: 'auto' | 'conflict' | 'new';
  suggested_warehouse_product_id?: number | null;
  candidates: StorePreviewCandidate[];
  match_explanation?: string | null;
}

export interface WarehouseProductOption {
  id: number;
  name: string;
}

export interface StorePreviewCandidate {
  id: number;
  name: string;
  score: number;
  overlap_count: number;
  overlap_total: number;
  reasons: string[];
}

export interface StoreImportPreview {
  grouped_products: StorePreviewGroup[];
  available_warehouse_products: WarehouseProductOption[];
}

export interface StoreProductLinkDecision {
  base_name: string;
  group_key?: string;
  offer_ids?: string[];
  warehouse_product_id?: number;
  warehouse_product_name?: string;
}


export interface StoreSyncKindStatus {
  kind: string;
  status: 'idle' | 'queued' | 'running' | 'success' | 'failed' | 'skipped' | 'cancelled';
  message: string;
  task_id?: string | null;
  phase?: string | null;
  phase_label?: string | null;
  progress_percent?: number | null;
  months_requested?: number | null;
  months_completed?: number | null;
  start_month?: string | null;
  end_month?: string | null;
  current_month?: string | null;
  queued_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  updated_at?: string | null;
  last_success_at?: string | null;
  last_failure_at?: string | null;
}

export interface StoreSyncStatus {
  store_id: number;
  store_name?: string;
  status: 'idle' | 'queued' | 'running' | 'success' | 'failed' | 'cancelled' | 'skipped';
  message: string;
  active_sync_kinds?: string[];
  active_sync?: {
    kind?: string | null;
    source?: string | null;
    task_id?: string | null;
    started_at?: string | null;
  } | null;
  task_id?: string | null;
  queued_at?: string | null;
  started_at?: string | null;
  finished_at?: string | null;
  updated_at?: string | null;
  bootstrap_state?: string | null;
  sync_kinds?: Record<string, StoreSyncKindStatus>;
}

const EMPTY_STORE: Store = {
  id: 0,
  user_id: 0,
  name: '',
  client_id: '',
  is_active: false,
  created_at: '',
  warehouse_id: undefined,
  bootstrap_state: null,
  economics_vat_mode: 'none',
  economics_tax_mode: 'usn_income_expenses',
  economics_tax_rate: 15,
  economics_default_sale_price_gross: null,
  economics_effective_from: null,
};

const normalizeStore = (value: unknown): Store => ensureObject(value, EMPTY_STORE);

const normalizeValidationResult = (value: unknown): ValidationResult => {
  const data = ensureObject(value, {
    valid: false,
    message: 'Не удалось проверить магазин',
    data: { products_count: 0, has_products: false },
    store_info: { name: '', client_id: '', is_valid: false },
  });
  return {
    ...data,
    data: ensureObject(data.data, { products_count: 0, has_products: false }),
    store_info: ensureObject(data.store_info, { name: '', client_id: '', is_valid: false }),
  };
};

const normalizePreviewVariant = (value: unknown): StorePreviewVariant =>
  ensureObject(value, { offer_id: '', pack_size: 1, color: '', size: '' });

const normalizePreviewSizeGroup = (value: unknown): StorePreviewSizeGroup => {
  const data = ensureObject(value, { size: '', variants: [] as StorePreviewVariant[] });
  return {
    size: data.size,
    variants: ensureArray(data.variants).map(normalizePreviewVariant),
  };
};

const normalizePreviewColorGroup = (value: unknown): StorePreviewColorGroup => {
  const data = ensureObject(value, { color: '', sizes: [] as StorePreviewSizeGroup[] });
  return {
    color: data.color,
    sizes: ensureArray(data.sizes).map(normalizePreviewSizeGroup),
  };
};

const normalizePreviewCandidate = (value: unknown): StorePreviewCandidate => {
  const data = ensureObject(value, {
    id: 0,
    name: '',
    score: 0,
    overlap_count: 0,
    overlap_total: 0,
    reasons: [] as string[],
  });
  return {
    ...data,
    reasons: ensureArray<string>(data.reasons),
  };
};

const normalizePreviewGroup = (value: unknown): StorePreviewGroup => {
  const data = ensureObject(value, {
    group_key: '',
    base_name: '',
    product_name: '',
    image_url: null,
    total_variants: 0,
    colors: [] as StorePreviewColorGroup[],
    match_status: 'new' as StorePreviewGroup['match_status'],
    suggested_warehouse_product_id: null,
    candidates: [] as StorePreviewCandidate[],
    match_explanation: null,
  });
  return {
    ...data,
    group_key: data.group_key || data.base_name,
    match_status: ['auto', 'conflict', 'new'].includes(data.match_status) ? data.match_status : 'new',
    colors: ensureArray(data.colors).map(normalizePreviewColorGroup),
    candidates: ensureArray(data.candidates).map(normalizePreviewCandidate),
  };
};

const normalizeWarehouseProductOption = (value: unknown): WarehouseProductOption =>
  ensureObject(value, { id: 0, name: '' });

const normalizeStoreImportPreview = (value: unknown): StoreImportPreview => {
  const data = ensureObject(value, {
    grouped_products: [] as StorePreviewGroup[],
    available_warehouse_products: [] as WarehouseProductOption[],
  });
  return {
    grouped_products: ensureArray(data.grouped_products).map(normalizePreviewGroup),
    available_warehouse_products: ensureArray(data.available_warehouse_products).map(normalizeWarehouseProductOption),
  };
};

const SYNC_STATUS_CACHE_TTL_MS = 4000;
const syncStatusCache = new Map<number, { expiresAt: number; value: StoreSyncStatus }>();
const syncStatusInFlight = new Map<number, Promise<StoreSyncStatus>>();

function invalidateSyncStatusCache(id: number) {
  syncStatusCache.delete(id);
  syncStatusInFlight.delete(id);
}

const normalizeStoreSyncStatus = (value: unknown, id: number): StoreSyncStatus => {
  const data = ensureObject(value, {
    store_id: id,
    store_name: undefined,
    status: 'idle' as StoreSyncStatus['status'],
    message: 'Синхронизация не запускалась',
    active_sync_kinds: [] as string[],
    active_sync: null,
    task_id: null,
    queued_at: null,
    started_at: null,
    finished_at: null,
    updated_at: null,
    bootstrap_state: null,
    sync_kinds: {},
  });
  return {
    ...data,
    status: ['idle', 'queued', 'running', 'success', 'failed', 'cancelled', 'skipped'].includes(data.status)
      ? data.status
      : 'idle',
    active_sync_kinds: ensureArray<string>(data.active_sync_kinds),
    active_sync: data.active_sync && typeof data.active_sync === 'object' ? ensureObject(data.active_sync, { kind: null, source: null, task_id: null, started_at: null }) : null,
    sync_kinds: ensureObject(data.sync_kinds, {}),
  };
};

export const storesAPI = {
  getAll: async (): Promise<Store[]> => {
    const response = await apiClient.get('/stores/');
    return ensureArray(response.data).map(normalizeStore);
  },

  getById: async (id: number): Promise<Store> => {
    const response = await apiClient.get(`/stores/${id}`);
    return normalizeStore(response.data);
  },

  create: async (data: StoreCreate): Promise<Store> => {
    const { user_id, ...cleanData } = data;
    const response = await apiClient.post('/stores/', cleanData);
    return normalizeStore(response.data);
  },

  update: async (id: number, data: Partial<StoreCreate>): Promise<Store> => {
    const response = await apiClient.put(`/stores/${id}`, data);
    return normalizeStore(response.data);
  },

  patch: async (
    id: number,
    data: Partial<Pick<
      Store,
      | 'name'
      | 'client_id'
      | 'is_active'
      | 'economics_vat_mode'
      | 'economics_tax_mode'
      | 'economics_tax_rate'
      | 'economics_default_sale_price_gross'
    > & { api_key?: string | null; economics_effective_from?: string | null }>
  ): Promise<Store> => {
    const response = await apiClient.patch(`/stores/${id}`, data);
    return normalizeStore(response.data);
  },

  delete: async (id: number): Promise<void> => {
    await apiClient.delete(`/stores/${id}`);
  },

  syncProducts: async (id: number): Promise<{ status: string; message: string; task_id?: string }> => {
    const response = await apiClient.post(`/sync/products/${id}`);
    invalidateSyncStatusCache(id);
    return ensureObject(response.data, { status: 'success', message: '', task_id: undefined });
  },

  syncStocks: async (id: number): Promise<{ status: string; message: string; task_id?: string }> => {
    const response = await apiClient.post(`/sync/stocks/${id}`);
    invalidateSyncStatusCache(id);
    return ensureObject(response.data, { status: 'queued', message: '', task_id: undefined });
  },

  syncSupplies: async (id: number): Promise<{ status: string; message: string; task_id?: string }> => {
    const response = await apiClient.post(`/sync/supplies/${id}`);
    invalidateSyncStatusCache(id);
    return ensureObject(response.data, { status: 'queued', message: '', task_id: undefined });
  },

  syncReports: async (id: number): Promise<{ status: string; message: string; task_id?: string }> => {
    const response = await apiClient.post(`/sync/reports/${id}`);
    invalidateSyncStatusCache(id);
    return ensureObject(response.data, { status: 'queued', message: '', task_id: undefined });
  },

  syncFinance: async (id: number): Promise<{ status: string; message: string; task_id?: string }> => {
    const response = await apiClient.post(`/sync/finance/${id}`);
    invalidateSyncStatusCache(id);
    return ensureObject(response.data, { status: 'queued', message: '', task_id: undefined });
  },

  cancelSync: async (id: number, kind: 'full' | 'products' | 'stocks' | 'supplies' | 'reports' | 'finance' | 'closed_months'): Promise<{ status: string; message?: string; task_id?: string }> => {
    const response = await apiClient.post(`/sync/${kind}/${id}/cancel`);
    invalidateSyncStatusCache(id);
    return ensureObject(response.data, { status: 'cancelled', message: '', task_id: undefined });
  },

  syncFull: async (id: number): Promise<{ status: string; message: string; task_id?: string }> => {
    const response = await apiClient.post(`/sync/full/${id}`);
    invalidateSyncStatusCache(id);
    return ensureObject(response.data, { status: 'queued', message: '', task_id: undefined });
  },

  getSyncStatus: async (id: number, options?: { force?: boolean }): Promise<StoreSyncStatus> => {
    const force = options?.force === true;
    const now = Date.now();
    const cached = syncStatusCache.get(id);
    if (!force && cached && cached.expiresAt > now) {
      return cached.value;
    }

    if (!force) {
      const existing = syncStatusInFlight.get(id);
      if (existing) {
        return existing;
      }
    }

    const request = apiClient
      .get(`/sync/status/${id}`)
      .then((response) => {
        const normalized = normalizeStoreSyncStatus(response.data, id);
        syncStatusCache.set(id, {
          expiresAt: Date.now() + SYNC_STATUS_CACHE_TTL_MS,
          value: normalized,
        });
        return normalized;
      })
      .finally(() => {
        syncStatusInFlight.delete(id);
      });

    syncStatusInFlight.set(id, request);
    return request;
  },

  validate: async (data: StoreValidate): Promise<ValidationResult> => {
    const payload = {
      name: data.name || 'Валидация магазина',
      client_id: data.client_id,
      api_key: data.api_key,
    };
    const response = await apiClient.post('/stores/validate', payload);
    return normalizeValidationResult(response.data);
  },

  getProductLinkPreview: async (data: StoreValidate): Promise<StoreImportPreview> => {
    const payload = {
      name: data.name || 'Валидация магазина',
      client_id: data.client_id,
      api_key: data.api_key,
    };
    const response = await apiClient.post('/stores/product-link-preview', payload);
    return normalizeStoreImportPreview(response.data);
  },
};
