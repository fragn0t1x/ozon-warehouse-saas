import { apiClient, ensureArray, ensureObject } from './client';

export interface StoreEconomicsHistoryEntry {
  id: number;
  store_id: number;
  effective_from: string;
  vat_mode: string;
  tax_mode: string;
  tax_rate: number;
  created_at: string;
}

export interface VariantCostHistoryEntry {
  id: number;
  variant_id: number;
  product_id: number;
  product_name: string;
  offer_id: string;
  pack_size: number;
  color?: string | null;
  size?: string | null;
  unit_cost?: number | null;
  effective_from: string;
  created_at: string;
  is_archived: boolean;
}

const normalizeStoreEconomicsHistoryEntry = (value: unknown): StoreEconomicsHistoryEntry =>
  ensureObject(value, {
    id: 0,
    store_id: 0,
    effective_from: '',
    vat_mode: 'none',
    tax_mode: 'usn_income_expenses',
    tax_rate: 0,
    created_at: '',
  });

const normalizeVariantCostHistoryEntry = (value: unknown): VariantCostHistoryEntry =>
  ensureObject(value, {
    id: 0,
    variant_id: 0,
    product_id: 0,
    product_name: '',
    offer_id: '',
    pack_size: 1,
    color: null,
    size: null,
    unit_cost: null,
    effective_from: '',
    created_at: '',
    is_archived: false,
  });

export const economicsHistoryAPI = {
  getStoreEconomicsHistory: async (storeId: number): Promise<StoreEconomicsHistoryEntry[]> => {
    const response = await apiClient.get(`/stores/${storeId}/economics-history`);
    return ensureArray(response.data).map(normalizeStoreEconomicsHistoryEntry);
  },

  getVariantCostHistory: async (storeId: number): Promise<VariantCostHistoryEntry[]> => {
    const response = await apiClient.get(`/products/cost-history/${storeId}`);
    return ensureArray(response.data).map(normalizeVariantCostHistoryEntry);
  },

  deleteStoreEconomicsHistory: async (storeId: number, historyId: number): Promise<void> => {
    await apiClient.delete(`/stores/${storeId}/economics-history/${historyId}`);
  },

  deleteVariantCostHistory: async (historyId: number): Promise<void> => {
    await apiClient.delete(`/products/cost-history/${historyId}`);
  },
};
