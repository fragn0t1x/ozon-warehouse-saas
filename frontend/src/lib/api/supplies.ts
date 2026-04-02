import { apiClient, ensureArray, ensureObject } from './client';

export interface SupplyItem {
  variant_id: number;
  offer_id: string;
  product_name: string;
  pack_size: number;
  attributes?: Record<string, string>;
  quantity: number;
  accepted_quantity?: number | null;
}

export interface Supply {
  id: number;
  order_number: string;
  status: string;
  reservation_waiting_for_stock?: boolean;
  reservation_wait_message?: string | null;
  completed_at?: string | null;
  eta_date?: string | null;
  timeslot_from?: string | null;
  timeslot_to?: string | null;
  created_at?: string | null;
  store_id?: number;
  store_name?: string | null;
  dropoff_warehouse_name?: string | null;
  storage_warehouse_name?: string | null;
  items?: SupplyItem[];
}

export interface SuppliesResponse {
  items: Supply[];
  total: number;
  page: number;
  page_size: number;
}

const normalizeSupplyItem = (value: unknown): SupplyItem => {
  const data = ensureObject(value, {
    variant_id: 0,
    offer_id: '',
    product_name: '',
    pack_size: 1,
    attributes: {} as Record<string, string>,
    quantity: 0,
    accepted_quantity: null,
  });
  return {
    ...data,
    attributes: ensureObject<Record<string, string>>(data.attributes, {}),
  };
};

const normalizeSupply = (value: unknown): Supply => {
  const data = ensureObject(value, {
    id: 0,
    order_number: '',
    status: '',
    reservation_waiting_for_stock: false,
    reservation_wait_message: null,
    completed_at: null,
    eta_date: null,
    timeslot_from: null,
    timeslot_to: null,
    created_at: null,
    store_id: undefined,
    store_name: null,
    dropoff_warehouse_name: null,
    storage_warehouse_name: null,
    items: [] as SupplyItem[],
  });
  return {
    ...data,
    items: ensureArray(data.items).map(normalizeSupplyItem),
  };
};

export const suppliesAPI = {
  getSupplies: async (params?: {
    statuses?: string[];
    page?: number;
    pageSize?: number;
    includeItems?: boolean;
    storeId?: number;
  }): Promise<SuppliesResponse> => {
    const query = new URLSearchParams();
    params?.statuses?.forEach((status) => query.append('status', status));
    if (params?.page) query.append('page', params.page.toString());
    if (params?.pageSize) query.append('page_size', params.pageSize.toString());
    if (params?.includeItems) query.append('include_items', 'true');
    if (params?.storeId) query.append('store_id', params.storeId.toString());

    const url = query.toString() ? `/supplies?${query.toString()}` : '/supplies';
    const response = await apiClient.get(url);
    const data = ensureObject(response.data, {
      items: [] as Supply[],
      total: 0,
      page: params?.page ?? 1,
      page_size: params?.pageSize ?? 15,
    });
    return {
      ...data,
      items: ensureArray(data.items).map(normalizeSupply),
    };
  },

  getSupplyItems: async (supplyId: number): Promise<SupplyItem[]> => {
    const response = await apiClient.get(`/supplies/${supplyId}/items`);
    return ensureArray(response.data).map(normalizeSupplyItem);
  },
};
