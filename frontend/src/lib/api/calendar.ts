import { apiClient, ensureArray, ensureObject } from './client';

export interface CalendarSupplyItem {
  variant_id: number;
  sku?: string | null;
  offer_id?: string | null;
  product_name: string;
  pack_size?: number | null;
  attributes?: Record<string, string>;
  quantity: number;
  accepted_quantity?: number | null;
}

export interface CalendarItem {
  id: number;
  order_number: string;
  status: string;
  reservation_waiting_for_stock?: boolean;
  reservation_wait_message?: string | null;
  created_at?: string | null;
  timeslot_from?: string | null;
  timeslot_to?: string | null;
  eta_date?: string | null;
  avg_delivery_days?: number;
  store_id?: number;
  store_name?: string | null;
  storage_warehouse_name?: string | null;
  items?: CalendarSupplyItem[];
}

export const calendarAPI = {
  getCalendar: async (params?: {
    storeId?: number;
    statuses?: string[];
    dateFrom?: string;
    dateTo?: string;
  }): Promise<{ items: CalendarItem[] }> => {
    const query = new URLSearchParams();
    if (params?.storeId) query.append('store_id', params.storeId.toString());
    params?.statuses?.forEach((status) => query.append('status', status));
    if (params?.dateFrom) query.append('date_from', params.dateFrom);
    if (params?.dateTo) query.append('date_to', params.dateTo);

    const url = query.toString() ? `/calendar?${query.toString()}` : '/calendar';
    const response = await apiClient.get(url);
    const data = ensureObject(response.data, { items: [] as CalendarItem[] });
    return {
      items: ensureArray<CalendarItem>(data.items),
    };
  },
};
