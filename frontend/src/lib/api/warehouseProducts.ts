import { apiClient, ensureArray, ensureObject } from './client';

export interface WarehouseProductVariant {
  id: number;
  offer_id: string;
  pack_size: number;
  color: string;
  size: string;
  attributes: Record<string, string>;
  store_id: number;
  store_name: string;
  source_product_id: number;
  source_product_name: string;
  source_base_name: string;
  image_url?: string | null;
  is_archived: boolean;
}

export interface WarehouseProductItem {
  id: number;
  name: string;
  variants_count: number;
  stores_count: number;
  variants: WarehouseProductVariant[];
}

export interface WarehouseProductsLinksResponse {
  warehouse_products: WarehouseProductItem[];
  unlinked_variants: WarehouseProductVariant[];
}

const EMPTY_VARIANT: WarehouseProductVariant = {
  id: 0,
  offer_id: '',
  pack_size: 1,
  color: '',
  size: '',
  attributes: {},
  store_id: 0,
  store_name: '',
  source_product_id: 0,
  source_product_name: '',
  source_base_name: '',
  image_url: null,
  is_archived: false,
};

const normalizeVariant = (value: unknown): WarehouseProductVariant => {
  const data = ensureObject(value, EMPTY_VARIANT);
  return {
    ...data,
    attributes: ensureObject<Record<string, string>>(data.attributes, {}),
  };
};

const normalizeWarehouseProduct = (value: unknown): WarehouseProductItem => {
  const data = ensureObject(value, {
    id: 0,
    name: '',
    variants_count: 0,
    stores_count: 0,
    variants: [] as WarehouseProductVariant[],
  });
  return {
    ...data,
    variants: ensureArray(data.variants).map(normalizeVariant),
  };
};

const normalizeLinksResponse = (value: unknown): WarehouseProductsLinksResponse => {
  const data = ensureObject(value, {
    warehouse_products: [] as WarehouseProductItem[],
    unlinked_variants: [] as WarehouseProductVariant[],
  });
  return {
    warehouse_products: ensureArray(data.warehouse_products).map(normalizeWarehouseProduct),
    unlinked_variants: ensureArray(data.unlinked_variants).map(normalizeVariant),
  };
};

export const warehouseProductsAPI = {
  getLinks: async (): Promise<WarehouseProductsLinksResponse> => {
    const response = await apiClient.get('/warehouse-products/links');
    return normalizeLinksResponse(response.data);
  },

  attachVariants: async (
    payload: { variant_ids: number[]; warehouse_product_id?: number; warehouse_product_name?: string }
  ): Promise<{ status: string; message: string; warehouse_product_id: number; warehouse_product_name: string; moved_variants: number }> => {
    const response = await apiClient.post('/warehouse-products/variants/attach', payload);
    return ensureObject(response.data, {
      status: 'ok',
      message: '',
      warehouse_product_id: 0,
      warehouse_product_name: '',
      moved_variants: 0,
    });
  },

  detachVariants: async (
    payload: { variant_ids: number[] }
  ): Promise<{ status: string; message: string; moved_variants: number }> => {
    const response = await apiClient.post('/warehouse-products/variants/detach', payload);
    return ensureObject(response.data, {
      status: 'ok',
      message: '',
      moved_variants: 0,
    });
  },

  deleteWarehouseProduct: async (
    warehouseProductId: number
  ): Promise<{ status: string; message: string; detached_variants: number }> => {
    const response = await apiClient.delete(`/warehouse-products/${warehouseProductId}`);
    return ensureObject(response.data, {
      status: 'ok',
      message: '',
      detached_variants: 0,
    });
  },
};
