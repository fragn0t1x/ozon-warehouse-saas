import { apiClient, ensureArray, ensureObject } from './client';

export interface Variant {
  id: number;
  offer_id: string;
  pack_size: number;
  is_archived: boolean;
  unit_cost?: number | null;
  barcode?: string;
  size?: string;
  attributes?: Record<string, string>;
  stock?: {
    total: number;
    by_warehouse: Record<string, number>;
  };
}

export interface SizeGroup {
  size: string;
  variants: Variant[];
}

export interface ColorGroup {
  color: string;
  sizes: SizeGroup[];
}

export interface GroupedProduct {
  id: number;
  name: string;
  image_url?: string;
  total_variants: number;
  is_archived: boolean;
  active_variants_count: number;
  archived_variants_count: number;
  colors: ColorGroup[];
  original_products?: number[];
}

export interface Product {
  id: number;
  name: string;
  image_url?: string;
  total_variants: number;
  variants: Variant[];
}

const EMPTY_VARIANT: Variant = {
  id: 0,
  offer_id: '',
  pack_size: 1,
  is_archived: false,
  barcode: '',
  size: '',
  attributes: {},
  stock: {
    total: 0,
    by_warehouse: {},
  },
};

const normalizeVariant = (value: unknown): Variant => {
  const data = ensureObject(value, EMPTY_VARIANT);
  return {
    ...EMPTY_VARIANT,
    ...data,
    unit_cost: typeof data.unit_cost === 'number' ? data.unit_cost : null,
    attributes: ensureObject<Record<string, string>>(data.attributes, {}),
    stock: ensureObject(data.stock, EMPTY_VARIANT.stock!),
  };
};

const normalizeSizeGroup = (value: unknown): SizeGroup => {
  const data = ensureObject(value, { size: '', variants: [] as Variant[] });
  return {
    size: data.size,
    variants: ensureArray(data.variants).map(normalizeVariant),
  };
};

const normalizeColorGroup = (value: unknown): ColorGroup => {
  const data = ensureObject(value, { color: '', sizes: [] as SizeGroup[] });
  return {
    color: data.color,
    sizes: ensureArray(data.sizes).map(normalizeSizeGroup),
  };
};

const normalizeGroupedProduct = (value: unknown): GroupedProduct => {
  const data = ensureObject(value, {
    id: 0,
    name: '',
    image_url: '',
    total_variants: 0,
    is_archived: false,
    active_variants_count: 0,
    archived_variants_count: 0,
    colors: [] as ColorGroup[],
    original_products: [] as number[],
  });
  return {
    ...data,
    colors: ensureArray(data.colors).map(normalizeColorGroup),
    original_products: ensureArray<number>(data.original_products),
  };
};

const normalizeProduct = (value: unknown): Product => {
  const data = ensureObject(value, {
    id: 0,
    name: '',
    image_url: '',
    total_variants: 0,
    variants: [] as Variant[],
  });
  return {
    ...data,
    variants: ensureArray(data.variants).map(normalizeVariant),
  };
};

export const productsAPI = {
  getGrouped: async (storeId?: number | 'all'): Promise<GroupedProduct[]> => {
    const url = storeId && storeId !== 'all' ? `/products/grouped/${storeId}` : '/products/grouped';
    const response = await apiClient.get(url);
    return ensureArray(response.data).map(normalizeGroupedProduct);
  },

  getAll: async (storeId: number = 1, limit: number = 100): Promise<Product[]> => {
    const response = await apiClient.get(`/products/with-attributes/${storeId}?limit=${limit}`);
    return ensureArray(response.data).map(normalizeProduct);
  },

  getById: async (id: number): Promise<Product> => {
    const response = await apiClient.get(`/products/${id}`);
    return normalizeProduct(response.data);
  },

  getVariants: async (productId: number): Promise<Variant[]> => {
    const response = await apiClient.get(`/products/${productId}/variants`);
    return ensureArray(response.data).map(normalizeVariant);
  },

  getVariantAttributes: async (variantId: number): Promise<{ name: string; value: string }[]> => {
    const response = await apiClient.get(`/warehouse/variant/${variantId}/attributes`);
    return ensureArray<{ name: string; value: string }>(response.data).map((item) =>
      ensureObject(item, { name: '', value: '' })
    );
  },

  search: async (query: string): Promise<Product[]> => {
    const response = await apiClient.get(`/products/search?q=${encodeURIComponent(query)}`);
    return ensureArray(response.data).map(normalizeProduct);
  },

  updateVariantCost: async (
    variantId: number,
    unitCost: number | null,
    effectiveFrom?: string | null
  ): Promise<{ id: number; unit_cost: number | null }> => {
    const response = await apiClient.patch(`/products/variants/${variantId}/cost`, {
      unit_cost: unitCost,
      effective_from: effectiveFrom ?? null,
    });
    return ensureObject(response.data, { id: variantId, unit_cost: unitCost });
  },

  bulkUpdateGroupedCost: async (
    productIds: number[],
    unitCost: number | null,
    effectiveFrom?: string | null
  ): Promise<{ updated_variants: number; updated_products: number; unit_cost: number | null }> => {
    const response = await apiClient.patch('/products/grouped/cost', {
      product_ids: productIds,
      unit_cost: unitCost,
      effective_from: effectiveFrom ?? null,
    });
    return ensureObject(response.data, {
      updated_variants: 0,
      updated_products: 0,
      unit_cost: unitCost,
    });
  },
};
