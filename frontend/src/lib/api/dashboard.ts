import { apiClient } from './client';

export interface DashboardSummary {
  stores: number;
  products: number;
  variants: number;
  warehouses: number;
  today_supplies: number;
  active_supplies: number;
  waiting_for_stock_supplies: number;
  warehouse_mode: 'shared' | 'per_store';
  packing_mode: 'simple' | 'advanced';
  discrepancy_mode: 'loss' | 'correction';
  is_first_login: boolean;
  shipments_start_date: string | null;
  shipments_accounting_enabled: boolean;
  shipments_accounting_enabled_at: string | null;
  status_counts: Record<string, number>;
  stock: {
    unpacked_units: number;
    packed_boxes: number;
    packed_units: number;
    reserved_units: number;
    available_units: number;
    source: 'warehouse' | 'ozon';
    note: string;
    updated_at: string | null;
    ordered_30d_units: number;
    orders_updated_at: string | null;
    orders_period_from: string | null;
    orders_period_to: string | null;
    orders_stores_covered: number;
    orders_stores_missing: number;
  };
  warehouse_breakdown: Array<{
    warehouse_id: number;
    warehouse_name: string;
    store_id: number | null;
    store_name: string | null;
    unpacked_units: number;
    packed_boxes: number;
    packed_units: number;
    reserved_units: number;
    available_units: number;
    updated_at: string | null;
  }>;
  stock_by_store: Array<{
    store_id: number;
    store_name: string;
    warehouse_scope: 'shared' | 'per_store';
    warehouse_id: number | null;
    warehouse_name: string | null;
    warehouse_unpacked_units: number;
    warehouse_packed_boxes: number;
    warehouse_packed_units: number;
    warehouse_reserved_units: number;
    warehouse_available_units: number;
    warehouse_updated_at: string | null;
    ozon_available_units: number;
    ozon_in_transit_units: number;
    ozon_updated_at: string | null;
    ordered_30d_units: number;
    orders_updated_at: string | null;
  }>;
  sales: {
    source: 'report_snapshot' | 'unavailable';
    period_days: number;
    total_units: number;
    total_revenue: number;
    stores_covered: number;
    stores_missing: number;
    updated_at: string | null;
    top_offers: Array<{
      store_id: number;
      store_name: string;
      offer_id: string;
      title: string;
      units: number;
      revenue: number;
    }>;
    top_offers_by_store: Array<{
      store_id: number;
      store_name: string;
      updated_at: string | null;
      items: Array<{
        store_id: number;
        store_name: string;
        offer_id: string;
        title: string;
        units: number;
        revenue: number;
      }>;
    }>;
    month_to_date: {
      available: boolean;
      stores_covered: number;
      stores_missing: number;
      ordered_units: number;
      ordered_revenue: number;
      previous_ordered_units: number;
      previous_ordered_revenue: number;
      delta_ordered_units: number;
      delta_ordered_revenue: number;
      period_label: string | null;
      compare_period_label: string | null;
    };
  };
  finance: {
    source: 'finance_snapshot' | 'unavailable';
    period_days: number;
    orders_amount: number;
    returns_amount: number;
    commission_amount: number;
    services_amount: number;
    logistics_amount: number;
    net_payout: number;
    compensation_amount: number;
    decompensation_amount: number;
    net_payout_adjusted: number;
    stores_covered: number;
    stores_missing: number;
    updated_at: string | null;
    realization_closed_month: {
      available: boolean;
      period: string | null;
      stores_covered: number;
      stores_missing: number;
      sold_units: number;
      sold_amount: number;
      sold_fee: number;
      sold_bonus: number;
      sold_incentives: number;
      returned_units: number;
      returned_amount: number;
      returned_fee: number;
      returned_bonus: number;
      returned_incentives: number;
      net_units: number;
      net_amount: number;
      net_total: number;
      net_fee: number;
      net_bonus: number;
      net_incentives: number;
      compensation_amount: number;
      decompensation_amount: number;
      net_adjustment: number;
      store_breakdown: Array<{
        store_name: string;
        sold_units: number;
        sold_amount: number;
        sold_fee: number;
        sold_bonus: number;
        sold_incentives: number;
        returned_units: number;
        returned_amount: number;
        returned_fee: number;
        returned_bonus: number;
        returned_incentives: number;
        net_units: number;
        net_amount: number;
        net_total: number;
        net_fee: number;
        net_bonus: number;
        net_incentives: number;
      }>;
    };
    realization_month_compare: {
      available: boolean;
      current_period: string | null;
      previous_period: string | null;
      current: {
        sold_units: number;
        sold_amount: number;
        sold_fee: number;
        sold_bonus: number;
        sold_incentives: number;
        returned_units: number;
        returned_amount: number;
        returned_fee: number;
        returned_bonus: number;
        returned_incentives: number;
        net_units: number;
        net_amount: number;
        net_total: number;
        net_fee: number;
        net_bonus: number;
        net_incentives: number;
      };
      previous: {
        sold_units: number;
        sold_amount: number;
        sold_fee: number;
        sold_bonus: number;
        sold_incentives: number;
        returned_units: number;
        returned_amount: number;
        returned_fee: number;
        returned_bonus: number;
        returned_incentives: number;
        net_units: number;
        net_amount: number;
        net_total: number;
        net_fee: number;
        net_bonus: number;
        net_incentives: number;
      };
      delta: {
        sold_units: number;
        sold_amount: number;
        sold_fee: number;
        sold_bonus: number;
        sold_incentives: number;
        returned_units: number;
        returned_amount: number;
        returned_fee: number;
        returned_bonus: number;
        returned_incentives: number;
        net_units: number;
        net_amount: number;
        net_total: number;
        net_fee: number;
        net_bonus: number;
        net_incentives: number;
      };
    };
    closed_month_cashflow: {
      orders_amount: number;
      returns_amount: number;
      commission_amount: number;
      services_amount: number;
      logistics_amount: number;
    },
    current_month_to_date: {
      available: boolean;
      stores_covered: number;
      stores_missing: number;
      orders_amount: 0,
      returns_amount: 0,
      previous_orders_amount: 0,
      previous_returns_amount: 0,
      delta_orders_amount: 0,
      delta_returns_amount: 0,
      returned_units: null,
      previous_returned_units: null,
      delta_returned_units: null,
      returned_units_available: false,
      returned_units_delta_available: false,
    },
    transactions_recent: {
      available: boolean;
      period_days: number;
      stores_covered: number;
      stores_missing: number;
      accruals_for_sale: number;
      compensation_amount: number;
      money_transfer: number;
      others_amount: number;
      processing_and_delivery: number;
      refunds_and_cancellations: number;
      sale_commission: number;
      services_amount: number;
      service_buckets: {
        marketing: number;
        storage: number;
        acquiring: number;
        returns: number;
        logistics: number;
        other: number;
      };
      top_services: Array<{
        name: string;
        bucket: string;
        amount: number;
        count: number;
      }>;
    };
    marketing_recent: {
      available: boolean;
      period_days: number;
      stores_covered: number;
      stores_missing: number;
      amount_total: number;
      services_count: number;
      share_of_orders: number;
      store_breakdown: Array<{
        store_name: string;
        marketing_amount: number;
        services_count: number;
        share_of_orders: number;
      }>;
      top_services: Array<{
        name: string;
        bucket: string;
        amount: number;
        count: number;
      }>;
    };
    placement_by_products_recent: {
      available: boolean;
      period_days: number;
      stores_covered: number;
      stores_missing: number;
      amount_total: number;
      rows_count: number;
      offers_count: number;
      store_breakdown: Array<{
        store_name: string;
        amount_total: number;
        rows_count: number;
        offers_count: number;
      }>;
      top_items: Array<{
        store_name: string;
        offer_id: string;
        title: string;
        amount: number;
        quantity: number;
        days: number;
      }>;
    };
    placement_by_supplies_recent: {
      available: boolean;
      period_days: number;
      stores_covered: number;
      stores_missing: number;
      metric_kind: 'amount' | 'stock_days';
      amount_total: number;
      stock_days_total: number;
      rows_count: number;
      supplies_count: number;
      store_breakdown: Array<{
        store_name: string;
        amount_total: number;
        stock_days_total: number;
        rows_count: number;
        supplies_count: number;
      }>;
      top_items: Array<{
        store_name: string;
        supply_ref: string;
        warehouse_name: string;
        amount: number;
        stock_days_total: number;
        items_count: number;
        days: number;
      }>;
    };
    removals_recent: {
      available: boolean;
      period_days: number;
      stores_covered: number;
      stores_missing: number;
      rows_count: number;
      returns_count: number;
      offers_count: number;
      quantity_total: number;
      delivery_price_total: number;
      auto_returns_count: number;
      utilization_count: number;
      source_breakdown: Array<{
        kind: 'from_stock' | 'from_supply';
        kind_label: string;
        rows_count: number;
        returns_count: number;
        offers_count: number;
        quantity_total: number;
        delivery_price_total: number;
        auto_returns_count: number;
        utilization_count: number;
      }>;
      store_breakdown: Array<{
        store_name: string;
        rows_count: number;
        quantity_total: number;
        delivery_price_total: number;
        auto_returns_count: number;
        utilization_count: number;
      }>;
      top_items: Array<{
        store_name: string;
        kind: 'from_stock' | 'from_supply';
        kind_label: string;
        offer_id: string;
        title: string;
        quantity_total: number;
        delivery_price_total: number;
        auto_returns_count: number;
        utilization_count: number;
        last_return_state: string;
        delivery_type: string;
        stock_type: string;
      }>;
      top_states: Array<{
        state: string;
        count: number;
        quantity_total: number;
        delivery_price_total: number;
      }>;
    };
    store_breakdown: Array<{
      store_name: string;
      orders_amount: number;
      returns_amount: number;
      commission_amount: number;
      services_amount: number;
      logistics_amount: number;
      net_payout: number;
      compensation_amount: number;
      decompensation_amount: number;
      net_payout_adjusted: number;
    }>;
  };
  unit_economics: {
    source: 'estimated' | 'unavailable';
    basis: 'realization_closed_month' | 'orders_recent' | 'mixed' | 'unavailable';
    basis_label: string;
    period_label: string;
    revenue_label: string;
    units_label: string;
    profit_label: string;
    profit_hint: string;
    details_note: string;
    offers_total: number;
    offers_with_cost: number;
    cost_coverage_ratio: number;
    revenue_coverage_ratio: number;
    tracked_units: number;
    tracked_revenue: number;
    tracked_revenue_net_of_vat: number;
    tracked_cogs: number;
    gross_profit_before_ozon: number;
    allocated_commission: number;
    allocated_services: number;
    allocated_logistics: number;
    allocated_marketing: number;
    allocated_compensation: number;
    allocated_other: number;
    profit_before_tax: number;
    tax_amount: number;
    estimated_net_profit: number;
    top_profitable_offers: Array<{
      store_name: string;
      offer_id: string;
      title: string;
      units: number;
      revenue: number;
      revenue_net_of_vat: number;
      average_sale_price_gross?: number;
      unit_cost: number;
      cogs: number;
      gross_profit_before_ozon: number;
      profit_before_tax?: number;
      tax_amount?: number;
      estimated_net_profit: number;
    }>;
    top_loss_offers: Array<{
      store_name: string;
      offer_id: string;
      title: string;
      units: number;
      revenue: number;
      revenue_net_of_vat: number;
      average_sale_price_gross?: number;
      unit_cost: number;
      cogs: number;
      gross_profit_before_ozon: number;
      profit_before_tax?: number;
      tax_amount?: number;
      estimated_net_profit: number;
    }>;
  };
  recent_supplies: Array<{
    id: number;
    order_number: string;
    status: string;
    store_id: number;
    reservation_waiting_for_stock?: boolean;
    reservation_wait_message?: string | null;
    store_name: string;
    eta_date: string | null;
    timeslot_from: string | null;
  }>;
  recent_supplies_by_store: Array<{
    store_id: number;
    store_name: string;
    items: Array<{
      id: number;
      order_number: string;
      status: string;
      store_id: number;
      reservation_waiting_for_stock?: boolean;
      reservation_wait_message?: string | null;
      store_name: string;
      eta_date: string | null;
      timeslot_from: string | null;
    }>;
  }>;
  admin_events: Array<{
    event_type: string;
    title: string;
    severity: 'success' | 'warning' | 'error';
    details: Record<string, string | number | null>;
  }>;
}

export interface UnitEconomicsRow {
  store_id: number;
  store_name: string;
  offer_id: string;
  title: string;
  units: number;
  revenue: number;
  revenue_net_of_vat?: number;
  average_sale_price_gross?: number;
  unit_cost: number;
  cogs: number;
  gross_profit_before_ozon: number;
  profit_before_tax?: number;
  tax_amount?: number;
  estimated_net_profit: number;
  margin_ratio: number;
  allocated_commission?: number;
  allocated_services?: number;
  allocated_logistics?: number;
  allocated_marketing?: number;
  allocated_compensation?: number;
  allocated_other?: number;
  basis?: 'realization_closed_month' | 'orders_recent';
  current_profitability_available?: boolean;
  current_price_gross?: number | null;
  current_old_price_gross?: number | null;
  current_min_price_gross?: number | null;
  current_revenue_net_of_vat?: number;
  current_gross_profit_before_ozon?: number;
  current_allocated_commission?: number;
  current_allocated_services?: number;
  current_allocated_logistics?: number;
  current_allocated_marketing?: number;
  current_allocated_compensation?: number;
  current_allocated_other?: number;
  current_profit_before_tax?: number;
  current_tax_amount?: number;
  current_estimated_net_profit?: number;
  current_margin_ratio?: number;
  current_price_index_color?: string | null;
  current_price_index_label?: string | null;
  current_ozon_minimal_price_gross?: number | null;
  current_external_minimal_price_gross?: number | null;
  current_commission_percent?: number | null;
  current_commission_value?: number | null;
  current_delivery_amount?: number | null;
  current_return_amount?: number | null;
  current_return_rate?: number;
  current_return_reserve?: number;
  historical_services_per_unit?: number;
  historical_acquiring_per_unit?: number;
  historical_other_per_unit?: number;
  historical_marketing_per_unit?: number;
  historical_profile_months?: number;
  historical_profile_sales_units?: number;
}

export interface UnitEconomicsReport {
  summary: DashboardSummary['unit_economics'];
  filters: {
    store_id: number | null;
    query: string;
    profitability: 'all' | 'loss' | 'profit';
  };
  filtered_totals: {
    rows_count: number;
    units: number;
    revenue: number;
    revenue_net_of_vat?: number;
    cogs: number;
    gross_profit_before_ozon?: number;
    profit_before_tax?: number;
    tax_amount?: number;
    estimated_net_profit: number;
  };
  rows: UnitEconomicsRow[];
  rows_total: number;
}

const EMPTY_SUMMARY: DashboardSummary = {
  stores: 0,
  products: 0,
  variants: 0,
  warehouses: 0,
  today_supplies: 0,
  active_supplies: 0,
  waiting_for_stock_supplies: 0,
  warehouse_mode: 'shared',
  packing_mode: 'simple',
  discrepancy_mode: 'loss',
  is_first_login: false,
  shipments_start_date: null,
  shipments_accounting_enabled: false,
  shipments_accounting_enabled_at: null,
  status_counts: {},
  stock: {
    unpacked_units: 0,
    packed_boxes: 0,
    packed_units: 0,
    reserved_units: 0,
    available_units: 0,
    source: 'warehouse',
    note: 'Сводка по складу временно недоступна.',
    updated_at: null,
    ordered_30d_units: 0,
    orders_updated_at: null,
    orders_period_from: null,
    orders_period_to: null,
    orders_stores_covered: 0,
    orders_stores_missing: 0,
  },
  warehouse_breakdown: [],
  stock_by_store: [],
  sales: {
    source: 'unavailable',
    period_days: 30,
    total_units: 0,
    total_revenue: 0,
    stores_covered: 0,
    stores_missing: 0,
    updated_at: null,
    top_offers: [],
    top_offers_by_store: [],
      month_to_date: {
        available: false,
        stores_covered: 0,
        stores_missing: 0,
        ordered_units: 0,
      ordered_revenue: 0,
      previous_ordered_units: 0,
      previous_ordered_revenue: 0,
      delta_ordered_units: 0,
      delta_ordered_revenue: 0,
      period_label: null,
      compare_period_label: null,
    },
  },
  finance: {
    source: 'unavailable',
    period_days: 62,
    orders_amount: 0,
    returns_amount: 0,
    commission_amount: 0,
    services_amount: 0,
    logistics_amount: 0,
    net_payout: 0,
    compensation_amount: 0,
    decompensation_amount: 0,
    net_payout_adjusted: 0,
    stores_covered: 0,
    stores_missing: 0,
    updated_at: null,
    realization_closed_month: {
      available: false,
      period: null,
      stores_covered: 0,
      stores_missing: 0,
      sold_units: 0,
      sold_amount: 0,
      sold_fee: 0,
      sold_bonus: 0,
      sold_incentives: 0,
      returned_units: 0,
      returned_amount: 0,
      returned_fee: 0,
      returned_bonus: 0,
      returned_incentives: 0,
      net_units: 0,
      net_amount: 0,
      net_total: 0,
      net_fee: 0,
      net_bonus: 0,
      net_incentives: 0,
      compensation_amount: 0,
      decompensation_amount: 0,
      net_adjustment: 0,
      store_breakdown: [],
    },
    realization_month_compare: {
      available: false,
      current_period: null,
      previous_period: null,
      current: {
        sold_units: 0,
        sold_amount: 0,
        sold_fee: 0,
        sold_bonus: 0,
        sold_incentives: 0,
        returned_units: 0,
        returned_amount: 0,
        returned_fee: 0,
        returned_bonus: 0,
        returned_incentives: 0,
        net_units: 0,
        net_amount: 0,
        net_total: 0,
        net_fee: 0,
        net_bonus: 0,
        net_incentives: 0,
      },
      previous: {
        sold_units: 0,
        sold_amount: 0,
        sold_fee: 0,
        sold_bonus: 0,
        sold_incentives: 0,
        returned_units: 0,
        returned_amount: 0,
        returned_fee: 0,
        returned_bonus: 0,
        returned_incentives: 0,
        net_units: 0,
        net_amount: 0,
        net_total: 0,
        net_fee: 0,
        net_bonus: 0,
        net_incentives: 0,
      },
      delta: {
        sold_units: 0,
        sold_amount: 0,
        sold_fee: 0,
        sold_bonus: 0,
        sold_incentives: 0,
        returned_units: 0,
        returned_amount: 0,
        returned_fee: 0,
        returned_bonus: 0,
        returned_incentives: 0,
        net_units: 0,
        net_amount: 0,
        net_total: 0,
        net_fee: 0,
        net_bonus: 0,
        net_incentives: 0,
      },
    },
    closed_month_cashflow: {
      orders_amount: 0,
      returns_amount: 0,
      commission_amount: 0,
      services_amount: 0,
      logistics_amount: 0,
    },
      current_month_to_date: {
        available: false,
        stores_covered: 0,
        stores_missing: 0,
        orders_amount: 0,
      returns_amount: 0,
      previous_orders_amount: 0,
      previous_returns_amount: 0,
      delta_orders_amount: 0,
      delta_returns_amount: 0,
      returned_units: null,
      previous_returned_units: null,
      delta_returned_units: null,
      returned_units_available: false,
      returned_units_delta_available: false,
    },
    transactions_recent: {
      available: false,
      period_days: 30,
      stores_covered: 0,
      stores_missing: 0,
      accruals_for_sale: 0,
      compensation_amount: 0,
      money_transfer: 0,
      others_amount: 0,
      processing_and_delivery: 0,
      refunds_and_cancellations: 0,
      sale_commission: 0,
      services_amount: 0,
      service_buckets: {
        marketing: 0,
        storage: 0,
        acquiring: 0,
        returns: 0,
        logistics: 0,
        other: 0,
      },
      top_services: [],
    },
    marketing_recent: {
      available: false,
      period_days: 30,
      stores_covered: 0,
      stores_missing: 0,
      amount_total: 0,
      services_count: 0,
      share_of_orders: 0,
      store_breakdown: [],
      top_services: [],
    },
    placement_by_products_recent: {
      available: false,
      period_days: 30,
      stores_covered: 0,
      stores_missing: 0,
      amount_total: 0,
      rows_count: 0,
      offers_count: 0,
      store_breakdown: [],
      top_items: [],
    },
    placement_by_supplies_recent: {
      available: false,
      period_days: 30,
      stores_covered: 0,
      stores_missing: 0,
      metric_kind: 'amount',
      amount_total: 0,
      stock_days_total: 0,
      rows_count: 0,
      supplies_count: 0,
      store_breakdown: [],
      top_items: [],
    },
    removals_recent: {
      available: false,
      period_days: 30,
      stores_covered: 0,
      stores_missing: 0,
      rows_count: 0,
      returns_count: 0,
      offers_count: 0,
      quantity_total: 0,
      delivery_price_total: 0,
      auto_returns_count: 0,
      utilization_count: 0,
      source_breakdown: [],
      store_breakdown: [],
      top_items: [],
      top_states: [],
    },
    store_breakdown: [],
  },
  unit_economics: {
    source: 'unavailable',
    basis: 'unavailable',
    basis_label: 'Нет данных',
    period_label: 'Период неизвестен',
    revenue_label: 'Выручка',
    units_label: 'Шт.',
    profit_label: 'Оценочная прибыль',
    profit_hint: 'Данных для построения экономики пока нет.',
    details_note: 'Ждем свежие snapshot-ы Ozon.',
    offers_total: 0,
    offers_with_cost: 0,
    cost_coverage_ratio: 0,
    revenue_coverage_ratio: 0,
    tracked_units: 0,
    tracked_revenue: 0,
    tracked_revenue_net_of_vat: 0,
    tracked_cogs: 0,
    gross_profit_before_ozon: 0,
    allocated_commission: 0,
    allocated_services: 0,
    allocated_logistics: 0,
    allocated_marketing: 0,
    allocated_compensation: 0,
    allocated_other: 0,
    profit_before_tax: 0,
    tax_amount: 0,
    estimated_net_profit: 0,
    top_profitable_offers: [],
    top_loss_offers: [],
  },
  recent_supplies: [],
  recent_supplies_by_store: [],
  admin_events: [],
};

export function normalizeDashboardSummary(data: Partial<DashboardSummary> | null | undefined): DashboardSummary {
  return {
    ...EMPTY_SUMMARY,
    ...data,
    status_counts: data?.status_counts ?? {},
    stock: {
      ...EMPTY_SUMMARY.stock,
      ...(data?.stock ?? {}),
    },
    warehouse_breakdown: data?.warehouse_breakdown ?? [],
    stock_by_store: data?.stock_by_store ?? [],
    sales: {
      ...EMPTY_SUMMARY.sales,
      ...(data?.sales ?? {}),
      top_offers: data?.sales?.top_offers ?? [],
      top_offers_by_store: data?.sales?.top_offers_by_store ?? [],
      month_to_date: {
        ...EMPTY_SUMMARY.sales.month_to_date,
        ...(data?.sales?.month_to_date ?? {}),
      },
    },
    finance: {
      ...EMPTY_SUMMARY.finance,
      ...(data?.finance ?? {}),
      realization_closed_month: {
        ...EMPTY_SUMMARY.finance.realization_closed_month,
        ...(data?.finance?.realization_closed_month ?? {}),
        store_breakdown: data?.finance?.realization_closed_month?.store_breakdown ?? [],
      },
      realization_month_compare: {
        ...EMPTY_SUMMARY.finance.realization_month_compare,
        ...(data?.finance?.realization_month_compare ?? {}),
        current: {
          ...EMPTY_SUMMARY.finance.realization_month_compare.current,
          ...(data?.finance?.realization_month_compare?.current ?? {}),
        },
        previous: {
          ...EMPTY_SUMMARY.finance.realization_month_compare.previous,
          ...(data?.finance?.realization_month_compare?.previous ?? {}),
        },
        delta: {
          ...EMPTY_SUMMARY.finance.realization_month_compare.delta,
          ...(data?.finance?.realization_month_compare?.delta ?? {}),
        },
      },
      closed_month_cashflow: {
        ...EMPTY_SUMMARY.finance.closed_month_cashflow,
        ...(data?.finance?.closed_month_cashflow ?? {}),
      },
      current_month_to_date: {
        ...EMPTY_SUMMARY.finance.current_month_to_date,
        ...(data?.finance?.current_month_to_date ?? {}),
      },
      transactions_recent: {
        ...EMPTY_SUMMARY.finance.transactions_recent,
        ...(data?.finance?.transactions_recent ?? {}),
        service_buckets: {
          ...EMPTY_SUMMARY.finance.transactions_recent.service_buckets,
          ...(data?.finance?.transactions_recent?.service_buckets ?? {}),
        },
        top_services: data?.finance?.transactions_recent?.top_services ?? [],
      },
      marketing_recent: {
        ...EMPTY_SUMMARY.finance.marketing_recent,
        ...(data?.finance?.marketing_recent ?? {}),
        store_breakdown: data?.finance?.marketing_recent?.store_breakdown ?? [],
        top_services: data?.finance?.marketing_recent?.top_services ?? [],
      },
      placement_by_products_recent: {
        ...EMPTY_SUMMARY.finance.placement_by_products_recent,
        ...(data?.finance?.placement_by_products_recent ?? {}),
        store_breakdown: data?.finance?.placement_by_products_recent?.store_breakdown ?? [],
        top_items: data?.finance?.placement_by_products_recent?.top_items ?? [],
      },
      placement_by_supplies_recent: {
        ...EMPTY_SUMMARY.finance.placement_by_supplies_recent,
        ...(data?.finance?.placement_by_supplies_recent ?? {}),
        store_breakdown: data?.finance?.placement_by_supplies_recent?.store_breakdown ?? [],
        top_items: data?.finance?.placement_by_supplies_recent?.top_items ?? [],
      },
      removals_recent: {
        ...EMPTY_SUMMARY.finance.removals_recent,
        ...(data?.finance?.removals_recent ?? {}),
        source_breakdown: data?.finance?.removals_recent?.source_breakdown ?? [],
        store_breakdown: data?.finance?.removals_recent?.store_breakdown ?? [],
        top_items: data?.finance?.removals_recent?.top_items ?? [],
        top_states: data?.finance?.removals_recent?.top_states ?? [],
      },
      store_breakdown: data?.finance?.store_breakdown ?? [],
    },
    unit_economics: {
      ...EMPTY_SUMMARY.unit_economics,
      ...(data?.unit_economics ?? {}),
      top_profitable_offers: data?.unit_economics?.top_profitable_offers ?? [],
      top_loss_offers: data?.unit_economics?.top_loss_offers ?? [],
    },
    recent_supplies: data?.recent_supplies ?? [],
    recent_supplies_by_store: data?.recent_supplies_by_store ?? [],
    admin_events: data?.admin_events ?? [],
  };
}

export const dashboardAPI = {
  normalizeSummary: normalizeDashboardSummary,
  getSummary: async (storeId?: number | null): Promise<DashboardSummary> => {
    const suffix = typeof storeId === 'number' ? `?store_id=${storeId}` : '';
    const response = await apiClient.get(`/dashboard/summary${suffix}`);
    return normalizeDashboardSummary(response.data);
  },
  refreshCommercialData: async (): Promise<{ queued: number; store_names: string[]; message: string }> => {
    const response = await apiClient.post('/dashboard/refresh-commercial');
    return response.data;
  },
  getUnitEconomicsReport: async (params?: {
    storeId?: number | null;
    query?: string;
    profitability?: 'all' | 'loss' | 'profit';
    limit?: number;
  }): Promise<UnitEconomicsReport> => {
    const searchParams = new URLSearchParams();
    if (typeof params?.storeId === 'number') {
      searchParams.set('store_id', String(params.storeId));
    }
    if (params?.query) {
      searchParams.set('query', params.query);
    }
    if (params?.profitability) {
      searchParams.set('profitability', params.profitability);
    }
    if (params?.limit) {
      searchParams.set('limit', String(params.limit));
    }
    const suffix = searchParams.toString() ? `?${searchParams.toString()}` : '';
    const response = await apiClient.get(`/dashboard/unit-economics${suffix}`);
    const data = response.data ?? {};
    return {
      summary: normalizeDashboardSummary({ unit_economics: data.summary }).unit_economics,
      filters: {
        store_id: typeof data?.filters?.store_id === 'number' ? data.filters.store_id : null,
        query: data?.filters?.query ?? '',
        profitability: data?.filters?.profitability ?? 'all',
      },
      filtered_totals: {
        rows_count: data?.filtered_totals?.rows_count ?? 0,
        units: data?.filtered_totals?.units ?? 0,
        revenue: data?.filtered_totals?.revenue ?? 0,
        revenue_net_of_vat: data?.filtered_totals?.revenue_net_of_vat ?? 0,
        cogs: data?.filtered_totals?.cogs ?? 0,
        gross_profit_before_ozon: data?.filtered_totals?.gross_profit_before_ozon ?? 0,
        profit_before_tax: data?.filtered_totals?.profit_before_tax ?? 0,
        tax_amount: data?.filtered_totals?.tax_amount ?? 0,
        estimated_net_profit: data?.filtered_totals?.estimated_net_profit ?? 0,
      },
      rows: Array.isArray(data?.rows) ? data.rows : [],
      rows_total: data?.rows_total ?? 0,
    };
  },
};
