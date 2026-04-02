export type VatMode = 'none' | 'usn_5' | 'usn_7' | 'osno_10' | 'osno_22';
export type TaxMode = 'before_tax' | 'usn_income' | 'usn_income_expenses' | 'custom_profit';

export interface CalculatorInputs {
  salePriceGross: number;
  vatMode: VatMode;
  taxMode: TaxMode;
  taxRate: number;
  cogs: number;
  inboundLogistics: number;
  packaging: number;
  ozonCommissionRate: number;
  fulfillmentPerOrder: number;
  lastMilePerOrder: number;
  otherMarketplaceCosts: number;
  marketingRate: number;
  marketingFixed: number;
  storagePerUnit: number;
  returnRate: number;
  reverseLogistics: number;
  returnHandling: number;
  returnWriteoffRate: number;
  returnAdditionalLoss: number;
  monthlyFixedCosts: number;
  plannedUnitsPerMonth: number;
}

export interface CalculationResult {
  vatRate: number;
  revenueNetOfVat: number;
  commissionAmount: number;
  marketingAmount: number;
  returnReserve: number;
  variableCosts: number;
  contributionBeforeTax: number;
  taxAmount: number;
  netProfit: number;
  margin: number;
  roi: number;
  breakEvenPriceGross: number | null;
  breakEvenUnits: number | null;
  monthlyProfit: number;
  maxExtraAdSpend: number;
}

export const UNIT_ECONOMICS_CALCULATOR_STORAGE_KEY = 'unit-economics-calculator:v1';

export const DEFAULT_UNIT_ECONOMICS_INPUTS: CalculatorInputs = {
  salePriceGross: 1990,
  vatMode: 'none',
  taxMode: 'usn_income_expenses',
  taxRate: 15,
  cogs: 620,
  inboundLogistics: 45,
  packaging: 18,
  ozonCommissionRate: 18,
  fulfillmentPerOrder: 55,
  lastMilePerOrder: 42,
  otherMarketplaceCosts: 15,
  marketingRate: 8,
  marketingFixed: 0,
  storagePerUnit: 12,
  returnRate: 7,
  reverseLogistics: 70,
  returnHandling: 18,
  returnWriteoffRate: 12,
  returnAdditionalLoss: 0,
  monthlyFixedCosts: 60000,
  plannedUnitsPerMonth: 250,
};

export const VAT_OPTIONS: Array<{ value: VatMode; label: string; rate: number; note: string }> = [
  { value: 'none', label: 'Без НДС', rate: 0, note: 'Подходит, если вы освобождены от НДС.' },
  { value: 'usn_5', label: 'УСН + НДС 5%', rate: 0.05, note: 'Пониженная ставка НДС.' },
  { value: 'usn_7', label: 'УСН + НДС 7%', rate: 0.07, note: 'Пониженная ставка НДС.' },
  { value: 'osno_10', label: 'НДС 10%', rate: 0.1, note: 'Для льготных товарных категорий.' },
  { value: 'osno_22', label: 'НДС 22%', rate: 0.22, note: 'Общая ставка НДС.' },
];

export const TAX_OPTIONS: Array<{ value: TaxMode; label: string; defaultRate: number; note: string }> = [
  {
    value: 'before_tax',
    label: 'До налогов',
    defaultRate: 0,
    note: 'Только операционная unit economics без налога.',
  },
  {
    value: 'usn_income',
    label: 'УСН доходы',
    defaultRate: 6,
    note: 'Налог считается с полного дохода от покупателя без НДС, комиссия Ozon не вычитается.',
  },
  {
    value: 'usn_income_expenses',
    label: 'УСН доходы минус расходы',
    defaultRate: 15,
    note: 'Налог считается с прибыли, но помните про минимальный налог 1% от доходов.',
  },
  {
    value: 'custom_profit',
    label: 'Кастомный налог на прибыль',
    defaultRate: 25,
    note: 'Для управленческой модели, если хотите подставить свою ставку.',
  },
];

export function getVatRate(mode: VatMode) {
  return VAT_OPTIONS.find((item) => item.value === mode)?.rate ?? 0;
}

export function getTaxRatePreset(mode: TaxMode) {
  return TAX_OPTIONS.find((item) => item.value === mode)?.defaultRate ?? 0;
}

function round2(value: number) {
  return Math.round(value * 100) / 100;
}

function calculateUnitEconomicsCore(inputs: CalculatorInputs) {
  const vatRate = getVatRate(inputs.vatMode);
  const revenueNetOfVat = inputs.salePriceGross / (1 + vatRate);
  const commissionAmount = inputs.salePriceGross * (inputs.ozonCommissionRate / 100);
  const marketingAmount = inputs.salePriceGross * (inputs.marketingRate / 100) + inputs.marketingFixed;
  const returnReserve =
    (inputs.returnRate / 100) *
    (inputs.reverseLogistics +
      inputs.returnHandling +
      inputs.returnAdditionalLoss +
      inputs.cogs * (inputs.returnWriteoffRate / 100));

  const variableCosts =
    inputs.cogs +
    inputs.inboundLogistics +
    inputs.packaging +
    commissionAmount +
    inputs.fulfillmentPerOrder +
    inputs.lastMilePerOrder +
    inputs.otherMarketplaceCosts +
    marketingAmount +
    inputs.storagePerUnit +
    returnReserve;

  const contributionBeforeTax = revenueNetOfVat - variableCosts;

  let taxAmount = 0;
  if (inputs.taxMode === 'usn_income') {
    taxAmount = revenueNetOfVat * (inputs.taxRate / 100);
  } else if (inputs.taxMode === 'usn_income_expenses') {
    const profitTax = Math.max(contributionBeforeTax, 0) * (inputs.taxRate / 100);
    const minimalTax = revenueNetOfVat * 0.01;
    taxAmount = Math.max(profitTax, minimalTax);
  } else if (inputs.taxMode === 'custom_profit') {
    taxAmount = Math.max(contributionBeforeTax, 0) * (inputs.taxRate / 100);
  }

  const netProfit = contributionBeforeTax - taxAmount;
  const margin = revenueNetOfVat > 0 ? netProfit / revenueNetOfVat : 0;
  const roi = inputs.cogs > 0 ? netProfit / inputs.cogs : 0;
  const monthlyProfit = netProfit * inputs.plannedUnitsPerMonth - inputs.monthlyFixedCosts;
  const breakEvenUnits = netProfit > 0 ? inputs.monthlyFixedCosts / netProfit : null;
  const maxExtraAdSpend = Math.max(netProfit, 0);

  return {
    vatRate,
    revenueNetOfVat,
    commissionAmount,
    marketingAmount,
    returnReserve,
    variableCosts,
    contributionBeforeTax,
    taxAmount,
    netProfit,
    margin,
    roi,
    monthlyProfit,
    breakEvenUnits,
    maxExtraAdSpend,
  };
}

function findBreakEvenGrossPrice(inputs: CalculatorInputs) {
  const evaluate = (price: number) => calculateUnitEconomicsCore({ ...inputs, salePriceGross: price }).netProfit;

  let low = 0;
  let high = Math.max(inputs.salePriceGross * 3, 1000);
  let highResult = evaluate(high);

  let safety = 0;
  while (highResult < 0 && safety < 20) {
    high *= 1.5;
    highResult = evaluate(high);
    safety += 1;
  }

  if (highResult < 0) {
    return null;
  }

  for (let index = 0; index < 40; index += 1) {
    const middle = (low + high) / 2;
    const result = evaluate(middle);
    if (result >= 0) {
      high = middle;
    } else {
      low = middle;
    }
  }

  return high;
}

export function calculateUnitEconomics(inputs: CalculatorInputs): CalculationResult {
  const core = calculateUnitEconomicsCore(inputs);
  const breakEvenPriceGross = findBreakEvenGrossPrice(inputs);

  return {
    vatRate: core.vatRate,
    revenueNetOfVat: round2(core.revenueNetOfVat),
    commissionAmount: round2(core.commissionAmount),
    marketingAmount: round2(core.marketingAmount),
    returnReserve: round2(core.returnReserve),
    variableCosts: round2(core.variableCosts),
    contributionBeforeTax: round2(core.contributionBeforeTax),
    taxAmount: round2(core.taxAmount),
    netProfit: round2(core.netProfit),
    margin: round2(core.margin),
    roi: round2(core.roi),
    breakEvenPriceGross: breakEvenPriceGross === null ? null : round2(breakEvenPriceGross),
    breakEvenUnits: core.breakEvenUnits === null ? null : round2(core.breakEvenUnits),
    monthlyProfit: round2(core.monthlyProfit),
    maxExtraAdSpend: round2(core.maxExtraAdSpend),
  };
}
