type SupplyStatusMeta = {
  label: string;
  tone: string;
  bg: string;
  text: string;
  border: string;
};

export const SUPPLY_STATUS_META: Record<string, SupplyStatusMeta> = {
  DATA_FILLING: { label: 'Подготовка к поставкам', tone: 'bg-violet-100 text-violet-800', bg: '#ede9fe', text: '#5b21b6', border: '#c4b5fd' },
  READY_TO_SUPPLY: { label: 'Готова к отгрузке', tone: 'bg-amber-100 text-amber-800', bg: '#fef3c7', text: '#92400e', border: '#fcd34d' },
  ACCEPTED_AT_SUPPLY_WAREHOUSE: { label: 'Принята на точке отгрузки', tone: 'bg-sky-100 text-sky-800', bg: '#e0f2fe', text: '#075985', border: '#7dd3fc' },
  IN_TRANSIT: { label: 'В пути', tone: 'bg-indigo-100 text-indigo-800', bg: '#e0e7ff', text: '#3730a3', border: '#a5b4fc' },
  ACCEPTANCE_AT_STORAGE_WAREHOUSE: { label: 'На приемке на складе OZON', tone: 'bg-cyan-100 text-cyan-800', bg: '#cffafe', text: '#155e75', border: '#67e8f9' },
  REPORTS_CONFIRMATION_AWAITING: { label: 'Ожидает подтверждения актов', tone: 'bg-fuchsia-100 text-fuchsia-800', bg: '#fae8ff', text: '#86198f', border: '#f0abfc' },
  REPORT_REJECTED: { label: 'Акт приемки отклонен', tone: 'bg-rose-100 text-rose-800', bg: '#ffe4e6', text: '#9f1239', border: '#fda4af' },
  COMPLETED: { label: 'Завершена', tone: 'bg-emerald-100 text-emerald-800', bg: '#d1fae5', text: '#065f46', border: '#6ee7b7' },
  CANCELLED: { label: 'Отменена', tone: 'bg-slate-100 text-slate-700', bg: '#f1f5f9', text: '#334155', border: '#cbd5e1' },
  REJECTED_AT_SUPPLY_WAREHOUSE: { label: 'Отказано в приемке', tone: 'bg-rose-100 text-rose-800', bg: '#ffe4e6', text: '#9f1239', border: '#fda4af' },
  OVERDUE: { label: 'Просрочена', tone: 'bg-orange-100 text-orange-800', bg: '#ffedd5', text: '#9a3412', border: '#fdba74' },
};

export const SUPPLY_STATUS_OPTIONS = [
  'DATA_FILLING',
  'READY_TO_SUPPLY',
  'ACCEPTED_AT_SUPPLY_WAREHOUSE',
  'IN_TRANSIT',
  'ACCEPTANCE_AT_STORAGE_WAREHOUSE',
  'REPORTS_CONFIRMATION_AWAITING',
  'REPORT_REJECTED',
  'COMPLETED',
  'CANCELLED',
  'REJECTED_AT_SUPPLY_WAREHOUSE',
  'OVERDUE',
].map((value) => ({
  value,
  label: SUPPLY_STATUS_META[value].label,
}));

export function getSupplyStatusLabel(status: string) {
  return SUPPLY_STATUS_META[status]?.label || status;
}

export function getSupplyStatusStyle(status: string) {
  const meta = SUPPLY_STATUS_META[status];
  if (!meta) {
    return { backgroundColor: '#f1f5f9', color: '#334155', borderColor: '#cbd5e1' };
  }

  return {
    backgroundColor: meta.bg,
    color: meta.text,
    borderColor: meta.border,
  };
}
