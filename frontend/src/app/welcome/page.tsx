'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import type { AxiosError } from 'axios';
import {
  ArrowLeftIcon,
  ArrowRightIcon,
  CheckCircleIcon,
  CheckIcon,
  PlusIcon,
  SparklesIcon,
  XMarkIcon,
} from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { TelegramConnectPanel } from '@/components/TelegramConnectPanel';
import {
  settingsAPI,
  type TelegramConnectStatus,
  type UserSettings,
} from '@/lib/api/settings';
import {
  storesAPI,
  type StoreImportPreview,
  type StoreProductLinkDecision,
  type ValidationResult,
} from '@/lib/api/stores';
import { useAuth } from '@/lib/context/AuthContext';
import { useStoreContext } from '@/lib/context/StoreContext';

const STEPS = [
  { id: 1, title: 'Как хранить товары' },
  { id: 2, title: 'Как учитывать товары' },
  { id: 3, title: 'Уведомления' },
  { id: 4, title: 'Подключение магазина OZON' },
  { id: 5, title: 'Проверка связей товаров' },
  { id: 6, title: 'Ручная проверка товаров' },
] as const;

const TIMEZONE_OPTIONS = [
  { value: 'Europe/Moscow', label: 'Москва (Europe/Moscow)' },
  { value: 'Europe/Kaliningrad', label: 'Калининград (Europe/Kaliningrad)' },
  { value: 'Europe/Samara', label: 'Самара (Europe/Samara)' },
  { value: 'Asia/Yekaterinburg', label: 'Екатеринбург (Asia/Yekaterinburg)' },
  { value: 'Asia/Omsk', label: 'Омск (Asia/Omsk)' },
  { value: 'Asia/Novosibirsk', label: 'Новосибирск (Asia/Novosibirsk)' },
  { value: 'Asia/Krasnoyarsk', label: 'Красноярск (Asia/Krasnoyarsk)' },
  { value: 'Asia/Irkutsk', label: 'Иркутск (Asia/Irkutsk)' },
  { value: 'Asia/Yakutsk', label: 'Якутск (Asia/Yakutsk)' },
  { value: 'Asia/Vladivostok', label: 'Владивосток (Asia/Vladivostok)' },
  { value: 'Asia/Magadan', label: 'Магадан (Asia/Magadan)' },
  { value: 'Asia/Kamchatka', label: 'Камчатка (Asia/Kamchatka)' },
];

type DecisionState = Record<string, { warehouse_product_id?: number; warehouse_product_name: string }>;
type ManualVariantDecision = {
  source_group_key: string;
  source_base_name: string;
  source_product_name: string;
  offer_id: string;
  color: string;
  size: string;
  pack_size: number;
  warehouse_product_id?: number;
  warehouse_product_name: string;
};

type ManualPreviewGroup = {
  key: string;
  warehouseProductName: string;
  warehouseProductId?: number;
  items: ManualVariantDecision[];
  isExisting: boolean;
};

type ManualTargetOption = {
  key: string;
  label: string;
  warehouse_product_id?: number;
  warehouse_product_name: string;
};

type FormState = {
  warehouse_mode: UserSettings['warehouse_mode'];
  packing_mode: UserSettings['packing_mode'];
  discrepancy_mode: UserSettings['discrepancy_mode'];
  shipments_start_date: string;
  shipments_accounting_enabled: boolean;
  telegram_chat_id: string;
  notification_timezone: string;
  today_supplies_time_local: string;
  daily_report_time_local: string;
  notify_today_supplies: boolean;
  notify_losses: boolean;
  notify_daily_report: boolean;
  notify_rejection: boolean;
  notify_acceptance_status: boolean;
  store_name: string;
  client_id: string;
  api_key: string;
};

const DEFAULT_FORM: FormState = {
  warehouse_mode: 'shared',
  packing_mode: 'simple',
  discrepancy_mode: 'loss',
  shipments_start_date: '',
  shipments_accounting_enabled: false,
  telegram_chat_id: '',
  notification_timezone: 'Europe/Moscow',
  today_supplies_time_local: '08:00',
  daily_report_time_local: '09:00',
  notify_today_supplies: true,
  notify_losses: true,
  notify_daily_report: true,
  notify_rejection: true,
  notify_acceptance_status: true,
  store_name: '',
  client_id: '',
  api_key: '',
};

function cn(...classes: Array<string | false | null | undefined>) {
  return classes.filter(Boolean).join(' ');
}

function formatDateForInput(value?: string | null) {
  if (!value) {
    return '';
  }

  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return '';
  }

  return `${date.getFullYear()}-${String(date.getMonth() + 1).padStart(2, '0')}-${String(date.getDate()).padStart(2, '0')}`;
}

function parseDateInput(value: string) {
  const trimmed = value.trim();
  if (!trimmed) {
    return { iso: null, error: null };
  }
  const isoMatch = trimmed.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    const [, year, month, day] = isoMatch;
    const isoDate = `${year}-${month}-${day}`;
    const parsed = new Date(`${isoDate}T00:00:00`);

    if (
      Number.isNaN(parsed.getTime()) ||
      parsed.getDate() !== Number(day) ||
      parsed.getMonth() + 1 !== Number(month)
    ) {
      return { iso: null, error: 'Похоже, дата указана некорректно' };
    }

    return { iso: `${isoDate}T00:00:00`, error: null };
  }

  const dotMatch = trimmed.match(/^(\d{2})\.(\d{2})\.(\d{4})$/);
  if (!dotMatch) {
    return { iso: null, error: 'Выберите дату в календаре или оставьте поле пустым' };
  }

  const [, day, month, year] = dotMatch;
  const isoDate = `${year}-${month}-${day}`;
  const parsed = new Date(`${isoDate}T00:00:00`);

  if (
    Number.isNaN(parsed.getTime()) ||
    parsed.getDate() !== Number(day) ||
    parsed.getMonth() + 1 !== Number(month)
  ) {
    return { iso: null, error: 'Похоже, дата указана некорректно' };
  }

  return { iso: `${isoDate}T00:00:00`, error: null };
}

function formatPackSizeLabel(packSize: number) {
  return packSize > 1 ? `${packSize} шт.` : '1 шт.';
}

function buildSuggestedProductName(variants: Array<{ source_base_name: string }>) {
  const candidates = variants
    .map((variant) => variant.source_base_name?.trim())
    .filter(Boolean) as string[];

  if (!candidates.length) {
    return 'Новый товар';
  }

  const counts = new Map<string, number>();
  candidates.forEach((candidate) => {
    counts.set(candidate, (counts.get(candidate) || 0) + 1);
  });

  return Array.from(counts.entries()).sort((left, right) => right[1] - left[1] || left[0].localeCompare(right[0], 'ru'))[0][0];
}

function formatError(error: unknown, fallback: string) {
  const typed = error as AxiosError<{ detail?: string | Array<{ msg?: string }> }>;

  if (!typed.response?.data?.detail) {
    return fallback;
  }

  if (typeof typed.response.data.detail === 'string') {
    return typed.response.data.detail;
  }

  if (Array.isArray(typed.response.data.detail)) {
    return typed.response.data.detail.map((item) => item.msg).filter(Boolean).join(', ') || fallback;
  }

  return fallback;
}

export default function WelcomePage() {
  const router = useRouter();
  const { isInitialized, mustCompleteOnboarding, refreshBootstrapStatus, refreshOnboardingStatus, user } = useAuth();
  const { refreshStores, setSelectedStoreId } = useStoreContext();

  const [step, setStep] = useState(1);
  const [manualMode, setManualMode] = useState(false);
  const [expandedAutoGroups, setExpandedAutoGroups] = useState<Record<string, boolean>>({});
  const [form, setForm] = useState<FormState>(DEFAULT_FORM);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [validationResult, setValidationResult] = useState<ValidationResult | null>(null);
  const [preview, setPreview] = useState<StoreImportPreview | null>(null);
  const [decisions, setDecisions] = useState<DecisionState>({});
  const [manualDecisions, setManualDecisions] = useState<Record<string, ManualVariantDecision>>({});
  const [selectedManualOfferIds, setSelectedManualOfferIds] = useState<string[]>([]);
  const [createManualModalOpen, setCreateManualModalOpen] = useState(false);
  const [attachManualModalOpen, setAttachManualModalOpen] = useState(false);
  const [newManualProductName, setNewManualProductName] = useState('');
  const [selectedManualTargetKey, setSelectedManualTargetKey] = useState('');
  const [validatedSignature, setValidatedSignature] = useState<string | null>(null);
  const [redirectingToBootstrap, setRedirectingToBootstrap] = useState(false);
  const [telegramConnected, setTelegramConnected] = useState(false);

  const signature = `${form.store_name.trim()}|${form.client_id.trim()}|${form.api_key.trim()}`;
  const visibleSteps = useMemo(() => (manualMode ? STEPS : STEPS.slice(0, 5)), [manualMode]);

  useEffect(() => {
    if (!isInitialized || !user) {
      return;
    }

    if (redirectingToBootstrap) {
      return;
    }

    if (user.is_admin || !mustCompleteOnboarding) {
      router.replace('/dashboard');
      router.refresh();
    }
  }, [isInitialized, mustCompleteOnboarding, redirectingToBootstrap, router, user]);

  useEffect(() => {
    if (!user || user.is_admin || !mustCompleteOnboarding) {
      setLoading(false);
      return;
    }

    const load = async () => {
      try {
        const settings = await settingsAPI.getSettings();
        setForm((current) => ({
          ...current,
          warehouse_mode: settings.warehouse_mode,
          packing_mode: settings.packing_mode,
          discrepancy_mode: settings.discrepancy_mode,
          shipments_start_date: formatDateForInput(settings.shipments_start_date),
          shipments_accounting_enabled: settings.shipments_accounting_enabled,
          telegram_chat_id: settings.telegram_chat_id || '',
          notification_timezone: settings.notification_timezone,
          today_supplies_time_local: settings.today_supplies_time_local,
          daily_report_time_local: settings.daily_report_time_local,
          notify_today_supplies: settings.notify_today_supplies,
          notify_losses: settings.notify_losses,
          notify_daily_report: settings.notify_daily_report,
          notify_rejection: settings.notify_rejection,
          notify_acceptance_status: settings.notify_acceptance_status,
        }));
        setTelegramConnected(Boolean(settings.telegram_chat_id));
      } catch {
        toast.error('Не удалось загрузить мастер первого запуска');
      } finally {
        setLoading(false);
      }
    };

    void load();
  }, [mustCompleteOnboarding, user]);

  useEffect(() => {
    if (validatedSignature && validatedSignature !== signature) {
      setValidationResult(null);
      setPreview(null);
      setDecisions({});
      setManualDecisions({});
      setSelectedManualOfferIds([]);
      setCreateManualModalOpen(false);
      setAttachManualModalOpen(false);
      setNewManualProductName('');
      setSelectedManualTargetKey('');
      setManualMode(false);
      setStep((current) => Math.min(current, 4));
      setValidatedSignature(null);
    }
  }, [signature, validatedSignature]);

  const manualPreviewGroups = useMemo<ManualPreviewGroup[]>(() => {
    const grouped = new Map<string, ManualPreviewGroup>();

    Object.values(manualDecisions).forEach((item) => {
      const warehouseProductName = item.warehouse_product_name.trim();
      if (!warehouseProductName) {
        return;
      }
      const key = item.warehouse_product_id
        ? `existing:${item.warehouse_product_id}`
        : `new:${warehouseProductName.toLocaleLowerCase('ru')}`;
      const bucket = grouped.get(key);
      if (bucket) {
        bucket.items.push(item);
        return;
      }
      grouped.set(key, {
        key,
        warehouseProductName,
        warehouseProductId: item.warehouse_product_id,
        items: [item],
        isExisting: Boolean(item.warehouse_product_id),
      });
    });

    return Array.from(grouped.values())
      .map((group) => ({
        ...group,
        items: group.items.sort((left, right) => {
          if (left.color !== right.color) {
            return left.color.localeCompare(right.color, 'ru');
          }
          if (left.size !== right.size) {
            return left.size.localeCompare(right.size, 'ru');
          }
          if (left.pack_size !== right.pack_size) {
            return left.pack_size - right.pack_size;
          }
          return left.offer_id.localeCompare(right.offer_id, 'ru');
        }),
      }))
      .sort((left, right) => left.warehouseProductName.localeCompare(right.warehouseProductName, 'ru'));
  }, [manualDecisions]);

  const progress = useMemo(() => Math.round((step / visibleSteps.length) * 100), [step, visibleSteps.length]);
  const hasSelectedNotifications = useMemo(
    () => [
      form.notify_today_supplies,
      form.notify_losses,
      form.notify_daily_report,
      form.notify_rejection,
      form.notify_acceptance_status,
    ].some(Boolean),
    [
      form.notify_acceptance_status,
      form.notify_daily_report,
      form.notify_losses,
      form.notify_rejection,
      form.notify_today_supplies,
    ],
  );
  const hasWarehouseOptions = (preview?.available_warehouse_products.length || 0) > 0;
  const flatPreviewVariants = useMemo(() => {
    if (!preview) {
      return [];
    }

    return preview.grouped_products.flatMap((group) =>
      group.colors.flatMap((colorGroup) =>
        colorGroup.sizes.flatMap((sizeGroup) =>
          sizeGroup.variants.map((variant) => ({
            source_group_key: group.group_key,
            source_base_name: group.base_name,
            source_product_name: group.product_name,
            offer_id: variant.offer_id,
            color: variant.color,
            size: variant.size,
            pack_size: variant.pack_size,
          })),
        ),
      ),
    );
  }, [preview]);

  const unlinkedManualVariants = useMemo(
    () => flatPreviewVariants.filter((variant) => !manualDecisions[variant.offer_id]?.warehouse_product_name?.trim()),
    [flatPreviewVariants, manualDecisions],
  );

  const selectedManualVariants = useMemo(
    () => unlinkedManualVariants.filter((variant) => selectedManualOfferIds.includes(variant.offer_id)),
    [selectedManualOfferIds, unlinkedManualVariants],
  );

  const manualTargetOptions = useMemo<ManualTargetOption[]>(() => {
    const existingOptions = (preview?.available_warehouse_products || []).map((item) => ({
      key: `existing:${item.id}`,
      label: `${item.name} · существующий товар`,
      warehouse_product_id: item.id,
      warehouse_product_name: item.name,
    }));

    const newOptions = manualPreviewGroups
      .filter((group) => !group.warehouseProductId)
      .map((group) => ({
        key: group.key,
        label: `${group.warehouseProductName} · новый товар`,
        warehouse_product_name: group.warehouseProductName,
      }));

    return [...existingOptions, ...newOptions];
  }, [manualPreviewGroups, preview]);

  const selectedManualCount = selectedManualOfferIds.length;

  const update = <K extends keyof FormState>(key: K, value: FormState[K]) => {
    setForm((current) => ({ ...current, [key]: value }));
  };

  const handleTelegramStatusChange = useCallback((status: TelegramConnectStatus | null) => {
    const chatId = status?.telegram_chat_id || '';
    setTelegramConnected(status?.status === 'connected');
    setForm((current) => {
      if (current.telegram_chat_id === chatId) {
        return current;
      }
      return {
        ...current,
        telegram_chat_id: chatId,
      };
    });
  }, []);

  const validateCurrentStep = useCallback((currentStep: number) => {
    if (currentStep === 2) {
      if (!form.shipments_accounting_enabled) {
        return true;
      }
      const parsed = parseDateInput(form.shipments_start_date);
      if (parsed.error) {
        toast.error(parsed.error);
        return false;
      }
    }

    if (currentStep === 3) {
      return true;
    }

    if (currentStep === 4) {
      if (!form.store_name.trim() || !form.client_id.trim() || !form.api_key.trim()) {
        toast.error('На этом шаге нужно заполнить название магазина, Client ID и API-ключ');
        return false;
      }
    }

    return true;
  }, [form]);

  const validateStore = useCallback(async () => {
    if (!validateCurrentStep(4)) {
      return;
    }

    if (validatedSignature === signature && preview && validationResult?.valid) {
      setStep(5);
      return;
    }

    setPreviewLoading(true);
    try {
      const result = await storesAPI.validate({
        name: form.store_name,
        client_id: form.client_id,
        api_key: form.api_key,
      });
      setValidationResult(result);

      if (!result.valid) {
        toast.error(result.message || 'Не удалось проверить магазин');
        setPreview(null);
        setDecisions({});
        return;
      }

      const nextPreview = await storesAPI.getProductLinkPreview({
        name: form.store_name,
        client_id: form.client_id,
        api_key: form.api_key,
      });

      setPreview(nextPreview);
      setValidatedSignature(signature);
      setManualMode(false);
      setExpandedAutoGroups({});
      setDecisions(
        Object.fromEntries(
          nextPreview.grouped_products.map((group) => [
            group.group_key,
            {
              warehouse_product_id: group.match_status === 'auto' ? group.suggested_warehouse_product_id || undefined : undefined,
              warehouse_product_name:
                group.match_status === 'auto'
                  ? nextPreview.available_warehouse_products.find((item) => item.id === group.suggested_warehouse_product_id)?.name || group.base_name
                  : group.base_name,
            },
          ]),
        ),
      );
      setManualDecisions(
        Object.fromEntries(
          nextPreview.grouped_products.flatMap((group) =>
            group.colors.flatMap((colorGroup) =>
              colorGroup.sizes.flatMap((sizeGroup) =>
                sizeGroup.variants.map((variant) => [
                  variant.offer_id,
                  {
                    source_group_key: group.group_key,
                    source_base_name: group.base_name,
                    source_product_name: group.product_name,
                    offer_id: variant.offer_id,
                    color: variant.color,
                    size: variant.size,
                    pack_size: variant.pack_size,
                    warehouse_product_name: '',
                  } satisfies ManualVariantDecision,
                ]),
              ),
            ),
          ),
        ),
      );
      setSelectedManualOfferIds([]);
      setCreateManualModalOpen(false);
      setAttachManualModalOpen(false);
      setNewManualProductName('');
      setSelectedManualTargetKey('');

      setStep(5);
      toast.success('Магазин проверен. Остался последний шаг.');
    } catch (error: unknown) {
      toast.error(formatError(error, 'Не удалось проверить магазин'));
      setPreview(null);
      setDecisions({});
      setExpandedAutoGroups({});
    } finally {
      setPreviewLoading(false);
    }
  }, [form.api_key, form.client_id, form.store_name, preview, signature, validateCurrentStep, validatedSignature, validationResult]);

  const nextStep = async () => {
    if (step === 4) {
      await validateStore();
      return;
    }

    if (!validateCurrentStep(step)) {
      return;
    }

    setStep((current) => Math.min(current + 1, visibleSteps.length));
  };

  const enableManualMode = () => {
    if (!preview || validatedSignature !== signature) {
      toast.error('Сначала дождитесь проверки магазина и автопредложений');
      return;
    }

    setManualMode(true);
    setSelectedManualOfferIds([]);
    setStep(6);
  };

  const buildAutomaticProductLinks = useCallback((): StoreProductLinkDecision[] | null => {
    if (!preview || validatedSignature !== signature) {
      toast.error('Сначала проверьте магазин и дождитесь группировки товаров');
      return null;
    }

    const productLinks: StoreProductLinkDecision[] = preview.grouped_products.map((group) => {
      const decision = decisions[group.group_key];
      return {
        base_name: group.base_name,
        group_key: group.group_key,
        warehouse_product_id: decision?.warehouse_product_id,
        warehouse_product_name: decision?.warehouse_product_name?.trim(),
      };
    });

    const invalid = productLinks.find((item) => !item.warehouse_product_id && !item.warehouse_product_name);
    if (invalid) {
      toast.error(`Для группы «${invalid.base_name}» нужно выбрать связь или новое название`);
      return null;
    }

    return productLinks;
  }, [decisions, preview, signature, validatedSignature]);

  const buildManualProductLinks = useCallback((): StoreProductLinkDecision[] | null => {
    if (!preview || validatedSignature !== signature) {
      toast.error('Сначала проверьте магазин и дождитесь загрузки товаров');
      return null;
    }

    const missingVariant = Object.values(manualDecisions).find((item) => !item.warehouse_product_name.trim());
    if (missingVariant) {
      toast.error(`Для артикула ${missingVariant.offer_id} укажите название товара склада`);
      return null;
    }

    const groupedDecisions = new Map<string, StoreProductLinkDecision>();

    Object.values(manualDecisions).forEach((item) => {
      const targetName = item.warehouse_product_name.trim();
      if (!targetName) {
        return;
      }

      const decisionKey = `${item.source_group_key}::${item.warehouse_product_id || targetName.toLocaleLowerCase('ru')}`;
      const existing = groupedDecisions.get(decisionKey);
      if (existing) {
        existing.offer_ids = [...(existing.offer_ids || []), item.offer_id];
        return;
      }

      groupedDecisions.set(decisionKey, {
        base_name: item.source_base_name,
        group_key: item.source_group_key,
        offer_ids: [item.offer_id],
        warehouse_product_id: item.warehouse_product_id,
        warehouse_product_name: item.warehouse_product_id ? undefined : targetName,
      });
    });

    return Array.from(groupedDecisions.values()).sort((left, right) => {
      const groupCompare = (left.group_key || '').localeCompare(right.group_key || '', 'ru');
      if (groupCompare !== 0) {
        return groupCompare;
      }
      return (left.warehouse_product_name || '').localeCompare(right.warehouse_product_name || '', 'ru');
    });
  }, [manualDecisions, preview, signature, validatedSignature]);

  const toggleManualVariant = (offerId: string) => {
    setSelectedManualOfferIds((current) =>
      current.includes(offerId) ? current.filter((id) => id !== offerId) : [...current, offerId],
    );
  };

  const clearManualSelection = () => {
    setSelectedManualOfferIds([]);
  };

  const assignSelectedManualVariants = (
    target: Pick<ManualTargetOption, 'warehouse_product_id' | 'warehouse_product_name'>,
  ) => {
    if (!selectedManualVariants.length) {
      toast.error('Сначала выбери вариации');
      return;
    }

    setManualDecisions((current) => {
      const next = { ...current };
      selectedManualVariants.forEach((variant) => {
        next[variant.offer_id] = {
          ...(current[variant.offer_id] || variant),
          warehouse_product_id: target.warehouse_product_id,
          warehouse_product_name: target.warehouse_product_name,
        };
      });
      return next;
    });

    setSelectedManualOfferIds([]);
    setCreateManualModalOpen(false);
    setAttachManualModalOpen(false);
    setNewManualProductName('');
    setSelectedManualTargetKey('');
  };

  const detachManualVariant = (offerId: string) => {
    setManualDecisions((current) => ({
      ...current,
      [offerId]: {
        ...current[offerId],
        warehouse_product_id: undefined,
        warehouse_product_name: '',
      },
    }));
  };

  const removeManualGroup = (group: ManualPreviewGroup) => {
    setManualDecisions((current) => {
      const next = { ...current };
      group.items.forEach((item) => {
        next[item.offer_id] = {
          ...next[item.offer_id],
          warehouse_product_id: undefined,
          warehouse_product_name: '',
        };
      });
      return next;
    });
  };

  const completeOnboarding = async (mode: 'auto' | 'manual' = manualMode ? 'manual' : 'auto') => {
    if (!preview || validatedSignature !== signature) {
      toast.error('Сначала проверьте магазин и дождитесь группировки товаров');
      return;
    }

    const parsedDate = form.shipments_accounting_enabled
      ? parseDateInput(form.shipments_start_date)
      : { iso: null, error: null };
    if (parsedDate.error) {
      toast.error(parsedDate.error || 'Проверьте дату начала списания');
      setStep(2);
      return;
    }

    const productLinks = mode === 'manual' ? buildManualProductLinks() : buildAutomaticProductLinks();
    if (!productLinks) {
      if (mode === 'manual') {
        setStep(6);
      }
      return;
    }

    setSaving(true);
    try {
      await settingsAPI.updateSettings({
        warehouse_mode: form.warehouse_mode,
        packing_mode: form.packing_mode,
        discrepancy_mode: form.discrepancy_mode,
        shipments_accounting_enabled: form.shipments_accounting_enabled,
        shipments_start_date: parsedDate.iso,
        telegram_chat_id: form.telegram_chat_id.trim() || null,
        notification_timezone: form.notification_timezone,
        today_supplies_time_local: form.today_supplies_time_local,
        daily_report_time_local: form.daily_report_time_local,
        notify_today_supplies: form.notify_today_supplies,
        notify_losses: form.notify_losses,
        notify_daily_report: form.notify_daily_report,
        notify_rejection: form.notify_rejection,
        notify_acceptance_status: form.notify_acceptance_status,
      });

      const createdStore = await storesAPI.create({
        name: form.store_name.trim(),
        client_id: form.client_id.trim(),
        api_key: form.api_key.trim(),
        product_links: productLinks,
      });

      await refreshStores(createdStore.id);
      setSelectedStoreId(createdStore.id);

      await settingsAPI.completeOnboarding();
      setRedirectingToBootstrap(true);
      await refreshOnboardingStatus(user);
      await refreshBootstrapStatus(user);

      toast.success('Магазин подключен. Загружаем первую синхронизацию.');
      router.replace(`/onboarding-sync?store=${createdStore.id}`);
      router.refresh();
    } catch (error: unknown) {
      toast.error(formatError(error, 'Не удалось завершить первичную настройку'));
    } finally {
      setSaving(false);
    }
  };

  if (loading) {
    return (
      <ProtectedRoute>
        <div className="flex min-h-screen items-center justify-center bg-[radial-gradient(circle_at_top_left,_rgba(48,214,255,0.18),_transparent_28%),radial-gradient(circle_at_bottom_right,_rgba(255,124,102,0.16),_transparent_28%),linear-gradient(180deg,#f8fbff_0%,#eef4ff_100%)]">
          <div className="h-12 w-12 animate-spin rounded-full border-b-2 border-sky-600" />
        </div>
      </ProtectedRoute>
    );
  }

  return (
    <ProtectedRoute>
      <div className="min-h-screen bg-[radial-gradient(circle_at_top_left,_rgba(48,214,255,0.18),_transparent_28%),radial-gradient(circle_at_bottom_right,_rgba(255,124,102,0.16),_transparent_28%),linear-gradient(180deg,#f8fbff_0%,#eef4ff_100%)] px-4 py-6 sm:px-6 lg:px-8">
        <div className="mx-auto grid max-w-7xl gap-5 xl:grid-cols-[300px_minmax(0,1fr)]">
          <aside className="rounded-[32px] border border-white/70 bg-[linear-gradient(180deg,rgba(8,18,35,0.96),rgba(15,37,64,0.93))] p-5 text-white shadow-[0_24px_60px_rgba(15,23,42,0.18)]">
            <div className="inline-flex items-center gap-2 rounded-full border border-white/10 bg-white/10 px-4 py-1 text-xs font-semibold uppercase tracking-[0.2em] text-cyan-200">
              <SparklesIcon className="h-4 w-4" />
              Первый запуск
            </div>
            <h1 className="mt-4 text-2xl font-semibold tracking-tight">Сначала быстро настроим основу кабинета, а потом откроем рабочий интерфейс.</h1>
            <p className="mt-3 text-sm leading-6 text-slate-200">Мастер нужен один раз. Если что-то хочется поменять позже, это всегда можно сделать в разделе «Настройки».</p>
            <div className="mt-6 h-2 overflow-hidden rounded-full bg-white/10">
              <div className="h-full rounded-full bg-cyan-300 transition-all" style={{ width: `${progress}%` }} />
            </div>
            <div className="mt-2 text-xs uppercase tracking-[0.16em] text-cyan-200">Готово на {progress}%</div>
            <div className="mt-6 space-y-2.5">
              {visibleSteps.map((item) => {
                const isActive = item.id === step;
                const isDone = item.id < step;

                return (
                  <div key={item.id} className={cn('rounded-2xl border px-3.5 py-3', isActive ? 'border-cyan-300 bg-white/10' : 'border-white/10 bg-white/5')}>
                    <div className="flex items-center gap-2.5">
                      <div
                        className={cn(
                          'flex h-8 w-8 items-center justify-center rounded-full border text-sm font-semibold',
                          isDone ? 'border-emerald-300 bg-emerald-400/15 text-emerald-200' : isActive ? 'border-cyan-300 bg-cyan-300/10 text-cyan-100' : 'border-white/15 text-slate-300',
                        )}
                      >
                        {isDone ? <CheckIcon className="h-5 w-5" /> : item.id}
                      </div>
                      <div className="text-sm font-semibold text-white">{item.title}</div>
                    </div>
                  </div>
                );
              })}
            </div>
          </aside>

          <section className="rounded-[32px] border border-white/70 bg-white/88 p-5 shadow-[0_24px_60px_rgba(15,23,42,0.12)] backdrop-blur sm:p-6">
            <div className="border-b border-slate-200 pb-5">
              <p className="text-xs font-semibold uppercase tracking-[0.22em] text-sky-700">Шаг {step} из {visibleSteps.length}</p>
              <h2 className="mt-2.5 text-2xl font-semibold tracking-tight text-slate-950">{visibleSteps.find((item) => item.id === step)?.title || visibleSteps[0].title}</h2>
            </div>

            <div className="mt-6 space-y-5">
              {step === 1 && (
                <div className="space-y-4">
                  <div className="rounded-2xl border border-sky-100 bg-sky-50 px-4 py-3 text-sm text-sky-900">
                    <span className="font-semibold">Рекомендуем для старта:</span> если физически товары лежат вместе, выбирай один общий склад. Если учет и остатки у магазинов реально раздельные, выбирай отдельный склад на каждый магазин.
                  </div>

                  <div className="grid gap-4 lg:grid-cols-2">
                    {[
                      ['shared', 'Один склад на все магазины', 'Подходит, если весь товар хранится в одном месте и ты хочешь видеть общий пул остатков.'],
                      ['per_store', 'Отдельный склад на каждый магазин', 'Подходит, если у каждого магазина свой остаток, свой приход и отдельный учет.'],
                    ].map(([value, title, desc]) => (
                      <button
                        key={value}
                        type="button"
                        onClick={() => update('warehouse_mode', value as UserSettings['warehouse_mode'])}
                        className={cn('rounded-3xl border p-5 text-left transition', form.warehouse_mode === value ? 'border-sky-400 bg-sky-50' : 'border-slate-200 bg-white')}
                      >
                        <div className="text-lg font-semibold text-slate-950">{title}</div>
                        <div className="mt-2 text-sm leading-6 text-slate-500">{desc}</div>
                      </button>
                    ))}
                  </div>
                </div>
              )}

              {step === 2 && (
                <div className="space-y-6">
                  <div className="rounded-2xl border border-sky-100 bg-sky-50 px-4 py-3 text-sm text-sky-900">
                    <span className="font-semibold">Рекомендуем для старта:</span> оставить простой режим упаковки, а автоматический учет отправок включать только когда уже внесен приход или ты точно готов начать списание.
                  </div>

                  <div className="grid gap-5 lg:grid-cols-2">
                    <div className="rounded-3xl border border-slate-200 bg-slate-50/70 p-5">
                      <h3 className="text-lg font-semibold text-slate-950">Режим упаковки</h3>
                      <div className="mt-4 space-y-3">
                        {[
                          ['simple', 'Простой — без отдельного учёта упаковок'],
                          ['advanced', 'Расширенный — учитываем упакованные упаковки'],
                        ].map(([value, label]) => (
                          <label key={value} className="flex items-center gap-3 rounded-2xl border border-slate-200 bg-white p-4">
                            <input type="radio" checked={form.packing_mode === value} onChange={() => update('packing_mode', value as UserSettings['packing_mode'])} className="h-4 w-4 text-sky-600" />
                            <span className="text-sm font-medium text-slate-800">{label}</span>
                          </label>
                        ))}
                      </div>
                    </div>

                    <div className="rounded-3xl border border-slate-200 bg-slate-50/70 p-5">
                      <h3 className="text-lg font-semibold text-slate-950">Режим расхождений при приёмке</h3>
                      <div className="mt-4 space-y-3">
                        {[
                          ['loss', 'Считать расхождения потерями'],
                          ['correction', 'Возвращать расхождения на наш склад'],
                        ].map(([value, label]) => (
                          <label key={value} className="flex items-center gap-3 rounded-2xl border border-slate-200 bg-white p-4">
                            <input type="radio" checked={form.discrepancy_mode === value} onChange={() => update('discrepancy_mode', value as UserSettings['discrepancy_mode'])} className="h-4 w-4 text-sky-600" />
                            <span className="text-sm font-medium text-slate-800">{label}</span>
                          </label>
                        ))}
                      </div>
                    </div>
                  </div>

                  <div>
                    <label className="flex items-center gap-3 rounded-2xl border border-slate-200 bg-white p-4">
                      <input
                        type="checkbox"
                        checked={form.shipments_accounting_enabled}
                        onChange={(event) => update('shipments_accounting_enabled', event.target.checked)}
                        className="h-4 w-4 rounded border-slate-300 text-sky-600"
                      />
                      <div>
                        <div className="text-sm font-medium text-slate-900">Включить автоматический учёт отправок OZON</div>
                        <div className="mt-1 text-xs text-slate-500">Если приход еще не внесен, это поле лучше пока не включать. Потом его можно спокойно включить в настройках.</div>
                      </div>
                    </label>
                    <label className="mt-4 block text-sm font-medium text-slate-700">Учитывать только поставки, созданные не раньше этой даты</label>
                    <input
                      type="date"
                      value={form.shipments_start_date}
                      onChange={(event) => update('shipments_start_date', event.target.value)}
                      disabled={!form.shipments_accounting_enabled}
                      className="mt-2 block w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100 disabled:cursor-not-allowed disabled:bg-slate-100"
                    />
                    <p className="mt-2 text-xs text-slate-500">Берем именно дату создания поставки в OZON. Если оставить поле пустым, учет начнется с момента, когда ты включишь этот режим.</p>
                  </div>
                </div>
              )}

              {step === 3 && (
                <div className="space-y-6">
                  <div
                    className={cn(
                      'rounded-2xl border px-4 py-3 text-sm',
                      telegramConnected
                        ? 'border-emerald-200 bg-emerald-50 text-emerald-900'
                        : hasSelectedNotifications
                          ? 'border-amber-200 bg-amber-50 text-amber-900'
                          : 'border-slate-200 bg-slate-50 text-slate-700',
                    )}
                  >
                    {telegramConnected
                      ? 'Telegram уже подключен, уведомления начнут приходить по выбранным правилам.'
                      : hasSelectedNotifications
                        ? 'Telegram можно подключить позже. Настройки уведомлений сохранятся сейчас, а сами сообщения начнут приходить после подключения бота в настройках.'
                        : 'Telegram на первом входе не обязателен. Этот шаг можно пропустить и подключить бота позже в настройках.'}
                  </div>

                  <div className="rounded-2xl border border-sky-100 bg-sky-50 px-4 py-3 text-sm text-sky-900">
                    <span className="font-semibold">Рекомендуем для старта:</span> оставь полезные уведомления включенными, даже если бот пока не подключен. Позже просто привяжешь Telegram, и они начнут работать.
                  </div>

                  <div className="grid gap-4 lg:grid-cols-2">
                    {[
                      ['notify_today_supplies', 'Уведомлять о поставках на сегодня'],
                      ['notify_losses', 'Уведомлять о потерях и расхождениях'],
                      ['notify_rejection', 'Уведомлять об отказе в приёмке'],
                      ['notify_acceptance_status', 'Уведомлять о смене статуса отправки'],
                      ['notify_daily_report', 'Уведомлять ежедневным отчётом'],
                    ].map(([key, label]) => (
                      <label key={key} className="flex items-center gap-3 rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                        <input
                          type="checkbox"
                          checked={Boolean(form[key as keyof FormState])}
                          onChange={(event) => update(key as keyof FormState, event.target.checked as never)}
                          className="h-4 w-4 rounded border-slate-300 text-sky-600"
                        />
                        <span className="text-sm font-medium text-slate-800">{label}</span>
                      </label>
                    ))}
                  </div>

                  <TelegramConnectPanel onStatusChange={handleTelegramStatusChange} />

                  <div className="grid gap-5 lg:grid-cols-2">
                    <div>
                      <label className="block text-sm font-medium text-slate-700">Часовой пояс уведомлений</label>
                      <select
                        value={form.notification_timezone}
                        onChange={(event) => update('notification_timezone', event.target.value)}
                        className="mt-2 block w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                      >
                        {TIMEZONE_OPTIONS.map((option) => (
                          <option key={option.value} value={option.value}>
                            {option.label}
                          </option>
                        ))}
                      </select>
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-slate-700">Поставки на сегодня</label>
                      <input
                        type="time"
                        value={form.today_supplies_time_local}
                        onChange={(event) => update('today_supplies_time_local', event.target.value)}
                        className="mt-2 block w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-slate-700">Ежедневный отчёт</label>
                      <input
                        type="time"
                        value={form.daily_report_time_local}
                        onChange={(event) => update('daily_report_time_local', event.target.value)}
                        className="mt-2 block w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                      />
                    </div>
                  </div>
                </div>
              )}

              {step === 4 && (
                <div className="grid gap-5 xl:grid-cols-[1fr_0.9fr]">
                  <div className="space-y-4">
                    <div className="rounded-2xl border border-sky-100 bg-sky-50 px-4 py-3 text-sm text-sky-900">
                      <span className="font-semibold">Что будет дальше:</span> после проверки мы сами подтянем товары магазина, предложим связи и дадим тебе быстро подтвердить результат.
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-slate-700">Название магазина</label>
                      <input
                        type="text"
                        value={form.store_name}
                        onChange={(event) => update('store_name', event.target.value)}
                        className="mt-2 block w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-slate-700">Client ID</label>
                      <input
                        type="text"
                        value={form.client_id}
                        onChange={(event) => update('client_id', event.target.value)}
                        className="mt-2 block w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                      />
                    </div>

                    <div>
                      <label className="block text-sm font-medium text-slate-700">API-ключ</label>
                      <input
                        type="password"
                        value={form.api_key}
                        onChange={(event) => update('api_key', event.target.value)}
                        className="mt-2 block w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                      />
                    </div>

                    {validationResult && (
                      <div className={cn('rounded-2xl border px-4 py-3 text-sm', validationResult.valid ? 'border-emerald-200 bg-emerald-50 text-emerald-900' : 'border-rose-200 bg-rose-50 text-rose-900')}>
                        {validationResult.message}
                      </div>
                    )}
                  </div>

                  <div className="rounded-3xl border border-slate-200 bg-[linear-gradient(180deg,#eff6ff_0%,#ffffff_100%)] p-5">
                    <div className="font-semibold text-slate-950">Где взять Client ID и API-ключ</div>
                    <ol className="mt-4 list-decimal space-y-3 pl-5 text-sm leading-6 text-slate-600">
                      <li>Откройте кабинет продавца Ozon и перейдите в раздел API-ключей.</li>
                      <li>На этой странице виден Client ID и кнопка создания нового ключа.</li>
                      <li>Нажмите Generate key, задайте понятное имя ключа и выберите роль.</li>
                      <li>Для нашего сценария рекомендуем начать с Admin read only. Если Ozon не показывает этот вариант или проверка не проходит, используйте роль Admin.</li>
                    </ol>
                    <div className="mt-4 flex flex-wrap gap-2">
                      <Link href="https://seller.ozon.ru/app/settings/api-keys" target="_blank" className="rounded-full bg-slate-950 px-3 py-1.5 text-xs font-semibold text-white">
                        Открыть API-ключи OZON
                      </Link>
                      <Link href="https://docs.ozon.com/global/en/api/intro/" target="_blank" className="rounded-full border border-slate-200 px-3 py-1.5 text-xs font-semibold text-slate-700">
                        Официальная справка
                      </Link>
                    </div>
                  </div>
                </div>
              )}

              {step === 5 && (
                <div className="space-y-4">
                  {previewLoading && (
                    <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4 text-sm text-slate-600">
                      Проверяем магазин, собираем товары и подготавливаем связи...
                    </div>
                  )}

                  {preview && (
                    <div className="space-y-4">
                      <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4 text-sm text-slate-600">
                        Мы нашли {preview.grouped_products.length} групп товаров. Ниже можно быстро подтвердить автосвязи, создать новые товары или привязать группы к уже существующим.
                      </div>
                      <div className="rounded-2xl border border-sky-100 bg-sky-50 px-4 py-4 text-sm text-sky-900">
                        <span className="font-semibold">Рекомендуем для старта:</span> если автосвязи выглядят разумно, можно просто оставить их как есть и идти дальше. Если что-то кажется сомнительным, переходи в ручной режим.
                      </div>
                      <div className="flex justify-end">
                        <button
                          type="button"
                          onClick={enableManualMode}
                          disabled={saving || previewLoading}
                          className="inline-flex items-center justify-center gap-2 rounded-2xl border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
                        >
                          Настроить вручную
                        </button>
                      </div>
                      <div className="grid gap-3 sm:grid-cols-3">
                        <div className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-center shadow-sm">
                          <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">Групп</div>
                          <div className="mt-1 text-xl font-semibold text-slate-950">{preview.grouped_products.length}</div>
                        </div>
                        <div className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-center shadow-sm">
                          <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">Вариаций</div>
                          <div className="mt-1 text-xl font-semibold text-slate-950">{flatPreviewVariants.length}</div>
                        </div>
                        <div className="rounded-2xl border border-slate-200 bg-white px-4 py-3 text-center shadow-sm">
                          <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">Складских товаров</div>
                          <div className="mt-1 text-xl font-semibold text-slate-950">{preview.available_warehouse_products.length}</div>
                        </div>
                      </div>

                      {!hasWarehouseOptions && (
                        <div className="rounded-2xl border border-dashed border-slate-300 bg-white px-4 py-3 text-sm text-slate-500">
                          Складских товаров пока нет, поэтому система просто создаст новые позиции автоматически.
                        </div>
                      )}

                      {preview.grouped_products.map((group) => {
                        const decision = decisions[group.group_key] || { warehouse_product_name: group.base_name };
                        const variantSummary = group.colors.flatMap((colorGroup) =>
                          colorGroup.sizes.flatMap((sizeGroup) =>
                            sizeGroup.variants.map((variant) => ({
                              key: variant.offer_id || `${colorGroup.color}-${sizeGroup.size}-${variant.pack_size}`,
                              label: variant.offer_id || 'Без артикула',
                            })),
                          ),
                        );
                        const isExpanded = expandedAutoGroups[group.group_key] ?? false;
                        const visibleVariants = isExpanded ? variantSummary : variantSummary.slice(0, 6);

                        return (
                          <div key={group.group_key} className="rounded-3xl border border-slate-200 bg-white p-4 shadow-sm">
                            <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr),340px]">
                              <div>
                                <div className="flex flex-wrap items-center gap-2">
                                  <div className="text-base font-semibold text-slate-950">{group.base_name}</div>
                                  {group.match_status === 'auto' && (
                                    <span className="rounded-full bg-emerald-100 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.14em] text-emerald-700">
                                      Автосвязь
                                    </span>
                                  )}
                                  {group.match_status === 'conflict' && (
                                    <span className="rounded-full bg-amber-100 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.14em] text-amber-700">
                                      Нужно выбрать
                                    </span>
                                  )}
                                  {group.match_status === 'new' && (
                                    <span className="rounded-full bg-slate-100 px-2.5 py-1 text-[11px] font-semibold uppercase tracking-[0.14em] text-slate-600">
                                      Новый товар
                                    </span>
                                  )}
                                  <span className="rounded-full bg-sky-50 px-2.5 py-1 text-[11px] font-medium text-sky-700">
                                    {group.total_variants} вариаций
                                  </span>
                                </div>
                                <div className="mt-1 text-sm text-slate-500">Исходное название: {group.product_name}</div>
                                <div className="mt-3 rounded-2xl border border-sky-100 bg-sky-50 px-3 py-2 text-sm text-sky-900">
                                  {decision.warehouse_product_id
                                    ? `Будет привязано к товару «${decision.warehouse_product_name}».`
                                    : `Будет создан новый товар «${decision.warehouse_product_name || group.base_name}».`}
                                </div>
                                <div className="mt-3 flex flex-wrap gap-2">
                                  {visibleVariants.map((item) => (
                                    <div key={item.key} className="rounded-full border border-slate-200 bg-slate-50 px-3 py-1 text-xs text-slate-600">
                                      <span className="font-medium text-slate-800">{item.label}</span>
                                    </div>
                                  ))}
                                  {variantSummary.length > 6 && (
                                    <button
                                      type="button"
                                      onClick={() =>
                                        setExpandedAutoGroups((current) => ({
                                          ...current,
                                          [group.group_key]: !isExpanded,
                                        }))
                                      }
                                      className="rounded-full border border-sky-200 bg-sky-50 px-3 py-1 text-xs font-medium text-sky-700 transition hover:bg-sky-100"
                                    >
                                      {isExpanded ? 'Свернуть' : `Еще ${variantSummary.length - visibleVariants.length}`}
                                    </button>
                                  )}
                                </div>
                                {group.match_explanation && (
                                  <div className="mt-3 rounded-2xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-600">
                                    {group.match_explanation}
                                  </div>
                                )}
                              </div>
                              <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                                <div>
                                  <label className="block text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Куда привязать</label>
                                  <select
                                    value={decision.warehouse_product_id ?? ''}
                                    disabled={!hasWarehouseOptions}
                                    onChange={(event) => {
                                      const selectedId = event.target.value ? Number(event.target.value) : undefined;
                                      const selectedProduct = preview.available_warehouse_products.find((item) => item.id === selectedId);
                                      setDecisions((current) => ({
                                        ...current,
                                        [group.group_key]: {
                                          warehouse_product_id: selectedId,
                                          warehouse_product_name: selectedProduct?.name || current[group.group_key]?.warehouse_product_name || group.base_name,
                                        },
                                      }));
                                    }}
                                    className="mt-2 block w-full rounded-2xl border border-slate-300 bg-white px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100 disabled:cursor-not-allowed disabled:bg-slate-100"
                                  >
                                    <option value="">Создать новый товар</option>
                                    {preview.available_warehouse_products.map((item) => (
                                      <option key={item.id} value={item.id}>
                                        {item.name}
                                      </option>
                                    ))}
                                  </select>
                                </div>

                                <div className="mt-4">
                                  <label className="block text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Название, если создаем новый</label>
                                  <input
                                    type="text"
                                    value={decision.warehouse_product_name}
                                    disabled={Boolean(decision.warehouse_product_id)}
                                    onChange={(event) =>
                                      setDecisions((current) => ({
                                        ...current,
                                        [group.group_key]: {
                                          ...current[group.group_key],
                                          warehouse_product_name: event.target.value,
                                        },
                                      }))
                                    }
                                    className="mt-2 block w-full rounded-2xl border border-slate-300 bg-white px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100 disabled:cursor-not-allowed disabled:bg-slate-100"
                                  />
                                </div>
                              </div>
                            </div>
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              )}

              {step === 6 && (
                <div className="space-y-6">
                  <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-4 text-sm text-slate-600">
                    Здесь можно вручную собрать товары из вариаций. Выбери нужные артикулы, создай товар или добавь их к существующему, а ниже сразу увидишь итоговую структуру.
                  </div>

                  <section className="rounded-[32px] border border-slate-200 bg-white p-5 shadow-sm">
                    <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                      <div>
                        <h3 className="text-xl font-semibold text-slate-950">Непривязанные вариации</h3>
                        <p className="mt-1 text-sm text-slate-500">
                          Отметь нужные артикулы и выбери, что с ними сделать.
                        </p>
                      </div>

                      <div className="flex flex-wrap gap-2">
                        <button
                          type="button"
                          onClick={() => setSelectedManualOfferIds(unlinkedManualVariants.map((variant) => variant.offer_id))}
                          disabled={!unlinkedManualVariants.length}
                          className="rounded-2xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:opacity-50"
                        >
                          Выбрать все
                        </button>
                        <button
                          type="button"
                          onClick={clearManualSelection}
                          disabled={!selectedManualCount}
                          className="rounded-2xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:opacity-50"
                        >
                          Снять выбор
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            if (!selectedManualCount) {
                              toast.error('Сначала выбери вариации');
                              return;
                            }
                            setNewManualProductName(buildSuggestedProductName(selectedManualVariants));
                            setCreateManualModalOpen(true);
                          }}
                          disabled={!selectedManualCount}
                          className="inline-flex items-center gap-2 rounded-2xl bg-slate-950 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:opacity-50"
                        >
                          <PlusIcon className="h-4 w-4" />
                          Создать товар
                        </button>
                        <button
                          type="button"
                          onClick={() => {
                            if (!selectedManualCount) {
                              toast.error('Сначала выбери вариации');
                              return;
                            }
                            if (!manualTargetOptions.length) {
                              toast.error('Пока некуда добавлять. Сначала создай товар.');
                              return;
                            }
                            setSelectedManualTargetKey(manualTargetOptions[0].key);
                            setAttachManualModalOpen(true);
                          }}
                          disabled={!selectedManualCount || !manualTargetOptions.length}
                          className="inline-flex items-center gap-2 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-2 text-sm font-medium text-sky-700 transition hover:bg-sky-100 disabled:opacity-50"
                        >
                          <CheckIcon className="h-4 w-4" />
                          Добавить к товару
                        </button>
                      </div>
                    </div>

                    <div className="mt-4 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                      Выбрано вариаций: <span className="font-semibold text-slate-900">{selectedManualCount}</span>
                    </div>

                    {!unlinkedManualVariants.length ? (
                      <div className="mt-5 rounded-3xl border border-dashed border-slate-300 bg-slate-50 px-6 py-10 text-center text-sm text-slate-500">
                        Все вариации уже распределены по товарам.
                      </div>
                    ) : (
                      <div className="mt-5 overflow-hidden rounded-3xl border border-slate-200">
                        <div className="grid grid-cols-[52px_minmax(0,1fr)_170px_150px_120px] gap-3 bg-slate-50 px-4 py-3 text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                          <div></div>
                          <div>Артикул</div>
                          <div>Исходная группа</div>
                          <div>Цвет / Размер</div>
                          <div>Упаковка</div>
                        </div>
                        <div className="divide-y divide-slate-200">
                          {unlinkedManualVariants.map((variant) => (
                            <label
                              key={variant.offer_id}
                              className="grid cursor-pointer grid-cols-[52px_minmax(0,1fr)_170px_150px_120px] gap-3 px-4 py-3 text-sm text-slate-700 transition hover:bg-slate-50"
                            >
                              <div className="flex items-center">
                                <input
                                  type="checkbox"
                                  checked={selectedManualOfferIds.includes(variant.offer_id)}
                                  onChange={() => toggleManualVariant(variant.offer_id)}
                                  className="h-4 w-4 rounded border-slate-300 text-sky-600"
                                />
                              </div>
                              <div>
                                <div className="font-medium text-slate-950">{variant.offer_id}</div>
                                <div className="mt-1 truncate text-xs text-slate-500">{variant.source_product_name}</div>
                              </div>
                              <div className="truncate">{variant.source_base_name}</div>
                              <div className="truncate">{variant.color || 'Без цвета'} / {variant.size || 'Без размера'}</div>
                              <div>{formatPackSizeLabel(variant.pack_size)}</div>
                            </label>
                          ))}
                        </div>
                      </div>
                    )}
                  </section>

                  <div className="rounded-3xl border border-slate-200 bg-[linear-gradient(180deg,#eff6ff_0%,#ffffff_100%)] p-5">
                    <div className="text-lg font-semibold text-slate-950">Что получится в итоге</div>
                    <p className="mt-2 text-sm leading-6 text-slate-600">
                      Ниже видно, как будут выглядеть товары после сохранения магазина.
                    </p>

                    <div className="mt-5 space-y-4">
                      {manualPreviewGroups.length === 0 ? (
                        <div className="rounded-2xl border border-dashed border-slate-300 bg-white px-4 py-4 text-sm text-slate-500">
                          Когда ты создашь товар или добавишь вариации к существующему, структура сразу появится здесь.
                        </div>
                      ) : (
                        manualPreviewGroups.map((group) => (
                          <div key={group.warehouseProductName} className="rounded-3xl border border-white bg-white px-5 py-4 shadow-sm">
                            <div className="flex flex-col gap-4 border-b border-slate-200 pb-4 lg:flex-row lg:items-center lg:justify-between">
                              <div>
                                <div className="text-base font-semibold text-slate-950">{group.warehouseProductName}</div>
                                <div className="mt-1 text-sm text-slate-500">
                                  Вариаций: {group.items.length} · {group.isExisting ? 'Существующий товар' : 'Новый товар'}
                                </div>
                              </div>
                              <div className="flex flex-wrap gap-2">
                                <button
                                  type="button"
                                  onClick={() =>
                                    assignSelectedManualVariants({
                                      warehouse_product_id: group.warehouseProductId,
                                      warehouse_product_name: group.warehouseProductName,
                                    })
                                  }
                                  disabled={!selectedManualCount}
                                  className="rounded-2xl border border-sky-200 bg-sky-50 px-4 py-2 text-sm font-medium text-sky-700 transition hover:bg-sky-100 disabled:opacity-50"
                                >
                                  Добавить выбранные вариации
                                </button>
                                {!group.isExisting && (
                                  <button
                                    type="button"
                                    onClick={() => removeManualGroup(group)}
                                    className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-2 text-sm font-medium text-rose-700 transition hover:bg-rose-100"
                                  >
                                    Удалить товар
                                  </button>
                                )}
                              </div>
                            </div>
                            <div className="mt-4 divide-y divide-slate-200">
                              {group.items.map((item) => (
                                <div key={`${group.key}:${item.offer_id}`} className="grid grid-cols-[minmax(0,1fr)_150px_120px_120px] gap-3 py-3 text-sm text-slate-700">
                                  <div className="min-w-0">
                                    <div className="truncate font-medium text-slate-950">{item.offer_id}</div>
                                    <div className="truncate text-xs text-slate-500">{item.source_base_name}</div>
                                  </div>
                                  <div className="truncate">{item.color || 'Без цвета'} / {item.size || 'Без размера'}</div>
                                  <div>{formatPackSizeLabel(item.pack_size)}</div>
                                  <div className="flex justify-end">
                                    <button
                                      type="button"
                                      onClick={() => detachManualVariant(item.offer_id)}
                                      className="rounded-2xl border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:bg-slate-50"
                                    >
                                      Отвязать
                                    </button>
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>
                        ))
                      )}
                    </div>
                  </div>
                </div>
              )}
            </div>

            <div className="mt-6 flex flex-col gap-3 border-t border-slate-200 pt-5 sm:flex-row sm:items-center sm:justify-between">
              <div className="text-sm text-slate-500">
                После завершения пользователь увидит обычный кабинет и сможет менять всё это в разделе «Настройки».
              </div>

              <div className="flex flex-col gap-3 sm:flex-row">
                {step > 1 && (
                  <button
                    type="button"
                    onClick={() => {
                      if (step === 6) {
                        if (createManualModalOpen) {
                          setCreateManualModalOpen(false);
                        }
                        if (attachManualModalOpen) {
                          setAttachManualModalOpen(false);
                        }
                        setManualMode(false);
                        setStep(5);
                        return;
                      }
                      setStep((current) => Math.max(1, current - 1));
                    }}
                    className="inline-flex items-center justify-center gap-2 rounded-2xl border border-slate-300 bg-white px-5 py-3 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
                  >
                    <ArrowLeftIcon className="h-4 w-4" />
                    Назад
                  </button>
                )}

                {step === 3 && (
                  <button
                    type="button"
                    onClick={() => setStep((current) => Math.min(current + 1, visibleSteps.length))}
                    disabled={previewLoading}
                    className="inline-flex items-center justify-center gap-2 rounded-2xl border border-slate-300 bg-white px-5 py-3 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    Пропустить и настроить позже
                  </button>
                )}

                {step < visibleSteps.length ? (
                  <button
                    type="button"
                    onClick={() => void nextStep()}
                    disabled={previewLoading}
                    className="inline-flex items-center justify-center gap-2 rounded-2xl bg-sky-600 px-5 py-3 text-sm font-medium text-white transition hover:bg-sky-700 disabled:cursor-not-allowed disabled:opacity-60"
                  >
                    Далее
                    <ArrowRightIcon className="h-4 w-4" />
                  </button>
                ) : (
                  <>
                    {step === 5 && (
                      <button
                        type="button"
                        onClick={enableManualMode}
                        disabled={saving || previewLoading}
                        className="inline-flex items-center justify-center gap-2 rounded-2xl border border-slate-300 bg-white px-5 py-3 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:cursor-not-allowed disabled:opacity-60"
                      >
                        Настроить вручную
                      </button>
                    )}
                    <button
                      type="button"
                      onClick={() => void completeOnboarding(step === 6 ? 'manual' : 'auto')}
                      disabled={saving || previewLoading}
                      className="inline-flex items-center justify-center gap-2 rounded-2xl bg-slate-950 px-5 py-3 text-sm font-medium text-white transition hover:bg-slate-800 disabled:cursor-not-allowed disabled:opacity-60"
                    >
                      {saving ? 'Сохраняем...' : 'Завершить настройку'}
                      {!saving && <CheckCircleIcon className="h-4 w-4" />}
                    </button>
                  </>
                )}
              </div>
            </div>
          </section>
        </div>
      </div>

      {createManualModalOpen && (
        <div className="fixed inset-0 z-[70] flex items-center justify-center bg-slate-950/50 p-4 backdrop-blur-sm">
          <div className="w-full max-w-xl rounded-[28px] bg-white shadow-[0_28px_80px_rgba(15,23,42,0.24)]">
            <div className="flex items-start justify-between border-b border-slate-200 px-6 py-5">
              <div>
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Новый товар</div>
                <h3 className="mt-1 text-xl font-semibold text-slate-950">Создать товар из выбранных вариаций</h3>
                <p className="mt-1 text-sm text-slate-500">Выбрано вариаций: {selectedManualCount}</p>
              </div>
              <button
                type="button"
                onClick={() => setCreateManualModalOpen(false)}
                className="rounded-2xl p-2 text-slate-400 transition hover:bg-slate-100 hover:text-slate-700"
              >
                <XMarkIcon className="h-5 w-5" />
              </button>
            </div>

            <div className="space-y-4 px-6 py-5">
              <div>
                <label className="block text-sm font-medium text-slate-700">Название товара</label>
                <input
                  type="text"
                  value={newManualProductName}
                  onChange={(event) => setNewManualProductName(event.target.value)}
                  className="mt-2 block w-full rounded-2xl border border-slate-300 bg-white px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                  placeholder="Например, Носки базовые"
                />
              </div>
              <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                После сохранения приложение само сгруппирует вариации внутри товара по цвету, размеру и упаковке.
              </div>
            </div>

            <div className="flex justify-end gap-3 border-t border-slate-200 px-6 py-5">
              <button
                type="button"
                onClick={() => setCreateManualModalOpen(false)}
                className="rounded-2xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
              >
                Отмена
              </button>
              <button
                type="button"
                onClick={() => {
                  if (!newManualProductName.trim()) {
                    toast.error('Укажи название нового товара');
                    return;
                  }
                  assignSelectedManualVariants({ warehouse_product_name: newManualProductName.trim() });
                }}
                className="rounded-2xl bg-slate-950 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800"
              >
                Создать товар
              </button>
            </div>
          </div>
        </div>
      )}

      {attachManualModalOpen && (
        <div className="fixed inset-0 z-[70] flex items-center justify-center bg-slate-950/50 p-4 backdrop-blur-sm">
          <div className="w-full max-w-xl rounded-[28px] bg-white shadow-[0_28px_80px_rgba(15,23,42,0.24)]">
            <div className="flex items-start justify-between border-b border-slate-200 px-6 py-5">
              <div>
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Привязка вариаций</div>
                <h3 className="mt-1 text-xl font-semibold text-slate-950">Добавить к товару</h3>
                <p className="mt-1 text-sm text-slate-500">Выбрано вариаций: {selectedManualCount}</p>
              </div>
              <button
                type="button"
                onClick={() => setAttachManualModalOpen(false)}
                className="rounded-2xl p-2 text-slate-400 transition hover:bg-slate-100 hover:text-slate-700"
              >
                <XMarkIcon className="h-5 w-5" />
              </button>
            </div>

            <div className="space-y-4 px-6 py-5">
              <div>
                <label className="block text-sm font-medium text-slate-700">Куда добавить вариации</label>
                <select
                  value={selectedManualTargetKey}
                  onChange={(event) => setSelectedManualTargetKey(event.target.value)}
                  className="mt-2 block w-full rounded-2xl border border-slate-300 bg-white px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                >
                  {manualTargetOptions.map((option) => (
                    <option key={option.key} value={option.key}>
                      {option.label}
                    </option>
                  ))}
                </select>
              </div>
            </div>

            <div className="flex justify-end gap-3 border-t border-slate-200 px-6 py-5">
              <button
                type="button"
                onClick={() => setAttachManualModalOpen(false)}
                className="rounded-2xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
              >
                Отмена
              </button>
              <button
                type="button"
                onClick={() => {
                  const target = manualTargetOptions.find((option) => option.key === selectedManualTargetKey);
                  if (!target) {
                    toast.error('Выбери товар, к которому нужно добавить вариации');
                    return;
                  }
                  assignSelectedManualVariants(target);
                }}
                className="rounded-2xl bg-slate-950 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800"
              >
                Добавить
              </button>
            </div>
          </div>
        </div>
      )}
    </ProtectedRoute>
  );
}
