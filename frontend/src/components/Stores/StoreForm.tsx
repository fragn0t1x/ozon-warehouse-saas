'use client';

import { useEffect, useMemo, useState } from 'react';
import { useForm } from 'react-hook-form';
import type { AxiosError } from 'axios';
import { CheckIcon, PlusIcon, XMarkIcon } from '@heroicons/react/24/outline';
import toast from 'react-hot-toast';

import {
  storesAPI,
  type Store,
  type StoreImportPreview,
  type StoreProductLinkDecision,
  type ValidationResult,
} from '@/lib/api/stores';

interface StoreFormValues {
  name: string;
  client_id: string;
  api_key: string;
}

interface StoreFormProps {
  store?: Store | null;
  onClose: () => void;
  onSuccess: (store?: Store) => void | Promise<void>;
}

interface LinkDecisionState {
  warehouse_product_id?: number;
  warehouse_product_name: string;
}

interface PreviewVariantItem {
  source_group_key: string;
  source_base_name: string;
  source_product_name: string;
  offer_id: string;
  color: string;
  size: string;
  pack_size: number;
}

interface ManualAssignment extends PreviewVariantItem {
  warehouse_product_id?: number;
  warehouse_product_name: string;
}

interface ManualPlannedProductGroup {
  key: string;
  warehouse_product_id?: number;
  warehouse_product_name: string;
  variants: ManualAssignment[];
  isExisting: boolean;
}

interface ManualTargetOption {
  key: string;
  label: string;
  warehouse_product_id?: number;
  warehouse_product_name: string;
}

function formatPackSizeLabel(packSize: number) {
  return `${packSize || 1} шт`;
}

function buildSuggestedProductName(variants: Array<Pick<PreviewVariantItem, 'source_base_name'>>) {
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

export function StoreForm({ store, onClose, onSuccess }: StoreFormProps) {
  const [isValidating, setIsValidating] = useState(false);
  const [isSubmitting, setIsSubmitting] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [validationResult, setValidationResult] = useState<ValidationResult | null>(null);
  const [validationError, setValidationError] = useState<string | null>(null);
  const [importPreview, setImportPreview] = useState<StoreImportPreview | null>(null);
  const [linkDecisions, setLinkDecisions] = useState<Record<string, LinkDecisionState>>({});
  const [manualMode, setManualMode] = useState(false);
  const [manualAssignments, setManualAssignments] = useState<Record<string, ManualAssignment>>({});
  const [selectedOfferIds, setSelectedOfferIds] = useState<string[]>([]);
  const [createManualModalOpen, setCreateManualModalOpen] = useState(false);
  const [attachManualModalOpen, setAttachManualModalOpen] = useState(false);
  const [newManualProductName, setNewManualProductName] = useState('');
  const [selectedManualTargetKey, setSelectedManualTargetKey] = useState<string>('');
  const [expandedAutoGroups, setExpandedAutoGroups] = useState<Record<string, boolean>>({});
  const [validatedSignature, setValidatedSignature] = useState<string | null>(null);

  const { register, handleSubmit, watch, setValue, formState: { errors } } = useForm<StoreFormValues>({
    defaultValues: {
      name: store?.name || '',
      client_id: store?.client_id || '',
      api_key: '',
    }
  });

  const clientId = watch('client_id');
  const apiKey = watch('api_key');
  const storeName = watch('name');
  const currentSignature = `${storeName.trim()}|${clientId.trim()}|${apiKey.trim()}`;

  useEffect(() => {
    if (store) {
      setValue('client_id', store.client_id);
    }
  }, [store, setValue]);

  useEffect(() => {
    if (!validatedSignature || store) {
      return;
    }

    if (validatedSignature !== currentSignature) {
      setValidationResult(null);
      setValidationError(null);
      setImportPreview(null);
      setLinkDecisions({});
      setManualMode(false);
      setManualAssignments({});
      setSelectedOfferIds([]);
      setCreateManualModalOpen(false);
      setAttachManualModalOpen(false);
      setNewManualProductName('');
      setSelectedManualTargetKey('');
      setExpandedAutoGroups({});
      setValidatedSignature(null);
    }
  }, [currentSignature, store, validatedSignature]);

  const getErrorMessage = (error: unknown, fallback: string) => {
    const typedError = error as AxiosError<{ detail?: string | Array<{ msg?: string }> }>;

    if (!typedError.response?.data?.detail) {
      return fallback;
    }

    if (typeof typedError.response.data.detail === 'string') {
      return typedError.response.data.detail;
    }

    if (Array.isArray(typedError.response.data.detail)) {
      return typedError.response.data.detail.map((err) => err.msg).filter(Boolean).join(', ') || fallback;
    }

    return fallback;
  };

  const handleValidate = async () => {
    if (!clientId || !apiKey) {
      toast.error('Заполните Client-ID и API ключ');
      return;
    }

    setIsValidating(true);
    setValidationError(null);

    try {
      const result = await storesAPI.validate({
        name: watch('name') || 'Валидация магазина',
        client_id: clientId,
        api_key: apiKey,
      });

      setValidationResult(result);
      setValidationError(null);

      if (result.valid) {
        setValidatedSignature(currentSignature);
        toast.success('Подключение успешно');
        setPreviewLoading(true);
        try {
          const preview = await storesAPI.getProductLinkPreview({
            name: watch('name') || 'Валидация магазина',
            client_id: clientId,
            api_key: apiKey,
          });
          setImportPreview(preview);
          setLinkDecisions(
            Object.fromEntries(
              preview.grouped_products.map((group) => [
                group.group_key,
                {
                  warehouse_product_id: group.match_status === 'auto' ? group.suggested_warehouse_product_id || undefined : undefined,
                  warehouse_product_name:
                    group.match_status === 'auto'
                      ? preview.available_warehouse_products.find((item) => item.id === group.suggested_warehouse_product_id)?.name || group.base_name
                      : group.base_name,
                },
              ])
            )
          );
          setManualMode(false);
          setManualAssignments({});
          setSelectedOfferIds([]);
          setCreateManualModalOpen(false);
          setAttachManualModalOpen(false);
          setNewManualProductName('');
          setSelectedManualTargetKey('');
          setExpandedAutoGroups({});
        } catch (previewError: unknown) {
          const previewErrorMessage = getErrorMessage(previewError, 'Не удалось загрузить группы товаров для связывания');
          setImportPreview(null);
          setLinkDecisions({});
          setManualMode(false);
          setManualAssignments({});
          setSelectedOfferIds([]);
          setExpandedAutoGroups({});
          toast.error(previewErrorMessage);
        } finally {
          setPreviewLoading(false);
        }
      } else {
        toast.error(result.message || 'Ошибка валидации');
      }
    } catch (error: unknown) {
      const errorMessage = getErrorMessage(error, 'Ошибка при валидации');
      setValidationError(errorMessage);
      setValidationResult(null);
      toast.error(errorMessage);
    } finally {
      setIsValidating(false);
    }
  };

  const onSubmit = async (data: StoreFormValues) => {
    if (!store && (!validationResult?.valid || validatedSignature !== currentSignature)) {
      toast.error('Сначала выполните валидацию');
      return;
    }

    let productLinks: StoreProductLinkDecision[] = [];
    if (!store && importPreview?.grouped_products.length) {
      if (manualMode) {
        const unassignedVariant = flatPreviewVariants.find((variant) => !manualAssignments[variant.offer_id]?.warehouse_product_name?.trim());
        if (unassignedVariant) {
          toast.error(`Сначала привяжи артикул ${unassignedVariant.offer_id}`);
          return;
        }

        const groupedManualDecisions = new Map<string, StoreProductLinkDecision>();
        Object.values(manualAssignments).forEach((assignment) => {
          const targetName = assignment.warehouse_product_name.trim();
          if (!targetName) {
            return;
          }

          const decisionKey = `${assignment.source_group_key}::${assignment.warehouse_product_id || targetName.toLocaleLowerCase('ru')}`;
          const existing = groupedManualDecisions.get(decisionKey);
          if (existing) {
            existing.offer_ids = [...(existing.offer_ids || []), assignment.offer_id];
            return;
          }

          groupedManualDecisions.set(decisionKey, {
            base_name: assignment.source_base_name,
            group_key: assignment.source_group_key,
            offer_ids: [assignment.offer_id],
            warehouse_product_id: assignment.warehouse_product_id,
            warehouse_product_name: assignment.warehouse_product_id ? undefined : targetName,
          });
        });

        productLinks = Array.from(groupedManualDecisions.values());
      } else {
        productLinks = importPreview.grouped_products.map((group) => {
          const decision = linkDecisions[group.group_key];
          return {
            base_name: group.base_name,
            group_key: group.group_key,
            warehouse_product_id: decision?.warehouse_product_id,
            warehouse_product_name: decision?.warehouse_product_name?.trim(),
          };
        });

        const invalidDecision = productLinks.find(
          (decision) => !decision.warehouse_product_id && !decision.warehouse_product_name
        );

        if (invalidDecision) {
          toast.error(`Укажи новый товар или связь для группы "${invalidDecision.base_name}"`);
          return;
        }
      }
    }

    setIsSubmitting(true);
    try {
      if (store) {
        const payload = data.api_key.trim()
          ? data
          : { name: data.name, client_id: data.client_id };

        const updatedStore = await storesAPI.update(store.id, payload);
        toast.success('Магазин обновлен');
        await onSuccess(updatedStore);
      } else {
        const createdStore = await storesAPI.create({
          ...data,
          product_links: productLinks,
        });
        toast.success('Магазин успешно добавлен');
        await onSuccess(createdStore);
      }
      onClose();
    } catch (error: unknown) {
      toast.error(
        getErrorMessage(error, store ? 'Ошибка при обновлении' : 'Ошибка при добавлении магазина')
      );
    } finally {
      setIsSubmitting(false);
    }
  };

  const hasWarehouseOptions = (importPreview?.available_warehouse_products.length || 0) > 0;

  const flatPreviewVariants = useMemo<PreviewVariantItem[]>(() => {
    if (!importPreview) {
      return [];
    }

    return importPreview.grouped_products.flatMap((group) =>
      group.colors.flatMap((colorGroup) =>
        colorGroup.sizes.flatMap((sizeGroup) =>
          sizeGroup.variants.map((variant) => ({
            source_group_key: group.group_key,
            source_base_name: group.base_name,
            source_product_name: group.product_name,
            offer_id: variant.offer_id,
            color: variant.color || 'Без цвета',
            size: variant.size || 'Без размера',
            pack_size: variant.pack_size,
          })),
        ),
      ),
    );
  }, [importPreview]);

  const unlinkedPreviewVariants = useMemo(
    () => flatPreviewVariants.filter((variant) => !manualAssignments[variant.offer_id]?.warehouse_product_name?.trim()),
    [flatPreviewVariants, manualAssignments],
  );

  const selectedPreviewVariants = useMemo(
    () => unlinkedPreviewVariants.filter((variant) => selectedOfferIds.includes(variant.offer_id)),
    [selectedOfferIds, unlinkedPreviewVariants],
  );

  const plannedProducts = useMemo<ManualPlannedProductGroup[]>(() => {
    const groups = new Map<string, ManualPlannedProductGroup>();
    Object.values(manualAssignments).forEach((assignment) => {
      const targetName = assignment.warehouse_product_name.trim();
      if (!targetName) {
        return;
      }

      const key = assignment.warehouse_product_id
        ? `existing:${assignment.warehouse_product_id}`
        : `new:${targetName.toLocaleLowerCase('ru')}`;
      const existing = groups.get(key);
      if (existing) {
        existing.variants.push(assignment);
        return;
      }

      groups.set(key, {
        key,
        warehouse_product_id: assignment.warehouse_product_id,
        warehouse_product_name: targetName,
        variants: [assignment],
        isExisting: Boolean(assignment.warehouse_product_id),
      });
    });

    return Array.from(groups.values()).sort((left, right) =>
      left.warehouse_product_name.localeCompare(right.warehouse_product_name, 'ru'),
    );
  }, [manualAssignments]);

  const manualTargetOptions = useMemo<ManualTargetOption[]>(() => {
    const existingOptions = (importPreview?.available_warehouse_products || []).map((item) => ({
      key: `existing:${item.id}`,
      label: `${item.name} · существующий товар`,
      warehouse_product_id: item.id,
      warehouse_product_name: item.name,
    }));

    const newOptions = plannedProducts
      .filter((group) => !group.warehouse_product_id)
      .map((group) => ({
        key: group.key,
        label: `${group.warehouse_product_name} · новый товар`,
        warehouse_product_name: group.warehouse_product_name,
      }));

    return [...existingOptions, ...newOptions];
  }, [importPreview, plannedProducts]);

  const selectedCount = selectedOfferIds.length;

  const toggleManualVariant = (offerId: string) => {
    setSelectedOfferIds((current) =>
      current.includes(offerId) ? current.filter((id) => id !== offerId) : [...current, offerId],
    );
  };

  const clearManualSelection = () => {
    setSelectedOfferIds([]);
  };

  const assignSelectedVariants = (target: ManualTargetOption | { warehouse_product_name: string; warehouse_product_id?: number }) => {
    if (!selectedPreviewVariants.length) {
      toast.error('Сначала выбери вариации');
      return;
    }

    setManualAssignments((current) => {
      const next = { ...current };
      selectedPreviewVariants.forEach((variant) => {
        next[variant.offer_id] = {
          ...variant,
          warehouse_product_id: target.warehouse_product_id,
          warehouse_product_name: target.warehouse_product_name,
        };
      });
      return next;
    });

    clearManualSelection();
    setCreateManualModalOpen(false);
    setAttachManualModalOpen(false);
    setNewManualProductName('');
    setSelectedManualTargetKey('');
  };

  const detachManualVariant = (offerId: string) => {
    setManualAssignments((current) => {
      const next = { ...current };
      delete next[offerId];
      return next;
    });
  };

  const removePlannedGroup = (group: ManualPlannedProductGroup) => {
    setManualAssignments((current) => {
      const next = { ...current };
      group.variants.forEach((variant) => {
        delete next[variant.offer_id];
      });
      return next;
    });
  };

  return (
    <div className="fixed inset-0 bg-gray-500 bg-opacity-75 flex items-center justify-center p-4 z-50">
      <div className={`bg-white rounded-lg w-full max-h-[90vh] overflow-y-auto ${store ? 'max-w-md' : 'max-w-5xl'}`}>
        <div className="flex justify-between items-center p-6 border-b">
          <h2 className="text-xl font-semibold text-gray-900">
            {store ? 'Редактировать магазин' : 'Добавить магазин'}
          </h2>
          <button
            onClick={onClose}
            className="text-gray-400 hover:text-gray-500"
          >
            <XMarkIcon className="h-6 w-6" />
          </button>
        </div>

        <form onSubmit={handleSubmit(onSubmit)} className="p-6 space-y-4">
          <div>
            <label htmlFor="name" className="block text-sm font-medium text-gray-700">
              Название магазина <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              id="name"
              {...register('name', { required: 'Название обязательно' })}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm text-gray-900 bg-white"
              placeholder="Например, основной магазин"
            />
            {errors.name && (
              <p className="mt-1 text-sm text-red-600">{errors.name.message}</p>
            )}
          </div>

          <div>
            <label htmlFor="client_id" className="block text-sm font-medium text-gray-700">
              Client-ID <span className="text-red-500">*</span>
            </label>
            <input
              type="text"
              id="client_id"
              {...register('client_id', { required: 'Client-ID обязателен' })}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm text-gray-900 bg-white"
              placeholder="Введите Client-ID"
              disabled={!!store}
            />
            {errors.client_id && (
              <p className="mt-1 text-sm text-red-600">{errors.client_id.message}</p>
            )}
          </div>

          <div>
            <label htmlFor="api_key" className="block text-sm font-medium text-gray-700">
              API ключ {!store && <span className="text-red-500">*</span>}
              {store && <span className="text-gray-400 text-xs ml-2">(оставьте пустым, чтобы не менять)</span>}
            </label>
            <input
              type="password"
              id="api_key"
              {...register('api_key', {
                required: store ? false : 'API ключ обязателен'
              })}
              className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 sm:text-sm text-gray-900 bg-white"
              placeholder={store ? '••••••••' : 'Введите API ключ'}
            />
            {errors.api_key && (
              <p className="mt-1 text-sm text-red-600">{errors.api_key.message}</p>
            )}
          </div>

          {!store && (
            <button
              type="button"
              onClick={handleValidate}
              disabled={isValidating || !clientId || !apiKey}
              className="w-full inline-flex justify-center items-center px-4 py-2 border border-gray-300 shadow-sm text-sm font-medium rounded-md text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
            >
              {isValidating ? (
                <>
                  <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-gray-700" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                  Проверка...
                </>
              ) : 'Проверить подключение'}
            </button>
          )}

          {validationResult && (
            <div className={`p-4 rounded-md ${
              validationResult.valid ? 'bg-green-50' : 'bg-red-50'
            }`}>
              <p className={`text-sm ${
                validationResult.valid ? 'text-green-800' : 'text-red-800'
              }`}>
                {validationResult.message}
              </p>
              {validationResult.valid && validationResult.data && (
                <p className="mt-1 text-sm text-green-600">
                  Товаров в магазине: {validationResult.data.products_count}
                </p>
              )}
            </div>
          )}

          {!store && previewLoading && (
            <div className="rounded-xl border border-slate-200 bg-slate-50 p-4 text-sm text-slate-600">
              Готовим группы товаров и варианты связывания...
            </div>
          )}

          {!store && importPreview && !manualMode && (
            <div className="space-y-4 rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex items-start justify-between gap-4">
                <div>
                  <h3 className="text-base font-semibold text-slate-900">Связи товаров нового магазина</h3>
                  <p className="mt-1 text-sm text-slate-500">
                    Для каждой группы можно быстро подтвердить автосвязь, создать новый товар или выбрать существующий.
                  </p>
                </div>
                <div className="grid grid-cols-3 gap-2 text-center">
                  <div className="rounded-2xl bg-white px-3 py-2 shadow-sm">
                    <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">Групп</div>
                    <div className="mt-1 text-lg font-semibold text-slate-950">{importPreview.grouped_products.length}</div>
                  </div>
                  <div className="rounded-2xl bg-white px-3 py-2 shadow-sm">
                    <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">Вариаций</div>
                    <div className="mt-1 text-lg font-semibold text-slate-950">
                      {flatPreviewVariants.length}
                    </div>
                  </div>
                  <div className="rounded-2xl bg-white px-3 py-2 shadow-sm">
                    <div className="text-[11px] uppercase tracking-[0.16em] text-slate-500">Товаров</div>
                    <div className="mt-1 text-lg font-semibold text-slate-950">
                      {importPreview.available_warehouse_products.length}
                    </div>
                  </div>
                </div>
              </div>

              {!hasWarehouseOptions && (
                <div className="rounded-xl border border-dashed border-slate-300 bg-white px-4 py-3 text-sm text-slate-500">
                  Пока не с чем связывать: складских товаров ещё нет. Для этих групп будут созданы новые товары.
                </div>
              )}

              <div className="flex justify-end">
                <button
                  type="button"
                  onClick={() => {
                    setManualMode(true);
                    clearManualSelection();
                  }}
                  className="inline-flex items-center gap-2 rounded-2xl border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
                >
                  <PlusIcon className="h-4 w-4" />
                  Перейти в ручной режим
                </button>
              </div>

              <div className="space-y-3">
                {importPreview.grouped_products.map((group) => {
                  const decision = linkDecisions[group.group_key] || { warehouse_product_name: group.base_name };
                  const isLinkedToExisting = Boolean(decision.warehouse_product_id);
                  const suggestedCandidates = group.candidates || [];
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
                    <section key={group.group_key} className="rounded-2xl border border-slate-200 bg-white p-4 shadow-sm">
                      <div className="grid gap-4 xl:grid-cols-[minmax(0,1fr),340px]">
                        <div>
                          <div className="flex flex-wrap items-center gap-2">
                            <h4 className="text-base font-semibold text-slate-950">{group.base_name}</h4>
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
                          <p className="mt-1 text-sm text-slate-500">Исходное название: {group.product_name}</p>
                          {group.match_status === 'auto' && suggestedCandidates[0] && (
                            <p className="mt-2 text-sm text-emerald-700">
                              Система уверенно связала группу с товаром «{suggestedCandidates[0].name}».
                            </p>
                          )}
                          {group.match_status === 'conflict' && (
                            <p className="mt-2 text-sm text-amber-700">
                              Нашли несколько похожих товаров. Лучше выбрать вручную, чтобы не склеить разные позиции.
                            </p>
                          )}
                          {group.match_status === 'new' && hasWarehouseOptions && (
                            <p className="mt-2 text-sm text-slate-500">
                              Подходящей уверенной связи не нашли, поэтому по умолчанию будет создан новый товар.
                            </p>
                          )}
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
                            <div className="mt-3 rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-600">
                              {group.match_explanation}
                            </div>
                          )}
                        </div>

                        <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                          <label className="block text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                            Куда привязать
                          </label>
                          <select
                            value={decision.warehouse_product_id ?? ''}
                            disabled={!hasWarehouseOptions}
                            onChange={(event) => {
                              const selectedId = event.target.value ? Number(event.target.value) : undefined;
                              const selectedProduct = importPreview.available_warehouse_products.find((item) => item.id === selectedId);

                              setLinkDecisions((current) => ({
                                ...current,
                                [group.group_key]: {
                                  warehouse_product_id: selectedId,
                                  warehouse_product_name: selectedProduct?.name || current[group.group_key]?.warehouse_product_name || group.base_name,
                                },
                              }));
                            }}
                            className="mt-2 block w-full rounded-xl border-gray-300 bg-white text-sm text-slate-900 shadow-sm focus:border-primary-500 focus:ring-primary-500 disabled:cursor-not-allowed disabled:bg-slate-100"
                          >
                            <option value="">Создать новый товар</option>
                            {importPreview.available_warehouse_products.map((item) => (
                              <option key={item.id} value={item.id}>
                                {item.name}
                              </option>
                            ))}
                          </select>

                          {suggestedCandidates.length > 0 && (
                            <div className="mt-3">
                              <div className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                                Варианты от системы
                              </div>
                              <div className="mt-2 space-y-2">
                                {suggestedCandidates.slice(0, 3).map((candidate) => (
                                  <button
                                    key={candidate.id}
                                    type="button"
                                    onClick={() => {
                                      const selectedProduct = importPreview.available_warehouse_products.find((item) => item.id === candidate.id);
                                      setLinkDecisions((current) => ({
                                        ...current,
                                        [group.group_key]: {
                                          warehouse_product_id: candidate.id,
                                          warehouse_product_name: selectedProduct?.name || candidate.name,
                                        },
                                      }));
                                    }}
                                    className={`flex w-full items-start justify-between rounded-2xl border px-3 py-2 text-left text-xs transition ${
                                      decision.warehouse_product_id === candidate.id
                                        ? 'border-sky-500 bg-sky-50'
                                        : 'border-slate-200 bg-white hover:border-sky-200'
                                    }`}
                                  >
                                    <div className="min-w-0">
                                      <div className="truncate font-medium text-slate-800">{candidate.name}</div>
                                      {candidate.reasons.length > 0 && (
                                        <div className="mt-1 line-clamp-2 text-slate-500">{candidate.reasons.join(' • ')}</div>
                                      )}
                                    </div>
                                    <div className="ml-3 shrink-0 rounded-full bg-slate-100 px-2 py-1 text-[11px] font-semibold text-slate-600">
                                      {candidate.overlap_count}/{candidate.overlap_total}
                                    </div>
                                  </button>
                                ))}
                              </div>
                            </div>
                          )}

                          <label className="mt-4 block text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">
                            Название, если создаем новый
                          </label>
                          <input
                            type="text"
                            value={decision.warehouse_product_name}
                            disabled={isLinkedToExisting}
                            onChange={(event) => {
                              const value = event.target.value;
                              setLinkDecisions((current) => ({
                                ...current,
                                [group.group_key]: {
                                  ...current[group.group_key],
                                  warehouse_product_id: current[group.group_key]?.warehouse_product_id,
                                  warehouse_product_name: value,
                                },
                              }));
                            }}
                            className="mt-2 block w-full rounded-xl border-gray-300 bg-white text-sm text-slate-900 shadow-sm focus:border-primary-500 focus:ring-primary-500 disabled:cursor-not-allowed disabled:bg-slate-100"
                            placeholder="Например, Носки"
                          />
                        </div>
                      </div>

                      <div className="mt-4 space-y-3">
                        {group.colors.map((colorGroup) => (
                          <div key={`${group.group_key}-${colorGroup.color}`} className="rounded-2xl border border-slate-200 bg-slate-50/70 p-3">
                            <div className="text-xs font-semibold uppercase tracking-[0.16em] text-slate-500">Цвет</div>
                            <div className="mt-1 text-sm font-semibold text-slate-950">{colorGroup.color}</div>

                            <div className="mt-3 space-y-2">
                              {colorGroup.sizes.map((sizeGroup) => (
                                <div key={`${colorGroup.color}-${sizeGroup.size}`} className="rounded-xl border border-white bg-white px-3 py-3 shadow-sm">
                                  <div className="flex flex-col gap-3 md:flex-row md:items-start md:justify-between">
                                    <div className="min-w-[110px]">
                                      <div className="text-xs uppercase tracking-[0.16em] text-slate-500">Размер</div>
                                      <div className="mt-1 text-sm font-semibold text-slate-950">{sizeGroup.size}</div>
                                    </div>

                                    <div className="flex-1">
                                      <div className="text-xs uppercase tracking-[0.16em] text-slate-500">Артикулы</div>
                                      <div className="mt-2 flex flex-wrap gap-2">
                                        {sizeGroup.variants.map((variant) => (
                                          <div key={`${variant.offer_id}-${variant.pack_size}`} className="rounded-xl border border-slate-200 bg-slate-50 px-3 py-2 text-sm text-slate-700">
                                            <div className="font-medium text-slate-950">{variant.offer_id}</div>
                                            <div className="mt-1 text-xs text-slate-500">{variant.pack_size} шт</div>
                                          </div>
                                        ))}
                                      </div>
                                    </div>
                                  </div>
                                </div>
                              ))}
                            </div>
                          </div>
                        ))}
                      </div>
                    </section>
                  );
                })}
              </div>
            </div>
          )}

          {!store && importPreview && manualMode && (
            <div className="space-y-6 rounded-2xl border border-slate-200 bg-slate-50 p-4">
              <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                <div>
                  <h3 className="text-base font-semibold text-slate-900">Ручная настройка связей</h3>
                  <p className="mt-1 text-sm text-slate-500">
                    Выдели нужные вариации, создай из них товар или добавь к уже существующему. После привязки вариации сразу исчезают из верхнего списка.
                  </p>
                </div>
                <div className="flex flex-wrap gap-2">
                  <button
                    type="button"
                    onClick={() => {
                      setManualMode(false);
                      clearManualSelection();
                    }}
                    className="rounded-2xl border border-slate-300 bg-white px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50"
                  >
                    Вернуться к автосвязям
                  </button>
                </div>
              </div>

              <section className="rounded-3xl border border-slate-200 bg-white p-5 shadow-sm">
                <div className="flex flex-col gap-4 lg:flex-row lg:items-start lg:justify-between">
                  <div>
                    <h4 className="text-lg font-semibold text-slate-950">Непривязанные вариации</h4>
                    <p className="mt-1 text-sm text-slate-500">
                      Отметь нужные артикулы и выбери действие.
                    </p>
                  </div>
                  <div className="flex flex-wrap gap-2">
                    <button
                      type="button"
                      onClick={() => setSelectedOfferIds(unlinkedPreviewVariants.map((variant) => variant.offer_id))}
                      disabled={!unlinkedPreviewVariants.length}
                      className="rounded-2xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:opacity-50"
                    >
                      Выбрать все
                    </button>
                    <button
                      type="button"
                      onClick={clearManualSelection}
                      disabled={!selectedCount}
                      className="rounded-2xl border border-slate-300 px-4 py-2 text-sm font-medium text-slate-700 transition hover:bg-slate-50 disabled:opacity-50"
                    >
                      Снять выбор
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        if (!selectedCount) {
                          toast.error('Сначала выбери вариации');
                          return;
                        }
                        setNewManualProductName(buildSuggestedProductName(selectedPreviewVariants));
                        setCreateManualModalOpen(true);
                      }}
                      disabled={!selectedCount}
                      className="inline-flex items-center gap-2 rounded-2xl bg-slate-950 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800 disabled:opacity-50"
                    >
                      <PlusIcon className="h-4 w-4" />
                      Создать товар
                    </button>
                    <button
                      type="button"
                      onClick={() => {
                        if (!selectedCount) {
                          toast.error('Сначала выбери вариации');
                          return;
                        }
                        if (!manualTargetOptions.length) {
                          toast.error('Пока некуда добавлять. Сначала создай новый товар или используй существующий.');
                          return;
                        }
                        setSelectedManualTargetKey(manualTargetOptions[0].key);
                        setAttachManualModalOpen(true);
                      }}
                      disabled={!selectedCount || !manualTargetOptions.length}
                      className="inline-flex items-center gap-2 rounded-2xl border border-sky-200 bg-sky-50 px-4 py-2 text-sm font-medium text-sky-700 transition hover:bg-sky-100 disabled:opacity-50"
                    >
                      <CheckIcon className="h-4 w-4" />
                      Добавить к товару
                    </button>
                  </div>
                </div>

                <div className="mt-4 rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                  Выбрано вариаций: <span className="font-semibold text-slate-900">{selectedCount}</span>
                </div>

                {!unlinkedPreviewVariants.length ? (
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
                      {unlinkedPreviewVariants.map((variant) => (
                        <label
                          key={variant.offer_id}
                          className="grid cursor-pointer grid-cols-[52px_minmax(0,1fr)_170px_150px_120px] gap-3 px-4 py-3 text-sm text-slate-700 transition hover:bg-slate-50"
                        >
                          <div className="flex items-center">
                            <input
                              type="checkbox"
                              checked={selectedOfferIds.includes(variant.offer_id)}
                              onChange={() => toggleManualVariant(variant.offer_id)}
                              className="h-4 w-4 rounded border-slate-300 text-sky-600"
                            />
                          </div>
                          <div>
                            <div className="font-medium text-slate-950">{variant.offer_id}</div>
                            <div className="mt-1 truncate text-xs text-slate-500">{variant.source_product_name}</div>
                          </div>
                          <div className="truncate">{variant.source_base_name}</div>
                          <div className="truncate">{variant.color} / {variant.size}</div>
                          <div>{formatPackSizeLabel(variant.pack_size)}</div>
                        </label>
                      ))}
                    </div>
                  </div>
                )}
              </section>

              <section className="space-y-4">
                <div>
                  <h4 className="text-lg font-semibold text-slate-950">Товары после импорта</h4>
                  <p className="mt-1 text-sm text-slate-500">
                    Здесь видно, как новые вариации будут привязаны после сохранения магазина.
                  </p>
                </div>

                {!plannedProducts.length ? (
                  <div className="rounded-3xl border border-dashed border-slate-300 bg-white px-6 py-10 text-center text-sm text-slate-500 shadow-sm">
                    Пока здесь пусто. Выбери вариации выше и создай первый товар или добавь их к существующему.
                  </div>
                ) : (
                  plannedProducts.map((group) => (
                    <section key={group.key} className="overflow-hidden rounded-[28px] border border-slate-200 bg-white shadow-sm">
                      <div className="flex flex-col gap-4 border-b border-slate-200 bg-slate-50 px-5 py-4 lg:flex-row lg:items-center lg:justify-between">
                        <div>
                          <h5 className="text-lg font-semibold text-slate-950">{group.warehouse_product_name}</h5>
                          <p className="mt-1 text-sm text-slate-500">
                            Вариаций: {group.variants.length} · {group.isExisting ? 'Существующий товар' : 'Новый товар'}
                          </p>
                        </div>
                        <div className="flex flex-wrap gap-2">
                          <button
                            type="button"
                            onClick={() =>
                              assignSelectedVariants({
                                warehouse_product_id: group.warehouse_product_id,
                                warehouse_product_name: group.warehouse_product_name,
                              })
                            }
                            disabled={!selectedCount}
                            className="rounded-2xl border border-sky-200 bg-sky-50 px-4 py-2 text-sm font-medium text-sky-700 transition hover:bg-sky-100 disabled:opacity-50"
                          >
                            Добавить выбранные вариации
                          </button>
                          {!group.isExisting && (
                            <button
                              type="button"
                              onClick={() => removePlannedGroup(group)}
                              className="rounded-2xl border border-rose-200 bg-rose-50 px-4 py-2 text-sm font-medium text-rose-700 transition hover:bg-rose-100"
                            >
                              Удалить товар
                            </button>
                          )}
                        </div>
                      </div>

                      <div className="divide-y divide-slate-200">
                        {group.variants.map((variant) => (
                          <div
                            key={`${group.key}:${variant.offer_id}`}
                            className="grid grid-cols-[minmax(0,1fr)_150px_120px_120px] gap-3 px-5 py-3 text-sm text-slate-700"
                          >
                            <div className="min-w-0">
                              <div className="truncate font-medium text-slate-950">{variant.offer_id}</div>
                              <div className="truncate text-xs text-slate-500">{variant.source_base_name}</div>
                            </div>
                            <div className="truncate">{variant.color} / {variant.size}</div>
                            <div>{formatPackSizeLabel(variant.pack_size)}</div>
                            <div className="flex justify-end">
                              <button
                                type="button"
                                onClick={() => detachManualVariant(variant.offer_id)}
                                className="rounded-2xl border border-slate-300 px-3 py-1.5 text-xs font-medium text-slate-700 transition hover:bg-slate-50"
                              >
                                Отвязать
                              </button>
                            </div>
                          </div>
                        ))}
                      </div>
                    </section>
                  ))
                )}
              </section>
            </div>
          )}

          {validationError && !validationResult && (
            <div className="p-4 rounded-md bg-red-50">
              <p className="text-sm text-red-800">{validationError}</p>
            </div>
          )}

          <div className="flex justify-end space-x-3 pt-4 border-t">
            <button
              type="button"
              onClick={onClose}
              className="px-4 py-2 border border-gray-300 rounded-md text-sm font-medium text-gray-700 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500"
            >
              Отмена
            </button>
            <button
              type="submit"
              disabled={isSubmitting || (!store && !validationResult?.valid)}
              className="px-4 py-2 border border-transparent rounded-md shadow-sm text-sm font-medium text-white bg-primary-600 hover:bg-primary-700 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary-500 disabled:opacity-50"
            >
              {isSubmitting ? (
                <>
                  <svg className="animate-spin -ml-1 mr-3 h-5 w-5 text-white" xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24">
                    <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4"></circle>
                    <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4zm2 5.291A7.962 7.962 0 014 12H0c0 3.042 1.135 5.824 3 7.938l3-2.647z"></path>
                  </svg>
                  Сохранение...
                </>
              ) : (store ? 'Сохранить изменения' : 'Добавить магазин')}
            </button>
          </div>
        </form>
      </div>

      {createManualModalOpen && (
        <div className="fixed inset-0 z-[60] flex items-center justify-center bg-slate-950/50 p-4 backdrop-blur-sm">
          <div className="w-full max-w-xl rounded-[28px] bg-white shadow-[0_28px_80px_rgba(15,23,42,0.24)]">
            <div className="flex items-start justify-between border-b border-slate-200 px-6 py-5">
              <div>
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Новый товар</div>
                <h3 className="mt-1 text-xl font-semibold text-slate-950">Создать товар из выбранных вариаций</h3>
                <p className="mt-1 text-sm text-slate-500">Выбрано вариаций: {selectedCount}</p>
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
                  className="mt-2 block w-full rounded-2xl border border-slate-300 px-4 py-3 text-sm text-slate-900 outline-none transition focus:border-sky-500 focus:ring-2 focus:ring-sky-100"
                  placeholder="Например, Носки базовые"
                />
              </div>
              <div className="rounded-2xl border border-slate-200 bg-slate-50 px-4 py-3 text-sm text-slate-600">
                После сохранения приложение само разложит вариации внутри товара по цвету, размеру и упаковке.
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
                  assignSelectedVariants({ warehouse_product_name: newManualProductName.trim() });
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
        <div className="fixed inset-0 z-[60] flex items-center justify-center bg-slate-950/50 p-4 backdrop-blur-sm">
          <div className="w-full max-w-xl rounded-[28px] bg-white shadow-[0_28px_80px_rgba(15,23,42,0.24)]">
            <div className="flex items-start justify-between border-b border-slate-200 px-6 py-5">
              <div>
                <div className="text-xs uppercase tracking-[0.18em] text-slate-500">Привязка вариаций</div>
                <h3 className="mt-1 text-xl font-semibold text-slate-950">Добавить к товару</h3>
                <p className="mt-1 text-sm text-slate-500">Выбрано вариаций: {selectedCount}</p>
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
                  assignSelectedVariants(target);
                }}
                className="rounded-2xl bg-slate-950 px-4 py-2 text-sm font-medium text-white transition hover:bg-slate-800"
              >
                Добавить
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
