'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { Layout } from '@/components/Layout';
import { TelegramConnectPanel } from '@/components/TelegramConnectPanel';
import { WebPushPanel } from '@/components/WebPushPanel';
import { authAPI } from '@/lib/api/auth';
import {
  settingsAPI,
  type TelegramConnectStatus,
  type UserSettings,
} from '@/lib/api/settings';
import { useStoreContext } from '@/lib/context/StoreContext';

interface PasswordFormState {
  old_password: string;
  new_password: string;
  confirm_password: string;
}

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

const SYNC_INTERVAL_OPTIONS = {
  sync_supplies_interval_minutes: [5, 10, 15, 20, 30, 45, 60],
  sync_stocks_interval_minutes: [20, 30, 45, 60, 90, 120],
  sync_products_interval_minutes: [360, 720, 1440],
  sync_reports_interval_minutes: [180, 360, 720, 1440],
  sync_finance_interval_minutes: [360, 720, 1440],
} as const;

const SYNC_INTERVAL_META: Array<{
  key: keyof Pick<
    UserSettings,
    | 'sync_supplies_interval_minutes'
    | 'sync_stocks_interval_minutes'
    | 'sync_products_interval_minutes'
    | 'sync_reports_interval_minutes'
    | 'sync_finance_interval_minutes'
  >;
  title: string;
  description: string;
  minimumLabel: string;
}> = [
  {
    key: 'sync_supplies_interval_minutes',
    title: 'Поставки',
    description: 'Проверяем новые и изменившиеся поставки Ozon.',
    minimumLabel: 'Не реже чем раз в 5 минут',
  },
  {
    key: 'sync_stocks_interval_minutes',
    title: 'Остатки',
    description: 'Обновляем остатки Ozon по подключенным магазинам.',
    minimumLabel: 'Не реже чем раз в 20 минут',
  },
  {
    key: 'sync_products_interval_minutes',
    title: 'Товары',
    description: 'Подтягиваем карточки товаров и изменения каталога.',
    minimumLabel: 'Не реже чем раз в 6 часов',
  },
  {
    key: 'sync_reports_interval_minutes',
    title: 'Отчеты',
    description: 'Обновляем периодические отчеты и витрины для аналитики.',
    minimumLabel: 'Не реже чем раз в 3 часа',
  },
  {
    key: 'sync_finance_interval_minutes',
    title: 'Финансы',
    description: 'Обновляем финансовые данные и выплаты Ozon.',
    minimumLabel: 'Не реже чем раз в 6 часов',
  },
];

function formatIntervalOptionLabel(minutes: number) {
  if (minutes % (24 * 60) === 0) {
    const days = minutes / (24 * 60);
    const suffix = days === 1 ? 'день' : days >= 2 && days <= 4 ? 'дня' : 'дней';
    return days === 1 ? 'Раз в день' : `Раз в ${days} ${suffix}`;
  }
  if (minutes % 60 === 0) {
    const hours = minutes / 60;
    const suffix = hours === 1 ? 'час' : hours >= 2 && hours <= 4 ? 'часа' : 'часов';
    return hours === 1 ? 'Каждый час' : `Каждые ${hours} ${suffix}`;
  }
  return `Каждые ${minutes} мин.`;
}


export default function SettingsPage() {
  const { stores, isLoading: storesLoading } = useStoreContext();
  const [settings, setSettings] = useState<UserSettings | null>(null);
  const [loading, setLoading] = useState(true);
  const [loadError, setLoadError] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);
  const [passwordSaving, setPasswordSaving] = useState(false);
  const [passwordForm, setPasswordForm] = useState<PasswordFormState>({
    old_password: '',
    new_password: '',
    confirm_password: '',
  });

  const loadData = useCallback(async () => {
    setLoading(true);
    setLoadError(null);
    try {
      const settingsData = await settingsAPI.getSettings();
      setSettings(settingsData);
    } catch {
      setSettings(null);
      setLoadError('Не удалось загрузить настройки кабинета.');
      toast.error('Ошибка загрузки настроек');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadData();
  }, [loadData]);

  const handleTelegramStatusChange = useCallback((status: TelegramConnectStatus | null) => {
    setSettings((current) => {
      if (!current) {
        return current;
      }
      const chatId = status?.telegram_chat_id || null;
      if (current.telegram_chat_id === chatId) {
        return current;
      }
      return {
        ...current,
        telegram_chat_id: chatId,
      };
    });
  }, []);

  const updateSettings = async (newSettings: Partial<UserSettings>) => {
    if (!settings) {
      return;
    }

    setSaving(true);
    try {
      const businessFields: Array<keyof UserSettings> = [
        'warehouse_mode',
        'packing_mode',
        'discrepancy_mode',
        'shipments_start_date',
        'shipments_accounting_enabled',
        'sync_products_interval_minutes',
        'sync_supplies_interval_minutes',
        'sync_stocks_interval_minutes',
        'sync_reports_interval_minutes',
        'sync_finance_interval_minutes',
      ];
      const nextSettings = {
        ...settings,
        ...newSettings,
      };
      const {
        telegram_chat_id: _telegramChatId,
        id: _id,
        user_id: _userId,
        shared_warehouse_id: _sharedWarehouseId,
        is_first_login: _isFirstLogin,
        created_at: _createdAt,
        updated_at: _updatedAt,
        ...payload
      } = nextSettings;
      if (!settings.can_manage_business_settings) {
        const mutablePayload = payload as Record<string, unknown>;
        for (const key of businessFields) {
          delete mutablePayload[key];
        }
      }
      const response = await settingsAPI.updateSettings(payload);
      setSettings(response);
      toast.success('Настройки сохранены');
    } catch {
      toast.error('Ошибка сохранения');
    } finally {
      setSaving(false);
    }
  };

  const changePassword = async (event: React.FormEvent) => {
    event.preventDefault();

    if (passwordForm.new_password.length < 6) {
      toast.error('Новый пароль должен быть не короче 6 символов');
      return;
    }

    if (passwordForm.new_password !== passwordForm.confirm_password) {
      toast.error('Подтверждение пароля не совпадает');
      return;
    }

    setPasswordSaving(true);
    try {
      await authAPI.changePassword({
        old_password: passwordForm.old_password,
        new_password: passwordForm.new_password,
      });
      setPasswordForm({
        old_password: '',
        new_password: '',
        confirm_password: '',
      });
      toast.success('Пароль обновлен');
    } catch {
      toast.error('Не удалось обновить пароль');
    } finally {
      setPasswordSaving(false);
    }
  };

  const warehouseStatusText = useMemo(() => {
    if (!settings) {
      return null;
    }

    if (settings.warehouse_mode === 'shared') {
      return (
        <div>
          <p className="font-medium">Общий склад</p>
          <p className="mt-1 text-sm text-gray-500">
            ID: {settings.shared_warehouse_id || 'Не создан'}
          </p>
        </div>
      );
    }

    if (stores.length === 0) {
      return (
        <div>
          <p className="font-medium">Склады по магазинам</p>
          <p className="mt-1 text-sm text-gray-500">
            Пока нет подключенных магазинов. Добавьте первый магазин, и для него будет создан отдельный склад.
          </p>
        </div>
      );
    }

    return (
      <div className="space-y-3">
        {stores.map((store) => (
          <div key={store.id} className="flex items-center justify-between">
            <span>{store.name}</span>
            <span className="text-sm text-gray-500">
              {store.warehouse_id ? 'Склад создан' : 'Склад будет создан при первом использовании'}
            </span>
          </div>
        ))}
      </div>
    );
  }, [settings, stores]);

  if (loading || storesLoading) {
    return (
      <ProtectedRoute>
        <Layout>
          <div className="flex h-64 items-center justify-center">
            <div className="h-12 w-12 animate-spin rounded-full border-b-2 border-primary-600"></div>
          </div>
        </Layout>
      </ProtectedRoute>
    );
  }

  if (loadError || !settings) {
    return (
      <ProtectedRoute>
        <Layout>
          <div className="mx-auto max-w-4xl">
            <div className="rounded-lg border border-amber-200 bg-amber-50 p-6 text-amber-900">
              <h1 className="text-lg font-semibold">Не удалось открыть настройки</h1>
              <p className="mt-2 text-sm">{loadError || 'Настройки временно недоступны.'}</p>
              <button
                type="button"
                onClick={() => void loadData()}
                className="mt-4 inline-flex items-center rounded-lg bg-amber-600 px-4 py-2 text-sm font-medium text-white transition hover:bg-amber-700"
              >
                Повторить загрузку
              </button>
            </div>
          </div>
        </Layout>
      </ProtectedRoute>
    );
  }

  const canManageBusinessSettings = settings.can_manage_business_settings;

  return (
    <ProtectedRoute>
      <Layout>
        <div className="mx-auto max-w-4xl">
          <div className="mb-8">
            <h1 className="text-2xl font-semibold text-gray-900">Настройки</h1>
            <p className="mt-1 text-sm text-gray-500">
              Режим склада, упаковка, уведомления и безопасность кабинета
            </p>
          </div>

          {settings.is_first_login && (
            <div className="mb-6 rounded-lg border border-amber-200 bg-amber-50 p-4 text-sm text-amber-900">
              Это ваш первый вход. Завершите базовую настройку склада и режима упаковки, чтобы продолжить работу по сценарию из PDF.
            </div>
          )}

          {!canManageBusinessSettings && (
            <div className="mb-6 rounded-lg border border-sky-200 bg-sky-50 p-4 text-sm text-sky-900">
              Вы работаете как участник кабинета. Общие режимы склада и упаковки задает owner, а здесь можно управлять только личными уведомлениями и безопасностью.
            </div>
          )}

          <div className="space-y-6">
            <div className="rounded-lg bg-white shadow">
              <div className="border-b p-6">
                <h2 className="text-lg font-medium text-gray-900">Режим работы склада</h2>
              </div>
              <div className="space-y-4 p-6">
                <label className="flex items-start space-x-3">
                  <input
                    type="radio"
                    name="warehouse_mode"
                    value="shared"
                    checked={settings.warehouse_mode === 'shared'}
                    disabled={!canManageBusinessSettings}
                    onChange={() => void updateSettings({ warehouse_mode: 'shared' })}
                    className="mt-1 h-4 w-4 text-primary-600"
                  />
                  <div>
                    <span className="font-medium text-gray-900">Один склад на все магазины</span>
                    <p className="text-sm text-gray-500">
                      Все товары учитываются на одном складе, что удобно для общего пула остатков.
                    </p>
                  </div>
                </label>

                <label className="flex items-start space-x-3">
                  <input
                    type="radio"
                    name="warehouse_mode"
                    value="per_store"
                    checked={settings.warehouse_mode === 'per_store'}
                    disabled={!canManageBusinessSettings}
                    onChange={() => void updateSettings({ warehouse_mode: 'per_store' })}
                    className="mt-1 h-4 w-4 text-primary-600"
                  />
                  <div>
                    <span className="font-medium text-gray-900">Отдельный склад на каждый магазин</span>
                    <p className="text-sm text-gray-500">
                      У каждого магазина свой отдельный склад и независимый учёт операций.
                    </p>
                  </div>
                </label>
                {!canManageBusinessSettings && (
                  <p className="text-sm text-gray-500">Только owner может менять режим работы склада.</p>
                )}
              </div>
            </div>

            <div className="rounded-lg bg-white shadow">
              <div className="border-b p-6">
                <h2 className="text-lg font-medium text-gray-900">Статус складов</h2>
              </div>
              <div className="p-6">
                <div className="rounded-lg bg-gray-50 p-4">{warehouseStatusText}</div>
              </div>
            </div>

            <div className="rounded-lg bg-white shadow">
              <div className="border-b p-6">
                <h2 className="text-lg font-medium text-gray-900">Бизнес-правила</h2>
              </div>
              <div className="space-y-6 p-6">
                <div>
                  <label className="mb-2 block text-sm font-medium text-gray-700">Режим упаковки</label>
                  <div className="space-y-2">
                    <label className="flex items-center space-x-3">
                      <input
                        type="radio"
                        name="packing_mode"
                        value="simple"
                        checked={settings.packing_mode === 'simple'}
                        disabled={!canManageBusinessSettings}
                        onChange={() => void updateSettings({ packing_mode: 'simple' })}
                        className="h-4 w-4 text-primary-600"
                      />
                      <span className="text-sm text-gray-700">Простой — без отдельного учёта упаковок</span>
                    </label>
                    <label className="flex items-center space-x-3">
                      <input
                        type="radio"
                        name="packing_mode"
                        value="advanced"
                        checked={settings.packing_mode === 'advanced'}
                        disabled={!canManageBusinessSettings}
                        onChange={() => void updateSettings({ packing_mode: 'advanced' })}
                        className="h-4 w-4 text-primary-600"
                      />
                      <span className="text-sm text-gray-700">Расширенный — учитываем упакованные упаковки</span>
                    </label>
                  </div>
                </div>

                <div>
                  <label className="mb-2 block text-sm font-medium text-gray-700">Режим расхождений при приёмке</label>
                  <div className="space-y-2">
                    <label className="flex items-center space-x-3">
                      <input
                        type="radio"
                        name="discrepancy_mode"
                        value="loss"
                        checked={settings.discrepancy_mode === 'loss'}
                        disabled={!canManageBusinessSettings}
                        onChange={() => void updateSettings({ discrepancy_mode: 'loss' })}
                        className="h-4 w-4 text-primary-600"
                      />
                      <span className="text-sm text-gray-700">Считать расхождения потерями</span>
                    </label>
                    <label className="flex items-center space-x-3">
                      <input
                        type="radio"
                        name="discrepancy_mode"
                        value="correction"
                        checked={settings.discrepancy_mode === 'correction'}
                        disabled={!canManageBusinessSettings}
                        onChange={() => void updateSettings({ discrepancy_mode: 'correction' })}
                        className="h-4 w-4 text-primary-600"
                      />
                      <span className="text-sm text-gray-700">Возвращать расхождения на наш склад</span>
                    </label>
                  </div>
                </div>

                <div>
                  <label className="flex items-center space-x-3">
                    <input
                      type="checkbox"
                      checked={settings.shipments_accounting_enabled}
                      disabled={!canManageBusinessSettings}
                      onChange={(event) => void updateSettings({ shipments_accounting_enabled: event.target.checked })}
                      className="h-4 w-4 rounded text-primary-600"
                    />
                    <span className="text-sm font-medium text-gray-700">Включить автоматический учёт отправок Ozon</span>
                  </label>
                  <p className="mt-1 text-xs text-gray-500">
                    Пока режим выключен, новые отправки не списываются с нашего склада автоматически.
                  </p>
                  {settings.shipments_accounting_enabled && !settings.shipments_start_date && settings.shipments_accounting_enabled_at && (
                    <p className="mt-1 text-xs text-sky-700">
                      Сейчас учет идет с момента включения: {new Date(settings.shipments_accounting_enabled_at).toLocaleString('ru-RU', {
                        day: '2-digit',
                        month: '2-digit',
                        year: 'numeric',
                        hour: '2-digit',
                        minute: '2-digit',
                      })}
                    </p>
                  )}
                </div>

                <div>
                  <label className="mb-1 block text-sm font-medium text-gray-700">
                    Учитывать только поставки, созданные не раньше этой даты
                  </label>
                  <input
                    type="date"
                    value={settings.shipments_start_date ? settings.shipments_start_date.slice(0, 10) : ''}
                    disabled={!canManageBusinessSettings || !settings.shipments_accounting_enabled}
                    onChange={(event) =>
                      void updateSettings({
                        shipments_start_date: event.target.value ? `${event.target.value}T00:00:00` : null,
                      })
                    }
                    className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-primary-500 focus:ring-primary-500"
                  />
                  <p className="mt-1 text-xs text-gray-500">
                    Если оставить поле пустым, учет начнется с момента включения режима.
                  </p>
                  {!canManageBusinessSettings && (
                    <p className="mt-1 text-xs text-gray-500">Только owner может менять правила учета отправок.</p>
                  )}
                </div>
              </div>
            </div>

            <div className="rounded-lg bg-white shadow">
              <div className="border-b p-6">
                <h2 className="text-lg font-medium text-gray-900">Фоновые синхронизации OZON</h2>
                <p className="mt-1 text-sm text-gray-500">
                  Здесь задается, как часто сервис будет обновлять данные автоматически.
                </p>
              </div>
              <div className="grid gap-4 p-6 md:grid-cols-2">
                {SYNC_INTERVAL_META.map((item) => (
                  <div key={item.key} className="rounded-lg border border-gray-200 p-4">
                    <div className="text-sm font-medium text-gray-900">{item.title}</div>
                    <p className="mt-1 text-xs text-gray-500">{item.description}</p>
                    <div className="mt-3">
                      <select
                        value={settings[item.key]}
                        disabled={!canManageBusinessSettings}
                        onChange={(event) =>
                          void updateSettings({
                            [item.key]: Number(event.target.value),
                          })
                        }
                        className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-primary-500 focus:ring-primary-500 disabled:bg-gray-50"
                      >
                        {SYNC_INTERVAL_OPTIONS[item.key].map((minutes) => (
                          <option key={minutes} value={minutes}>
                            {formatIntervalOptionLabel(minutes)}
                          </option>
                        ))}
                      </select>
                    </div>
                    <p className="mt-2 text-xs text-gray-500">{item.minimumLabel}</p>
                    {!canManageBusinessSettings && (
                      <p className="mt-1 text-xs text-gray-500">Изменять периодичность может только owner.</p>
                    )}
                  </div>
                ))}
              </div>
            </div>

            <div className="rounded-lg bg-white shadow">
              <div className="border-b p-6">
                <h2 className="text-lg font-medium text-gray-900">Уведомления</h2>
              </div>
              <div className="space-y-4 p-6">
                <label className="flex items-center space-x-3">
                  <input
                    type="checkbox"
                    checked={settings.notify_today_supplies}
                    onChange={(event) => void updateSettings({ notify_today_supplies: event.target.checked })}
                    className="h-4 w-4 rounded text-primary-600"
                  />
                  <span className="text-sm text-gray-700">Уведомлять о поставках на сегодня</span>
                </label>
                <label className="flex items-center space-x-3">
                  <input
                    type="checkbox"
                    checked={settings.notify_losses}
                    onChange={(event) => void updateSettings({ notify_losses: event.target.checked })}
                    className="h-4 w-4 rounded text-primary-600"
                  />
                  <span className="text-sm text-gray-700">Уведомлять о потерях и расхождениях</span>
                </label>
                <label className="flex items-center space-x-3">
                  <input
                    type="checkbox"
                    checked={settings.notify_rejection}
                    onChange={(event) => void updateSettings({ notify_rejection: event.target.checked })}
                    className="h-4 w-4 rounded text-primary-600"
                  />
                  <span className="text-sm text-gray-700">Уведомлять об отказе в приёмке</span>
                </label>
                <label className="flex items-center space-x-3">
                  <input
                    type="checkbox"
                    checked={settings.notify_acceptance_status}
                    onChange={(event) => void updateSettings({ notify_acceptance_status: event.target.checked })}
                    className="h-4 w-4 rounded text-primary-600"
                  />
                  <span className="text-sm text-gray-700">Уведомлять о смене статуса отправки</span>
                </label>
                <TelegramConnectPanel compact onStatusChange={handleTelegramStatusChange} />
                <WebPushPanel compact />
                <div className="rounded-2xl border border-slate-200 bg-slate-50 p-4">
                  <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                    <div>
                      <h3 className="text-sm font-semibold text-slate-900">Email как резервный канал</h3>
                      <p className="mt-1 text-xs text-slate-500">
                        Если Telegram недоступен, отчеты и важные события можно получать на email.
                      </p>
                    </div>
                    <label className="flex items-center space-x-3">
                      <input
                        type="checkbox"
                        checked={settings.email_notifications_enabled}
                        onChange={(event) => void updateSettings({ email_notifications_enabled: event.target.checked })}
                        className="h-4 w-4 rounded text-primary-600"
                      />
                      <span className="text-sm text-gray-700">Включить email</span>
                    </label>
                  </div>

                  <div className={`mt-4 grid gap-3 md:grid-cols-2 ${settings.email_notifications_enabled ? '' : 'opacity-60'}`}>
                    <label className="flex items-center space-x-3">
                      <input
                        type="checkbox"
                        checked={settings.email_today_supplies}
                        disabled={!settings.email_notifications_enabled}
                        onChange={(event) => void updateSettings({ email_today_supplies: event.target.checked })}
                        className="h-4 w-4 rounded text-primary-600"
                      />
                      <span className="text-sm text-gray-700">Поставки на сегодня</span>
                    </label>
                    <label className="flex items-center space-x-3">
                      <input
                        type="checkbox"
                        checked={settings.email_daily_report}
                        disabled={!settings.email_notifications_enabled}
                        onChange={(event) => void updateSettings({ email_daily_report: event.target.checked })}
                        className="h-4 w-4 rounded text-primary-600"
                      />
                      <span className="text-sm text-gray-700">Ежедневный отчет</span>
                    </label>
                    <label className="flex items-center space-x-3">
                      <input
                        type="checkbox"
                        checked={settings.email_losses}
                        disabled={!settings.email_notifications_enabled}
                        onChange={(event) => void updateSettings({ email_losses: event.target.checked })}
                        className="h-4 w-4 rounded text-primary-600"
                      />
                      <span className="text-sm text-gray-700">Потери и расхождения</span>
                    </label>
                    <label className="flex items-center space-x-3">
                      <input
                        type="checkbox"
                        checked={settings.email_rejection}
                        disabled={!settings.email_notifications_enabled}
                        onChange={(event) => void updateSettings({ email_rejection: event.target.checked })}
                        className="h-4 w-4 rounded text-primary-600"
                      />
                      <span className="text-sm text-gray-700">Отказ в приёмке</span>
                    </label>
                    <label className="flex items-center space-x-3 md:col-span-2">
                      <input
                        type="checkbox"
                        checked={settings.email_acceptance_status}
                        disabled={!settings.email_notifications_enabled}
                        onChange={(event) => void updateSettings({ email_acceptance_status: event.target.checked })}
                        className="h-4 w-4 rounded text-primary-600"
                      />
                      <span className="text-sm text-gray-700">Смена статуса отправки и просроченные поставки</span>
                    </label>
                  </div>
                </div>
                <div className="grid gap-4 md:grid-cols-3">
                  <div>
                    <label className="mb-1 block text-sm font-medium text-gray-700">Часовой пояс уведомлений</label>
                    <select
                      value={settings.notification_timezone}
                      onChange={(event) => void updateSettings({ notification_timezone: event.target.value })}
                      className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-primary-500 focus:ring-primary-500"
                    >
                      {TIMEZONE_OPTIONS.map((option) => (
                        <option key={option.value} value={option.value}>
                          {option.label}
                        </option>
                      ))}
                    </select>
                    <p className="mt-1 text-xs text-gray-500">По этому часовому поясу будут отправляться отчеты в Telegram.</p>
                  </div>
                  <div>
                    <label className="mb-1 block text-sm font-medium text-gray-700">Поставки на сегодня</label>
                    <input
                      type="time"
                      value={settings.today_supplies_time_local}
                      onChange={(event) => void updateSettings({ today_supplies_time_local: event.target.value })}
                      className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-primary-500 focus:ring-primary-500"
                    />
                    <p className="mt-1 text-xs text-gray-500">Во сколько отправлять список поставок на текущий день.</p>
                  </div>
                  <div>
                    <label className="mb-1 block text-sm font-medium text-gray-700">Ежедневный отчет</label>
                    <input
                      type="time"
                      value={settings.daily_report_time_local}
                      onChange={(event) => void updateSettings({ daily_report_time_local: event.target.value })}
                      className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm shadow-sm focus:border-primary-500 focus:ring-primary-500"
                    />
                    <p className="mt-1 text-xs text-gray-500">Во сколько присылать сводку по складу, поставкам и магазинам.</p>
                  </div>
                </div>
              </div>
            </div>

            <div className="rounded-lg bg-white shadow">
              <div className="border-b p-6">
                <h2 className="text-lg font-medium text-gray-900">Безопасность</h2>
              </div>
              <form onSubmit={changePassword} className="grid gap-4 p-6 md:grid-cols-3">
                <div>
                  <label className="mb-1 block text-sm font-medium text-gray-700">Текущий пароль</label>
                  <input
                    type="password"
                    value={passwordForm.old_password}
                    onChange={(event) => setPasswordForm((prev) => ({ ...prev, old_password: event.target.value }))}
                    className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900"
                    required
                  />
                </div>
                <div>
                  <label className="mb-1 block text-sm font-medium text-gray-700">Новый пароль</label>
                  <input
                    type="password"
                    value={passwordForm.new_password}
                    onChange={(event) => setPasswordForm((prev) => ({ ...prev, new_password: event.target.value }))}
                    className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900"
                    required
                  />
                </div>
                <div>
                  <label className="mb-1 block text-sm font-medium text-gray-700">Подтверждение</label>
                  <input
                    type="password"
                    value={passwordForm.confirm_password}
                    onChange={(event) => setPasswordForm((prev) => ({ ...prev, confirm_password: event.target.value }))}
                    className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900"
                    required
                  />
                </div>
                <div className="md:col-span-3">
                  <button
                    type="submit"
                    disabled={passwordSaving}
                    className="rounded-md bg-gray-900 px-4 py-2 text-sm font-medium text-white hover:bg-gray-800 disabled:opacity-50"
                  >
                    {passwordSaving ? 'Сохраняем...' : 'Обновить пароль'}
                  </button>
                </div>
              </form>
            </div>
          </div>

          {saving && (
            <div className="fixed bottom-4 right-4 rounded-lg bg-green-50 px-4 py-2 text-green-800 shadow-lg">
              Сохранение...
            </div>
          )}
        </div>
      </Layout>
    </ProtectedRoute>
  );
}
