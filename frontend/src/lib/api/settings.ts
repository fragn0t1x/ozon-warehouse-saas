import { apiClient, ensureObject } from './client';

export interface UserSettings {
  id: number;
  user_id: number;
  role: 'owner' | 'member';
  cabinet_owner_id: number;
  can_manage_business_settings: boolean;
  warehouse_mode: 'shared' | 'per_store';
  packing_mode: 'simple' | 'advanced';
  shipments_start_date?: string | null;
  shipments_accounting_enabled: boolean;
  shipments_accounting_enabled_at?: string | null;
  sync_products_interval_minutes: number;
  sync_supplies_interval_minutes: number;
  sync_stocks_interval_minutes: number;
  sync_reports_interval_minutes: number;
  sync_finance_interval_minutes: number;
  telegram_chat_id?: string | null;
  notification_timezone: string;
  today_supplies_time_local: string;
  daily_report_time_local: string;
  notify_today_supplies: boolean;
  notify_losses: boolean;
  notify_daily_report: boolean;
  notify_rejection: boolean;
  notify_acceptance_status: boolean;
  email_notifications_enabled: boolean;
  email_today_supplies: boolean;
  email_losses: boolean;
  email_daily_report: boolean;
  email_rejection: boolean;
  email_acceptance_status: boolean;
  web_push_notifications_enabled: boolean;
  discrepancy_mode: 'loss' | 'correction';
  shared_warehouse_id?: number | null;
  is_first_login: boolean;
  created_at: string;
  updated_at?: string | null;
}

export interface FirstLoginStatus {
  is_first_login: boolean;
  has_settings: boolean;
}

export interface TelegramConnectStatus {
  configured: boolean;
  status: 'not_configured' | 'not_connected' | 'pending' | 'connected';
  bot_available: boolean;
  bot_last_seen_at?: string | null;
  bot_status_message?: string | null;
  bot_username?: string | null;
  connect_url?: string | null;
  qr_code_url?: string | null;
  manual_code?: string | null;
  manual_command?: string | null;
  telegram_chat_id?: string | null;
  expires_at?: string | null;
  connected_at?: string | null;
  message?: string | null;
}

export interface WebPushStatus {
  configured: boolean;
  library_available: boolean;
  enabled: boolean;
  subscription_count: number;
  public_key?: string | null;
  message?: string | null;
}

export interface WebPushTestResult {
  sent_count: number;
  message: string;
}

export const settingsAPI = {
  getSettings: async (): Promise<UserSettings> => {
    const response = await apiClient.get('/settings/');
    return ensureObject(response.data, {
      id: 0,
      user_id: 0,
      role: 'owner',
      cabinet_owner_id: 0,
      can_manage_business_settings: false,
      warehouse_mode: 'shared',
      packing_mode: 'simple',
      shipments_start_date: null,
      shipments_accounting_enabled: false,
      shipments_accounting_enabled_at: null,
      sync_products_interval_minutes: 360,
      sync_supplies_interval_minutes: 5,
      sync_stocks_interval_minutes: 20,
      sync_reports_interval_minutes: 180,
      sync_finance_interval_minutes: 360,
      telegram_chat_id: null,
      notification_timezone: 'Europe/Moscow',
      today_supplies_time_local: '08:00',
      daily_report_time_local: '09:00',
      notify_today_supplies: false,
      notify_losses: false,
      notify_daily_report: false,
      notify_rejection: false,
      notify_acceptance_status: false,
      email_notifications_enabled: false,
      email_today_supplies: true,
      email_losses: true,
      email_daily_report: true,
      email_rejection: true,
      email_acceptance_status: true,
      web_push_notifications_enabled: false,
      discrepancy_mode: 'loss',
      shared_warehouse_id: null,
      is_first_login: true,
      created_at: '',
      updated_at: null,
    } satisfies UserSettings);
  },

  updateSettings: async (payload: Partial<UserSettings>): Promise<UserSettings> => {
    const response = await apiClient.put('/settings/', payload);
    return ensureObject(response.data, {
      id: 0,
      user_id: 0,
      role: 'owner',
      cabinet_owner_id: 0,
      can_manage_business_settings: false,
      warehouse_mode: 'shared',
      packing_mode: 'simple',
      shipments_start_date: null,
      shipments_accounting_enabled: false,
      shipments_accounting_enabled_at: null,
      sync_products_interval_minutes: 360,
      sync_supplies_interval_minutes: 5,
      sync_stocks_interval_minutes: 20,
      sync_reports_interval_minutes: 180,
      sync_finance_interval_minutes: 360,
      telegram_chat_id: null,
      notification_timezone: 'Europe/Moscow',
      today_supplies_time_local: '08:00',
      daily_report_time_local: '09:00',
      notify_today_supplies: false,
      notify_losses: false,
      notify_daily_report: false,
      notify_rejection: false,
      notify_acceptance_status: false,
      email_notifications_enabled: false,
      email_today_supplies: true,
      email_losses: true,
      email_daily_report: true,
      email_rejection: true,
      email_acceptance_status: true,
      web_push_notifications_enabled: false,
      discrepancy_mode: 'loss',
      shared_warehouse_id: null,
      is_first_login: true,
      created_at: '',
      updated_at: null,
    } satisfies UserSettings);
  },

  getFirstLoginStatus: async (): Promise<FirstLoginStatus> => {
    const response = await apiClient.get('/settings/first-login-status');
    return ensureObject(response.data, {
      is_first_login: true,
      has_settings: false,
    } satisfies FirstLoginStatus);
  },

  completeOnboarding: async (): Promise<UserSettings> => {
    const response = await apiClient.post('/settings/complete-onboarding');
    return ensureObject(response.data, {
      id: 0,
      user_id: 0,
      role: 'owner',
      cabinet_owner_id: 0,
      can_manage_business_settings: false,
      warehouse_mode: 'shared',
      packing_mode: 'simple',
      shipments_start_date: null,
      shipments_accounting_enabled: false,
      shipments_accounting_enabled_at: null,
      sync_products_interval_minutes: 360,
      sync_supplies_interval_minutes: 5,
      sync_stocks_interval_minutes: 20,
      sync_reports_interval_minutes: 180,
      sync_finance_interval_minutes: 360,
      telegram_chat_id: null,
      notification_timezone: 'Europe/Moscow',
      today_supplies_time_local: '08:00',
      daily_report_time_local: '09:00',
      notify_today_supplies: false,
      notify_losses: false,
      notify_daily_report: false,
      notify_rejection: false,
      notify_acceptance_status: false,
      email_notifications_enabled: false,
      email_today_supplies: true,
      email_losses: true,
      email_daily_report: true,
      email_rejection: true,
      email_acceptance_status: true,
      web_push_notifications_enabled: false,
      discrepancy_mode: 'loss',
      shared_warehouse_id: null,
      is_first_login: false,
      created_at: '',
      updated_at: null,
    } satisfies UserSettings);
  },

  getTelegramConnectStatus: async (): Promise<TelegramConnectStatus> => {
    const response = await apiClient.get('/settings/telegram/connect-status');
    return ensureObject(response.data, {
      configured: false,
      status: 'not_connected',
      bot_available: false,
      bot_last_seen_at: null,
      bot_status_message: null,
      bot_username: null,
      connect_url: null,
      qr_code_url: null,
      manual_code: null,
      manual_command: null,
      telegram_chat_id: null,
      expires_at: null,
      connected_at: null,
      message: null,
    } satisfies TelegramConnectStatus);
  },

  createTelegramConnectLink: async (forceNew = false): Promise<TelegramConnectStatus> => {
    const response = await apiClient.post(`/settings/telegram/connect?force_new=${forceNew ? 'true' : 'false'}`);
    return ensureObject(response.data, {
      configured: false,
      status: 'not_connected',
      bot_available: false,
      bot_last_seen_at: null,
      bot_status_message: null,
      bot_username: null,
      connect_url: null,
      qr_code_url: null,
      manual_code: null,
      manual_command: null,
      telegram_chat_id: null,
      expires_at: null,
      connected_at: null,
      message: null,
    } satisfies TelegramConnectStatus);
  },

  disconnectTelegram: async (): Promise<TelegramConnectStatus> => {
    const response = await apiClient.post('/settings/telegram/disconnect');
    return ensureObject(response.data, {
      configured: false,
      status: 'not_connected',
      bot_available: false,
      bot_last_seen_at: null,
      bot_status_message: null,
      bot_username: null,
      connect_url: null,
      qr_code_url: null,
      manual_code: null,
      manual_command: null,
      telegram_chat_id: null,
      expires_at: null,
      connected_at: null,
      message: null,
    } satisfies TelegramConnectStatus);
  },

  getWebPushStatus: async (): Promise<WebPushStatus> => {
    const response = await apiClient.get('/notifications/web-push/status');
    return ensureObject(response.data, {
      configured: false,
      library_available: false,
      enabled: false,
      subscription_count: 0,
      public_key: null,
      message: null,
    } satisfies WebPushStatus);
  },

  subscribeWebPush: async (payload: {
    endpoint: string;
    p256dh_key: string;
    auth_key: string;
    user_agent?: string;
  }): Promise<WebPushStatus> => {
    const response = await apiClient.post('/notifications/web-push/subscribe', payload);
    return ensureObject(response.data, {
      configured: false,
      library_available: false,
      enabled: false,
      subscription_count: 0,
      public_key: null,
      message: null,
    } satisfies WebPushStatus);
  },

  unsubscribeWebPush: async (endpoint?: string | null): Promise<WebPushStatus> => {
    const response = await apiClient.post('/notifications/web-push/unsubscribe', {
      endpoint: endpoint || null,
    });
    return ensureObject(response.data, {
      configured: false,
      library_available: false,
      enabled: false,
      subscription_count: 0,
      public_key: null,
      message: null,
    } satisfies WebPushStatus);
  },

  sendWebPushTest: async (): Promise<WebPushTestResult> => {
    const response = await apiClient.post('/notifications/web-push/test');
    return ensureObject(response.data, {
      sent_count: 0,
      message: '',
    } satisfies WebPushTestResult);
  },
};
