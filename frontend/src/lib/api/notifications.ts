import { apiClient, ensureArray, ensureObject } from './client';

export interface UserNotification {
  id: number;
  kind: string;
  title: string;
  body: string;
  action_url?: string | null;
  severity: string;
  is_important: boolean;
  read_at?: string | null;
  created_at: string;
}

export interface NotificationListResponse {
  items: UserNotification[];
  unread_count: number;
}

export const notificationsAPI = {
  async list(): Promise<NotificationListResponse> {
    const response = await apiClient.get('/notifications');
    const data = ensureObject(response.data, { items: [], unread_count: 0 });
    return {
      unread_count: Number(data.unread_count || 0),
      items: ensureArray<UserNotification>(data.items).map((item) =>
        ensureObject(item, {
          id: 0,
          kind: '',
          title: '',
          body: '',
          action_url: null,
          severity: 'info',
          is_important: false,
          read_at: null,
          created_at: '',
        } satisfies UserNotification),
      ),
    };
  },

  async getUnreadCount(): Promise<number> {
    const response = await apiClient.get('/notifications/unread-count');
    const data = ensureObject(response.data, { unread_count: 0 });
    return Number(data.unread_count || 0);
  },

  async markRead(notificationId: number): Promise<number> {
    const response = await apiClient.post(`/notifications/${notificationId}/read`);
    const data = ensureObject(response.data, { unread_count: 0 });
    return Number(data.unread_count || 0);
  },

  async markAllRead(): Promise<number> {
    const response = await apiClient.post('/notifications/read-all');
    const data = ensureObject(response.data, { unread_count: 0 });
    return Number(data.unread_count || 0);
  },
};
