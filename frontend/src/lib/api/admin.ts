import { apiClient, ensureArray, ensureObject } from './client';
import type { User } from './auth';

export interface AdminUserCreatePayload {
  email: string;
  password?: string;
  is_admin?: boolean;
}

export interface AdminUserCreateResponse {
  user: User;
  generated_password?: string | null;
}

const normalizeUser = (value: unknown): User =>
  ensureObject(value, {
    id: 0,
    email: '',
    is_admin: false,
    role: 'owner',
    owner_user_id: null,
    cabinet_owner_id: 0,
    can_manage_business_settings: false,
    is_active: false,
    created_at: '',
  });

export const adminAPI = {
  getUsers: async (): Promise<User[]> => {
    const response = await apiClient.get('/auth/admin/users');
    return ensureArray(response.data).map(normalizeUser);
  },

  createUser: async (payload: AdminUserCreatePayload): Promise<AdminUserCreateResponse> => {
    const response = await apiClient.post('/auth/admin/users', payload);
    const data = ensureObject(response.data, {
      user: normalizeUser(null),
      generated_password: null,
    });
    return {
      ...data,
      user: normalizeUser(data.user),
    };
  },

  toggleUserActive: async (userId: number): Promise<{ id: number; email: string; is_active: boolean }> => {
    const response = await apiClient.patch(`/auth/admin/users/${userId}/toggle-active`);
    return ensureObject(response.data, { id: userId, email: '', is_active: false });
  },

  deleteUser: async (userId: number): Promise<{ deleted_user_ids: number[]; deleted_user_emails: string[]; deleted_count: number }> => {
    const response = await apiClient.delete(`/auth/admin/users/${userId}`);
    return ensureObject(response.data, {
      deleted_user_ids: [],
      deleted_user_emails: [],
      deleted_count: 0,
    });
  },

  resetUser: async (userId: number): Promise<{
    status: string;
    user_id: number;
    email: string;
    deleted_stores: number;
    cleared_notifications: number;
    cleared_web_push_subscriptions: number;
    cabinet_users_kept: number;
  }> => {
    const response = await apiClient.post(`/auth/admin/users/${userId}/reset`);
    return ensureObject(response.data, {
      status: 'reset',
      user_id: userId,
      email: '',
      deleted_stores: 0,
      cleared_notifications: 0,
      cleared_web_push_subscriptions: 0,
      cabinet_users_kept: 1,
    });
  },
};
