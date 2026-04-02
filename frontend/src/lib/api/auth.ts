import { apiClient, ensureObject } from './client';

export interface LoginResponse {
  access_token: string;
  refresh_token: string;
  token_type: string;
}

export interface User {
  id: number;
  email: string;
  is_admin: boolean;
  role: 'owner' | 'member';
  owner_user_id?: number | null;
  cabinet_owner_id: number;
  can_manage_business_settings: boolean;
  is_active: boolean;
  created_at?: string;
}

export interface ChangePasswordPayload {
  old_password: string;
  new_password: string;
}

const normalizeLoginResponse = (value: unknown): LoginResponse =>
  ensureObject(value, { access_token: '', refresh_token: '', token_type: 'bearer' });

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

export const authAPI = {
  login: async (email: string, password: string): Promise<LoginResponse> => {
    const response = await apiClient.post('/auth/login', { email, password });
    return normalizeLoginResponse(response.data);
  },

  refreshToken: async (refreshToken?: string | null): Promise<LoginResponse> => {
    const response = await apiClient.post('/auth/refresh', refreshToken ? {
      refresh_token: refreshToken,
    } : {});
    return normalizeLoginResponse(response.data);
  },

  getMe: async (token?: string): Promise<User> => {
    const config = token ? { headers: { Authorization: `Bearer ${token}` } } : {};
    const response = await apiClient.get('/auth/me', config);
    return normalizeUser(response.data);
  },

  changePassword: async (payload: ChangePasswordPayload): Promise<{ message: string }> => {
    const response = await apiClient.post('/auth/change-password', payload);
    return ensureObject(response.data, { message: '' });
  },

  logout: async (): Promise<{ message: string }> => {
    const response = await apiClient.post('/auth/logout');
    return ensureObject(response.data, { message: '' });
  },
};
