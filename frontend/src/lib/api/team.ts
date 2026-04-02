import { apiClient, ensureArray, ensureObject } from './client';
import type { User } from './auth';

export interface TeamMemberCreatePayload {
  email: string;
  password?: string;
}

export interface TeamMemberCreateResponse {
  user: User;
  generated_password?: string | null;
}

const normalizeUser = (value: unknown): User =>
  ensureObject(value, {
    id: 0,
    email: '',
    is_admin: false,
    role: 'member',
    owner_user_id: null,
    cabinet_owner_id: 0,
    can_manage_business_settings: false,
    is_active: false,
    created_at: '',
  });

export const teamAPI = {
  getUsers: async (): Promise<User[]> => {
    const response = await apiClient.get('/auth/team/users');
    return ensureArray(response.data).map(normalizeUser);
  },

  createUser: async (payload: TeamMemberCreatePayload): Promise<TeamMemberCreateResponse> => {
    const response = await apiClient.post('/auth/team/users', payload);
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
    const response = await apiClient.patch(`/auth/team/users/${userId}/toggle-active`);
    return ensureObject(response.data, { id: userId, email: '', is_active: false });
  },
};
