'use client';

export interface StoredUser {
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

const USER_KEY = 'user';
const LEGACY_ACCESS_TOKEN_KEY = 'access_token';
const LEGACY_REFRESH_TOKEN_KEY = 'refresh_token';

function normalizeStoredUser(value: Partial<StoredUser>): StoredUser {
  const id = Number(value.id || 0);
  return {
    id,
    email: value.email || '',
    is_admin: Boolean(value.is_admin),
    role: value.role === 'member' ? 'member' : 'owner',
    owner_user_id: value.owner_user_id ?? null,
    cabinet_owner_id: Number(value.cabinet_owner_id || value.owner_user_id || id || 0),
    can_manage_business_settings: Boolean(
      value.can_manage_business_settings ?? (!value.is_admin && value.role !== 'member'),
    ),
    is_active: Boolean(value.is_active),
    created_at: value.created_at,
  };
}

function isBrowser() {
  return typeof window !== 'undefined';
}

export function getStoredAccessToken(): string | null {
  if (isBrowser()) {
    localStorage.removeItem(LEGACY_ACCESS_TOKEN_KEY);
  }
  return null;
}

export function getStoredRefreshToken(): string | null {
  if (isBrowser()) {
    localStorage.removeItem(LEGACY_REFRESH_TOKEN_KEY);
  }
  return null;
}

export function getStoredUser(): StoredUser | null {
  if (!isBrowser()) {
    return null;
  }

  const rawUser = localStorage.getItem(USER_KEY);
  if (!rawUser) {
    return null;
  }

  try {
    return normalizeStoredUser(JSON.parse(rawUser) as Partial<StoredUser>);
  } catch {
    localStorage.removeItem(USER_KEY);
    return null;
  }
}

export function setStoredUser(user: StoredUser) {
  if (!isBrowser()) {
    return;
  }

  localStorage.setItem(USER_KEY, JSON.stringify(normalizeStoredUser(user)));
}

export function setAuthTokens(accessToken: string, refreshToken?: string | null) {
  void accessToken;
  void refreshToken;
  if (!isBrowser()) {
    return;
  }

  localStorage.removeItem(LEGACY_ACCESS_TOKEN_KEY);
  localStorage.removeItem(LEGACY_REFRESH_TOKEN_KEY);
}

export function clearStoredAuth() {
  if (!isBrowser()) {
    return;
  }

  localStorage.removeItem(USER_KEY);
  localStorage.removeItem(LEGACY_ACCESS_TOKEN_KEY);
  localStorage.removeItem(LEGACY_REFRESH_TOKEN_KEY);
}

export function syncAuthFromCookies() {
  if (!isBrowser()) {
    return;
  }

  // access/refresh cookies теперь httpOnly и недоступны из JS.
  localStorage.removeItem(LEGACY_ACCESS_TOKEN_KEY);
  localStorage.removeItem(LEGACY_REFRESH_TOKEN_KEY);
}
