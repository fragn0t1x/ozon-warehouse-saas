// frontend/src/lib/context/AuthContext.tsx
'use client';

import { createContext, useCallback, useContext, useEffect, useState } from 'react';
import { useRouter } from 'next/navigation';
import toast from 'react-hot-toast';

import { authAPI } from '../api/auth';
import { storesAPI } from '../api/stores';
import { settingsAPI } from '../api/settings';
import {
  clearStoredAuth,
  getStoredUser,
  setAuthTokens,
  setStoredUser,
  syncAuthFromCookies,
  type StoredUser,
} from '../utils/authStorage';

interface AuthContextType {
  user: StoredUser | null;
  token: string | null;
  login: (email: string, password: string, redirectTo?: string) => Promise<void>;
  logout: () => void;
  isLoading: boolean;
  isInitialized: boolean;
  mustCompleteOnboarding: boolean;
  mustWaitForBootstrap: boolean;
  bootstrapStoreId: number | null;
  refreshOnboardingStatus: (targetUser?: StoredUser | null) => Promise<boolean>;
  refreshBootstrapStatus: (targetUser?: StoredUser | null) => Promise<boolean>;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);
const COOKIE_AUTH_MARKER = '__cookie_session__';

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const router = useRouter();
  const [user, setUser] = useState<StoredUser | null>(null);
  const [token, setToken] = useState<string | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [isInitialized, setIsInitialized] = useState(false);
  const [mustCompleteOnboarding, setMustCompleteOnboarding] = useState(false);
  const [mustWaitForBootstrap, setMustWaitForBootstrap] = useState(false);
  const [bootstrapStoreId, setBootstrapStoreId] = useState<number | null>(null);

  const refreshOnboardingStatus = useCallback(async (targetUser?: StoredUser | null) => {
    const effectiveUser = targetUser ?? getStoredUser();

    if (!effectiveUser || effectiveUser.is_admin) {
      setMustCompleteOnboarding(false);
      return false;
    }

    try {
      const status = await settingsAPI.getFirstLoginStatus();
      setMustCompleteOnboarding(status.is_first_login);
      return status.is_first_login;
    } catch {
      setMustCompleteOnboarding(false);
      return false;
    }
  }, []);

  const refreshBootstrapStatus = useCallback(async (targetUser?: StoredUser | null) => {
    const effectiveUser = targetUser ?? getStoredUser();

    if (!effectiveUser || effectiveUser.is_admin) {
      setMustWaitForBootstrap(false);
      setBootstrapStoreId(null);
      return false;
    }

    try {
      const stores = await storesAPI.getAll();
      const activeStores = stores.filter((store) => store.is_active);
      const shouldRequireBootstrapGate = activeStores.length <= 1;
      const waitingStore =
        activeStores.find((store) => store.bootstrap_state === 'running') ||
        activeStores.find((store) => store.bootstrap_state === 'pending') ||
        activeStores.find((store) => store.bootstrap_state === 'failed');

      const mustWait = shouldRequireBootstrapGate && Boolean(waitingStore);
      setMustWaitForBootstrap(mustWait);
      setBootstrapStoreId(waitingStore?.id ?? null);
      return mustWait;
    } catch {
      setMustWaitForBootstrap(false);
      setBootstrapStoreId(null);
      return false;
    }
  }, []);

  const checkAuth = useCallback(async () => {
    syncAuthFromCookies();
    const savedUser = getStoredUser();

    if (savedUser) {
      setUser(savedUser);
    }

    try {
      const userData = await authAPI.getMe();
      setUser(userData);
      setStoredUser(userData);
      setToken(COOKIE_AUTH_MARKER);
      const isFirstLogin = await refreshOnboardingStatus(userData);
      if (!isFirstLogin) {
        await refreshBootstrapStatus(userData);
      } else {
        setMustWaitForBootstrap(false);
        setBootstrapStoreId(null);
      }
    } catch {
      clearStoredAuth();
      setUser(null);
      setToken(null);
      setMustCompleteOnboarding(false);
      setMustWaitForBootstrap(false);
      setBootstrapStoreId(null);
    }

    setIsInitialized(true);
  }, [refreshBootstrapStatus, refreshOnboardingStatus]);

  useEffect(() => {
    void checkAuth();
  }, [checkAuth]);

  const login = async (email: string, password: string, redirectTo = '/dashboard') => {
    setIsLoading(true);
    try {
      const response = await authAPI.login(email, password);
      setAuthTokens(response.access_token, response.refresh_token);
      setToken(COOKIE_AUTH_MARKER);

      const userData = await authAPI.getMe(response.access_token);
      setUser(userData);
      setStoredUser(userData);

      const isFirstLogin = await refreshOnboardingStatus(userData);
      const mustWaitForInitialSync = !isFirstLogin && await refreshBootstrapStatus(userData);

      toast.success('Вход выполнен успешно');
      if (isFirstLogin) {
        router.replace('/welcome');
      } else if (mustWaitForInitialSync) {
        router.replace('/onboarding-sync');
      } else {
        router.replace(redirectTo);
      }
      router.refresh();
    } catch (error: unknown) {
      const message =
        typeof error === 'object' &&
        error !== null &&
        'response' in error &&
        typeof error.response === 'object' &&
        error.response !== null &&
        'data' in error.response &&
        typeof error.response.data === 'object' &&
        error.response.data !== null &&
        'detail' in error.response.data &&
        typeof error.response.data.detail === 'string'
          ? error.response.data.detail
          : 'Ошибка входа';

      toast.error(message);
    } finally {
      setIsLoading(false);
    }
  };

  const logout = () => {
    void authAPI.logout().catch(() => undefined);
    clearStoredAuth();
    setUser(null);
    setToken(null);
    setMustCompleteOnboarding(false);
    setMustWaitForBootstrap(false);
    setBootstrapStoreId(null);
    toast.success('Выход выполнен');
    router.replace('/login');
    router.refresh();
  };

  return (
    <AuthContext.Provider value={{
      user,
      token,
      login,
      logout,
      isLoading,
      isInitialized,
      mustCompleteOnboarding,
      mustWaitForBootstrap,
      bootstrapStoreId,
      refreshOnboardingStatus,
      refreshBootstrapStatus,
    }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const context = useContext(AuthContext);
  if (context === undefined) {
    throw new Error('useAuth must be used within an AuthProvider');
  }
  return context;
}
