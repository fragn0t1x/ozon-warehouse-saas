import axios from 'axios';
import type { InternalAxiosRequestConfig } from 'axios';

import {
  clearStoredAuth,
} from '@/lib/utils/authStorage';

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || '/api';

export const apiClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 15000,
  withCredentials: true,
  headers: {
    'Content-Type': 'application/json',
  },
});

const refreshClient = axios.create({
  baseURL: API_BASE_URL,
  timeout: 15000,
  withCredentials: true,
  headers: {
    'Content-Type': 'application/json',
  },
});

apiClient.interceptors.response.use(
  (response) => response,
  async (error) => {
    const originalRequest = error.config as (InternalAxiosRequestConfig & { _retry?: boolean }) | undefined;
    const requestUrl = originalRequest?.url || '';
    const canRefresh =
      !requestUrl.includes('/auth/login') &&
      !requestUrl.includes('/auth/refresh');

    if (error.response?.status === 401 && originalRequest && !originalRequest._retry && canRefresh) {
      originalRequest._retry = true;

      try {
        await refreshClient.post('/auth/refresh', {});
        if (originalRequest.headers?.Authorization) {
          delete originalRequest.headers.Authorization;
        }

        return apiClient(originalRequest);
      } catch (refreshError) {
        clearStoredAuth();
        return Promise.reject(refreshError);
      }
    }

    if (error.response?.status === 401) {
      clearStoredAuth();
    }

    return Promise.reject(error);
  }
);

export default apiClient;

export function ensureArray<T>(value: unknown): T[] {
  return Array.isArray(value) ? (value as T[]) : [];
}

export function ensureObject<T extends object>(value: unknown, fallback: T): T {
  return value && typeof value === 'object' && !Array.isArray(value) ? ({ ...fallback, ...(value as Partial<T>) } as T) : fallback;
}
