'use client';

import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState } from 'react';

import { storesAPI, type Store } from '@/lib/api/stores';
import { useAuth } from '@/lib/context/AuthContext';

const STORAGE_KEY = 'ozon-active-store-id';

interface StoreContextType {
  stores: Store[];
  selectedStoreId: number | null;
  selectedStore: Store | null;
  isLoading: boolean;
  setSelectedStoreId: (storeId: number | null) => void;
  refreshStores: (preferredStoreId?: number | null) => Promise<Store[]>;
}

const StoreContext = createContext<StoreContextType | undefined>(undefined);

function readStoredStoreId(): number | null {
  if (typeof window === 'undefined') {
    return null;
  }

  const value = window.localStorage.getItem(STORAGE_KEY);
  if (!value) {
    return null;
  }

  const parsed = Number(value);
  return Number.isFinite(parsed) ? parsed : null;
}

export function StoreProvider({ children }: { children: React.ReactNode }) {
  const { isInitialized, user } = useAuth();
  const [stores, setStores] = useState<Store[]>([]);
  const [selectedStoreId, setSelectedStoreIdState] = useState<number | null>(null);
  const [isLoading, setIsLoading] = useState(true);
  const selectedStoreIdRef = useRef<number | null>(null);
  const refreshSequenceRef = useRef(0);

  const syncSelection = useCallback((availableStores: Store[], preferredId?: number | null) => {
    const storedId = preferredId ?? selectedStoreIdRef.current ?? readStoredStoreId();
    const exists = availableStores.find((store) => store.id === storedId);
    const nextId = exists?.id ?? availableStores[0]?.id ?? null;

    selectedStoreIdRef.current = nextId;
    setSelectedStoreIdState(nextId);

    if (typeof window !== 'undefined') {
      if (nextId === null) {
        window.localStorage.removeItem(STORAGE_KEY);
      } else {
        window.localStorage.setItem(STORAGE_KEY, String(nextId));
      }
    }
  }, []);

  const refreshStores = useCallback(async (preferredStoreId?: number | null) => {
    const sequence = ++refreshSequenceRef.current;

    if (!user) {
      setStores([]);
      selectedStoreIdRef.current = null;
      setSelectedStoreIdState(null);
      setIsLoading(false);
      return [];
    }

    setIsLoading(true);

    try {
      const data = await storesAPI.getAll();
      if (sequence !== refreshSequenceRef.current) {
        return data;
      }
      setStores(data);
      syncSelection(data, preferredStoreId ?? selectedStoreIdRef.current);
      return data;
    } catch {
      if (sequence !== refreshSequenceRef.current) {
        return [];
      }
      setStores([]);
      syncSelection([], null);
      return [];
    } finally {
      if (sequence === refreshSequenceRef.current) {
        setIsLoading(false);
      }
    }
  }, [syncSelection, user]);

  useEffect(() => {
    if (!isInitialized) {
      return;
    }

    if (!user) {
      setStores([]);
      selectedStoreIdRef.current = null;
      setSelectedStoreIdState(null);
      setIsLoading(false);
      return;
    }

    void refreshStores();
  }, [isInitialized, refreshStores, user]);

  const setSelectedStoreId = useCallback((storeId: number | null) => {
    selectedStoreIdRef.current = storeId;
    setSelectedStoreIdState(storeId);

    if (typeof window !== 'undefined') {
      if (storeId === null) {
        window.localStorage.removeItem(STORAGE_KEY);
      } else {
        window.localStorage.setItem(STORAGE_KEY, String(storeId));
      }
    }
  }, []);

  const selectedStore = useMemo(
    () => stores.find((store) => store.id === selectedStoreId) ?? null,
    [selectedStoreId, stores]
  );

  return (
    <StoreContext.Provider value={{
      stores,
      selectedStoreId,
      selectedStore,
      isLoading,
      setSelectedStoreId,
      refreshStores,
    }}>
      {children}
    </StoreContext.Provider>
  );
}

export function useStoreContext() {
  const context = useContext(StoreContext);
  if (!context) {
    throw new Error('useStoreContext must be used within a StoreProvider');
  }
  return context;
}
