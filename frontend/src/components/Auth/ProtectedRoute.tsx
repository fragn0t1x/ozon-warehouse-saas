// frontend/src/components/Auth/ProtectedRoute.tsx
'use client';

import { useEffect } from 'react';
import { usePathname, useRouter } from 'next/navigation';

import { useAuth } from '@/lib/context/AuthContext';

export function ProtectedRoute({ children }: { children: React.ReactNode }) {
  const { user, isInitialized, mustCompleteOnboarding, mustWaitForBootstrap, bootstrapStoreId } = useAuth();
  const router = useRouter();
  const pathname = usePathname();
  const safePathname = pathname ?? '/';
  const onboardingSyncHref = bootstrapStoreId ? `/onboarding-sync?store=${bootstrapStoreId}` : '/onboarding-sync';

  useEffect(() => {
    if (isInitialized && !user) {
      router.replace(`/login?next=${encodeURIComponent(safePathname)}`);
      return;
    }

    const shouldForceOnboarding = Boolean(user && !user.is_admin && mustCompleteOnboarding);

    if (isInitialized && shouldForceOnboarding && safePathname !== '/welcome') {
      router.replace('/welcome');
      return;
    }

    const shouldWaitForBootstrap = Boolean(user && !user.is_admin && !mustCompleteOnboarding && mustWaitForBootstrap);
    if (isInitialized && shouldWaitForBootstrap && safePathname !== '/onboarding-sync') {
      router.replace(onboardingSyncHref);
    }
  }, [bootstrapStoreId, isInitialized, mustCompleteOnboarding, mustWaitForBootstrap, onboardingSyncHref, router, safePathname, user]);

  if (!isInitialized) {
    return (
      <div className="min-h-screen flex items-center justify-center">
        <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-primary-600"></div>
      </div>
    );
  }

  const shouldForceOnboarding = Boolean(user && !user.is_admin && mustCompleteOnboarding);
  const shouldWaitForBootstrap = Boolean(user && !user.is_admin && !mustCompleteOnboarding && mustWaitForBootstrap);

  if (
    !user ||
    (shouldForceOnboarding && safePathname !== '/welcome') ||
    (shouldWaitForBootstrap && safePathname !== '/onboarding-sync')
  ) {
    return null;
  }

  return <>{children}</>;
}
