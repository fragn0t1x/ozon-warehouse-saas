'use client';

import { useState } from 'react';
import { Header } from './Header';
import { Sidebar } from './Sidebar';

const SIDEBAR_COLLAPSED_STORAGE_KEY = 'layout.sidebar.collapsed';

export function Layout({ children }: { children: React.ReactNode }) {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(() => {
    if (typeof window === 'undefined') {
      return false;
    }
    try {
      return window.localStorage.getItem(SIDEBAR_COLLAPSED_STORAGE_KEY) === '1';
    } catch {
      return false;
    }
  });

  const handleToggleCollapsed = () => {
    setSidebarCollapsed((current) => {
      const next = !current;
      try {
        window.localStorage.setItem(SIDEBAR_COLLAPSED_STORAGE_KEY, next ? '1' : '0');
      } catch {}
      return next;
    });
  };

  return (
    <div className="min-h-screen bg-transparent">
      <div
        className={`fixed inset-0 z-40 bg-slate-950/45 backdrop-blur-sm transition md:hidden ${
          sidebarOpen ? 'pointer-events-auto opacity-100' : 'pointer-events-none opacity-0'
        }`}
        onClick={() => setSidebarOpen(false)}
      />

      <aside
        className={`fixed inset-y-0 left-0 z-50 w-80 max-w-[85vw] ${
          sidebarCollapsed ? 'md:w-24' : 'md:w-72'
        } ${
          sidebarOpen ? 'translate-x-0' : '-translate-x-full md:translate-x-0'
        }`}
      >
        <Sidebar
          collapsed={sidebarCollapsed}
          onNavigate={() => setSidebarOpen(false)}
          onToggleCollapsed={handleToggleCollapsed}
        />
      </aside>

      <div className={`min-h-screen ${sidebarCollapsed ? 'md:pl-24' : 'md:pl-72'}`}>
        <Header setSidebarOpen={setSidebarOpen} />
        <main className="px-4 pb-8 pt-3 sm:px-6 lg:px-8">
          <div className="mx-auto max-w-7xl">{children}</div>
        </main>
      </div>
    </div>
  );
}
