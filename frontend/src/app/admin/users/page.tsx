'use client';

import { useCallback, useEffect, useMemo, useState } from 'react';
import toast from 'react-hot-toast';

import { ProtectedRoute } from '@/components/Auth/ProtectedRoute';
import { Layout } from '@/components/Layout';
import { adminAPI } from '@/lib/api/admin';
import type { User } from '@/lib/api/auth';
import { useAuth } from '@/lib/context/AuthContext';

export default function AdminUsersPage() {
  const { user } = useAuth();
  const [users, setUsers] = useState<User[]>([]);
  const [loading, setLoading] = useState(true);
  const [submitting, setSubmitting] = useState(false);
  const [generatedPassword, setGeneratedPassword] = useState<string | null>(null);
  const [form, setForm] = useState({
    email: '',
    password: '',
    is_admin: false,
  });

  const loadUsers = useCallback(async () => {
    setLoading(true);
    try {
      const data = await adminAPI.getUsers();
      setUsers(data);
    } catch {
      toast.error('Не удалось загрузить пользователей');
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    if (user?.is_admin) {
      void loadUsers();
    }
  }, [loadUsers, user?.is_admin]);

  const totals = useMemo(() => ({
    total: users.length,
    active: users.filter((item) => item.is_active).length,
    admins: users.filter((item) => item.is_admin).length,
  }), [users]);

  const createUser = async (event: React.FormEvent) => {
    event.preventDefault();
    setSubmitting(true);
    setGeneratedPassword(null);

    try {
      const response = await adminAPI.createUser({
        email: form.email,
        password: form.password.trim() || undefined,
        is_admin: form.is_admin,
      });
      setGeneratedPassword(response.generated_password || null);
      setForm({ email: '', password: '', is_admin: false });
      toast.success('Пользователь создан');
      await loadUsers();
    } catch {
      toast.error('Не удалось создать пользователя');
    } finally {
      setSubmitting(false);
    }
  };

  const toggleUser = async (targetUser: User) => {
    try {
      await adminAPI.toggleUserActive(targetUser.id);
      toast.success(targetUser.is_active ? 'Пользователь деактивирован' : 'Пользователь активирован');
      await loadUsers();
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
          : 'Не удалось изменить статус пользователя';

      toast.error(message);
    }
  };

  const deleteUser = async (targetUser: User) => {
    const confirmed = window.confirm(
      `Удалить пользователя "${targetUser.email}" и все связанные данные кабинета?\n\nЭто удалит магазины, складские данные, уведомления и историю без возможности восстановления.`
    );

    if (!confirmed) {
      return;
    }

    try {
      const result = await adminAPI.deleteUser(targetUser.id);
      const deletedCount = Number(result.deleted_count || 0);
      toast.success(
        deletedCount > 1
          ? `Удален кабинет (${deletedCount} пользователей)`
          : 'Пользователь удален'
      );
      await loadUsers();
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
          : 'Не удалось удалить пользователя';

      toast.error(message);
    }
  };

  const resetUser = async (targetUser: User) => {
    const confirmed = window.confirm(
      `Сбросить кабинет пользователя "${targetUser.email}" до состояния первого входа?\n\nПользователь останется, но магазины, складские данные, уведомления и настройки кабинета будут очищены.`
    );

    if (!confirmed) {
      return;
    }

    try {
      const result = await adminAPI.resetUser(targetUser.id);
      toast.success(
        `Кабинет сброшен: магазинов удалено ${result.deleted_stores}`
      );
      await loadUsers();
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
          : 'Не удалось сбросить кабинет пользователя';

      toast.error(message);
    }
  };

  if (!user?.is_admin) {
    return (
      <ProtectedRoute>
        <Layout>
          <div className="rounded-lg bg-white p-8 text-center text-gray-600 shadow">
            Эта страница доступна только администратору.
          </div>
        </Layout>
      </ProtectedRoute>
    );
  }

  return (
    <ProtectedRoute>
      <Layout>
        <div className="mb-8 flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-semibold text-gray-900">Пользователи</h1>
            <p className="mt-1 text-sm text-gray-500">
              Создание пользователей и управление доступом
            </p>
          </div>
        </div>

        <div className="mb-8 grid grid-cols-1 gap-4 md:grid-cols-3">
          <div className="rounded-lg bg-white p-5 shadow">
            <div className="text-sm text-gray-500">Всего пользователей</div>
            <div className="mt-2 text-3xl font-semibold text-gray-900">{totals.total}</div>
          </div>
          <div className="rounded-lg bg-white p-5 shadow">
            <div className="text-sm text-gray-500">Активных</div>
            <div className="mt-2 text-3xl font-semibold text-emerald-600">{totals.active}</div>
          </div>
          <div className="rounded-lg bg-white p-5 shadow">
            <div className="text-sm text-gray-500">Администраторов</div>
            <div className="mt-2 text-3xl font-semibold text-sky-600">{totals.admins}</div>
          </div>
        </div>

        <div className="mb-8 rounded-lg bg-white p-6 shadow">
          <h2 className="mb-4 text-lg font-medium text-gray-900">Добавить пользователя</h2>
          <form onSubmit={createUser} className="grid gap-4 md:grid-cols-2">
            <div>
              <label className="mb-1 block text-sm font-medium text-gray-700">Email</label>
              <input
                type="email"
                value={form.email}
                onChange={(event) => setForm((prev) => ({ ...prev, email: event.target.value }))}
                required
                className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900"
              />
            </div>
            <div>
              <label className="mb-1 block text-sm font-medium text-gray-700">Пароль</label>
              <input
                type="text"
                value={form.password}
                onChange={(event) => setForm((prev) => ({ ...prev, password: event.target.value }))}
                placeholder="Оставьте пустым для генерации"
                className="block w-full rounded-md border border-gray-300 px-3 py-2 text-sm text-gray-900"
              />
            </div>
            <label className="flex items-center gap-3 text-sm text-gray-700 md:col-span-2">
              <input
                type="checkbox"
                checked={form.is_admin}
                onChange={(event) => setForm((prev) => ({ ...prev, is_admin: event.target.checked }))}
                className="h-4 w-4 rounded border-gray-300 text-primary-600"
              />
              Выдать права администратора
            </label>
            <div className="md:col-span-2 flex items-center gap-3">
              <button
                type="submit"
                disabled={submitting}
                className="rounded-md bg-primary-600 px-4 py-2 text-sm font-medium text-white hover:bg-primary-700 disabled:opacity-50"
              >
                {submitting ? 'Создание...' : 'Создать пользователя'}
              </button>
              {generatedPassword && (
                <div className="rounded-md bg-amber-50 px-4 py-2 text-sm text-amber-800">
                  Сгенерированный пароль: <span className="font-mono">{generatedPassword}</span>
                </div>
              )}
            </div>
          </form>
        </div>

        <div className="overflow-hidden rounded-lg bg-white shadow">
          <div className="border-b px-6 py-4">
            <h2 className="text-lg font-medium text-gray-900">Пользователи</h2>
          </div>
          {loading ? (
            <div className="p-8 text-center text-gray-500">Загрузка...</div>
          ) : (
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Email</th>
                    <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Роль</th>
                    <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Статус</th>
                    <th className="px-6 py-3 text-left text-xs font-medium uppercase tracking-wider text-gray-500">Создан</th>
                    <th className="px-6 py-3 text-right text-xs font-medium uppercase tracking-wider text-gray-500">Действия</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200 bg-white">
                  {users.map((item) => (
                    <tr key={item.id}>
                      <td className="px-6 py-4 text-sm font-medium text-gray-900">{item.email}</td>
                      <td className="px-6 py-4 text-sm text-gray-600">{item.is_admin ? 'Администратор' : 'Пользователь'}</td>
                      <td className="px-6 py-4">
                        <span className={`inline-flex rounded-full px-2.5 py-0.5 text-xs font-medium ${
                          item.is_active ? 'bg-emerald-100 text-emerald-700' : 'bg-slate-100 text-slate-700'
                        }`}>
                          {item.is_active ? 'Активен' : 'Отключен'}
                        </span>
                      </td>
                      <td className="px-6 py-4 text-sm text-gray-600">
                        {item.created_at ? new Date(item.created_at).toLocaleString('ru-RU') : '—'}
                      </td>
                      <td className="px-6 py-4 text-right">
                        <div className="flex justify-end gap-2">
                          <button
                            type="button"
                            onClick={() => toggleUser(item)}
                            className="rounded-md border border-gray-300 px-3 py-1.5 text-sm text-gray-700 hover:bg-gray-50"
                          >
                            {item.is_active ? 'Деактивировать' : 'Активировать'}
                          </button>
                          <button
                            type="button"
                            onClick={() => resetUser(item)}
                            className="rounded-md border border-amber-200 px-3 py-1.5 text-sm text-amber-700 hover:bg-amber-50"
                          >
                            Сбросить
                          </button>
                          <button
                            type="button"
                            onClick={() => deleteUser(item)}
                            className="rounded-md border border-rose-200 px-3 py-1.5 text-sm text-rose-700 hover:bg-rose-50"
                          >
                            Удалить
                          </button>
                        </div>
                      </td>
                    </tr>
                  ))}
                  {users.length === 0 && (
                    <tr>
                      <td colSpan={5} className="px-6 py-8 text-center text-gray-500">
                        Пользователей пока нет
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </Layout>
    </ProtectedRoute>
  );
}
