import Link from 'next/link';

export default function NotFound() {
  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50">
      <div className="max-w-md w-full space-y-8 text-center p-6 bg-white shadow-lg rounded-lg">
        <div>
          <h2 className="text-4xl font-extrabold text-red-600 mb-4">404</h2>
          <h3 className="text-2xl font-bold text-gray-900">Страница не найдена</h3>
          <p className="mt-2 text-sm text-gray-600">
            Извините, мы не смогли найти страницу, которую вы ищете. Возможно, она была удалена или перемещена.
          </p>
        </div>
        <div className="flex justify-center gap-4">
          <Link
            href="/dashboard"
            className="inline-flex items-center px-6 py-3 border border-transparent
                       text-base font-medium rounded-md text-white bg-blue-600
                       hover:bg-blue-700 transition-colors"
          >
            Вернуться на дашборд
          </Link>
          <Link
            href="/"
            className="inline-flex items-center px-6 py-3 border border-gray-300
                       text-base font-medium rounded-md text-gray-700 bg-white
                       hover:bg-gray-50 transition-colors"
          >
            На главную
          </Link>
        </div>
      </div>
    </div>
  );
}