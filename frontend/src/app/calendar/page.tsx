import { redirect } from 'next/navigation';

type CalendarRedirectPageProps = {
  searchParams?: Record<string, string | string[] | undefined>;
};

export default function CalendarRedirectPage({ searchParams = {} }: CalendarRedirectPageProps) {
  const query = new URLSearchParams();

  Object.entries(searchParams).forEach(([key, value]) => {
    if (typeof value === 'string' && value.length > 0) {
      query.set(key, value);
      return;
    }

    if (Array.isArray(value)) {
      value.forEach((item) => {
        if (item) {
          query.append(key, item);
        }
      });
    }
  });

  const nextUrl = query.toString() ? `/supplies?${query.toString()}` : '/supplies';
  redirect(nextUrl);
}
