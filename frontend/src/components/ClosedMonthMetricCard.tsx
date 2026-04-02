'use client';

import { Fragment } from 'react';
import { Popover, Transition } from '@headlessui/react';
import { InformationCircleIcon } from '@heroicons/react/24/outline';

type ClosedMonthMetricCardProps = {
  label: string;
  value: string;
  description: string;
  info?: string | null;
  infoAlign?: 'start' | 'end';
  tone?: 'default' | 'positive' | 'negative';
  size?: 'primary' | 'secondary';
};

function infoButtonClasses(size: 'primary' | 'secondary') {
  return size === 'primary'
    ? 'h-8 w-8 rounded-full border border-slate-200 text-slate-400 hover:border-sky-200 hover:text-sky-700'
    : 'h-7 w-7 rounded-full border border-slate-200 text-slate-400 hover:border-sky-200 hover:text-sky-700';
}

export function ClosedMonthMetricCard({
  label,
  value,
  description,
  info,
  infoAlign = 'end',
  tone = 'default',
  size = 'secondary',
}: ClosedMonthMetricCardProps) {
  const valueClassName =
    tone === 'positive'
      ? 'text-emerald-700'
      : tone === 'negative'
        ? 'text-rose-700'
        : 'text-slate-950';

  const shellClassName =
    size === 'primary'
      ? 'rounded-3xl border border-slate-200 bg-slate-50 p-4'
      : 'rounded-2xl border border-slate-200 bg-white px-4 py-3';

  return (
    <article className={shellClassName}>
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div
            className={
              size === 'primary'
                ? 'text-sm font-medium text-slate-500'
                : 'text-xs uppercase tracking-[0.14em] text-slate-400'
            }
          >
            {label}
          </div>
        </div>
        {info ? (
          <Popover className="relative z-[70] shrink-0">
            <Popover.Button
              className={`inline-flex items-center justify-center transition ${infoButtonClasses(size)}`}
              aria-label={`Подробнее: ${label}`}
            >
              <InformationCircleIcon className={size === 'primary' ? 'h-4.5 w-4.5' : 'h-4 w-4'} />
            </Popover.Button>
            <Transition
              as={Fragment}
              enter="transition duration-150 ease-out"
              enterFrom="translate-y-1 opacity-0"
              enterTo="translate-y-0 opacity-100"
              leave="transition duration-100 ease-in"
              leaveFrom="translate-y-0 opacity-100"
              leaveTo="translate-y-1 opacity-0"
            >
              <Popover.Panel
                className={`absolute top-10 z-[80] w-72 max-w-[calc(100vw-8rem)] rounded-2xl border border-slate-200 bg-white p-3 text-left text-xs leading-5 text-slate-600 shadow-[0_18px_40px_rgba(15,23,42,0.12)] ${
                  infoAlign === 'start' ? 'left-0' : 'right-0'
                }`}
              >
                <div className="font-medium text-slate-900">{label}</div>
                <div className="mt-1 whitespace-pre-line">{info}</div>
              </Popover.Panel>
            </Transition>
          </Popover>
        ) : null}
      </div>
      <div className={`mt-3 font-semibold tracking-tight ${valueClassName} ${size === 'primary' ? 'text-3xl' : 'text-lg'}`}>
        {value}
      </div>
      <div className={`text-slate-500 ${size === 'primary' ? 'mt-2 text-xs' : 'mt-1 text-xs'}`}>{description}</div>
    </article>
  );
}
