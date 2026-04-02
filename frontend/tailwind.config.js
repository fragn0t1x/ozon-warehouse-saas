/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    './src/pages/**/*.{js,ts,jsx,tsx,mdx}',
    './src/components/**/*.{js,ts,jsx,tsx,mdx}',
    './src/app/**/*.{js,ts,jsx,tsx,mdx}',
    './src/lib/**/*.{js,ts,jsx,tsx,mdx}',
  ],
  safelist: [
    'bg-amber-100', 'text-amber-800',
    'bg-sky-100', 'text-sky-800',
    'bg-indigo-100', 'text-indigo-800',
    'bg-cyan-100', 'text-cyan-800',
    'bg-fuchsia-100', 'text-fuchsia-800',
    'bg-rose-100', 'text-rose-800',
    'bg-emerald-100', 'text-emerald-800',
    'bg-slate-100', 'text-slate-700',
    'bg-orange-100', 'text-orange-800',
  ],
  theme: {
    extend: {
      colors: {
        primary: {
          50: '#eff6ff',
          100: '#dbeafe',
          200: '#bfdbfe',
          300: '#93c5fd',
          400: '#60a5fa',
          500: '#3b82f6',
          600: '#2563eb',
          700: '#1d4ed8',
          800: '#1e40af',
          900: '#1e3a8a',
        },
      },
    },
  },
  plugins: [],
}
