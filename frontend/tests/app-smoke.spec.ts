import { test, expect } from '@playwright/test';

const email = process.env.SMOKE_USER_EMAIL || '';
const password = process.env.SMOKE_USER_PASSWORD || '';

test.describe('app smoke', () => {
  test.skip(!email || !password, 'SMOKE_USER_EMAIL and SMOKE_USER_PASSWORD are required');

  test('login and navigate core pages', async ({ page }) => {
    await page.goto('/login');
    await page.getByLabel('Email').fill(email);
    await page.getByLabel('Пароль').fill(password);
    await page.getByRole('button', { name: /войти в систему/i }).click();

    await expect(page).toHaveURL(/dashboard|welcome|onboarding-sync/);

    if (/welcome|onboarding-sync/.test(page.url())) {
      test.skip(true, 'Smoke user is in onboarding flow');
    }

    await page.getByRole('link', { name: 'Магазин сегодня' }).click();
    await expect(page).toHaveURL(/dashboard/);

    await page.getByRole('link', { name: 'Обновление данных' }).click();
    await expect(page).toHaveURL(/syncs/);

    await page.getByRole('link', { name: 'Магазины' }).click();
    await expect(page).toHaveURL(/stores/);
  });
});
