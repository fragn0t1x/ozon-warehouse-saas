self.addEventListener('push', (event) => {
  const payload = (() => {
    try {
      return event.data ? event.data.json() : {};
    } catch {
      return {};
    }
  })();

  const title = payload.title || 'Уведомление';
  const options = {
    body: payload.body || '',
    data: {
      url: payload.url || '/notifications',
    },
    tag: payload.tag || 'general',
  };

  event.waitUntil(self.registration.showNotification(title, options));
});

self.addEventListener('notificationclick', (event) => {
  event.notification.close();
  const targetUrl = event.notification?.data?.url || '/notifications';

  event.waitUntil((async () => {
    const windowClients = await clients.matchAll({
      type: 'window',
      includeUncontrolled: true,
    });

    for (const client of windowClients) {
      if ('focus' in client) {
        const currentUrl = new URL(client.url);
        if (currentUrl.pathname === targetUrl || client.url.endsWith(targetUrl)) {
          await client.focus();
          return;
        }
      }
    }

    if (clients.openWindow) {
      await clients.openWindow(targetUrl);
    }
  })());
});
