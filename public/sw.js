'use strict';

self.addEventListener('install', (event) => {
  // Activar inmediatamente la nueva versión del SW
  event.waitUntil(self.skipWaiting());
});

self.addEventListener('activate', (event) => {
  event.waitUntil(self.clients.claim());
});

// Utilidad para esperar una ventana cliente y postear mensaje con los archivos
async function focusOrOpenAndPostMessage(message) {
  // Intentar encontrar una ventana abierta de la app
  let clientsList = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
  let client = clientsList[0];

  if (client) {
    try { await client.focus(); } catch {}
  } else {
    // Abrir una nueva ventana
    try { client = await self.clients.openWindow('/?share=1'); } catch {}
  }

  // Reintentar conseguir el cliente si aún no está listo
  const maxAttempts = 10;
  for (let i = 0; i < maxAttempts && !client; i++) {
    await new Promise(r => setTimeout(r, 150));
    clientsList = await self.clients.matchAll({ type: 'window', includeUncontrolled: true });
    client = clientsList[0];
  }

  if (client) {
    try { client.postMessage(message); } catch (e) { /* noop */ }
  }
}

self.addEventListener('fetch', (event) => {
  const url = new URL(event.request.url);

  // Share Target: el manifest apunta a /share-target con POST multipart/form-data
  if (url.pathname === '/share-target' && event.request.method === 'POST') {
    event.respondWith((async () => {
      try {
        const formData = await event.request.formData();
        // Puede venir uno o varios archivos con el nombre de campo "file"
        const files = formData.getAll('file').filter(Boolean);

        // Pasar los archivos al cliente (ventana)
        focusOrOpenAndPostMessage({ type: 'shared-files', files });
      } catch (e) {
        // noop
      }
      // Redirigir a la app principal
      return Response.redirect('/?share=1', 303);
    })());
    return;
  }

  // Fallback para GET a /share-target (por si el navegador navega directo)
  if (url.pathname === '/share-target' && event.request.method === 'GET') {
    event.respondWith(Response.redirect('/?share=1', 303));
    return;
  }
});
