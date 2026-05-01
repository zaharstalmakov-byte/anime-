// ANIMEFLOW service worker — light caching for shell + posters
const CACHE = 'animeflow-v1';
const SHELL = [
  '/',
  '/static/css/custom.css',
  '/static/js/app.js',
  '/static/manifest.json',
  '/static/offline.html',
  '/static/img/favicon.svg',
  '/static/img/icon-192.svg',
];

self.addEventListener('install', (e) => {
  e.waitUntil(caches.open(CACHE).then((c) => c.addAll(SHELL)).catch(() => {}));
  self.skipWaiting();
});

self.addEventListener('activate', (e) => {
  e.waitUntil(
    caches.keys().then((keys) =>
      Promise.all(keys.filter((k) => k !== CACHE).map((k) => caches.delete(k)))
    )
  );
  self.clients.claim();
});

self.addEventListener('fetch', (e) => {
  const req = e.request;
  if (req.method !== 'GET') return;

  const url = new URL(req.url);

  // Never cache API or HLS — always go to network
  if (url.pathname.startsWith('/api/') || url.pathname.endsWith('.m3u8') || url.pathname.endsWith('.ts')) {
    return;
  }

  // Cache-first for static + images
  if (url.pathname.startsWith('/static/') || /\.(svg|png|jpg|jpeg|webp|css|js)$/.test(url.pathname)) {
    e.respondWith(
      caches.match(req).then((cached) => {
        if (cached) return cached;
        return fetch(req).then((res) => {
          if (res && res.status === 200) {
            const copy = res.clone();
            caches.open(CACHE).then((c) => c.put(req, copy)).catch(() => {});
          }
          return res;
        });
      })
    );
    return;
  }

  // Network-first for HTML pages, fallback to cache, then offline.html
  if (req.mode === 'navigate') {
    e.respondWith(
      fetch(req)
        .then((res) => {
          if (res && res.status === 200) {
            const copy = res.clone();
            caches.open(CACHE).then((c) => c.put(req, copy)).catch(() => {});
          }
          return res;
        })
        .catch(() =>
          caches.match(req).then((c) => c || caches.match('/static/offline.html') || caches.match('/'))
        )
    );
  }
});
