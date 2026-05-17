// Service worker para Cinemateca.
// Estrategia:
//   - Precache: index.html + assets locales (vendor, icons, manifest).
//   - catalogo.json: stale-while-revalidate (sirve cache rapido, refresca en background).
//   - Posters TMDb (image.tmdb.org): cache-first (URLs inmutables).
//   - API TMDb (api.themoviedb.org): network-only, falla offline sin interferencia.
//   - Same-origin GET no precacheado: cache-first.
// Para invalidar todos los caches: bumpear VERSION.

const VERSION = "v3";
const STATIC_CACHE = `cinemateca-static-${VERSION}`;
const POSTER_CACHE = `cinemateca-posters-${VERSION}`;
const CATALOG_CACHE = `cinemateca-catalog-${VERSION}`;

const PRECACHE = [
  "./",
  "./index.html",
  "./style.css",
  "./app.js",
  "./vendor/fuse.min.js",
  "./manifest.webmanifest",
  "./icons/icon-192.png",
  "./icons/icon-512.png",
  "./icons/icon-512-maskable.png",
  "./icons/apple-touch-icon.png",
  "./icons/favicon.svg",
  "./icons/favicon-32.png",
];

self.addEventListener("install", (e) => {
  e.waitUntil(
    caches
      .open(STATIC_CACHE)
      .then((c) => c.addAll(PRECACHE))
      .then(() => self.skipWaiting()),
  );
});

self.addEventListener("activate", (e) => {
  const valid = [STATIC_CACHE, POSTER_CACHE, CATALOG_CACHE];
  e.waitUntil(
    caches
      .keys()
      .then((keys) =>
        Promise.all(
          keys
            .filter((k) => k.startsWith("cinemateca-") && !valid.includes(k))
            .map((k) => caches.delete(k)),
        ),
      )
      .then(() => self.clients.claim()),
  );
});

self.addEventListener("fetch", (e) => {
  const req = e.request;
  if (req.method !== "GET") return;
  const url = new URL(req.url);

  if (url.pathname.endsWith("/catalogo.json")) {
    e.respondWith(staleWhileRevalidate(req, CATALOG_CACHE));
    return;
  }
  if (url.hostname === "image.tmdb.org") {
    e.respondWith(cacheFirst(req, POSTER_CACHE));
    return;
  }
  if (url.hostname === "api.themoviedb.org") return; // network-only
  if (url.hostname === "api.github.com") return; // network-only
  if (url.origin === self.location.origin) {
    e.respondWith(cacheFirst(req, STATIC_CACHE));
  }
});

async function cacheFirst(req, cacheName) {
  const cache = await caches.open(cacheName);
  const hit = await cache.match(req);
  if (hit) return hit;
  try {
    const res = await fetch(req);
    if (res.ok) cache.put(req, res.clone());
    return res;
  } catch {
    return new Response("Offline", { status: 503, statusText: "Offline" });
  }
}

async function staleWhileRevalidate(req, cacheName) {
  const cache = await caches.open(cacheName);
  const cachedPromise = cache.match(req);
  const networkPromise = fetch(req)
    .then((res) => {
      if (res.ok) cache.put(req, res.clone());
      return res;
    })
    .catch(() => null);
  const cached = await cachedPromise;
  return cached || (await networkPromise) || new Response("Offline", { status: 503 });
}
