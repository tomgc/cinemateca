#!/usr/bin/env node
// Re-fetches English-language poster_path and backdrop_path from TMDb
// for every movie in catalogo.json. Falls back to the existing value
// when TMDb returns no English asset.
//
// Usage:
//   TMDB_TOKEN=<bearer> node tools/refetch_posters_en.mjs [--dry-run]
//
// If TMDB_TOKEN is not set, falls back to the public read-only token
// already embedded in index.html.

import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

const __dirname = dirname(fileURLToPath(import.meta.url));
const CATALOG = join(__dirname, "..", "catalogo.json");

const TOKEN =
  process.env.TMDB_TOKEN ||
  "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIyOGU5ZmQ0ZmY1Yjg1ZmEwZTUyMGQ0N2YyOGYwZTNlNSIsIm5iZiI6MTc3NzU5MzA2Ni45MjcsInN1YiI6IjY5ZjNlYWVhMmQ0OGU2MDk0NWY5Zjk3ZCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.0M5J3xxbcGLOy2q2rcv5ibSy0WctOLhAp5kuMdeedmM";

const POSTER_BASE = "https://image.tmdb.org/t/p/w500";
const BACKDROP_BASE = "https://image.tmdb.org/t/p/w1280";
const CONCURRENCY = 8;
const DRY_RUN = process.argv.includes("--dry-run");

async function fetchEn(id, attempt = 1) {
  const res = await fetch(
    `https://api.themoviedb.org/3/movie/${id}?language=en-US`,
    { headers: { Authorization: `Bearer ${TOKEN}`, accept: "application/json" } },
  );
  if (res.status === 429 && attempt <= 5) {
    const wait = Number(res.headers.get("retry-after") || 1) * 1000;
    await new Promise((r) => setTimeout(r, wait));
    return fetchEn(id, attempt + 1);
  }
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

const catalog = JSON.parse(await readFile(CATALOG, "utf8"));
const queue = catalog
  .map((m, idx) => ({ m, idx }))
  .filter(({ m }) => m.tmdb_id);

console.log(`Catalog: ${catalog.length} entries, ${queue.length} with tmdb_id`);

let done = 0;
let posterUpdated = 0;
let backdropUpdated = 0;
let failed = 0;

async function worker() {
  while (queue.length) {
    const { m } = queue.shift();
    try {
      const d = await fetchEn(m.tmdb_id);
      const newPoster = d.poster_path ? POSTER_BASE + d.poster_path : null;
      const newBackdrop = d.backdrop_path ? BACKDROP_BASE + d.backdrop_path : null;
      if (newPoster && newPoster !== m.poster_path) {
        m.poster_path = newPoster;
        posterUpdated++;
      }
      if (newBackdrop && newBackdrop !== m.backdrop_path) {
        m.backdrop_path = newBackdrop;
        backdropUpdated++;
      }
    } catch (e) {
      failed++;
      console.error(`  ✗ ${m.tmdb_id} (${m.titulo_archivo || m.titulo_es}): ${e.message}`);
    }
    done++;
    if (done % 50 === 0) {
      process.stdout.write(
        `\r  ${done}/${queue.length + done} processed, ${posterUpdated} posters, ${backdropUpdated} backdrops, ${failed} failed`,
      );
    }
  }
}

const startedAt = Date.now();
await Promise.all(Array.from({ length: CONCURRENCY }, worker));
const elapsed = ((Date.now() - startedAt) / 1000).toFixed(1);

console.log(
  `\nDone in ${elapsed}s — ${posterUpdated} posters updated, ${backdropUpdated} backdrops updated, ${failed} failed`,
);

if (DRY_RUN) {
  console.log("Dry run — catalog not written.");
} else if (posterUpdated || backdropUpdated) {
  await writeFile(CATALOG, JSON.stringify(catalog, null, 2) + "\n");
  console.log(`Wrote ${CATALOG}`);
} else {
  console.log("No changes to write.");
}
