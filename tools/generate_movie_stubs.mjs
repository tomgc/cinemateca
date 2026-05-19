#!/usr/bin/env node
// Genera un HTML stub por cada película del catálogo en movie/<tmdb_id>/index.html.
//
// Cada stub:
//  - tiene meta tags Open Graph (og:title, og:description, og:image) para
//    que WhatsApp/Twitter/etc. muestren preview rico al pegar el link
//  - redirige por JS al hash route equivalente (/cinemateca/#/movie/<id>)
//    para que humanos accedan a la app normal
//
// Se ejecuta desde CI cuando cambia catalogo.json. Limpia stubs huérfanos
// (películas borradas del catálogo).
//
// Uso: node tools/generate_movie_stubs.mjs

import { readFile, writeFile, mkdir, readdir, rm } from "node:fs/promises";
import { join } from "node:path";

const SITE = "https://tomgc.github.io/cinemateca";
const MOVIE_DIR = "movie";
const CATALOG = "catalogo.json";
const MAX_DESCRIPTION = 200;

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function pickTitle(m) {
  return m.titulo_en || m.titulo_es || m.titulo_original || m.titulo_archivo || `Movie ${m.tmdb_id}`;
}

function pickDescription(m, title, year) {
  const text = m.sinopsis_en || m.sinopsis || m.tagline_en || m.tagline;
  if (text) return text.length > MAX_DESCRIPTION ? text.substring(0, MAX_DESCRIPTION - 1) + "…" : text;
  return `${title}${year ? ` (${year})` : ""} en Cinemateca`;
}

function renderStub(m) {
  const id = m.tmdb_id;
  const title = pickTitle(m);
  const year = m.anio || "";
  const description = pickDescription(m, title, year);
  const poster = m.poster_path || "";
  const ogTitle = title + (year ? ` (${year})` : "");
  const posterMeta = poster ? `<meta property="og:image" content="${escapeHtml(poster)}">\n` : "";

  return `<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="UTF-8">
<title>${escapeHtml(title)} — Cinemateca</title>
<meta property="og:title" content="${escapeHtml(ogTitle)}">
<meta property="og:description" content="${escapeHtml(description)}">
${posterMeta}<meta property="og:type" content="video.movie">
<meta property="og:url" content="${SITE}/${MOVIE_DIR}/${id}/">
<meta name="twitter:card" content="summary_large_image">
<link rel="canonical" href="${SITE}/${MOVIE_DIR}/${id}/">
<script>location.replace('../../#/movie/${id}');</script>
</head>
<body style="background:#0a0a0c;color:#e8e6e1;font-family:sans-serif;padding:2rem">
<p>Cargando <a style="color:#c9a84c" href="../../#/movie/${id}">${escapeHtml(title)}</a>…</p>
</body>
</html>
`;
}

const catalog = JSON.parse(await readFile(CATALOG, "utf8"));
const validIds = new Set();
let generated = 0;

for (const m of catalog) {
  if (!m || !m.tmdb_id) continue;
  const id = String(m.tmdb_id);
  validIds.add(id);
  const dir = join(MOVIE_DIR, id);
  await mkdir(dir, { recursive: true });
  await writeFile(join(dir, "index.html"), renderStub(m));
  generated++;
}

// Limpiar stubs huérfanos (películas borradas del catálogo o cambiadas de id)
let orphans = 0;
try {
  const existing = await readdir(MOVIE_DIR);
  for (const d of existing) {
    if (!validIds.has(d)) {
      await rm(join(MOVIE_DIR, d), { recursive: true, force: true });
      orphans++;
    }
  }
} catch {
  // movie/ no existía aún
}

console.log(`stubs generados: ${generated}`);
console.log(`stubs huérfanos eliminados: ${orphans}`);
