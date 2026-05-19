#!/usr/bin/env node
// Valida que catalogo.json cumple el schema mínimo esperado.
// Falla con exit 1 si hay errores estructurales; las advertencias se
// reportan pero no bloquean el CI.
//
// Uso: node tools/validate_catalog.mjs [path]

import { readFile } from "node:fs/promises";

const path = process.argv[2] || "catalogo.json";
const VALID_ESTADO = new Set(["pendiente", "coleccion", "descartada"]);
// 'Película' (con tilde) viene del flujo R, 'Pelicula' (sin tilde) de entradas
// históricas agregadas vía web antes de alinear la nomenclatura.
const VALID_TIPO = new Set(["Película", "Pelicula", "Documental"]);
const DATE_RE = /^\d{4}-\d{2}-\d{2}$/;
const TITLE_FIELDS = ["titulo_es", "titulo_en", "titulo_original", "titulo_archivo"];

let catalog;
try {
  catalog = JSON.parse(await readFile(path, "utf8"));
} catch (e) {
  console.error(`FATAL: no se pudo parsear ${path}: ${e.message}`);
  process.exit(1);
}

if (!Array.isArray(catalog)) {
  console.error("FATAL: el catálogo no es un array");
  process.exit(1);
}

const issues = [];

catalog.forEach((entry, i) => {
  const label = () => {
    const t = TITLE_FIELDS.map((f) => entry?.[f]).find(Boolean);
    return t ? `"${t}" (#${i})` : `entry #${i}`;
  };
  const err = (msg) => issues.push({ sev: "error", label: label(), msg });
  const warn = (msg) => issues.push({ sev: "warning", label: label(), msg });

  if (!entry || typeof entry !== "object" || Array.isArray(entry)) {
    err("no es un objeto");
    return;
  }
  if (!TITLE_FIELDS.some((f) => entry[f])) err("sin ningún título");

  if (entry.tmdb_id != null && typeof entry.tmdb_id !== "number") {
    err(`tmdb_id debe ser número, es ${typeof entry.tmdb_id}`);
  }

  if (entry.rating_personal != null) {
    const r = entry.rating_personal;
    if (!Number.isInteger(r) || r < 1 || r > 5) {
      warn(`rating_personal fuera de 1-5: ${r}`);
    }
  }

  if (entry.fecha_visionado != null && !DATE_RE.test(entry.fecha_visionado)) {
    warn(`fecha_visionado no es YYYY-MM-DD: ${entry.fecha_visionado}`);
  }

  if (entry.estado != null && !VALID_ESTADO.has(entry.estado)) {
    warn(`estado desconocido: ${entry.estado}`);
  }
  if (entry.tipo != null && !VALID_TIPO.has(entry.tipo)) {
    warn(`tipo desconocido: ${entry.tipo}`);
  }
  if (entry.partner_wants != null && entry.partner_wants !== true) {
    warn(`partner_wants debería ser true o ausente: ${entry.partner_wants}`);
  }
  if (entry.anio != null && (!Number.isInteger(entry.anio) || entry.anio < 1880 || entry.anio > 2100)) {
    warn(`anio sospechoso: ${entry.anio}`);
  }
});

const errors = issues.filter((x) => x.sev === "error");
const warnings = issues.filter((x) => x.sev === "warning");

issues.slice(0, 30).forEach(({ sev, label, msg }) => {
  console[sev === "error" ? "error" : "warn"](`  ${sev}: ${label}: ${msg}`);
});
if (issues.length > 30) console.log(`  ... y ${issues.length - 30} más`);

console.log(
  `\n${catalog.length} entradas. ${errors.length} errores, ${warnings.length} advertencias.`,
);

if (errors.length > 0) process.exit(1);
