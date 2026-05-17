# Cinemateca

Catálogo personal de películas: app web estática servida desde GitHub Pages
que lee `catalogo.json` (generado offline por los scripts en R desde un
escaneo de discos + enriquecimiento con TMDb).

## Preferencias de comunicación

- **Idioma:** responde siempre en **español neutro**. No uses voseo ni
  modismos rioplatenses (nada de "vos", "querés", "dale", "tenés", "acá",
  "che", "laburar", "boludo"). Usa "tú" o construcciones impersonales,
  léxico panhispánico.
- Mantén las respuestas concisas y directas. Sin emojis salvo que se
  pidan explícitamente.

## Preferencias de flujo de trabajo

- **Commitea y pushea sin pedir confirmación** después de cada cambio
  significativo. Mensaje de commit descriptivo en español neutro.
- **Trabaja sobre `main` directamente** si el cambio es chico y el usuario
  está esperando ver el resultado en el sitio. Para refactors grandes o
  cambios riesgosos, usa una rama de feature.
- **No pidas confirmación** para: operaciones git (add/commit/push/pull/
  rebase/merge/checkout), crear directorios, mover/copiar archivos,
  correr scripts en `tools/`, ejecutar `python3 -c` o `node --check`.
- **Sí pide confirmación** para: `git push --force`, `git reset --hard`,
  borrar archivos del catálogo, ejecutar `escaneo.R`/`enriquecimiento.R`
  (modifican el catálogo en masa), rotar el token de TMDb, cualquier
  operación que afecte irreversiblemente datos en producción.
- Las permisos detallados están en `.claude/settings.json` (commiteado
  al repo, aplica a todas las sesiones).

## Estructura

```
index.html              # estructura HTML (estilos y JS en archivos aparte)
style.css               # estilos
app.js                  # toda la lógica de la SPA
vendor/
  fuse.min.js           # búsqueda fuzzy bundleada local
catalogo.json           # datos del catálogo (raíz, lo lee la web)
manifest.webmanifest    # PWA manifest
service-worker.js       # PWA service worker (precache + offline)
icons/                  # iconos PWA + favicons
escaneo.R               # escanea discos -> datos/inventario_crudo.csv
enriquecimiento.R       # enriquece con TMDb -> catalogo.json
datos/
  inventario_crudo.csv
  catalogo_enriquecido.csv
  correcciones_manuales.csv
  tmdb_cache.json       # ignorado en git, regenerable
tools/
  refetch_posters_en.mjs  # script Node para re-bajar posters en inglés
.github/workflows/
  refetch-posters.yml     # workflow manual que corre el script anterior
  validate.yml            # CI: valida JSON y sintaxis JS en cada push
```

## Despliegue

GitHub Pages sirve desde la rama **main**, carpeta raíz. Cualquier cambio
en otra rama (por ejemplo `claude/*`) **no aparece en el sitio publicado**
hasta que se mergea a `main`.

URL: https://tomgc.github.io/cinemateca/

## Flujo de trabajo

1. Para cambios en la UI o features de la web: editar `index.html`.
2. Para nuevos discos o re-escaneo: correr `escaneo.R` -> `enriquecimiento.R`
   localmente (no se ejecutan desde la web).
3. Los cambios de estado/agregar/ocultar hechos desde la web se guardan en
   `localStorage` y se exportan vía el botón "Exportar" (muestra un diff
   antes de descargar). El JSON descargado reemplaza al del repo.
4. Para refrescar todos los posters al inglés: GitHub Actions ->
   "Refetch English posters" -> Run workflow.

## Detalles técnicos relevantes

- El token de TMDb está embebido en `index.html`, `escaneo.R` y
  `enriquecimiento.R`. Es un read-only bearer y, al estar en HTML público,
  no es un secreto; pero conviene tenerlo presente al rotar.
- `localStorage` keys: `cin_ov` (overrides de estado), `cin_ad` (agregadas
  desde la web), `cin_hi` (ocultas), `cin_partner` (watchlist de pareja).
- Los posters/backdrops se prefieren en inglés. `addId()` y
  `enriquecimiento.R` usan `dEn.poster_path` con fallback a `dEs`.
- Idioma por defecto de la UI: inglés. El toggle ES/EN cambia los textos
  de las películas (título, sinopsis, tagline).
