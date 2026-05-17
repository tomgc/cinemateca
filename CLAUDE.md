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

## Estructura

```
index.html              # SPA de un solo archivo (HTML+CSS+JS inline)
catalogo.json           # datos del catálogo (raíz, lo lee la web)
escaneo.R               # escanea discos -> datos/inventario_crudo.csv
enriquecimiento.R       # enriquece con TMDb -> catalogo.json
datos/
  inventario_crudo.csv
  catalogo_enriquecido.csv
  correcciones_manuales.csv
  tmdb_cache.json
tools/
  refetch_posters_en.mjs  # script Node para re-bajar posters en inglés
.github/workflows/
  refetch-posters.yml     # workflow manual que corre el script anterior
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
