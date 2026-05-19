# Cinemateca — Setup en GitHub Pages

## Estructura del repositorio

```
cinemateca/
├── index.html              ← estructura HTML de la web
├── style.css               ← estilos
├── app.js                  ← lógica de la SPA
├── catalogo.json           ← los datos de tus películas (raíz, lo lee la web)
├── manifest.webmanifest    ← PWA manifest
├── service-worker.js       ← offline + cache
├── vendor/fuse.min.js      ← búsqueda fuzzy bundleada
├── icons/                  ← iconos PWA y favicons
├── escaneo.R               ← script de escaneo (corre local)
├── enriquecimiento.R       ← script de enriquecimiento con TMDb
├── datos/
│   ├── correcciones_manuales.csv
│   ├── inventario_crudo.csv
│   ├── catalogo_enriquecido.csv (gitignored, regenerable)
│   └── tmdb_cache.json (gitignored, regenerable)
├── tools/
│   ├── refetch_posters_en.mjs
│   └── validate_catalog.mjs
└── .github/workflows/
    ├── refetch-posters.yml
    └── validate.yml
```

## Pasos para subir

```bash
# 1. Clonar el repo
cd ~/Desktop  # o donde prefieras
git clone https://github.com/tomgc/cinemateca.git
cd cinemateca

# 2. Copiar los archivos
cp /ruta/a/index.html .
cp /ruta/a/datos/catalogo.json .
cp /ruta/a/escaneo.R .
cp /ruta/a/enriquecimiento.R .
mkdir -p datos
cp /ruta/a/datos/correcciones_manuales.csv datos/
cp /ruta/a/datos/inventario_crudo.csv datos/
cp /ruta/a/datos/catalogo_enriquecido.csv datos/

# 3. Push
git add .
git commit -m "Primera versión de Cinemateca"
git push origin main

# 4. Configurar alias en tu .zshrc (opcional, como tus otros repos)
# alias cc-cinemateca="cd ~/Desktop/cinemateca && code ."
```

## Activar GitHub Pages

1. Ir a https://github.com/tomgc/cinemateca/settings/pages
2. Source: **Deploy from a branch**
3. Branch: **main** / carpeta **/ (root)**
4. Save

En ~1 minuto estará disponible en:
**https://tomgc.github.io/cinemateca/**

## Flujo de actualización

Cuando escanees nuevos discos o hagas cambios en la web:

```bash
# Opción A: Re-escaneo completo (nuevos discos)
# 1. Correr escaneo.R → enriquecimiento.R en Positron
#    enriquecimiento.R escribe directo a catalogo.json en la raíz del repo
#    y preserva los campos editados desde web (rating_personal,
#    fecha_visionado, partner_wants, películas agregadas manualmente).
# 2. Commitear y pushear
cd ~/Desktop/cinemateca
git add catalogo.json
git commit -m "Actualizar catálogo"
git push

# Opción B: Cambios desde la web (estado, ocultar, agregar manual)
# Opción B.1: Sync directo (requiere PAT configurado en Settings)
#   - Hacer cambios → Exportar → Sincronizar a GitHub
#   - El commit aparece solo en main; Pages despliega en 1-2 min.
#
# Opción B.2: Descarga manual (fallback si no usas el sync)
# 1. Click "Exportar JSON" → "Descargar JSON"
# 2. Reemplazar el archivo en el repo
mv ~/Downloads/catalogo.json ~/Desktop/cinemateca/
cd ~/Desktop/cinemateca
git add catalogo.json
git commit -m "Actualizar cambios desde web"
git push
```

## Notas

- El `catalogo.json` debe estar en la **raíz** del repo, al lado de `index.html`.
- Los cambios desde la web se guardan en `localStorage` y se materializan
  con "Exportar JSON" (descarga manual) o "Sincronizar a GitHub" (sync directo).
- Si limpias el caché del navegador antes de exportar/sincronizar, los
  cambios no persistidos se pierden.
