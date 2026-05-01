# Cinemateca — Setup en GitHub Pages

## Estructura del repositorio

```
cinemateca/
├── index.html          ← la plataforma web
├── catalogo.json       ← los datos de tus películas
├── escaneo.R           ← script de escaneo (no lo usa la web, pero lo guardas en el repo)
├── enriquecimiento.R   ← script de enriquecimiento
├── datos/
│   ├── correcciones_manuales.csv
│   ├── inventario_crudo.csv
│   └── catalogo_enriquecido.csv
└── README.md
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
# 2. Copiar el nuevo catalogo.json al repo
cp ~/Desktop/datos/catalogo.json ~/Desktop/cinemateca/
cd ~/Desktop/cinemateca
git add catalogo.json
git commit -m "Actualizar catálogo"
git push

# Opción B: Cambios desde la web (estado, ocultar, agregar manual)
# 1. En la web, click "Exportar JSON" → descarga catalogo.json
# 2. Reemplazar en el repo
mv ~/Downloads/catalogo.json ~/Desktop/cinemateca/
cd ~/Desktop/cinemateca
git add catalogo.json
git commit -m "Actualizar cambios desde web"
git push
```

## Notas

- El `catalogo.json` debe estar en la **raíz** del repo, al lado de `index.html`
- Los cambios que hagas en la web (estado, ocultar, agregar) se guardan en localStorage
- Para hacerlos permanentes, usa "Exportar JSON" y sube el archivo al repo
- Si limpias el caché del navegador, los cambios no exportados se pierden
