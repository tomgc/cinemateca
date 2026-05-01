# ==============================================================================
# escaneo.R — Escaneo de discos externos de películas
# Proyecto: Cinemateca
#
# Qué hace: Recorre discos externos, identifica archivos de video,
#            extrae información básica y genera un inventario en CSV.
#            Si ya existe un inventario previo, conserva los datos de
#            discos no conectados (acumulación incremental).
# ==============================================================================

library(fs)
library(dplyr)
library(stringr)
library(readr)

# --- Ruta del proyecto --------------------------------------------------------
# Todos los outputs van dentro de esta carpeta.
# Ajusta esta ruta si mueves el proyecto a otra ubicación.
proyecto <- path_expand("~/Desktop/Cinemateca")
dir_create(path(proyecto, "datos"))

# --- Configuración de fuentes ------------------------------------------------
# Cada fila es una carpeta a escanear.
# - disco:      nombre del disco físico en Finder
# - ruta:       path completo a la carpeta
# - coleccion:  etiqueta descriptiva
# - tipo:       "Película" o "Documental"
# - estado:     "pendiente" (to-watch) o "coleccion" (ya vista/catalogada)

fuentes <- tribble(
  ~disco,              ~ruta,                                                  ~coleccion,      ~tipo,           ~estado,
  # --- Disco: movies to-watch ---
  "movies to-watch",   "/Volumes/movies to-watch/movies to-watch",             "Películas",     "Película",      "pendiente",
  "movies to-watch",   "/Volumes/movies to-watch/documentaries to-watch",      "Documentales",  "Documental",    "pendiente",

  # --- Disco: horror ---
  "horror",            "/Volumes/horror/found footage collection",             "Found Footage", "Película",      "coleccion",
  "horror",            "/Volumes/horror/horror collection",                    "Terror",        "Película",      "coleccion",
  "horror",            "/Volumes/horror/horror to-watch",                      "Terror",        "Película",      "pendiente",

  # --- Disco: library_pt1 (subcarpetas por década) ---
  "library_pt1",       "/Volumes/library_pt1/1920s",                           "Colección",     "Película",      "coleccion",
  "library_pt1",       "/Volumes/library_pt1/1930s",                           "Colección",     "Película",      "coleccion",
  "library_pt1",       "/Volumes/library_pt1/1940s",                           "Colección",     "Película",      "coleccion",
  "library_pt1",       "/Volumes/library_pt1/1950s",                           "Colección",     "Película",      "coleccion",
  "library_pt1",       "/Volumes/library_pt1/1960s",                           "Colección",     "Película",      "coleccion",
  "library_pt1",       "/Volumes/library_pt1/1970s",                           "Colección",     "Película",      "coleccion",
  "library_pt1",       "/Volumes/library_pt1/1980s",                           "Colección",     "Película",      "coleccion",
  "library_pt1",       "/Volumes/library_pt1/1990s",                           "Colección",     "Película",      "coleccion",
  "library_pt1",       "/Volumes/library_pt1/2000s",                           "Colección",     "Película",      "coleccion",
  "library_pt1",       "/Volumes/library_pt1/2010s",                           "Colección",     "Película",      "coleccion",
  "library_pt1",       "/Volumes/library_pt1/2020s",                           "Colección",     "Película",      "coleccion",

  # --- Disco: library_pt2 ---
  "library_pt2",       "/Volumes/library_pt2/documentaries collection",        "Documentales",  "Documental",    "coleccion"
)

# Extensiones de video reconocidas
extensiones_video <- c("mkv", "mp4", "avi", "mov", "m4v", "wmv", "flv", "webm")

# Patrones de extras a excluir por nombre de archivo
patrones_extras <- regex(paste(
  "making.of", "behind.the.scenes", "featurette", "deleted.scene",
  "\\bextras?\\d*\\b", "\\binterview", "\\bpromo",
  "\\bq&a\\b", "\\bq.?&.?a\\b", "\\btrailer\\b",
  "\\bretrospective\\b", "gag.reel", "blooper", "commentary",
  "\\bshort\\s*-",
  sep = "|"
), ignore_case = TRUE)

# --- Función principal -------------------------------------------------------

escanear_fuente <- function(ruta, disco, coleccion, tipo, estado) {

  if (!dir_exists(ruta)) {
    cli::cli_alert_warning("No encontrada: {ruta} — ¿Está conectado '{disco}'?")
    return(NULL)
  }

  cli::cli_alert_info("Escaneando: {tipo} en '{coleccion}' ({ruta})")

  # Paso 1: Películas sueltas en la raíz
  archivos_raiz <- dir_ls(path = ruta, recurse = FALSE, type = "file")
  videos_raiz <- archivos_raiz[str_to_lower(path_ext(archivos_raiz)) %in% extensiones_video]

  # Paso 2: Carpetas de película (un nivel abajo) — tomar solo el archivo más grande
  subcarpetas <- dir_ls(path = ruta, recurse = FALSE, type = "directory")

  videos_subcarpetas <- subcarpetas |>
    purrr::map(function(carpeta_pelicula) {
      archivos_en_carpeta <- dir_ls(path = carpeta_pelicula, recurse = FALSE, type = "file")
      videos <- archivos_en_carpeta[str_to_lower(path_ext(archivos_en_carpeta)) %in% extensiones_video]
      if (length(videos) == 0) return(character(0))
      tamanios <- file_size(videos)
      videos[which.max(tamanios)]
    }) |>
    purrr::list_c()

  archivos_video <- c(videos_raiz, videos_subcarpetas)

  # Filtrar extras por nombre
  es_extra <- str_detect(path_file(archivos_video), patrones_extras)
  if (any(es_extra)) {
    cli::cli_alert_info("Excluidos {sum(es_extra)} extras")
  }
  archivos_video <- archivos_video[!es_extra]

  if (length(archivos_video) == 0) {
    cli::cli_alert_warning("Sin archivos de video en '{ruta}'.")
    return(NULL)
  }

  cli::cli_alert_success("{length(archivos_video)} archivos encontrados.")

  # Construir tibble
  tibble(ruta_completa = as.character(archivos_video)) |>
    mutate(
      nombre_archivo = path_file(ruta_completa),
      extension      = str_to_lower(path_ext(ruta_completa)),
      tamano_mb      = as.numeric(round(file_size(ruta_completa) / 1024^2, 1)),
      disco          = disco,
      coleccion      = coleccion,
      tipo           = tipo,
      estado         = estado,
      carpeta        = str_remove(path_dir(ruta_completa), ruta) |> str_remove("^/"),
      nombre_limpio  = path_ext_remove(nombre_archivo),
      anio_archivo   = case_when(
        str_detect(nombre_limpio, "^\\d{4}\\s*-\\s*") ~
          str_extract(nombre_limpio, "^(\\d{4})", group = 1) |> as.integer(),
        str_detect(nombre_limpio, "\\(\\d{4}\\)") ~
          str_extract(nombre_limpio, "\\((\\d{4})\\)", group = 1) |> as.integer(),
        TRUE ~ NA_integer_
      ),
      titulo_archivo = nombre_limpio |>
        str_remove("^\\d{4}\\s*-\\s*") |>
        str_remove("\\s*\\(\\d{4}\\)") |>
        str_remove_all(regex(
          "\\s*\\((?:4K|4K-DV|Remastered|New Remastered|Arrow Remastered|Director'?s Cut|Unrated Director'?s Cut|Criterion|Theatrical|Theatrical Cut|Unrated|Uncut|Extended|Extended Cut|IMAX)\\)",
          ignore_case = TRUE
        )) |>
        str_remove("\\s+copy$") |>
        str_remove("\\s+R$") |>
        str_remove("\\s*\\(version\\d*\\)") |>
        str_remove("\\s+\\d+$") |>
        str_trim()
    ) |>
    select(disco, coleccion, tipo, estado, titulo_archivo, anio_archivo,
           carpeta, nombre_archivo, extension, tamano_mb, ruta_completa, nombre_limpio)
}

# --- Ejecutar escaneo --------------------------------------------------------

inventario_nuevo <- fuentes |>
  purrr::pmap(~ escanear_fuente(
    ruta = ..2, disco = ..1, coleccion = ..3, tipo = ..4, estado = ..5
  )) |>
  bind_rows()

# --- Acumular con inventario previo ------------------------------------------
# UPSERT: discos escaneados se reemplazan, discos no conectados se conservan.

ruta_salida <- path(proyecto, "datos", "inventario_crudo.csv")
discos_escaneados <- unique(inventario_nuevo$disco)

if (file.exists(ruta_salida) && length(discos_escaneados) > 0) {
  inventario_previo <- read_csv(ruta_salida, show_col_types = FALSE)
  discos_conservados <- setdiff(unique(inventario_previo$disco), discos_escaneados)

  inventario_conservado <- inventario_previo |>
    filter(disco %in% discos_conservados)

  if (nrow(inventario_conservado) > 0) {
    cli::cli_alert_info(
      "Conservando {nrow(inventario_conservado)} archivos de: {paste(discos_conservados, collapse = ', ')}"
    )
  }

  inventario <- bind_rows(inventario_nuevo, inventario_conservado)
} else {
  inventario <- inventario_nuevo
}

# --- Guardar resultado -------------------------------------------------------

if (nrow(inventario) > 0) {
  cli::cli_h2("Resumen del inventario")
  cli::cli_alert_info("Total: {nrow(inventario)} archivos")
  cli::cli_alert_info("Discos escaneados ahora: {paste(discos_escaneados, collapse = ', ')}")

  inventario |>
    count(disco, estado, coleccion, tipo, name = "n_archivos") |>
    print(n = Inf)

  write_csv(inventario, ruta_salida)
  cli::cli_alert_success("Guardado en: {ruta_salida}")

  # Backup con fecha
  write_csv(inventario, path(proyecto, "datos",
    paste0("inventario_crudo_", format(Sys.Date(), "%Y%m%d"), ".csv")))
} else {
  cli::cli_alert_danger("No se encontraron archivos.")
}
