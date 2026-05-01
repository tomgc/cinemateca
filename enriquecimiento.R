# ==============================================================================
# enriquecimiento.R — Enriquecimiento con TMDb (ES + EN)
# Proyecto: Cinemateca
#
# Consulta TMDb en español e inglés para cada película.
# Guarda caché local para no re-consultar. Deduplica por tmdb_id.
# Usa paralelismo (furrr) para acelerar las consultas a la API.
# ==============================================================================

library(dplyr)
library(stringr)
library(readr)
library(purrr)
library(httr2)
library(jsonlite)
library(cli)
library(stringi)
library(furrr)      # install.packages("furrr") si no lo tienes

proyecto <- path.expand("~/Desktop/Cinemateca")

# --- API config --------------------------------------------------------------
tmdb_token <- "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIyOGU5ZmQ0ZmY1Yjg1ZmEwZTUyMGQ0N2YyOGYwZTNlNSIsIm5iZiI6MTc3NzU5MzA2Ni45MjcsInN1YiI6IjY5ZjNlYWVhMmQ0OGU2MDk0NWY5Zjk3ZCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.0M5J3xxbcGLOy2q2rcv5ibSy0WctOLhAp5kuMdeedmM"
tmdb_base   <- "https://api.themoviedb.org/3"
poster_base <- "https://image.tmdb.org/t/p/w500"

# Pausa entre llamadas. TMDb permite ~40 req/s.
# Con 4 workers a 0.15s cada uno = ~26 req/s, bien dentro del límite.
pausa <- 0.15

# --- Caché -------------------------------------------------------------------
ruta_cache <- file.path(proyecto, "datos", "tmdb_cache.json")

if (file.exists(ruta_cache)) {
  cache_list <- fromJSON(ruta_cache, simplifyDataFrame = FALSE)
  cli_alert_success("Caché: {length(cache_list)} películas")
} else {
  cache_list <- list()
  cli_alert_info("Sin caché previa")
}

# --- Funciones ---------------------------------------------------------------

safe_chr <- function(x, field) {
  tryCatch({
    if (is.null(x) || !is.list(x)) return("")
    val <- x[[field]]
    if (is.null(val) || length(val) == 0) return("")
    as.character(val)
  }, error = function(e) return(""))
}

tmdb_request <- function(endpoint, lang = "es-CL", ...) {
  request(paste0(tmdb_base, endpoint)) |>
    req_headers(Authorization = paste("Bearer", tmdb_token), accept = "application/json") |>
    req_url_query(language = lang, ...)
}

buscar_pelicula <- function(titulo, anio = NA) {
  params <- list(query = titulo)
  if (!is.na(anio)) params$year <- anio

  resp <- tryCatch({
    tmdb_request("/search/movie") |>
      req_url_query(!!!params) |> req_perform() |> resp_body_json()
  }, error = function(e) {
    cli_alert_warning("Error buscando '{titulo}': {e$message}")
    return(NULL)
  })
  Sys.sleep(pausa)

  if (is.null(resp) || length(resp$results) == 0) {
    cli_alert_warning("No encontrada: {titulo} ({anio})")
    return(NA_integer_)
  }
  resp$results[[1]]$id
}

# Esta función NO escribe a la caché directamente (no es compatible con workers).
# Devuelve el tibble y la caché se arma después al combinar resultados.
obtener_detalle_api <- function(tmdb_id) {
  if (is.na(tmdb_id)) return(NULL)

  # Llamada 1: español (completa)
  resp_es <- tryCatch({
    tmdb_request(paste0("/movie/", tmdb_id), lang = "es-CL",
                 append_to_response = "credits,keywords,release_dates") |>
      req_perform() |> resp_body_json()
  }, error = function(e) { return(NULL) })
  Sys.sleep(pausa)
  if (is.null(resp_es)) return(NULL)

  # Llamada 2: inglés (textos)
  resp_en <- tryCatch({
    tmdb_request(paste0("/movie/", tmdb_id), lang = "en-US") |>
      req_perform() |> resp_body_json()
  }, error = function(e) { return(NULL) })
  Sys.sleep(pausa)

  elenco <- resp_es$credits$cast %||% list()
  crew   <- resp_es$credits$crew %||% list()

  # Certificación US
  cert <- ""
  release_dates <- resp_es$release_dates$results %||% list()
  us_rel <- keep(release_dates, ~ .x$iso_3166_1 == "US")
  if (length(us_rel) > 0) {
    certs <- map_chr(us_rel[[1]]$release_dates %||% list(), ~ safe_chr(.x, "certification"))
    certs <- certs[certs != ""]
    if (length(certs) > 0) cert <- certs[1]
  }

  productoras <- map_chr(resp_es$production_companies %||% list(), ~ safe_chr(.x, "name")) |>
    paste(collapse = ", ")
  idiomas_hablados <- map_chr(resp_es$spoken_languages %||% list(), ~ safe_chr(.x, "english_name")) |>
    paste(collapse = ", ")

  presupuesto <- resp_es$budget %||% 0
  recaudacion <- resp_es$revenue %||% 0
  anio <- str_sub(resp_es$release_date %||% "", 1, 4) |> as.integer()
  decada <- if (!is.na(anio)) paste0(floor(anio / 10) * 10, "s") else NA_character_

  tibble(
    tmdb_id           = resp_es$id,
    titulo_original   = resp_es$original_title %||% NA_character_,
    titulo_es         = resp_es$title %||% NA_character_,
    titulo_en         = resp_en$title %||% resp_es$original_title %||% NA_character_,
    anio              = anio,
    decada            = decada,
    fecha_estreno     = resp_es$release_date %||% NA_character_,
    sinopsis          = resp_es$overview %||% NA_character_,
    sinopsis_en       = resp_en$overview %||% NA_character_,
    generos           = map_chr(resp_es$genres %||% list(), ~ safe_chr(.x, "name")) |> paste(collapse = ", "),
    director          = crew |> keep(~ identical(.x[["job"]], "Director")) |> map_chr(~ safe_chr(.x, "name")) |> paste(collapse = ", "),
    guionistas        = crew |> keep(~ isTRUE(.x[["job"]] %in% c("Screenplay", "Writer"))) |> map_chr(~ safe_chr(.x, "name")) |> unique() |> paste(collapse = ", "),
    compositor        = crew |> keep(~ identical(.x[["job"]], "Original Music Composer")) |> map_chr(~ safe_chr(.x, "name")) |> paste(collapse = ", "),
    elenco            = map_chr(head(elenco, 6), ~ safe_chr(.x, "name")) |> paste(collapse = ", "),
    personajes        = map_chr(head(elenco, 6), ~ safe_chr(.x, "character")) |> paste(collapse = ", "),
    duracion_min      = resp_es$runtime %||% NA_integer_,
    certificacion     = if (cert != "") cert else NA_character_,
    idioma_original   = resp_es$original_language %||% NA_character_,
    idiomas_hablados  = idiomas_hablados,
    paises            = map_chr(resp_es$production_countries %||% list(), ~ safe_chr(.x, "name")) |> paste(collapse = ", "),
    productoras       = productoras,
    presupuesto_usd   = if (presupuesto > 0) presupuesto else NA_real_,
    recaudacion_usd   = if (recaudacion > 0) recaudacion else NA_real_,
    tmdb_rating       = resp_es$vote_average %||% NA_real_,
    tmdb_votos        = resp_es$vote_count %||% NA_integer_,
    popularidad       = resp_es$popularity %||% NA_real_,
    poster_path       = if (!is.null(resp_es$poster_path)) paste0(poster_base, resp_es$poster_path) else NA_character_,
    backdrop_path     = if (!is.null(resp_es$backdrop_path)) paste0("https://image.tmdb.org/t/p/w1280", resp_es$backdrop_path) else NA_character_,
    keywords          = map_chr(resp_es$keywords$keywords %||% list(), ~ safe_chr(.x, "name")) |> paste(collapse = ", "),
    coleccion_tmdb    = resp_es$belongs_to_collection$name %||% NA_character_,
    imdb_id           = resp_es$imdb_id %||% NA_character_,
    tagline           = resp_es$tagline %||% NA_character_,
    tagline_en        = resp_en$tagline %||% NA_character_
  )
}

# Wrapper que revisa caché primero, llama a API si no está
obtener_detalle <- function(tmdb_id, cache_ref) {
  id_str <- as.character(tmdb_id)
  if (id_str %in% names(cache_ref)) {
    return(as_tibble(cache_ref[[id_str]]))
  }
  obtener_detalle_api(tmdb_id)
}

# --- Procesar ----------------------------------------------------------------
cli_h1("Enriquecimiento con TMDb (ES + EN)")

inventario <- read_csv(file.path(proyecto, "datos", "inventario_crudo.csv"), show_col_types = FALSE)
cli_alert_info("Inventario: {nrow(inventario)} archivos")

# Correcciones manuales
ruta_correcciones <- file.path(proyecto, "datos", "correcciones_manuales.csv")
if (file.exists(ruta_correcciones)) {
  correcciones <- read_csv(ruta_correcciones, show_col_types = FALSE) |>
    select(titulo_archivo, anio_archivo, tmdb_id_manual = tmdb_id) |>
    mutate(titulo_norm = stri_trans_general(titulo_archivo, "Latin-ASCII") |>
             str_to_lower() |> str_replace_all("[^a-z0-9 ]", "") |> str_squish())
  cli_alert_success("Correcciones: {nrow(correcciones)}")
} else {
  correcciones <- tibble(titulo_archivo = character(), anio_archivo = double(),
                         tmdb_id_manual = integer(), titulo_norm = character())
}

normalizar <- function(x) {
  x |> stri_trans_general("Latin-ASCII") |> str_to_lower() |>
    str_replace_all("[^a-z0-9 ]", "") |> str_squish()
}

# Buscar IDs (secuencial — es rápido, ~0.15s por película)
cli_alert_info("Buscando IDs...")
catalogo <- inventario |>
  mutate(
    tmdb_id = map2_int(titulo_archivo, anio_archivo, ~ {
      titulo_norm <- normalizar(.x)
      match_idx <- which(
        correcciones$titulo_norm == titulo_norm &
        (correcciones$anio_archivo == .y | is.na(correcciones$anio_archivo) | is.na(.y))
      )
      if (length(match_idx) > 0) {
        id_manual <- correcciones$tmdb_id_manual[match_idx[1]]
        cli_alert_success("Corrección: {.x} -> id {id_manual}")
        return(as.integer(id_manual))
      }
      cli_alert("Buscando: {.x} ({.y})")
      buscar_pelicula(.x, .y)
    })
  )

# Obtener detalles
ids_encontrados    <- catalogo |> filter(!is.na(tmdb_id))
ids_no_encontrados <- catalogo |> filter(is.na(tmdb_id))

if (nrow(ids_no_encontrados) > 0) {
  cli_h2("NO encontradas ({nrow(ids_no_encontrados)})")
  ids_no_encontrados |> select(titulo_archivo, anio_archivo, tipo) |> print(n = Inf)
}

ids_unicos <- unique(ids_encontrados$tmdb_id)
ids_en_cache <- as.character(ids_unicos) %in% names(cache_list)
n_cache <- sum(ids_en_cache)
n_nuevos <- sum(!ids_en_cache)

cli_alert_info("{length(ids_unicos)} únicas: {n_cache} en caché, {n_nuevos} nuevas")

# --- Separar: caché vs API --------------------------------------------------
# Los que están en caché se leen directo (instantáneo).
# Los nuevos se procesan en paralelo con furrr.

ids_desde_cache <- ids_unicos[ids_en_cache]
ids_desde_api   <- ids_unicos[!ids_en_cache]

# Leer desde caché (instantáneo)
if (length(ids_desde_cache) > 0) {
  cli_alert_info("Leyendo {length(ids_desde_cache)} desde caché...")
  detalles_cache <- ids_desde_cache |>
    map(~ as_tibble(cache_list[[as.character(.x)]])) |>
    bind_rows()
} else {
  detalles_cache <- tibble()
}

# Consultar API en paralelo para los nuevos
if (length(ids_desde_api) > 0) {
  # Detectar núcleos disponibles. Usamos máximo 4 workers para no saturar la API.
  # Con 4 workers a 0.15s de pausa = ~26 req/s, bien bajo el límite de TMDb (~40/s).
  n_workers <- min(4, parallel::detectCores() - 1)
  cli_alert_info("Consultando {length(ids_desde_api)} nuevas en paralelo ({n_workers} workers)")
  cli_alert_info("~{round(length(ids_desde_api) * pausa * 2 / n_workers / 60, 1)} min estimados")

  # Activar paralelismo
  plan(multisession, workers = n_workers)

  detalles_api <- future_map(
    ids_desde_api,
    ~ obtener_detalle_api(.x),
    .options = furrr_options(seed = TRUE),
    .progress = TRUE
  ) |>
    compact() |>   # remover NULLs (películas que fallaron)
    bind_rows()

  # Volver a modo secuencial
  plan(sequential)
} else {
  detalles_api <- tibble()
}

# Combinar resultados
detalles <- bind_rows(detalles_cache, detalles_api)

# Actualizar caché con los nuevos resultados de la API
if (nrow(detalles_api) > 0) {
  cli_alert_info("Actualizando caché con {nrow(detalles_api)} películas nuevas...")
  for (i in seq_len(nrow(detalles_api))) {
    id_str <- as.character(detalles_api$tmdb_id[i])
    cache_list[[id_str]] <- as.list(detalles_api[i, ])
  }
}

# Guardar caché
write_lines(toJSON(cache_list, pretty = TRUE, auto_unbox = TRUE, na = "null"), ruta_cache)
cli_alert_success("Caché guardada: {length(cache_list)} películas")

# --- Unir y deduplicar -------------------------------------------------------
catalogo_completo <- catalogo |>
  left_join(detalles, by = "tmdb_id") |>
  select(
    tmdb_id, imdb_id, titulo_es, titulo_en, titulo_original, titulo_archivo,
    anio, decada, fecha_estreno,
    generos, director, guionistas, compositor,
    elenco, personajes, duracion_min, sinopsis, sinopsis_en, tagline, tagline_en,
    certificacion, idioma_original, idiomas_hablados, paises,
    productoras, keywords, coleccion_tmdb,
    presupuesto_usd, recaudacion_usd,
    tmdb_rating, tmdb_votos, popularidad,
    poster_path, backdrop_path,
    disco, coleccion, tipo, estado, carpeta, nombre_archivo, extension, tamano_mb,
    ruta_completa
  )

# Deduplicar: quedarse con la copia de mayor tamaño por tmdb_id.
# Guardar todas las ubicaciones como campo adicional.
sin_id <- catalogo_completo |> filter(is.na(tmdb_id))

con_id <- catalogo_completo |>
  filter(!is.na(tmdb_id)) |>
  mutate(tamano_num = as.numeric(tamano_mb)) |>
  group_by(tmdb_id) |>
  mutate(
    n_copias = n(),
    todas_ubicaciones = paste(
      paste0(disco, ": ", nombre_archivo, " (", tamano_mb, " MB)"),
      collapse = " | "
    )
  ) |>
  slice_max(tamano_num, n = 1, with_ties = FALSE) |>
  ungroup() |>
  select(-tamano_num)

catalogo_final <- bind_rows(con_id, sin_id)

n_dupes <- nrow(catalogo_completo) - nrow(catalogo_final)
if (n_dupes > 0) cli_alert_info("Deduplicadas: {n_dupes} copias removidas")

# --- Guardar -----------------------------------------------------------------
cli_h2("Guardando")

ruta_json <- file.path(proyecto, "datos", "catalogo.json")
catalogo_final |> toJSON(pretty = TRUE, na = "null") |> write_lines(ruta_json)
cli_alert_success("JSON: {ruta_json}")

ruta_csv <- file.path(proyecto, "datos", "catalogo_enriquecido.csv")
write_csv(catalogo_final, ruta_csv)
cli_alert_success("CSV: {ruta_csv}")

cli_h2("Resumen")
cli_alert_success("Total (sin duplicados): {nrow(catalogo_final)}")
cli_alert_success("Con metadatos: {sum(!is.na(catalogo_final$tmdb_id))}")
cli_alert_warning("Sin match: {sum(is.na(catalogo_final$tmdb_id))}")
catalogo_final |> count(tipo, estado, name = "n") |> print()
