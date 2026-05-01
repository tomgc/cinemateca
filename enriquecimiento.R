# ==============================================================================
# enriquecimiento.R — Enriquecimiento del inventario con metadatos de TMDb
# Proyecto: Cinemateca
#
# Qué hace: Toma inventario_crudo.csv y consulta TMDb para obtener metadatos.
#            Guarda una caché local (tmdb_cache.json) para no re-consultar
#            películas ya procesadas. Genera catalogo.json y catalogo_enriquecido.csv.
# ==============================================================================

library(dplyr)
library(stringr)
library(readr)
library(purrr)
library(httr2)
library(jsonlite)
library(cli)
library(stringi)

# --- Ruta del proyecto --------------------------------------------------------
proyecto <- path.expand("~/Desktop/Cinemateca")

# --- Configuración de la API -------------------------------------------------
tmdb_token <- "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIyOGU5ZmQ0ZmY1Yjg1ZmEwZTUyMGQ0N2YyOGYwZTNlNSIsIm5iZiI6MTc3NzU5MzA2Ni45MjcsInN1YiI6IjY5ZjNlYWVhMmQ0OGU2MDk0NWY5Zjk3ZCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.0M5J3xxbcGLOy2q2rcv5ibSy0WctOLhAp5kuMdeedmM"

tmdb_base   <- "https://api.themoviedb.org/3"
poster_base <- "https://image.tmdb.org/t/p/w500"
pausa       <- 0.3

# --- Caché local -------------------------------------------------------------
# Archivo JSON que acumula fichas de TMDb. Si ya consultamos una película,
# no vuelve a llamar a la API. Crece con cada corrida y nunca se borra.
ruta_cache <- file.path(proyecto, "datos", "tmdb_cache.json")

if (file.exists(ruta_cache)) {
  cache_list <- fromJSON(ruta_cache, simplifyDataFrame = FALSE)
  cli_alert_success("Caché cargada: {length(cache_list)} películas")
} else {
  cache_list <- list()
  cli_alert_info("Sin caché previa — se creará una nueva")
}

# --- Funciones auxiliares ----------------------------------------------------

safe_chr <- function(x, field) {
  tryCatch({
    if (is.null(x) || !is.list(x)) return("")
    val <- x[[field]]
    if (is.null(val) || length(val) == 0) return("")
    as.character(val)
  }, error = function(e) return(""))
}

tmdb_request <- function(endpoint, ...) {
  request(paste0(tmdb_base, endpoint)) |>
    req_headers(Authorization = paste("Bearer", tmdb_token), accept = "application/json") |>
    req_url_query(language = "es-CL", ...)
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

obtener_detalle <- function(tmdb_id) {
  if (is.na(tmdb_id)) return(NULL)

  id_str <- as.character(tmdb_id)

  # Cache hit — no llama a la API
  if (id_str %in% names(cache_list)) {
    cli_alert("Caché: {tmdb_id}")
    return(as_tibble(cache_list[[id_str]]))
  }

  # Cache miss — llamar a la API con release_dates para certificación
  resp <- tryCatch({
    tmdb_request(
      paste0("/movie/", tmdb_id),
      append_to_response = "credits,keywords,release_dates"
    ) |> req_perform() |> resp_body_json()
  }, error = function(e) {
    cli_alert_warning("Error detalle id {tmdb_id}: {e$message}")
    return(NULL)
  })

  Sys.sleep(pausa)
  if (is.null(resp)) return(NULL)

  elenco <- resp$credits$cast %||% list()
  crew   <- resp$credits$crew %||% list()

  # Certificación de edad (US primero, luego la primera disponible)
  cert <- ""
  release_dates <- resp$release_dates$results %||% list()
  us_rel <- keep(release_dates, ~ .x$iso_3166_1 == "US")
  if (length(us_rel) > 0) {
    certs <- map_chr(us_rel[[1]]$release_dates %||% list(), ~ safe_chr(.x, "certification"))
    certs <- certs[certs != ""]
    if (length(certs) > 0) cert <- certs[1]
  }

  # Productoras
  productoras <- map_chr(resp$production_companies %||% list(), ~ safe_chr(.x, "name")) |>
    paste(collapse = ", ")

  # Idiomas hablados
  idiomas_hablados <- map_chr(resp$spoken_languages %||% list(), ~ safe_chr(.x, "english_name")) |>
    paste(collapse = ", ")

  # Presupuesto y recaudación
  presupuesto <- resp$budget %||% 0
  recaudacion <- resp$revenue %||% 0

  # Año y década
  anio <- str_sub(resp$release_date %||% "", 1, 4) |> as.integer()
  decada <- if (!is.na(anio)) paste0(floor(anio / 10) * 10, "s") else NA_character_

  fila <- tibble(
    tmdb_id           = resp$id,
    titulo_original   = resp$original_title %||% NA_character_,
    titulo_es         = resp$title %||% NA_character_,
    anio              = anio,
    decada            = decada,
    fecha_estreno     = resp$release_date %||% NA_character_,
    sinopsis          = resp$overview %||% NA_character_,
    generos           = map_chr(resp$genres %||% list(), ~ safe_chr(.x, "name")) |> paste(collapse = ", "),
    director          = crew |> keep(~ identical(.x[["job"]], "Director")) |> map_chr(~ safe_chr(.x, "name")) |> paste(collapse = ", "),
    guionistas        = crew |> keep(~ isTRUE(.x[["job"]] %in% c("Screenplay", "Writer"))) |> map_chr(~ safe_chr(.x, "name")) |> unique() |> paste(collapse = ", "),
    compositor        = crew |> keep(~ identical(.x[["job"]], "Original Music Composer")) |> map_chr(~ safe_chr(.x, "name")) |> paste(collapse = ", "),
    elenco            = map_chr(head(elenco, 6), ~ safe_chr(.x, "name")) |> paste(collapse = ", "),
    personajes        = map_chr(head(elenco, 6), ~ safe_chr(.x, "character")) |> paste(collapse = ", "),
    duracion_min      = resp$runtime %||% NA_integer_,
    certificacion     = if (cert != "") cert else NA_character_,
    idioma_original   = resp$original_language %||% NA_character_,
    idiomas_hablados  = idiomas_hablados,
    paises            = map_chr(resp$production_countries %||% list(), ~ safe_chr(.x, "name")) |> paste(collapse = ", "),
    productoras       = productoras,
    presupuesto_usd   = if (presupuesto > 0) presupuesto else NA_real_,
    recaudacion_usd   = if (recaudacion > 0) recaudacion else NA_real_,
    tmdb_rating       = resp$vote_average %||% NA_real_,
    tmdb_votos        = resp$vote_count %||% NA_integer_,
    popularidad       = resp$popularity %||% NA_real_,
    poster_path       = if (!is.null(resp$poster_path)) paste0(poster_base, resp$poster_path) else NA_character_,
    backdrop_path     = if (!is.null(resp$backdrop_path)) paste0("https://image.tmdb.org/t/p/w1280", resp$backdrop_path) else NA_character_,
    keywords          = map_chr(resp$keywords$keywords %||% list(), ~ safe_chr(.x, "name")) |> paste(collapse = ", "),
    coleccion_tmdb    = resp$belongs_to_collection$name %||% NA_character_,
    imdb_id           = resp$imdb_id %||% NA_character_,
    tagline           = resp$tagline %||% NA_character_
  )

  # Guardar en caché (se acumula en memoria, se graba al final)
  cache_list[[id_str]] <<- as.list(fila)

  fila
}

# --- Procesar inventario ----------------------------------------------------
cli_h1("Enriquecimiento con TMDb")

inventario <- read_csv(file.path(proyecto, "datos", "inventario_crudo.csv"), show_col_types = FALSE)
cli_alert_info("Inventario: {nrow(inventario)} archivos")

# Correcciones manuales
ruta_correcciones <- file.path(proyecto, "datos", "correcciones_manuales.csv")

if (file.exists(ruta_correcciones)) {
  correcciones <- read_csv(ruta_correcciones, show_col_types = FALSE) |>
    select(titulo_archivo, anio_archivo, tmdb_id_manual = tmdb_id)
  cli_alert_success("Correcciones: {nrow(correcciones)} entradas")
} else {
  correcciones <- tibble(titulo_archivo = character(), anio_archivo = double(), tmdb_id_manual = integer())
}

# Función para normalizar strings: quita acentos, minúsculas, solo alfanuméricos
# Así "Nausicaä" y "Nausicaa" matchean, y "d'Amélie" matchea con "d'Amelie"
normalizar <- function(x) {
  x |>
    stringi::stri_trans_general("Latin-ASCII") |>
    str_to_lower() |>
    str_replace_all("[^a-z0-9 ]", "") |>
    str_squish()
}

# Pre-normalizar los títulos de las correcciones
correcciones <- correcciones |>
  mutate(titulo_norm = normalizar(titulo_archivo))

# Buscar IDs
cli_alert_info("Buscando IDs...")

catalogo <- inventario |>
  mutate(
    tmdb_id = map2_int(
      titulo_archivo, anio_archivo,
      ~ {
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
      }
    )
  )

# Obtener detalles (con caché)
ids_encontrados    <- catalogo |> filter(!is.na(tmdb_id))
ids_no_encontrados <- catalogo |> filter(is.na(tmdb_id))

if (nrow(ids_no_encontrados) > 0) {
  cli_h2("NO encontradas ({nrow(ids_no_encontrados)})")
  ids_no_encontrados |> select(titulo_archivo, anio_archivo, tipo) |> print(n = Inf)
}

ids_unicos <- unique(ids_encontrados$tmdb_id)
n_cache <- sum(as.character(ids_unicos) %in% names(cache_list))
n_nuevos <- length(ids_unicos) - n_cache

cli_alert_info("{length(ids_unicos)} únicas: {n_cache} en caché, {n_nuevos} nuevas")
if (n_nuevos > 0) cli_alert_info("~{round(n_nuevos * pausa * 2 / 60, 1)} min para las nuevas")

detalles <- ids_unicos |>
  map(~ obtener_detalle(.x)) |>
  bind_rows()

# Guardar caché
write_lines(toJSON(cache_list, pretty = TRUE, auto_unbox = TRUE, na = "null"), ruta_cache)
cli_alert_success("Caché guardada: {length(cache_list)} películas")

# Unir
catalogo_final <- catalogo |>
  left_join(detalles, by = "tmdb_id") |>
  select(
    tmdb_id, imdb_id, titulo_es, titulo_original, titulo_archivo,
    anio, decada, fecha_estreno,
    generos, director, guionistas, compositor,
    elenco, personajes, duracion_min, sinopsis, tagline,
    certificacion, idioma_original, idiomas_hablados, paises,
    productoras, keywords, coleccion_tmdb,
    presupuesto_usd, recaudacion_usd,
    tmdb_rating, tmdb_votos, popularidad,
    poster_path, backdrop_path,
    disco, coleccion, tipo, estado, carpeta, nombre_archivo, extension, tamano_mb,
    ruta_completa
  )

# --- Guardar -----------------------------------------------------------------
cli_h2("Guardando")

ruta_json <- file.path(proyecto, "datos", "catalogo.json")
catalogo_final |> toJSON(pretty = TRUE, na = "null") |> write_lines(ruta_json)
cli_alert_success("JSON: {ruta_json}")

ruta_csv <- file.path(proyecto, "datos", "catalogo_enriquecido.csv")
write_csv(catalogo_final, ruta_csv)
cli_alert_success("CSV: {ruta_csv}")

cli_h2("Resumen")
cli_alert_success("Total: {nrow(catalogo_final)}")
cli_alert_success("Con metadatos: {sum(!is.na(catalogo_final$tmdb_id))}")
cli_alert_warning("Sin match: {sum(is.na(catalogo_final$tmdb_id))}")
catalogo_final |> count(tipo, estado, name = "n") |> print()
