# ==============================================================================
# ====                            app_loader.R                              ====
# ==============================================================================
# Resolve project/output paths and load symbols/bundles for the Shiny apps.
# - Prefers project-specific outputs dir from YAML
# - Loads bundles with names that include project id and timestamp
# - Accepts both single "_" and legacy "__" separators
# - Supports .rds and .qs bundles
# - Accepts project-specific "_latest" and legacy generic "_latest"
# ==============================================================================

# --- tiny safe infix -----------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x)) y else x

# --- internal sanitizers / ids -------------------------------------------------
.sanitize_id <- function(x) {
  x <- gsub("[^A-Za-z0-9._-]+", "-", x)
  x <- gsub("-+", "-", x)
  x <- gsub("(^-|-$)", "", x)
  tolower(x)
}
.project_id <- function(cfg = NULL) {
  pid <- try(cfg$project$id, silent = TRUE)
  if (!inherits(pid, "try-error") && !is.null(pid) && nzchar(pid)) {
    return(.sanitize_id(pid))
  }
  gd <- try(cfg$paths$gdx_dir, silent = TRUE)
  if (!inherits(gd, "try-error") && !is.null(gd) && nzchar(gd)) {
    return(.sanitize_id(basename(gd)))
  }
  "project"
}

# --- resolve project root ------------------------------------------------------
#' @keywords internal
resolve_project_root <- function(root = NULL) {
  cand <- c(root, getOption("fidelio.project", ""), Sys.getenv("FIDELIO_PROJECT", ""), getwd())
  cand <- cand[nzchar(cand)]
  
  for (start in cand) {
    cur <- try(normalizePath(start, winslash = "/", mustWork = TRUE), silent = TRUE)
    if (inherits(cur, "try-error")) next
    repeat {
      has_cfg   <- file.exists(file.path(cur, "config", "project.yml"))
      has_rproj <- length(Sys.glob(file.path(cur, "*.Rproj"))) > 0
      if (has_cfg || has_rproj) return(cur)
      parent <- dirname(cur)
      if (identical(parent, cur)) break
      cur <- parent
    }
  }
  stop("Cannot resolve project root. Set options(fidelio.project='...') or FIDELIO_PROJECT.")
}

# --- derive outputs/derived from config ---------------------------------------
#' @keywords internal
outputs_dir_from_config <- function(root = NULL, config_path = NULL) {
  cfg_file <- if (!is.null(config_path)) {
    normalizePath(config_path, winslash = "/", mustWork = TRUE)
  } else {
    root <- resolve_project_root(root)
    file.path(root, "config", "project.yml")
  }
  if (!file.exists(cfg_file)) stop("Config file not found: ", cfg_file)
  cfg <- load_config(cfg_file)  # must exist in the package
  out <- file.path(cfg$paths$outputs, "derived")
  normalizePath(out, winslash = "/", mustWork = TRUE)
}

# --- looser resolver via options/env or default relative ----------------------
# Prefer config path if load_config() exists and works; else fall back.
#' @keywords internal
resolve_outputs_dir <- function(dir = NULL) {
  # First, try config-based location
  if (is.null(dir) && exists("load_config", mode = "function")) {
    cfg_try <- try(load_config(), silent = TRUE)
    if (!inherits(cfg_try, "try-error")) {
      od_try <- try(outputs_dir_from_config(), silent = TRUE)
      if (!inherits(od_try, "try-error") && dir.exists(od_try)) {
        return(normalizePath(od_try, winslash = "/", mustWork = TRUE))
      }
    }
  }
  
  cand <- c(
    dir,
    getOption("fidelio.outputs", ""),
    Sys.getenv("FIDELIO_OUTPUTS", ""),
    file.path(getwd(), "outputs", "derived")
  )
  cand <- normalizePath(cand[nzchar(cand)], winslash = "/", mustWork = FALSE)
  hit  <- cand[dir.exists(cand)]
  if (!length(hit)) stop(
    "Cannot find outputs directory. Set options(fidelio.outputs='...') ",
    "or env var FIDELIO_OUTPUTS, or ensure config paths are valid."
  )
  hit[[1]]
}

# --- manifest loader -----------------------------------------------------------
# Manifest columns expected: symbol, format, path  (older 'filename' is accepted)
#' @keywords internal
load_manifest <- function(dir = NULL) {
  dir  <- resolve_outputs_dir(dir)
  manr <- file.path(dir, "manifest.rds")
  manc <- file.path(dir, "manifest.csv")
  if (file.exists(manr)) return(readRDS(manr))
  if (file.exists(manc)) return(data.table::fread(manc))
  stop("No manifest.rds or manifest.csv found in: ", dir)
}

# --- symbol loader (chooses preferred format) ---------------------------------
#' @keywords internal
load_symbol <- function(symbol, dir = NULL,
                        prefer = c("parquet","feather","fst","rds","csv")) {
  dir <- resolve_outputs_dir(dir)
  man <- load_manifest(dir)
  
  # accept older 'filename' column
  if (!("path" %in% names(man)) && "filename" %in% names(man)) {
    man[, path := filename]
  }
  need <- c("symbol", "format", "path")
  if (!all(need %in% names(man))) {
    stop("Manifest must contain columns: symbol, format, path (or filename).")
  }
  
  # select this symbol's rows (data.table-safe scoping)
  symbol_ <- symbol
  man_sym <- man[symbol == symbol_]
  if (is.null(man_sym) || nrow(man_sym) == 0L) {
    stop("Symbol not found in manifest: ", symbol)
  }
  
  # pick first available format in 'prefer' order
  avail <- match(prefer, man_sym$format, nomatch = 0L)
  avail <- avail[avail > 0L]
  if (!length(avail)) stop("No preferred formats found for symbol '", symbol, "'.")
  row <- man_sym[avail[1L]]
  
  p <- ifelse(file.exists(row$path), row$path, file.path(dir, row$path))
  if (!file.exists(p)) stop("Data file listed in manifest not found: ", p)
  
  if (grepl("\\.parquet$", p, ignore.case = TRUE)) return(arrow::read_parquet(p))
  if (grepl("\\.feather$", p, ignore.case = TRUE)) return(arrow::read_feather(p))
  if (grepl("\\.fst$",     p, ignore.case = TRUE)) return(fst::read_fst(p, as.data.table = TRUE))
  if (grepl("\\.rds$",     p, ignore.case = TRUE)) return(readRDS(p))
  if (grepl("\\.csv$",     p, ignore.case = TRUE)) return(data.table::fread(p))
  stop("Unknown/unsupported format for: ", p)
}

# --- bundle path resolver ------------------------------------------------------
# Tries, in order, for name = "<app_kind>" or a logical bundle "name":
#  1) exact:   bundle_<name>.{rds,qs}
#  2) pointer: bundle_<name>_<pid>_latest.{rds,qs} (project-specific), then legacy bundle_<name>_latest.{rds,qs}
#  3) newest versioned for this project id:
#       bundle_<name>_<pid>_<YYYY-mm-dd_HH-MM-SS>.{rds,qs}
#       (also accepts legacy with double underscores: bundle_<name>__<pid>__<TS>.*)
#  4) newest versioned (any pid) if none match #3
.resolve_bundle_path <- function(dir, name, cfg = NULL) {
  pid <- .project_id(cfg)
  
  # 1) exact (rds/qs)
  exact <- file.path(dir, paste0("bundle_", name))
  exact_candidates <- c(paste0(exact, ".rds"), paste0(exact, ".qs"))
  exact_hit <- exact_candidates[file.exists(exact_candidates)]
  if (length(exact_hit)) return(exact_hit[1])
  
  # 2) project-specific latest, then legacy latest (rds/qs)
  latest_pid <- file.path(dir, paste0("bundle_", name, "_", pid, "_latest"))
  latest_any <- file.path(dir, paste0("bundle_", name, "_latest"))
  for (stem in c(latest_pid, latest_any)) {
    cands <- c(paste0(stem, ".rds"), paste0(stem, ".qs"))
    hit   <- cands[file.exists(cands)]
    if (length(hit)) return(hit[1])
  }
  
  # 3) versioned for this pid (accept "_" or "__")
  esc <- function(s) gsub("([.^$|()*+?{}\\[\\]\\\\])", "\\\\\\1", s)
  name_re <- esc(name); pid_re <- esc(pid)
  ts_re   <- "\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}"
  rx_pid  <- sprintf("^bundle_%s_+%s_+%s\\.(rds|qs)$", name_re, pid_re, ts_re)
  cand1   <- list.files(dir, pattern = rx_pid, full.names = TRUE, ignore.case = TRUE)
  
  # 4) any pid (accept "_" or "__")
  rx_any  <- sprintf("^bundle_%s_+.*_+%s\\.(rds|qs)$", name_re, ts_re)
  cand2   <- if (!length(cand1)) list.files(dir, pattern = rx_any, full.names = TRUE, ignore.case = TRUE) else character()
  
  candidates <- c(cand1, cand2)
  if (length(candidates)) {
    info <- file.info(candidates)
    return(rownames(info)[which.max(info$mtime)])
  }
  
  # Nothing found — return the exact rds (so caller can error with path shown)
  paste0(exact, ".rds")
}

# --- bundle loader using the resolver -----------------------------------------
#' @keywords internal
load_bundle <- function(name, dir = NULL, cfg = NULL) {
  dir <- resolve_outputs_dir(dir)
  
  # load cfg (for project id) if not provided
  if (is.null(cfg) && exists("load_config", mode = "function")) {
    cfg_try <- try(load_config(), silent = TRUE)
    if (!inherits(cfg_try, "try-error")) cfg <- cfg_try
  }
  
  p <- .resolve_bundle_path(dir, name, cfg = cfg)
  if (!file.exists(p)) {
    stop(
      "Bundle not found: ", p, "\n",
      "Looked for exact, project-specific _latest, legacy _latest, ",
      "and versioned variants in: ", dir, "\n",
      "Tip: run the pipeline to (re)create bundles for this project."
    )
  }
  
  if (grepl("\\.qs$", p, ignore.case = TRUE)) {
    if (!requireNamespace("qs", quietly = TRUE)) stop("Package 'qs' is required to read .qs")
    obj <- qs::qread(p)
  } else {
    obj <- readRDS(p)
  }
  
  # A tiny sanity: ensure it's a named list
  if (!is.list(obj)) stop("Bundle '", basename(p), "' is not a list.")
  if (is.null(names(obj))) stop("Bundle '", basename(p), "' has no names.")
  
  message("[load_bundle] Using bundle: ", basename(p))
  obj
}

# --- helper to list available bundles (nice for a dev picker) -----------------
#' @keywords internal
list_bundles <- function(dir = NULL, name = NULL) {
  dir <- resolve_outputs_dir(dir)
  pat <- if (is.null(name)) "^bundle_.*\\.(rds|qs)$"
  else sprintf("^bundle_%s.*\\.(rds|qs)$", gsub("([.^$|()*+?{}\\[\\]\\\\])", "\\\\\\1", name))
  files <- list.files(dir, pattern = pat, full.names = TRUE)
  if (!length(files)) {
    return(data.table::data.table(file = character(), mtime = as.POSIXct(character())))
  }
  info <- file.info(files)
  data.table::data.table(file = rownames(info), mtime = info$mtime)[order(-mtime)]
}
