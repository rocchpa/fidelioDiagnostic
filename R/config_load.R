# ==============================================================================
# ====                          Configuration                           ========
# ==============================================================================

# ----------------------------  Best-effort read--------------------------------

safe_read_yaml <- function(file) {
  if (!file.exists(file)) return(list())
  y <- tryCatch(yaml::read_yaml(file), error = function(e) NULL)
  if (is.null(y) || !is.list(y)) return(list())
  y
}

# ----------------------------  Load configuration -----------------------------
#' Load and validate configuration (defaults + overrides)
#' @export
load_config <- function(path = "config/project.yml", verbose = TRUE) {
  # -- helpers ---------------------------------------------------------------
  # Resolve paths relative to the project root used by proj_path()
  # (proj_path() is already available in your package; we reuse it)
  .is_abs <- function(p) {
    # Windows drive (C:\ or C:/), UNC (\\server\share), or Unix root (/)
    grepl("^[A-Za-z]:[/\\\\]|^/|^\\\\\\\\", p)
  }
  .to_abs <- function(p) {
    p <- path.expand(p)
    if (.is_abs(p)) {
      normalizePath(p, winslash = "/", mustWork = FALSE)
    } else {
      normalizePath(proj_path(p), winslash = "/", mustWork = FALSE)
    }
  }
  safe_read_yaml <- function(f) {
    if (is.null(f) || !nzchar(f) || !file.exists(f)) return(list())
    yaml::read_yaml(f)
  }
  
  # -- load default + project ------------------------------------------------
  def_file      <- proj_path("config", "default.yml")
  override_file <- if (is.character(path) && length(path) == 1L) proj_path(path) else NULL
  
  default  <- safe_read_yaml(def_file)
  override <- safe_read_yaml(override_file)
  
  # merge (project overrides default)
  cfg <- utils::modifyList(default, override, keep.null = TRUE)
  
  # -- ensure sections exist -------------------------------------------------
  if (is.null(cfg$paths))    cfg$paths    <- list()
  if (is.null(cfg$extract))  cfg$extract  <- list(include = character())
  if (is.null(cfg$derived))  cfg$derived  <- list(include = character())
  if (is.null(cfg$validate)) cfg$validate <- list(rules  = character())
  
  # -- defaults --------------------------------------------------------------
  if (is.null(cfg$paths$gdx_dir) || !nzchar(cfg$paths$gdx_dir)) cfg$paths$gdx_dir <- "data-raw/gdx"
  if (is.null(cfg$paths$outputs) || !nzchar(cfg$paths$outputs)) cfg$paths$outputs <- "outputs"
  if (is.null(cfg$paths$cache)   || !nzchar(cfg$paths$cache))   cfg$paths$cache   <- "outputs/cache"
  if (is.null(cfg$scenarios)     || !length(cfg$scenarios))     cfg$scenarios     <- "baseline"
  
  # -- optional session overrides -------------------------------------------
  # Let advanced users override these in the current R session
  cfg$paths$gdx_dir <- getOption("fidelio.gdx_dir", cfg$paths$gdx_dir)
  cfg$paths$outputs <- getOption("fidelio.outputs", cfg$paths$outputs)
  cfg$paths$cache   <- getOption("fidelio.cache",   cfg$paths$cache)
  
  # -- normalize to absolute paths ------------------------------------------
  cfg$paths$gdx_dir <- .to_abs(cfg$paths$gdx_dir)
  cfg$paths$outputs <- .to_abs(cfg$paths$outputs)
  cfg$paths$cache   <- .to_abs(cfg$paths$cache)
  
  # -- ensure dirs exist -----------------------------------------------------
  dir.create(cfg$paths$outputs, showWarnings = FALSE, recursive = TRUE)
  dir.create(cfg$paths$cache,   showWarnings = FALSE, recursive = TRUE)
  
  # -- threads default -------------------------------------------------------
  if (is.null(cfg$threads)) cfg$threads <- data.table::getDTthreads()
  
  # -- breadcrumbs -----------------------------------------------------------
  if (isTRUE(verbose)) {
    message("[CONFIG] Using gdx_dir = ", cfg$paths$gdx_dir)
    message("[CONFIG] Using outputs = ", cfg$paths$outputs)
  }
  
  cfg
}


#...............................................................................