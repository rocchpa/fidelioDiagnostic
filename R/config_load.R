# ==============================================================================
# ====                          Configuration                           ========
# ==============================================================================

# ----------------------------  Best-effort read --------------------------------
# Returns a list() in all cases (never NULL), even if the YAML file is empty.
safe_read_yaml <- function(file) {
  if (is.null(file) || !nzchar(file) || !file.exists(file)) return(list())
  obj <- tryCatch(yaml::read_yaml(file), error = function(e) NULL)
  if (is.null(obj) || !is.list(obj)) return(list())
  obj
}

# ----------------------------  Small path helpers ------------------------------
.is_abs <- function(p) {
  # Windows drive (C:\ or C:/), UNC (\\server\share), or Unix root (/)
  grepl("^[A-Za-z]:[/\\\\]|^/|^\\\\\\\\", p %||% "")
}
`%||%` <- function(x, y) if (is.null(x)) y else x

.to_abs <- function(p) {
  if (is.null(p) || !nzchar(p)) return("")
  p <- path.expand(p)
  if (.is_abs(p)) {
    normalizePath(p, winslash = "/", mustWork = FALSE)
  } else {
    # Resolve relative to the *current project root*.
    # If you have a proj_path() in your package, use it; otherwise use getwd().
    .proj_path <- get0("proj_path", mode = "function", inherits = TRUE,
                       ifnotfound = function(...) file.path(getwd(), ...))
    normalizePath(.proj_path(p), winslash = "/", mustWork = FALSE)
  }
}

# ----------------------------  Load configuration ------------------------------
#' Load and validate configuration (defaults + overrides)
#'
#' Robust to empty YAMLs. Ensures required sections/paths exist and are absolute.
#'
#' @param path Path to project YAML (default "config/project.yml"). If a list is
#'   supplied, it is treated as the overrides object directly.
#' @param verbose Print resolved paths.
#' @export
load_config <- function(path = "config/project.yml", verbose = FALSE) {
  # -- locate files ------------------------------------------------------------
  def_file      <- .to_abs("config/default.yml")
  override_file <- if (is.character(path) && length(path) == 1L) .to_abs(path) else NULL
  
  # -- read YAMLs safely (empty -> list()) ------------------------------------
  default  <- safe_read_yaml(def_file)
  override <- if (is.list(path)) path else safe_read_yaml(override_file)
  
  # -- merge (project overrides default) --------------------------------------
  # safe: both are lists by construction above
  cfg <- utils::modifyList(default, override, keep.null = TRUE)
  
  # -- ensure sections exist ---------------------------------------------------
  if (is.null(cfg$paths))    cfg$paths    <- list()
  if (is.null(cfg$extract))  cfg$extract  <- list(include = character())
  if (is.null(cfg$derived))  cfg$derived  <- list(include = character())
  if (is.null(cfg$validate)) cfg$validate <- list(rules  = character())
  
  # -- sane defaults -----------------------------------------------------------
  if (is.null(cfg$paths$gdx_dir) || !nzchar(cfg$paths$gdx_dir))
    cfg$paths$gdx_dir <- "data-raw/gdx"
  if (is.null(cfg$paths$outputs) || !nzchar(cfg$paths$outputs))
    cfg$paths$outputs <- "outputs"
  if (is.null(cfg$paths$cache)   || !nzchar(cfg$paths$cache))
    cfg$paths$cache   <- "outputs/cache"
  
# --- scenarios: must come from YAML; normalize & ensure baseline first if present
if (is.null(cfg$scenarios) || !length(cfg$scenarios)) {
  stop("load_config(): cfg$scenarios missing or empty in YAML. Define at least 'baseline'.")
}
scn <- unique(trimws(as.character(cfg$scenarios)))
if (!length(scn)) {
  stop("load_config(): cfg$scenarios resolved to empty after normalization.")
}
if ("baseline" %in% scn) {
  scn <- c("baseline", setdiff(scn, "baseline"))
}
cfg$scenarios <- scn

  
  # -- optional session overrides ---------------------------------------------
  cfg$paths$gdx_dir <- getOption("fidelio.gdx_dir", cfg$paths$gdx_dir)
  cfg$paths$outputs <- getOption("fidelio.outputs", cfg$paths$outputs)
  cfg$paths$cache   <- getOption("fidelio.cache",   cfg$paths$cache)
  
  # -- normalize to absolute paths --------------------------------------------
  cfg$paths$gdx_dir <- .to_abs(cfg$paths$gdx_dir)
  cfg$paths$outputs <- .to_abs(cfg$paths$outputs)
  cfg$paths$cache   <- .to_abs(cfg$paths$cache)
  
  # -- ensure dirs exist -------------------------------------------------------
  if (nzchar(cfg$paths$outputs))
    dir.create(cfg$paths$outputs, showWarnings = FALSE, recursive = TRUE)
  if (nzchar(cfg$paths$cache))
    dir.create(cfg$paths$cache,   showWarnings = FALSE, recursive = TRUE)
  
  # -- threads default ---------------------------------------------------------
  if (is.null(cfg$threads)) cfg$threads <- data.table::getDTthreads()
  
  # -- breadcrumbs -------------------------------------------------------------
  if (isTRUE(verbose)) {
    message("[CONFIG] Using gdx_dir = ", cfg$paths$gdx_dir)
    message("[CONFIG] Using outputs = ", cfg$paths$outputs)
  }
  
  cfg
}
