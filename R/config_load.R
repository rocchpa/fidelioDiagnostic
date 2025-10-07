
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
load_config <- function(path = "config/project.yml") {
  def_file <- proj_path("config", "default.yml")
  override_file <- if (is.character(path) && length(path) == 1L) proj_path(path) 
  else NULL
  
  default  <- safe_read_yaml(def_file)
  override <- if (!is.null(override_file)) safe_read_yaml(override_file) else list()
  
  # merge (override wins)
  cfg <- utils::modifyList(default, override, keep.null = TRUE)
  
  # ensure sections exist
  if (is.null(cfg$paths))    cfg$paths    <- list()
  if (is.null(cfg$extract))  cfg$extract  <- list(include = character())
  if (is.null(cfg$derived))  cfg$derived  <- list(include = character())
  if (is.null(cfg$validate)) cfg$validate <- list(rules  = character())
  
  # defaults
  if (is.null(cfg$paths$gdx_dir)) cfg$paths$gdx_dir <- "data-raw/gdx"
  if (is.null(cfg$paths$outputs)) cfg$paths$outputs <- "outputs"
  if (is.null(cfg$paths$cache))   cfg$paths$cache   <- "outputs/cache"
  if (is.null(cfg$scenarios))     cfg$scenarios     <- "baseline"
  
  # normalize to absolute paths
  to_abs <- function(p) if (grepl("^([A-Za-z]:|/|\\\\)", p)) p else proj_path(p)
  cfg$paths$gdx_dir <- to_abs(cfg$paths$gdx_dir)
  cfg$paths$outputs <- to_abs(cfg$paths$outputs)
  cfg$paths$cache   <- to_abs(cfg$paths$cache)
  
  dir.create(cfg$paths$outputs, showWarnings = FALSE, recursive = TRUE)
  dir.create(cfg$paths$cache,   showWarnings = FALSE, recursive = TRUE)
  
  if (is.null(cfg$threads)) cfg$threads <- data.table::getDTthreads()
  
  cfg
}

#...............................................................................