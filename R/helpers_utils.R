# ==============================================================================
# ====                           helpers_utils.R                          ======
# ==============================================================================
# General-purpose helpers: logging, project paths, and safe lookups.

# >>> print a timestamped message to the console--------------------------------
log_time <- function(msg) {
  message(sprintf("[%s] %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), msg))
}

# >>> find the project root ----------------------------------------------------
project_root <- function(start = getwd()) {
  cur <- normalizePath(start, winslash = "/", mustWork = FALSE)
  repeat {
    if (file.exists(file.path(cur, "DESCRIPTION")) ||
        length(list.files(cur, pattern = "\\.Rproj$", all.files = TRUE, no.. = TRUE)) > 0) {
      return(cur)
    }
    parent <- dirname(cur)
    if (identical(parent, cur)) break
    cur <- parent
  }
  normalizePath(start, winslash = "/", mustWork = FALSE)
}

# >>> resolve a relative path against the project root -------------------------
proj_path <- function(...) {
  root <- project_root()
  normalizePath(file.path(root, ...), winslash = "/", mustWork = FALSE)
}

# >>> print basic runtime info -------------------------------------------------
print_runtime_info <- function(cfg) {
  root <- project_root()
  log_time(paste("Project root:", root))
  log_time(paste("GDX dir     :", cfg$paths$gdx_dir))
  log_time(paste("Outputs dir :", cfg$paths$outputs))
}

# >>> fetch a symbol (table) from a list or return NULL ------------------------
require_symbol <- function(raw, name, require_cols = NULL, min_rows = 1L, quiet = FALSE) {
  if (!is.list(raw) || is.null(raw[[name]])) {
    if (!quiet) message("• Skipping derived: missing base symbol '", name, "'.")
    return(NULL)
  }
  DT <- raw[[name]]
  if (is.null(DT) || nrow(DT) < min_rows) {
    if (!quiet) message("• Skipping derived: symbol '", name, "' is empty.")
    return(NULL)
  }
  if (!is.null(require_cols)) {
    miss <- setdiff(require_cols, names(DT))
    if (length(miss)) {
      if (!quiet) message("• Skipping '", name, "': missing cols {", paste(miss, collapse = ", "), "}.")
      return(NULL)
    }
  }
  DT
}
