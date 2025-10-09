# ==============================================================================
# ====                          packages                                ========
# ==============================================================================

# Package-wide options (safe and lightweight)
.onLoad <- function(libname, pkgname) {
  # Respect user env var; otherwise let data.table decide
  if (nzchar(Sys.getenv("FIDELIO_DT_THREADS"))) {
    data.table::setDTthreads(as.integer(Sys.getenv("FIDELIO_DT_THREADS")))
  }
  
  # Consistent printing (feel free to tweak)
  options(
    datatable.print.nrows = 200L,
    datatable.print.topn  = 5L,
    datatable.print.class = TRUE
  )
}

# Optional: soft check for suggested packages in interactive Shiny sessions
check_suggested <- function(pkgs) {
  missing <- pkgs[!vapply(pkgs, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing) && interactive()) {
    msg <- paste0(
      "Missing suggested packages: ", paste(missing, collapse = ", "),
      ". Install with install.packages() (or dev/install_deps.R)."
    )
    message(msg)
  }
  invisible(missing)
}
