# ==============================================================================
# ====                          io_gdx                                  ========
# ==============================================================================

# ---- Paths & openers ----

# Build the path to a scenario's GDX file
gdx_path_for <- function(cfg, scenario) {
  file.path(cfg$paths$gdx_dir, paste0("results_all_", scenario, ".gdx"))
}

# Open a GDX using gdxtools (object used by extract_param)
open_gdx <- function(cfg, scenario) {
  fp <- gdx_path_for(cfg, scenario)
  if (!file.exists(fp)) stop("GDX not found: ", fp)
  if (!requireNamespace("gdxtools", quietly = TRUE)) {
    stop("Package 'gdxtools' is required. Install with:\n",
         "remotes::install_github('lolow/gdxtools')")
  }
  gdxtools::gdx(fp)
}

# ---- utility: check if a symbol exists in the GDX ----
symbol_exists <- function(gdx_obj, name) {
  nm <- unique(c(gdx_obj$variables$name, gdx_obj$parameters$name))
  isTRUE(name %in% nm)
}


# ---- low-level extraction (unchanged if gdxtools is your backend) ----
extract_param <- function(gdx_obj, name) {
  name <- as.character(name)[1L]   # <<< ensure scalar
  dt <- data.table::as.data.table(gdxtools::extract(gdx_obj, name))
  dt <- norm_value_col(dt)
  dt <- norm_key_types(dt)
  dt
}

# ---- one symbol across ALL scenarios (make sym scalar + existence check) ----
extract_symbol_all <- function(cfg, reg_row) {
  sym  <- as.character(reg_row[["symbol"]][1L])  # <<< force single name
  dims <- reg_row[["dims"]][[1L]]
  
  lst <- lapply(cfg$scenarios, function(scn) {
    gdx_obj <- open_gdx(cfg, scn)
    
    if (!symbol_exists(gdx_obj, sym)) {
      message("Skipping '", sym, "' in scenario '", scn, "' (not found in GDX).")
      return(NULL)
    }
    
    dt <- extract_param(gdx_obj, sym)
    if (!all(dims %in% names(dt))) {
      stop("Symbol ", sym, " missing expected dims: ",
           paste(setdiff(dims, names(dt)), collapse = ", "))
    }
    data.table::setDT(dt)
    data.table::setkeyv(dt, dims)
    dt[, scenario := scn]
    dt
  })
  
  lst <- Filter(Negate(is.null), lst)
  if (!length(lst)) return(data.table::data.table())  # empty if missing in all scenarios
  
  out <- data.table::rbindlist(lst, use.names = TRUE, fill = TRUE)
  data.table::setkeyv(out, c(dims, "scenario"))
  out
}

#' Extract all selected symbols for all scenarios (long format)
#' @export
extract_all <- function(cfg, reg) {
  res <- lapply(seq_len(nrow(reg)), function(i) extract_symbol_all(cfg, reg[i]))
  names(res) <- reg$symbol
  res
}

list_gdx_names <- function(cfg, scenario) {
  g <- open_gdx(cfg, scenario)
  sort(unique(c(g$parameters$name, g$variables$name)))
}
