# ==============================================================================
# ====                               R2csv.R                               =====
# ==============================================================================
# Convert the 'results_app' bundle into a long CSV with a PyPSA-like
# variable column and an extra pct_change column.

#' Export bundled results to a template CSV (PyPSA-like)
#'
#' @param cfg list from load_config() or a path to config YAML. If omitted,
#'   the function will call load_config() for you (robust to empty default.yml).
#' @param bundle_name name of the saved bundle to read (default "results_app").
#' @param out_basename file stem for the CSV (default "results_bundle_template").
#' @param model_name value for the `model` column (default "FIDELIO").
#' @param pct_as_percent if TRUE, multiplies pct by 100.
#' @param include_dim_names if TRUE, encode dims as `i=C24|n1=USA`; if FALSE, just `C24|USA`.
#' @param default_unit default unit string for all symbols except special cases.
#' @param unit_overrides named list mapping symbol -> unit string (wins over defaults).
#' @param csv_sep field separator passed to fwrite (use ";" for EU Excel).
#' @export
export_results_csv <- function(cfg = NULL,
                               bundle_name        = "results_app",
                               out_basename       = "results_bundle_template",
                               model_name         = "FIDELIO",
                               pct_as_percent     = TRUE,
                               include_dim_names  = TRUE,
                               default_unit       = "million EUR",
                               unit_overrides     = list(),
                               csv_sep            = ",") {
  # ---- helpers from the package namespace ----
  outputs_dir_from_config <- fidelioDiagnostics:::outputs_dir_from_config
  load_config             <- fidelioDiagnostics:::load_config
  
  `%||%` <- function(x, y) if (is.null(x)) y else x
  
  # ---- config (robust) ----
  if (is.null(cfg) || is.character(cfg)) {
    cfg <- tryCatch(load_config(), error = function(e) NULL)
    if (is.null(cfg)) {
      cfg <- list(paths = list(outputs = normalizePath("outputs", mustWork = FALSE)),
                  scenarios = c("baseline","ff55"))
    }
  }
  
  # Prefer explicit path from cfg; fall back to helper if needed
  outdir <- tryCatch(file.path(cfg$paths$outputs, "derived"),
                     error = function(e) NA_character_)
  if (is.na(outdir) || is.null(outdir) || !nzchar(outdir)) {
    outdir <- outputs_dir_from_config()
  }
  if (!dir.exists(outdir)) dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  
  # ---- load bundle ----
  bundle_path <- file.path(outdir, paste0("bundle_", bundle_name, ".rds"))
  if (!file.exists(bundle_path)) {
    stop("Bundle not found: ", bundle_path,
         "\nMake sure your pipeline built save/bundles$", bundle_name, " first.")
  }
  b <- readRDS(bundle_path)
  if (!length(b)) stop("Bundle '", bundle_name, "' is empty.")
  
  # ---- units (override > special > default; never blank) ----
  special_units <- c(
    "TB_GDP_t"               = "ratio",
    "OUT_COMP6_SHARE_REAL_t" = "share"
  )
  
  unit_of <- function(symbol) {
    symbol <- as.character(symbol)
    
    # explicit overrides
    if (length(unit_overrides) && symbol %in% names(unit_overrides)) {
      u <- unit_overrides[[symbol]]
      if (!is.null(u) && nzchar(u)) return(u)
    }
    
    # special cases
    if (symbol %in% names(special_units)) {
      return(special_units[[symbol]])
    }
    
    # default
    default_unit
  }
  
  
  # ---- dims and scenarios ----
  dim_priority <- c("i","c","n1","au","oc")    # 'n' becomes region
  scn_cfg <- cfg$scenarios %||% c("baseline","ff55")
  
  # ---- normalize wide -> long (scenario) ----
  to_long_scn <- function(DT) {
    data.table::setDT(DT)
    meas_wide <- intersect(names(DT), c(scn_cfg, "baseline", "ff55"))
    if (length(meas_wide) >= 1L) {
      id_cols <- setdiff(names(DT), c(meas_wide, "delta", "pct"))
      L <- data.table::melt(DT, id.vars = id_cols,
                            measure.vars = meas_wide,
                            variable.name = "scenario",
                            value.name    = "value")
    } else if (all(c("scenario","value") %in% names(DT))) {
      L <- data.table::copy(DT)
    } else {
      stop("Cannot normalize table; expected either wide (scenario columns) or long (scenario/value).")
    }
    if ("t" %in% names(L) && !"year" %in% names(L)) {
      L[, year := 2014L + as.integer(t)]
    }
    L[]
  }
  
  # ---- attach pct_change from wide 'pct' if present ----
  attach_pct <- function(W, L) {
    if (!("pct" %in% names(W))) {
      L[, pct_change := NA_real_]
      return(L[])
    }
    key_cols <- intersect(names(W), c("n","n1","i","c","au","oc","t"))
    X <- data.table::copy(W[, c(key_cols, "pct"), with = FALSE])
    if ("t" %in% names(X) && !"year" %in% names(X)) X[, year := 2014L + as.integer(t)]
    
    L2 <- merge(L, X, by = intersect(names(L), names(X)), all.x = TRUE)
    base_name <- scn_cfg[1] %||% "baseline"
    if (isTRUE(pct_as_percent) && "pct" %in% names(L2)) L2[, pct := 100 * pct]
    L2[, pct_change := ifelse(scenario == base_name, NA_real_, pct)]
    L2[, pct := NULL]
    L2[]
  }
  
  # ---- build PyPSA-like variable string ----
  make_var <- function(symbol, DT) {
    base <- sub("_t$", "", symbol)
    if (!nzchar(base)) base <- as.character(symbol)
    parts <- character(0)
    for (d in dim_priority) if (d %in% names(DT)) {
      vals <- as.character(DT[[d]])
      lbl  <- if (isTRUE(include_dim_names)) paste0(d, "=", vals) else vals
      parts <- if (length(parts)) paste(parts, lbl, sep = "|") else lbl
    }
    if (!length(parts)) base else paste(base, parts, sep = "|")
  }
  
  # ---- walk bundle, normalize and stack ----
  rows <- list()
  for (sym in names(b)) {
    W <- data.table::as.data.table(b[[sym]])
    if (!nrow(W)) next
    
    L <- to_long_scn(W)
    if (!"n" %in% names(L)) L[, n := NA_character_]
    L[, region := as.character(n)]
    L <- attach_pct(W, L)
    
    # variable and unit
    L[, variable := make_var(sym, L)]
    u <- unit_of(sym)
    if (is.null(u) || is.na(u) || !nzchar(u)) u <- default_unit
    L[, unit := u]
    
    keep <- c("scenario","region","variable","unit","year","value","pct_change")
    for (k in keep) if (!k %in% names(L)) L[, (k) := NA]
    L <- L[, keep, with = FALSE]
    
    L[, model := model_name]
    data.table::setcolorder(L, c("model", keep))
    
    rows[[sym]] <- L[]
  }
  
  ALL <- data.table::rbindlist(rows, use.names = TRUE, fill = TRUE)
  data.table::setorder(ALL, variable, region, scenario, year)
  
  # ---- write CSV ----
  out_file <- file.path(outdir, paste0(out_basename, ".csv"))
  data.table::fwrite(ALL, out_file, sep = csv_sep)
  message("Wrote template CSV: ", out_file)
  
  invisible(out_file)
}
