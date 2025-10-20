# ==============================================================================
# ====                               R2csv.R                               =====
# ==============================================================================
# Convert the 'results_app' bundle into a long CSV with a PyPSA-like
# variable column and an extra pct_change column (vs baseline) for ALL scenarios.

#' Export bundled results to a template CSV (PyPSA-like)
#'
#' @param cfg list from load_config() or a path to config YAML. If omitted,
#'   the function will call load_config() for you (robust to empty default.yml).
#' @param bundle_name name of the saved bundle to read (default "results_app").
#' @param out_basename file stem for the CSV (default "results_bundle_template").
#' @param model_name value for the `model` column (default "FIDELIO").
#' @param pct_as_percent if TRUE, multiplies pct_change by 100.
#' @param include_dim_names if TRUE, encode dims as `i=C24|n1=USA`;
#'   if FALSE (default), encode as `C24|USA` (i.e., no general set prefixes).
#' @param default_unit default unit string for all symbols except special cases.
#' @param unit_overrides named list mapping symbol -> unit string (wins over defaults).
#' @param csv_sep field separator passed to fwrite (use ";" for EU Excel).
#' @param tol_zero numeric tolerance for baseline ~ 0 (avoid Inf).
#' @export
export_results_csv <- function(cfg = NULL,
                               bundle_name        = "results_app",
                               out_basename       = "results_bundle_template",
                               model_name         = "FIDELIO",
                               pct_as_percent     = TRUE,
                               include_dim_names  = FALSE,
                               default_unit       = "million EUR",
                               unit_overrides     = list(),
                               csv_sep            = ",",
                               tol_zero           = 1e-12) {
  # ---- helpers from the package namespace ----
  outputs_dir_from_config <- fidelioDiagnostics:::outputs_dir_from_config
  load_config             <- fidelioDiagnostics:::load_config
  
  `%||%` <- function(x, y) if (is.null(x)) y else x
  
  # ---- config (robust) ----
  if (is.null(cfg) || is.character(cfg)) {
    cfg <- tryCatch(load_config(), error = function(e) NULL)
  }
  if (is.null(cfg)) {
    stop("export_results_csv(): cannot load config; pass cfg or fix load_config().")
  }
  if (is.null(cfg$scenarios) || length(cfg$scenarios) < 1) {
    stop("export_results_csv(): cfg$scenarios missing or empty; set it in YAML.")
  }
  scn_cfg   <- cfg$scenarios
  base_name <- scn_cfg[1]
  
  # Prefer explicit path from cfg; fall back to helper if needed
  outdir <- tryCatch(file.path(cfg$paths$outputs, "derived"),
                     error = function(e) NA_character_)
  if (is.na(outdir) || is.null(outdir) || !nzchar(outdir)) {
    outdir <- outputs_dir_from_config()
    outdir <- file.path(outdir, "derived")
  }
  if (!dir.exists(outdir)) dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  
  # ---- load bundle ----
  # Try exact, then _latest, then newest matching versioned
  resolve_bundle_path <- function(outdir, bundle_name) {
    exact   <- file.path(outdir, paste0("bundle_", bundle_name, ".rds"))
    latest  <- file.path(outdir, paste0("bundle_", bundle_name, "_latest.rds"))
    if (file.exists(exact))  return(exact)
    if (file.exists(latest)) return(latest)
    pats <- list.files(outdir, pattern = sprintf("^bundle_%s__.*__\\d{4}-\\d{2}-\\d{2}_\\d{2}-\\d{2}-\\d{2}\\.rds$", bundle_name),
                       full.names = TRUE)
    if (length(pats)) {
      # pick newest by mtime
      info <- file.info(pats)
      return(rownames(info)[which.max(info$mtime)])
    }
    return(exact)  # default fallback
  }
  
  bundle_path <- resolve_bundle_path(outdir, bundle_name)
  if (!file.exists(bundle_path)) {
    stop("Bundle not found: ", bundle_path,
         "\nLooked for exact, _latest, and versioned variants in: ", outdir)
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
    if (length(unit_overrides) && symbol %in% names(unit_overrides)) {
      u <- unit_overrides[[symbol]]
      if (!is.null(u) && nzchar(u)) return(u)
    }
    if (symbol %in% names(special_units)) return(special_units[[symbol]])
    default_unit
  }
  
  # ---- normalize wide -> long (scenario/value) ----
  to_long_scn <- function(DT, cfg = NULL) {
    data.table::setDT(DT)
    if (is.null(cfg)) cfg <- load_config()
    scn_cfg <- cfg$scenarios
    stopifnot(length(scn_cfg) >= 1)
    
    meas_wide <- intersect(names(DT), scn_cfg)
    if (length(meas_wide) >= 1L) {
      id_cols <- setdiff(names(DT), c(meas_wide, grep("^delta_|^pct_", names(DT), value = TRUE), "delta", "pct"))
      L <- data.table::melt(
        DT, id.vars = id_cols,
        measure.vars = meas_wide,
        variable.name = "scenario",
        value.name = "value"
      )
    } else if (all(c("scenario","value") %in% names(DT))) {
      L <- data.table::copy(DT)
    } else {
      stop("Cannot normalize table: expected either wide (scenario columns) or long (scenario/value).")
    }
    L[]
  }
  
  # ---- compute pct_change vs baseline per key (multi-scenario safe) ----------
  # pct_change = (value/base) - 1; NA for baseline; guard for ~0 baseline.
  compute_pct_change <- function(L, base_name, tol_zero, as_percent) {
    data.table::setDT(L)
    # Keys to identify the baseline for each row (exclude 'scenario' and 'value')
    key_candidates <- c("n","n1","i","c","au","oc","t")
    keys <- intersect(names(L), key_candidates)
    if (!length(keys)) keys <- character(0)
    
    base_tbl <- L[scenario == base_name, c(keys, "value"), with = FALSE]
    data.table::setnames(base_tbl, "value", "base_value")
    
    L2 <- merge(L, base_tbl, by = keys, all.x = TRUE)
    
    L2[, pct_change := {
      b <- base_value
      v <- value
      out <- ifelse(
        scenario == base_name, NA_real_,
        ifelse(is.na(b) | abs(b) <= tol_zero,
               ifelse(is.na(v) | abs(v) <= tol_zero, 0.0, NA_real_),
               (v / b) - 1.0)
      )
      if (as_percent) out <- 100 * out
      out
    }]
    L2[, base_value := NULL]
    L2[]
  }
  
  # ---- build PyPSA-like variable string (optionally with set labels) ---------
  make_var <- function(symbol, DT) {
    base <- sub("_t$", "", symbol)
    if (!nzchar(base)) base <- as.character(symbol)
    
    dim_priority <- c("i","c","n1","au","oc")  # (n becomes 'region' below)
    lab_list <- lapply(dim_priority, function(d) {
      if (!d %in% names(DT)) return(NULL)
      vals <- as.character(DT[[d]])
      vals[!nzchar(vals)] <- NA_character_
      if (isTRUE(include_dim_names)) paste0(d, "=", vals) else vals
    })
    lab_list <- lab_list[!vapply(lab_list, is.null, logical(1))]
    if (!length(lab_list)) return(base)
    
    labs_dt <- data.table::as.data.table(lab_list)
    parts <- apply(labs_dt, 1L, function(r) {
      r <- r[!is.na(r)]
      if (!length(r)) "" else paste(r, collapse = "|")
    })
    ifelse(nchar(parts) == 0L, base, paste(base, parts, sep = "|"))
  }
  
  # ---- walk bundle, normalize and stack --------------------------------------
  rows <- list()
  for (sym in names(b)) {
    W <- data.table::as.data.table(b[[sym]])
    if (!nrow(W)) next
    
    # long
    L <- to_long_scn(W, cfg = cfg)
    
    # region & year conveniences
    if (!"n" %in% names(L)) L[, n := NA_character_]
    L[, region := as.character(n)]
    if ("t" %in% names(L) && !"year" %in% names(L)) {
      L[, year := 2014L + as.integer(t)]
    }
    
    # pct_change vs baseline
    L <- compute_pct_change(L, base_name = base_name, tol_zero = tol_zero, as_percent = isTRUE(pct_as_percent))
    
    # variable and unit
    L[, variable := make_var(sym, L)]
    u <- unit_of(sym); if (is.null(u) || is.na(u) || !nzchar(u)) u <- default_unit
    L[, unit := u]
    
    # keep/arrange columns
    keep <- c("scenario","region","variable","unit","year","value","pct_change")
    for (k in keep) if (!k %in% names(L)) L[, (k) := NA]
    L <- L[, keep, with = FALSE]
    L[, model := model_name]
    data.table::setcolorder(L, c("model", keep))
    
    rows[[sym]] <- L[]
    rm(L, W); gc(FALSE)
  }
  
  ALL <- data.table::rbindlist(rows, use.names = TRUE, fill = TRUE)
  data.table::setorder(ALL, variable, region, scenario, year)
  
  # ---- write CSV ----
  out_file <- file.path(outdir, paste0(out_basename, ".csv"))
  data.table::fwrite(ALL, out_file, sep = csv_sep)
  message("Wrote template CSV: ", out_file)
  
  invisible(out_file)
}
