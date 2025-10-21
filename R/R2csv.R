# ==============================================================================
# ====                               R2csv.R                               =====
# ==============================================================================
# Convert a saved bundle (e.g. "results_app") into a long CSV with a PyPSA-like
# `variable` column and a `pct_change` vs baseline for ALL scenarios.

#' Export bundled results to a template CSV (PyPSA-like)
#'
#' @param cfg list from load_config() or a path to config YAML. If omitted,
#'   the function will call load_config() (quiet) for you.
#' @param bundle_name name of the saved bundle to read (e.g. "results_app").
#' @param out_basename file stem for the CSV (default "results_bundle_template").
#' @param model_name value for the `model` column (default "FIDELIO").
#' @param pct_as_percent if TRUE, multiplies pct_change by 100.
#' @param include_dim_names if TRUE, encode dims as `i=C24|n1=USA`;
#'   if FALSE, encode as `C24|USA` (no set prefixes).
#' @param default_unit fallback unit for symbols not listed in `unit_overrides`.
#' @param unit_overrides named list mapping symbol -> unit string.
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
  # ---- package helpers ----
  outputs_dir_from_config <- fidelioDiagnostics:::outputs_dir_from_config
  resolve_outputs_dir     <- fidelioDiagnostics:::resolve_outputs_dir
  load_config             <- fidelioDiagnostics:::load_config
  .project_id             <- fidelioDiagnostics:::.project_id
  
  `%||%` <- function(x, y) if (is.null(x)) y else x
  
  # ---- config (quiet & robust) ----
  if (is.null(cfg) || is.character(cfg)) {
    cfg <- tryCatch(load_config(verbose = FALSE), error = function(e) NULL)
  }
  if (is.null(cfg)) {
    stop("export_results_csv(): cannot load config; pass cfg or fix load_config().")
  }
  if (is.null(cfg$scenarios) || length(cfg$scenarios) < 1) {
    stop("export_results_csv(): cfg$scenarios missing or empty; set it in YAML.")
  }
  scn_cfg   <- cfg$scenarios
  base_name <- scn_cfg[1]
  
  # enforce scalar bundle_name
  if (length(bundle_name) != 1L) {
    stop("export_results_csv(): 'bundle_name' must be length 1, got: ",
         paste(bundle_name, collapse = ", "))
  }
  
  # ---- locate bundle file (prefer _latest; else newest versioned) ------------
  out_root    <- tryCatch(resolve_outputs_dir(cfg), error = function(e) cfg$paths$outputs)
  bundles_dir <- file.path(out_root, "derived")
  dir.create(bundles_dir, recursive = TRUE, showWarnings = FALSE)
  
  pid <- .project_id(cfg)
  
  bundle_latest <- file.path(bundles_dir, sprintf("bundle_%s_%s_latest.rds", bundle_name, pid))
  bundle_file   <- NULL
  if (file.exists(bundle_latest)) {
    bundle_file <- bundle_latest
  } else {
    pats <- sprintf("^bundle_%s_%s_.*\\.rds$", bundle_name, pid)
    cand <- list.files(bundles_dir, pattern = pats, full.names = TRUE)
    if (length(cand)) bundle_file <- cand[which.max(file.mtime(cand))]
  }
  if (is.null(bundle_file) || !file.exists(bundle_file)) {
    stop("Bundle not found. Looked for: ",
         sprintf("bundle_%s_%s_latest.rds", bundle_name, pid),
         " or the newest versioned file in: ", bundles_dir)
  }
  
  b <- readRDS(bundle_file)
  if (!length(b)) stop("Bundle '", bundle_name, "' is empty.")
  
  # ---- units (override > special > default; never blank) ---------------------
  special_units <- c(
    "TB_GDP_t"               = "ratio",
    "OUT_COMP6_SHARE_REAL_t" = "share"
  )
  unit_of <- function(symbol) {
    s <- as.character(symbol)
    if (length(unit_overrides) && s %in% names(unit_overrides)) {
      u <- unit_overrides[[s]]
      if (!is.null(u) && nzchar(u)) return(u)
    }
    if (s %in% names(special_units)) return(special_units[[s]])
    default_unit
  }
  
  # ---- normalize wide -> long (scenario/value) --------------------------------
  to_long_scn <- function(DT) {
    data.table::setDT(DT)
    meas_wide <- intersect(names(DT), scn_cfg)
    
    if (length(meas_wide) >= 1L) {
      # drop delta/pct columns if present
      drop_cols <- c(
        meas_wide,
        grep("^delta($|_)", names(DT), value = TRUE),
        grep("^pct($|_)",   names(DT), value = TRUE)
      )
      id_cols <- setdiff(names(DT), drop_cols)
      L <- data.table::melt(
        DT, id.vars = id_cols,
        measure.vars = meas_wide,
        variable.name = "scenario",
        value.name = "value"
      )
    } else if (all(c("scenario", "value") %in% names(DT))) {
      L <- data.table::copy(DT)
    } else {
      stop("Table must be either wide (scenario columns) or long (scenario/value).")
    }
    L[]
  }
  
  # ---- pct_change vs baseline (guard for ~0 baseline) ------------------------
  compute_pct_change <- function(L) {
    data.table::setDT(L)
    key_candidates <- c("n","n1","i","c","au","oc","t")
    keys <- intersect(names(L), key_candidates)
    
    base_tbl <- L[scenario == base_name, c(keys, "value"), with = FALSE]
    if (length(keys)) data.table::setkeyv(base_tbl, keys)
    data.table::setnames(base_tbl, "value", "base_value")
    
    L2 <- if (length(keys)) merge(L, base_tbl, by = keys, all.x = TRUE) else {
      # no keys -> one baseline value per whole table
      base_tbl[, scenario := base_name]
      L[, base_value := base_tbl$base_value[1]]
      L
    }
    
    L2[, pct_change := {
      b <- base_value
      v <- value
      out <- ifelse(
        scenario == base_name, NA_real_,
        ifelse(is.na(b) | abs(b) <= tol_zero,
               ifelse(is.na(v) | abs(v) <= tol_zero, 0.0, NA_real_),
               (v / b) - 1.0)
      )
      if (pct_as_percent) out <- 100 * out
      out
    }]
    L2[, base_value := NULL]
    L2[]
  }
  
  # ---- build PyPSA-like variable string --------------------------------------
  make_var <- function(symbol, DT) {
    base <- sub("_t$", "", as.character(symbol))
    
    # columns we never encode in the variable label
    ignore   <- c("n","region","t","year","scenario","value","delta","pct",
                  "pct_change","model","unit","variable")
    # preferred order of dimensions (the rest follow)
    priority <- c("i","c","n1","au","oc")
    
    dims_all <- setdiff(names(DT), ignore)
    dims_ord <- c(intersect(priority, dims_all), setdiff(dims_all, priority))
    if (!length(dims_ord)) return(base)
    
    # Build a character matrix of labels, row-aligned with DT
    # 1) coerce each dim column to character (avoids nzchar() on factors)
    labs <- lapply(dims_ord, function(d) as.character(DT[[d]]))
    labs_dt <- data.table::as.data.table(labs)
    
    # 2) empty strings -> NA (nzchar now safe because columns are character)
    for (j in seq_along(dims_ord)) {
      v <- labs_dt[[j]]
      v[!nzchar(v)] <- NA_character_
      labs_dt[[j]] <- v
    }
    
    # 3) optionally prefix with dim names (i=..., c=..., etc.)
    if (isTRUE(include_dim_names)) {
      for (j in seq_along(dims_ord)) {
        d <- dims_ord[j]
        v <- labs_dt[[j]]
        labs_dt[[j]] <- ifelse(is.na(v), NA_character_, paste0(d, "=", v))
      }
    }
    
    # 4) row-wise collapse of non-NA pieces with "|"
    parts <- apply(labs_dt, 1L, function(r) {
      r <- r[!is.na(r)]
      if (!length(r)) "" else paste(r, collapse = "|")
    })
    
    # 5) prepend the symbol base
    ifelse(parts == "", base, paste(base, parts, sep = "|"))
  }
  
  
  # ---- walk bundle, normalize and stack --------------------------------------
  rows <- vector("list", length(b))
  names(rows) <- names(b)
  
  for (sym in names(b)) {
    W <- data.table::as.data.table(b[[sym]])
    if (!nrow(W)) next
    
    # long
    L <- to_long_scn(W)
    
    # region & year conveniences
    if (!"n" %in% names(L)) L[, n := NA_character_]
    L[, region := as.character(n)]
    if ("t" %in% names(L) && !"year" %in% names(L)) {
      # if you later expose base year in cfg, replace 2014 with cfg$base_year
      L[, year := 2014L + as.integer(t)]
    }
    
    # pct_change vs baseline
    L <- compute_pct_change(L)
    
    # variable and unit
    L[, variable := make_var(sym, L)]
    u <- unit_of(sym); if (is.null(u) || is.na(u) || !nzchar(u)) u <- default_unit
    L[, unit := u]
    
    # assemble
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
  out_file <- file.path(bundles_dir, paste0(out_basename, ".csv"))
  data.table::fwrite(ALL, out_file, sep = csv_sep)
  message("Wrote template CSV: ", out_file)
  
  invisible(out_file)
}
