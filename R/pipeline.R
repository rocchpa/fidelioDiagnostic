# ==============================================================================
# ====                          pipeline                                ========
# ==============================================================================

#' Run the end-to-end pipeline: extract → wide → aggregates → deltas → derived → save
#' @export
run_pipeline <- function(config = "config/project.yml") {
  # --- config & runtime --------------------------------------------------------
  cfg <- if (is.list(config)) config else load_config()
  print_runtime_info(cfg)
  
  # convenience
  scenarios <- cfg$scenarios
  if (is.null(scenarios) || length(scenarios) == 0L) {
    stop("cfg$scenarios is empty. Ensure YAML defines 'scenarios:' with baseline first.")
  }
  base_scn <- scenarios[1]
  pol_scns <- scenarios[-1]
  
  # --- 1) what to extract ------------------------------------------------------
  reg <- plan_extractions(cfg)
  
  # --- 2) extract long (dims + scenario + value) -------------------------------
  raw <- extract_all(cfg, reg)
  
  # --- 3) wide with aggregates + Δ/%  (build results_by_symbol) ----------------
  additive_syms <- tryCatch(cfg$aggregations$additive_symbols, error = function(e) NULL)
  if (is.null(additive_syms)) {
    additive_syms <- c(
      "GDPr_t","HDY_VAL_t","GSUR_VAL_t","GINV_VAL_t",
      "Q_t","I_PP_t","K_t","L_t","U_t",
      "USE_PP_t","M_TOT_t","BITRADE_t","GHG_t",
      "P_USE_t","P_Mcif_t","P_I_t","P_Q_t","P_INPUT_t"
    )
  }
  
  postproc <- function(dt, name) {
    if (is.null(dt) || !nrow(dt)) return(NULL)
    
    # wide per scenario
    w <- wide_by_scenario(dt, scenarios = cfg$scenarios, cfg = cfg)
    
    # optional macro-regions for additive symbols with 'n'
    if ("n" %in% names(w) && name %in% additive_syms && !is.null(cfg$groups$EU28)) {
      w <- add_macroregions_additive(w, eu_members = cfg$groups$EU28, scenarios = cfg$scenarios, cfg = cfg)
    }
    
    # multi-policy deltas/% (no-op if there are no policy scenarios)
    if (length(pol_scns)) {
      w <- add_var_cols_multi(w, base = base_scn, policies = pol_scns, cfg = cfg)
    }
    
    w[]
  }
  
  results_by_symbol <- lapply(names(raw), function(sym) postproc(raw[[sym]], sym))
  names(results_by_symbol) <- names(raw)
  
  # --- 4) derived from wide ----------------------------------------------------
  derived <- derive_from_wide(results_by_symbol, cfg)
  
  # ---------- Generic promoter: base -> derived (config driven) ----------
  # Coerce to derived (wide) shape when needed
  ensure_derived_shape <- function(DT, key_cols = NULL) {
    if (is.null(DT)) return(DT)
    DT <- data.table::as.data.table(DT)
    
    # auto-detect keys if not given
    if (is.null(key_cols)) {
      key_cols <- intersect(names(DT), c("n","n1","i","c","au","oc","t"))
      if (!length(key_cols)) key_cols <- character(0)
    }
    
    # A) already wide (has scenario columns)
    sc_cols_present <- intersect(names(DT), cfg$scenarios)
    if (length(sc_cols_present) >= 1L) {
      # compute multi deltas/% for ALL available policy columns (in cfg order)
      base <- base_scn
      if (base %in% sc_cols_present && length(pol_scns)) {
        policies_here <- intersect(pol_scns, sc_cols_present)
        if (length(policies_here)) {
          add_var_cols_multi(DT, base = base, policies = policies_here, cfg = cfg)
        }
      }
      return(DT[])
    }
    
    # B) long (scenario/value) -> promote to wide
    need <- c(key_cols, "scenario", "value")
    if (all(need %in% names(DT))) {
      W <- data.table::dcast(
        DT,
        as.formula(paste(paste(key_cols, collapse = " + "), "~ scenario")),
        value.var = "value", fill = NA_real_
      )
      
      # compute multi deltas/% using available scenario columns
      sc_cols_present <- intersect(names(W), cfg$scenarios)
      if (base_scn %in% sc_cols_present && length(pol_scns)) {
        policies_here <- intersect(pol_scns, sc_cols_present)
        if (length(policies_here)) {
          add_var_cols_multi(W, base = base_scn, policies = policies_here, cfg = cfg)
        }
      }
      return(W[])
    }
    
    # otherwise leave as-is
    DT[]
  }
  
  # Read config lists (with safe defaults)
  `%||%` <- function(a, b) if (is.null(a)) b else a
  incl <- try(cfg$derive$include_from_base, silent = TRUE)
  if (inherits(incl, "try-error") || is.null(incl)) incl <- character(0)
  
  key_hints <- try(cfg$derive$keys, silent = TRUE)
  if (inherits(key_hints, "try-error") || is.null(key_hints)) key_hints <- list()
  
  # Promote each requested base symbol into 'derived' if not already there
  for (nm in incl) {
    if (nm %in% names(derived)) next
    if (!(nm %in% names(results_by_symbol))) next  # not in base
    
    D <- data.table::copy(results_by_symbol[[nm]])
    # add calendar year if useful
    if (!"year" %in% names(D) && "t" %in% names(D)) D[, year := 2014L + as.integer(t)]
    
    keys <- key_hints[[nm]] %||% NULL
    Dd   <- ensure_derived_shape(D, key_cols = keys)
    
    if (!is.null(Dd) && nrow(Dd)) derived[[nm]] <- Dd
  }
  
  # --- 5) save -----------------------------------------------------------------
  save_artifacts(results_by_symbol, cfg, subdir = "base")
  if (length(derived)) save_artifacts(derived, cfg, subdir = "derived")
  
  message("Built results_by_symbol: ", paste(names(results_by_symbol), collapse = ", "))
  if (length(derived)) message("Built derived: ", paste(names(derived), collapse = ", "))
  
  # --- 6) Optional CSV export (runs now, before returning) ---------------------
  if (isTRUE(cfg$export_csv$enabled)) {
    bundle_name <- if (!is.null(cfg$save$bundles$results_app)) cfg$save$bundles$results_app else "results_app"
    export_results_csv(
      cfg               = cfg,
      bundle_name       = bundle_name,
      out_basename      = cfg$export_csv$out_basename   %||% "results_bundle_template",
      model_name        = cfg$export_csv$model_name     %||% "FIDELIO",
      pct_as_percent    = isTRUE(cfg$export_csv$pct_as_percent),
      include_dim_names = isTRUE(cfg$export_csv$include_dim_names),
      unit_overrides    = cfg$export_csv$unit_overrides %||% list()
    )
  }
  
  # --- return ------------------------------------------------------------------
  invisible(list(cfg = cfg, raw = raw, results_by_symbol = results_by_symbol, derived = derived))
}
