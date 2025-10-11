# ==============================================================================
# ====                          pipeline                                ========
# ==============================================================================

#' Run the end-to-end pipeline: extract → wide → aggregates → deltas → derived → save
#' @export
run_pipeline <- function(config = "config/project.yml") {
  cfg <- if (is.list(config)) config else load_config()
  print_runtime_info(cfg)
  
  # 1) what to extract
  reg <- plan_extractions(cfg)
  
  # 2) extract long (dims + scenario + value)
  raw <- extract_all(cfg, reg)
  
  # 3) wide with aggregates + Δ/%  (this reproduces your results_by_symbol)
  additive_syms <- tryCatch(cfg$aggregations$additive_symbols, error = function(e) NULL)
  if (is.null(additive_syms)) {
    additive_syms <- c(
      "GDPr_t","HDY_VAL_t","GSUR_VAL_t","GINV_VAL_t",
      "Q_t","I_PP_t","K_t","L_t","U_t",
      "USE_PP_t","M_TOT_t","BITRADE_t","GHG_t",
      "P_USE_t","P_Mcif_t","P_I_t","P_Q_t","P_INPUT_t"
    )
  }
  base_scn <- cfg$scenarios[1]; pol_scn <- cfg$scenarios[length(cfg$scenarios)]
  
  postproc <- function(dt, name) {
    if (is.null(dt) || !nrow(dt)) return(NULL)
    w <- wide_by_scenario(dt, cfg$scenarios)
    if ("n" %in% names(w) && name %in% additive_syms && !is.null(cfg$groups$EU28)) {
      w <- add_macroregions_additive(w, eu_members = cfg$groups$EU28, scenarios = cfg$scenarios)
    }
    add_var_cols(w, base = base_scn, pol = pol_scn)
  }
  results_by_symbol <- lapply(names(raw), function(sym) postproc(raw[[sym]], sym))
  names(results_by_symbol) <- names(raw)
  
  # 4) your “derived from wide” block
  derived <- derive_from_wide(results_by_symbol, cfg)
  # ---------- Generic promoter: base -> derived (config driven) ----------
  # Coerce to derived (wide) shape when needed
  ensure_derived_shape <- function(DT, key_cols = NULL) {
    DT <- data.table::as.data.table(DT)
    
    # auto-detect keys if not given
    if (is.null(key_cols)) {
      key_cols <- intersect(names(DT), c("n","n1","i","c","au","oc","t"))
      if (!length(key_cols)) key_cols <- character(0)
    }
    
    # A) already wide (baseline/ff55 present)
    if (all(c("baseline","ff55") %in% names(DT))) {
      if (!"delta" %in% names(DT)) DT[, delta := ff55 - baseline]
      if (!"pct"   %in% names(DT)) DT[,  pct  := data.table::fifelse(abs(baseline) > .Machine$double.eps,
                                                                     delta / baseline, NA_real_)]
      return(DT[])
    }
    
    # B) long (scenario/value) -> promote to wide
    need <- c(key_cols, "scenario", "value")
    if (all(need %in% names(DT))) {
      W <- data.table::dcast(DT,
                             as.formula(paste(paste(key_cols, collapse = " + "), "~ scenario")),
                             value.var = "value")
      # Normalize scenario names if necessary
      sc_other <- setdiff(names(W), c(key_cols, "delta","pct"))
      if (!all(c("baseline","ff55") %in% names(W)) && length(sc_other) >= 2)
        data.table::setnames(W, sc_other[1:2], c("baseline","ff55"))
      W <- W[!is.na(baseline) & !is.na(ff55)]
      W[, delta := ff55 - baseline]
      W[,  pct  := data.table::fifelse(abs(baseline) > .Machine$double.eps,
                                       delta / baseline, NA_real_)]
      return(W[])
    }
    
    # otherwise leave as-is
    DT[]
  }
  
  # Read config lists (with safe defaults)
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
  
  # 5) save
  save_artifacts(results_by_symbol, cfg, subdir = "base")
  if (length(derived)) save_artifacts(derived, cfg, subdir = "derived")
  
  message("Built results_by_symbol: ", paste(names(results_by_symbol), collapse = ", "))
  if (length(derived)) message("Built derived: ", paste(names(derived), collapse = ", "))
  
  invisible(list(cfg = cfg, raw = raw, results_by_symbol = results_by_symbol, derived = derived))
  
  
  # Optional CSV exporter (reads the saved bundle)
  if (isTRUE(cfg$export_csv$enabled)) {
    export_results_csv(
      cfg            = cfg,
      bundle_name    = cfg$save$bundles$results_app %>% { if (length(.) ) "results_app" else "results_app" },
      out_basename   = cfg$export_csv$out_basename %||% "results_bundle_template",
      model_name     = cfg$export_csv$model_name    %||% "FIDELIO",
      pct_as_percent = isTRUE(cfg$export_csv$pct_as_percent),
      include_dim_names = isTRUE(cfg$export_csv$include_dim_names),
      unit_overrides = cfg$export_csv$unit_overrides %||% list()
    )
  }
  
  
  
  
}
