# ==============================================================================
# ====                          pipeline                                ========
# ==============================================================================

# ---- Run-level once-guards (single system) -----------------------------------
.pipeline_flags_env <- new.env(parent = emptyenv())

.pipeline_reset_flags <- function() {
  rm(list = ls(.pipeline_flags_env, all.names = TRUE), envir = .pipeline_flags_env)
}

.pipeline_flag_set <- function(key) {
  if (exists(key, envir = .pipeline_flags_env, inherits = FALSE)) return(TRUE)
  assign(key, TRUE, envir = .pipeline_flags_env)
  FALSE
}

#' Run the end-to-end pipeline: extract → wide → aggregates → deltas → derived → save
#' @export
run_pipeline <- function(config = "config/project.yml") {
  .pipeline_reset_flags()
  
  # --- config & runtime --------------------------------------------------------
  # NOTE: respect the 'config' argument (character path or pre-built list)
  cfg <- if (is.list(config)) {
    config
  } else {
    fidelioDiagnostics:::load_config(config, verbose = TRUE)
  }
  print_runtime_info(cfg)
  
  # ---- NEW: capture the config path for downstream apps -----------------------
  cfg_path <- if (is.character(config) && file.exists(config)) {
    normalizePath(config, winslash = "/", mustWork = TRUE)
  } else {
    ap <- try(attr(cfg, "config_path", exact = TRUE), silent = TRUE)
    if (!inherits(ap, "try-error") && !is.null(ap) && nzchar(ap) && file.exists(ap)) {
      normalizePath(ap, winslash = "/", mustWork = TRUE)
    } else {
      ""  # unknown (still fine; apps will fall back to other strategies)
    }
  }
  
  # Expose config & outputs for the Shiny apps (works across the whole session)
  # Apps will read these so they never depend on getwd()
  if (nzchar(cfg_path)) {
    options(fidelioDiagnostics.config  = cfg_path)
  }
  # Always set outputs option too (derived is what apps actually consume)
  derived_dir <- file.path(cfg$paths$outputs, "derived")
  dir.create(derived_dir, showWarnings = FALSE, recursive = TRUE)
  options(fidelioDiagnostics.outputs = normalizePath(derived_dir, winslash = "/", mustWork = FALSE))
  
  # Persist a tiny context file so apps can also discover paths across sessions
  context_file <- file.path(derived_dir, "last_run_context.rds")
  saveRDS(list(
    cfg_path    = if (nzchar(cfg_path)) cfg_path else NULL,
    outputs_dir = normalizePath(derived_dir, winslash = "/", mustWork = FALSE),
    project_id  = tryCatch(cfg$project$id, error = function(e) NULL),
    when        = Sys.time()
  ), context_file)
  
  ts <- .ts_stamp()
  `%||%` <- function(a, b) if (is.null(a)) b else a
  
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
  
  # --- 3) wide with aggregates + Δ/% (build results_by_symbol) -----------------
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
    w <- wide_by_scenario(dt, scenarios = cfg$scenarios, cfg = cfg)
    
    if ("n" %in% names(w) && name %in% additive_syms && !is.null(cfg$groups$EU28)) {
      w <- add_macroregions_additive(w, eu_members = cfg$groups$EU28, scenarios = cfg$scenarios, cfg = cfg)
    }
    if (length(pol_scns)) {
      w <- add_var_cols_multi(w, base = base_scn, policies = pol_scns, cfg = cfg)
    }
    w[]
  }
  
  results_by_symbol <- lapply(names(raw), function(sym) postproc(raw[[sym]], sym))
  names(results_by_symbol) <- names(raw)
  
  # --- 4) derived from wide ----------------------------------------------------
  derived <- derive_from_wide(results_by_symbol, cfg)
  
  ensure_derived_shape <- function(DT, key_cols = NULL) {
    if (is.null(DT)) return(DT)
    DT <- data.table::as.data.table(DT)
    
    if (is.null(key_cols)) {
      key_cols <- intersect(names(DT), c("n","n1","i","c","au","oc","t"))
      if (!length(key_cols)) key_cols <- character(0)
    }
    
    sc_cols_present <- intersect(names(DT), cfg$scenarios)
    if (length(sc_cols_present) >= 1L) {
      if (base_scn %in% sc_cols_present && length(pol_scns)) {
        policies_here <- intersect(pol_scns, sc_cols_present)
        if (length(policies_here)) add_var_cols_multi(DT, base = base_scn, policies = policies_here, cfg = cfg)
      }
      return(DT[])
    }
    
    need <- c(key_cols, "scenario", "value")
    if (all(need %in% names(DT))) {
      f <- if (length(key_cols)) as.formula(paste(paste(key_cols, collapse = " + "), "~ scenario"))
      else as.formula("1 ~ scenario")
      W <- data.table::dcast(DT, f, value.var = "value", fill = NA_real_)
      sc_cols_present <- intersect(names(W), cfg$scenarios)
      if (base_scn %in% sc_cols_present && length(pol_scns)) {
        policies_here <- intersect(pol_scns, sc_cols_present)
        if (length(policies_here)) add_var_cols_multi(W, base = base_scn, policies = policies_here, cfg = cfg)
      }
      return(W[])
    }
    DT[]
  }
  
  incl <- try(cfg$derive$include_from_base, silent = TRUE); if (inherits(incl, "try-error") || is.null(incl)) incl <- character(0)
  key_hints <- try(cfg$derive$keys, silent = TRUE);        if (inherits(key_hints, "try-error") || is.null(key_hints)) key_hints <- list()
  
  for (nm in incl) {
    if (nm %in% names(derived)) next
    if (!(nm %in% names(results_by_symbol))) next
    D <- data.table::copy(results_by_symbol[[nm]])
    if (!"year" %in% names(D) && "t" %in% names(D)) D[, year := 2014L + as.integer(t)]
    keys <- key_hints[[nm]] %||% NULL
    Dd   <- ensure_derived_shape(D, key_cols = keys)
    if (!is.null(Dd) && nrow(Dd)) derived[[nm]] <- Dd
  }
  
  # --- 5) save -----------------------------------------------------------------
  if (!.pipeline_flag_set("save-base")) {
    save_artifacts(results_by_symbol, cfg, subdir = "base",    ts_shared = ts)
  } else message("[SKIP] save_artifacts(base): already saved in this run.")
  
  if (length(derived)) {
    if (!.pipeline_flag_set("save-derived")) {
      save_artifacts(derived, cfg, subdir = "derived", ts_shared = ts)
    } else message("[SKIP] save_artifacts(derived): already saved in this run.")
  }
  
  message("Built results_by_symbol: ", paste(names(results_by_symbol), collapse = ", "))
  if (length(derived)) message("Built derived: ", paste(names(derived), collapse = ", "))
  
  # --- 6) One bundling pass at the end ----------------------------------------
  out_root <- tryCatch(resolve_outputs_dir(cfg), error = function(e) cfg$paths$outputs)
  
  universe <- results_by_symbol
  for (nm in names(derived)) universe[[nm]] <- derived[[nm]]
  
  make_bundles(
    objs         = universe,               # universe to pick from
    out_root     = out_root,
    bundles_spec = cfg$save$bundles,       # e.g. results_app / diagnostic_app
    cfg          = cfg,
    ts_shared    = ts
  )
  
  # --- 7) Optional CSV export (now uses a proper bundle name) ------------------
  if (isTRUE(cfg$export_csv$enabled)) {
    bundle_name <- cfg$export_csv$bundle_name %||% "results_app"
    if (!.pipeline_flag_set(paste0("csv:", bundle_name))) {
      export_results_csv(
        cfg               = cfg,
        bundle_name       = bundle_name,
        out_basename      = cfg$export_csv$out_basename   %||% "results_bundle_template",
        model_name        = cfg$export_csv$model_name     %||% "FIDELIO",
        pct_as_percent    = isTRUE(cfg$export_csv$pct_as_percent),
        include_dim_names = isTRUE(cfg$export_csv$include_dim_names),
        unit_overrides    = cfg$export_csv$unit_overrides %||% list()
      )
    } else {
      message("[SKIP] export_results_csv(): already written for '", bundle_name, "' in this run.")
    }
  }
  
  invisible(list(cfg = cfg, raw = raw, results_by_symbol = results_by_symbol, derived = derived))
}
