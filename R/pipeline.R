# ==============================================================================
# ====                          pipeline                                ========
# ==============================================================================

#' Run the end-to-end pipeline: extract → wide → aggregates → deltas → derived → save
#' @export
run_pipeline <- function(config = "config/project.yml") {
  cfg <- if (is.character(config)) load_config(config) else config
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
  
  # 5) save
  save_artifacts(results_by_symbol, cfg, subdir = "base")
  if (length(derived)) save_artifacts(derived, cfg, subdir = "derived")
  
  message("Built results_by_symbol: ", paste(names(results_by_symbol), collapse = ", "))
  if (length(derived)) message("Built derived: ", paste(names(derived), collapse = ", "))
  
  invisible(list(cfg = cfg, raw = raw, results_by_symbol = results_by_symbol, derived = derived))
}
