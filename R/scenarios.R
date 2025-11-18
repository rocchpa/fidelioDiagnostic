# Return cfg (if not provided) using project-aware loader
.get_cfg <- function(cfg = NULL) {
  if (!is.null(cfg)) return(cfg)
  
  # First, honour the same option / env as the apps + pipeline
  cfg_path <- getOption("fidelioDiagnostics.config",
                        Sys.getenv("FIDELIO_DIAG_CONFIG", ""))
  
  if (nzchar(cfg_path) && file.exists(cfg_path)) {
    return(load_config(cfg_path))
  }
  
  # Fallback: old behaviour (relative "config/project.yml")
  load_config()
}

# Baseline is the first scenario
base_scn <- function(cfg = NULL) {
  cfg <- .get_cfg(cfg)
  stopifnot(length(cfg$scenarios) >= 1)
  cfg$scenarios[[1]]
}

# All policy scenarios (may be length 0, 1, 2, ...)
policy_scns <- function(cfg = NULL) {
  cfg <- .get_cfg(cfg)
  if (length(cfg$scenarios) <= 1) character(0) else cfg$scenarios[-1]
}

# Given a data.frame/data.table, return the scenario columns it actually has
scenario_cols <- function(x, cfg = NULL) {
  cfg <- .get_cfg(cfg)
  intersect(names(x), cfg$scenarios)
}
