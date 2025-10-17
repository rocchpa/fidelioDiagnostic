# R/scenarios.R

# Return cfg (if not provided) using your existing loader
.get_cfg <- function(cfg = NULL) {
  if (is.null(cfg)) load_config() else cfg
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
