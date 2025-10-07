# ==============================================================================
#'====                          aaa_imports.R                           ========
# ==============================================================================

# -------------- Internal imports and global variables--------------------------

#' This file:
#' - declares package-level imports (e.g., {data.table})
#' - registers NSE symbols/column names used across the package so
#'   `R CMD check` doesn’t warn about “no visible binding”.

# ---------------- declares package-level imports ----------------------------

#' @details This file is only used for package-level settings.
#' It does not export any user-facing functions.

#' @keywords internal
#' @import data.table
#' @importFrom utils globalVariables
#' @noRd
NULL

# ----data.table NSE symbols and common column names created on the fly --------

if (getRversion() >= "2.15.1") {
  utils::globalVariables(c(
    ".SD", ".N", ".I", ".GRP",
    "value", "delta", "pct",
    "n", "n1", "au", "oc", "i", "c", "t", "year",
    "scenario", "baseline", "ff55"
  ))
}

#...............................................................................