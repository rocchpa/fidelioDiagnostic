# ==============================================================================
# ====                          extract_registry                        ========
# ==============================================================================

# ----------------------------   Helpers        --------------------------------

#' Symbols to extract and their key dimensions
#' @export
plan_extractions <- function(cfg) {
  # Build registry directly as a data.table with a list-column for dims
  reg <- data.table::data.table(
    symbol = c(
      # macro (n,t)
      "GDPr_t","HSAVR_t","HDY_VAL_t","GSUR_VAL_t","GINV_VAL_t","U_t","ir_t",
      "TB_t",
      # inv & prices (n,i,t)
      "Q_t","I_PP_t","P_I_t","K_t","L_t","P_Q_t",
      # input prices (n,i,oc,t)
      "P_INPUT_t",
      # CPI (n,au,t)
      "P_CPI_t",
      # trade (quantities + prices)
      "USE_PP_t","M_TOT_t","P_USE_t","P_Mcif_t","BITRADE_t",
      # emissions
      "GHG_t"
    ),
    dims = list(
      # macro
      c("n","t"), c("n","t"), c("n","t"), c("n","t"), c("n","t"), c("n","t"),
      c("n","t"), c("n","t"),
      # inv & prices
      c("n","i","t"), c("n","i","t"), c("n","i","t"), c("n","i","t"), c("n","i","t"), 
      c("n","i","t"),
      # input prices
      c("n","i","oc","t"),
      # CPI
      c("n","au","t"),
      # trade
      c("n","c","au","t"), c("n","c","t"), c("n","c","au","t"), c("n","c","t"), 
      c("n","n1","c","t"),
      # emissions
      c("n","i","t")
    ),
    label = c(
      "Real GDP","Household saving rate","HH disposable income (val)",
      "Gov surplus (val)","Gov investment (val)","Unemployment","Interest rate",
      "Trade balance","Output","Investment (PP)","Investment price","Capital","Labor",
      "Output price", "Input price (oc)","Consumer price index",
      "Use at purchasers' prices","Total imports","Use price",
      "Import price (cif)","Bilateral trade", "Emissions"
    )
  )
  
  # filter by YAML list if provided (use base indexing to avoid scoping issues)
  keep <- cfg$extract$include
  if (!is.null(keep) && length(keep) > 0L) {
    reg <- reg[reg[["symbol"]] %in% keep, ]
  }
  
  if (nrow(reg) == 0L) stop("No symbols selected in extract registry (after filtering).")
  reg
}
