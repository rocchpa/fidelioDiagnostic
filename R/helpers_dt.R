# ==============================================================================
# ====                            helpers_dt.R                             =====
# ==============================================================================

# data.table-centric helpers: type normalization, pivots, sums, aggregates.

# >>> ensure the numeric measure column is named 'value' -----------------------
norm_value_col <- function(DT) {
  if (is.null(DT) || nrow(DT) == 0L) return(DT)
  vc <- intersect(names(DT), c("value","val","VAL","Val"))
  if (length(vc) == 1L && vc != "value") data.table::setnames(DT, vc, "value")
  DT
}

# >>> coerce common key columns to stable types (chars) and t to numeric -------
norm_key_types <- function(DT) {
  if (is.null(DT)) return(DT)
  if (!data.table::is.data.table(DT)) DT <- data.table::as.data.table(DT)
  for (k in intersect(names(DT), c("n","i","c","em","au"))) DT[, (k) := as.character(get(k))]
  if ("t" %in% names(DT)) DT[, t := as.numeric(t)]
  DT
}

# >>> add delta (pol - base) and pct ((pol/base)-1) and reorder columns --------
add_var_cols <- function(DT, base = "baseline", pol = "ff55") {
  if (is.null(DT)) return(NULL)
  DT <- norm_key_types(DT)
  if (!all(c(base, pol) %in% names(DT))) return(DT)
  DT[, `:=`(
    delta = get(pol) - get(base),
    pct   = data.table::fifelse(is.na(get(base)) | get(base) == 0, NA_real_, (get(pol)/get(base)) - 1)
  )]
  key_cols <- setdiff(names(DT), c(base, pol, "delta", "pct"))
  data.table::setcolorder(DT, c(key_cols, base, pol, "delta", "pct"))
  DT[]
}

# >>> compute value = price * quantity, then sum by keys for given scenarios----
sum_value_by <- function(priceDT, qtyDT, by = c("n","t"), filter = NULL,
                         scenarios = c("baseline","ff55")) {
  if (is.null(priceDT) || is.null(qtyDT)) return(NULL)
  P <- norm_key_types(data.table::copy(priceDT))
  Q <- norm_key_types(data.table::copy(qtyDT))
  
  if (!is.null(filter)) {
    for (nm in names(filter)) {
      if (nm %in% names(P)) P <- P[get(nm) %in% filter[[nm]]]
      if (nm %in% names(Q)) Q <- Q[get(nm) %in% filter[[nm]]]
    }
  }
  
  scn_cols  <- c("baseline","ff55","delta","pct")
  join_keys <- intersect(setdiff(names(P), scn_cols), setdiff(names(Q), scn_cols))
  if (!all(by %in% join_keys)) by <- intersect(by, join_keys)
  if (length(join_keys) == 0L) return(NULL)
  
  scenarios <- intersect(scenarios, intersect(names(P), names(Q)))
  if (length(scenarios) == 0L) return(NULL)
  
  data.table::setnames(P, scenarios, paste0("P_", scenarios))
  data.table::setnames(Q, scenarios, paste0("Q_", scenarios))
  data.table::setkeyv(P, join_keys); data.table::setkeyv(Q, join_keys)
  
  M <- merge(P, Q, by = join_keys, allow.cartesian = TRUE)
  if (nrow(M) == 0L) return(NULL)
  
  if ("t" %in% names(M)) M[, t := as.numeric(t)]
  for (s in scenarios) M[, (s) := get(paste0("P_", s)) * get(paste0("Q_", s))]
  
  out <- M[, lapply(.SD, sum, na.rm = TRUE), .SDcols = scenarios, by = by]
  out[]
}

# >>> add EU28, NonEU28,and WORLD rows by summing additive variables over'n'----
add_macroregions_additive <- function(DT, eu_members,
                                      scenarios = c("baseline","ff55")) {
  if (is.null(DT) || !"n" %in% names(DT)) return(DT)
  DT <- norm_key_types(DT)
  
  base_rows <- DT[!(n %in% c("EU28","NonEU28","WORLD"))]
  
  key_cols <- setdiff(names(base_rows), c(scenarios, "delta","pct"))
  by_no_n  <- setdiff(key_cols, "n")
  if (length(by_no_n) == length(key_cols)) by_no_n <- key_cols  # safety fallback
  
  agg_subset <- function(sub, tag) {
    if (nrow(sub) == 0L) return(NULL)
    A <- sub[, lapply(.SD, sum, na.rm=TRUE), .SDcols = scenarios, by = by_no_n]
    A[, n := tag]
    data.table::setcolorder(A, c("n", by_no_n, scenarios))
    A[]
  }
  
  EU    <- agg_subset(base_rows[n %chin% eu_members],  "EU28")
  NonEU <- agg_subset(base_rows[!n %chin% eu_members], "NonEU28")
  
  WLD <- data.table::rbindlist(list(EU, NonEU), use.names = TRUE, fill = TRUE)
  if (!is.null(WLD)) {
    WLD <- WLD[, lapply(.SD, sum, na.rm=TRUE), .SDcols = scenarios, by = by_no_n]
    WLD[, n := "WORLD"]
    data.table::setcolorder(WLD, c("n", by_no_n, scenarios))
  }
  
  out <- data.table::rbindlist(list(base_rows, EU, NonEU, WLD), use.names = TRUE, fill = TRUE)
  out[]
}

# >>> pivot a long table with 'scenario' into a wide table with one col per scn----
wide_by_scenario <- function(DT, scenarios) {
  if (is.null(DT) || !nrow(DT)) return(DT)
  DT <- norm_key_types(DT)
  dims <- setdiff(names(DT), c("scenario","value"))
  if (!length(dims)) stop("No key dimensions found to pivot.")
  w <- data.table::dcast(DT, as.formula(paste(paste(dims, collapse = "+"), "~ scenario")),
                         value.var = "value", fill = NA_real_)
  for (s in scenarios) if (!s %in% names(w)) w[, (s) := NA_real_]
  data.table::setcolorder(w, c(dims, scenarios[scenarios %in% names(w)]))
  w[]
}
