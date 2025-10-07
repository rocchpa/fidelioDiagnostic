# ==============================================================================
# ====                          derive_indicators                       ========
# ==============================================================================

# ---- Sector group mapping: generalized by leading letter ---------------------
# high_energy_manufacturing stays explicit; everything else is pattern-based.
# - primary  = codes starting with A or B
# - low_energy_manufacturing = codes starting with C EXCEPT the high-energy list
# - utilities_construction   = codes starting with D, E, or F
# - market_services          = codes starting with G..N (G,H,I,J,K,L,M,N)
# - public_personal_services = codes starting with O..U (O,P,Q,R,S,T,U)
# Any leftover codes -> "other".
.sector_group6_vec <- function(ic,
                               high_energy = c("C17","C19","C20","C23","C24")) {
  ic <- as.character(ic)
  
  # match "C24", "C24.", "C24_xx", etc.
  he <- if (length(high_energy)) {
    Reduce(`|`, lapply(high_energy, function(h) startsWith(ic, h)))
  } else {
    rep(FALSE, length(ic))
  }
  
  primary   <- grepl("^[AB]",  ic)
  low_man   <- grepl("^C",     ic) & !he
  util_cons <- grepl("^[DEF]", ic)
  market    <- grepl("^[G-N]", ic)  # ASCII range G..N
  pub_pers  <- grepl("^[O-U]", ic)  # ASCII range O..U
  
  data.table::fcase(
    he,        "high_energy_manufacturing",
    low_man,   "low_energy_manufacturing",
    primary,   "primary",
    util_cons, "utilities_construction",
    market,    "market_services",
    pub_pers,  "public_personal_services",
    default =  "other"
  )
}

# Add 6-group sector aggregates to an (n,i,t, <scenarios...>) wide table.
# If `append_original = TRUE` it keeps original industries and ADDs grouped rows.
add_sector_groups_additive <- function(DT, scenarios, append_original = TRUE) {
  stopifnot(is.data.table(DT), all(scenarios %in% names(DT)))
  if (!"i" %in% names(DT)) return(DT)
  DT <- data.table::copy(DT)
  
  # drop any pre-aggregated industry rows you might carry (e.g., "TOT")
  if ("i" %in% names(DT))
    DT <- DT[i != "TOT"]
  
  # map to 6 groups and aggregate
  G <- DT[, .(i = .sector_group6_vec(i), n, t, across = 1L)]
  G <- cbind(G[, .(i, n, t)], DT[, ..scenarios])  # rebind scenario cols
  G <- G[, lapply(.SD, sum, na.rm = TRUE), .SDcols = scenarios, by = .(n, i, t)]
  
  # optional total across groups (nice for composition checks)
  TOT <- G[, lapply(.SD, sum, na.rm = TRUE), .SDcols = scenarios, by = .(n, t)]
  TOT[, i := "TOT_G6"]
  data.table::setcolorder(TOT, c("n","i","t", scenarios))
  
  out <- if (isTRUE(append_original)) {
    data.table::rbindlist(list(DT, G, TOT), use.names = TRUE)
  } else {
    data.table::rbindlist(list(G, TOT), use.names = TRUE)
  }
  out[]
}


# --- Derived indicators -----------------------------------


#' Compute all derived indicators requested in config
#' @export
derive_all <- function(raw, cfg) {
  # you can drive which ones to compute from YAML (cfg$derived$include)
  wanted <- cfg$derived$include
  if (is.null(wanted) || !length(wanted)) {
    # default: compute a few common ones
    wanted <- c("TB_GDP_t")
  }
  
  out <- list()
  
  if ("TB_GDP_t" %in% wanted)    out$TB_GDP_t      <- derive_TB_GDP(raw)

  # return only non-null results
  Filter(Negate(is.null), out)
}
# ---- helpers used by many derived indicators ----

# select base/policy columns from a wide table and rename with a prefix
.pick_base_pol <- function(DT, keys = c("n","t"), base_scn, pol_scn, prefix) {
  if (is.null(DT) || !all(c(base_scn, pol_scn) %in% names(DT))) return(NULL)
  cols <- c(intersect(keys, names(DT)), base_scn, pol_scn)
  out  <- DT[, ..cols]
  data.table::setnames(out, c(base_scn, pol_scn), paste0(prefix, c("_base","_pol")))
  out
}

# region map for BITRADE regionalization (you can later move these lists to YAML)
.region_map_vec <- function(cc) {
  seu     <- c("CYP","ESP","GRC","ITA","MLT","PRT")
  eeu     <- c("BGR","CZE","EST","HRV","HUN","LTU","LVA","POL","ROU","SVN","SVK")
  nweu    <- c("AUT","BEL","DEU","DNK","FIN","FRA","GBR","IRL","LUX","NLD","SWE")
  OECD_NO_USA <- c("CAN","JPN","KOR","AUS","CHE","NOR","TUR","MEX")  # USA separate
  nonOECD <- c("RUS","BRA","ARG","IDN","ZAF","SAU")
  
  data.table::fcase(
    cc %chin% eeu,          "EEU",
    cc %chin% nweu,         "NWEU",
    cc %chin% seu,          "SEU",
    cc == "USA",            "USA",
    cc == "CHN",            "CHN",
    cc == "IND",            "IND",
    cc %chin% OECD_NO_USA,  "OECD",
    cc %chin% nonOECD,      "NonOECD",
    default = "ROW"
  )
}

# ---- derive indicators from *wide* base tables (fixed .SD sums) ----

#' Compute all derived indicators that were previously built from wide tables
#' @param rs named list of wide data.tables (results_by_symbol)
#' @param cfg config (for scenarios + EU28)
#' @return named list of wide data.tables
#' @export
derive_from_wide <- function(rs, cfg) {
  if (is.null(rs) || !length(rs)) return(list())

  base_scn <- cfg$scenarios[1]
  pol_scn  <- cfg$scenarios[length(cfg$scenarios)]
  EU28     <- cfg$groups$EU28

  out <- list()

    # --- I_TOT_PP_t = sum_i I_PP_t ----------------------------------------------
  I_PP <- rs[["I_PP_t"]]
  if (!is.null(I_PP)) {
    I_PP <- norm_key_types(I_PP)
    I_TOT <- I_PP[, lapply(.SD, sum, na.rm = TRUE),
                  .SDcols = c(base_scn, pol_scn), by = .(n, t)]
    I_TOT <- add_macroregions_additive(I_TOT, EU28, scenarios = cfg$scenarios)
    I_TOT <- add_var_cols(I_TOT, base = base_scn, pol = pol_scn)
    out[["I_TOT_PP_t"]] <- I_TOT
  }
  
  # --- TB_t = X - M using value sums -----------------------------------------
  TB_X <- sum_value_by(
    priceDT   = rs[["P_USE_t"]],
    qtyDT     = rs[["USE_PP_t"]],
    by        = c("n","t"),
    filter    = list(au = "X"),
    scenarios = cfg$scenarios
  )
  TB_M <- sum_value_by(
    priceDT   = rs[["P_Mcif_t"]],
    qtyDT     = rs[["M_TOT_t"]],
    by        = c("n","t"),
    scenarios = cfg$scenarios
  )
  if (!is.null(TB_X) && !is.null(TB_M)) {
    TB <- merge(TB_X, TB_M, by = c("n","t"), all = TRUE, suffixes = c("_X","_M"))
    for (s in cfg$scenarios) {
      x <- paste0(s,"_X"); m <- paste0(s,"_M")
      if (all(c(x, m) %in% names(TB))) TB[, (s) := get(x) - get(m)]
    }
    TB <- TB[, c("n","t", base_scn, pol_scn), with = FALSE]
    TB <- add_macroregions_additive(TB, EU28, scenarios = cfg$scenarios)
    TB <- add_var_cols(TB, base = base_scn, pol = pol_scn)
    out[["TB_t"]] <- TB
  }

  # --- Real TB from BITRADE (no pre-agg rows) → TBr_t -------------------------
  BT <- rs[["BITRADE_t"]]
  if (!is.null(BT) && is.data.table(BT)) {
    BT <- norm_key_types(BT)
    if ("n"  %in% names(BT)) BT <- BT[!(n  %in% c("EU28","NonEU28","WORLD"))]
    if ("n1" %in% names(BT)) BT <- BT[!(n1 %in% c("EU28","NonEU28","WORLD"))]
    if ("c"  %in% names(BT)) BT <- BT[c != "TOT"]

    meas_cols <- intersect(names(BT), cfg$scenarios)
    if (length(meas_cols) >= 1) {
      # exporter totals
      X_tot <- BT[, lapply(.SD, sum), .SDcols = meas_cols, by = .(n1, t)]
      X_tot[, n := n1][, n1 := NULL]
      data.table::setcolorder(X_tot, c("n","t",meas_cols))
      # importer totals
      M_tot <- BT[, lapply(.SD, sum), .SDcols = meas_cols, by = .(n, t)]
      data.table::setcolorder(M_tot, c("n","t",meas_cols))
      # TB = X - M
      TB_cty <- merge(X_tot, M_tot, by = c("n","t"), all = TRUE, suffixes = c("_X","_M"))
      for (m in meas_cols) {
        x  <- paste0(m,"_X"); mm <- paste0(m,"_M")
        TB_cty[is.na(get(x)),  (x)  := 0]
        TB_cty[is.na(get(mm)), (mm) := 0]
        TB_cty[, (m) := get(x) - get(mm)]
      }
      TB_cty <- TB_cty[, c("n","t",meas_cols), with = FALSE]
      TBr <- add_macroregions_additive(TB_cty, EU28, scenarios = cfg$scenarios)
      TBr <- add_var_cols(TBr, base = base_scn, pol = pol_scn)
      out[["TBr_t"]] <- TBr
    }
  }
  
  # --- TB/GDP (from aggregated wide TB & GDP) ---------------------------------
  TB  <- out[["TB_t"]]
  GDP <- rs[["GDPr_t"]]
  if (!is.null(TB) && !is.null(GDP)) {
    TB  <- norm_key_types(TB);  GDP <- norm_key_types(GDP)
    TB_GDP <- merge(TB[,  c("n","t", base_scn, pol_scn), with = FALSE],
                    GDP[, c("n","t", base_scn, pol_scn), with = FALSE],
                    by = c("n","t"), suffixes = c("_TB","_GDP"))
    for (s in c(base_scn, pol_scn)) {
      TB_GDP[, (s) := fifelse(get(paste0(s,"_GDP")) == 0 | is.na(get(paste0(s,"_GDP"))),
                              NA_real_, get(paste0(s,"_TB")) / get(paste0(s,"_GDP")))]
    }
    TB_GDP <- TB_GDP[, c("n","t", base_scn, pol_scn), with = FALSE]
    TB_GDP <- add_var_cols(TB_GDP, base = base_scn, pol = pol_scn)
    out[["TB_GDP_t"]] <- TB_GDP
  }
  
  # --- Foreign Savings FS_t ---------------------------------------------------
  FS_INV <- sum_value_by(
    priceDT   = rs[["P_I_t"]],
    qtyDT     = rs[["I_PP_t"]],
    by        = c("n","t"),
    scenarios = cfg$scenarios
  )
  HSAV <- rs[["HSAVR_t"]]; HDY <- rs[["HDY_VAL_t"]]
  GSUR <- rs[["GSUR_VAL_t"]]; GINV <- rs[["GINV_VAL_t"]]

  objs <- list(FS_INV = FS_INV, TB = out[["TB_t"]], HSAV = HSAV, HDY = HDY, GSUR = GSUR, GINV = GINV)
  if (all(!vapply(objs, is.null, TRUE))) {
    for (nm in names(objs)) objs[[nm]] <- norm_key_types(objs[[nm]])
    keys <- unique(data.table::rbindlist(lapply(objs, function(D) D[, .(n,t)]), use.names = TRUE))
    data.table::setkey(keys, n, t)
    m2 <- function(L, R) merge(L, R, by = c("n","t"), all = TRUE)

    .pick <- function(D, prefix) {
      if (is.null(D)) return(keys[, .(n,t)][0])
      out <- D[, c("n","t", base_scn, pol_scn), with = FALSE]
      data.table::setnames(out, c(base_scn, pol_scn), paste0(prefix, c("_base","_pol")))
      out
    }

    X <- m2(keys, .pick(objs$FS_INV, "INV"))
    X <- m2(X,    .pick(objs$TB,    "TB"))
    X <- m2(X,    .pick(objs$HSAV,  "HSAV"))
    X <- m2(X,    .pick(objs$HDY,   "HDY"))
    X <- m2(X,    .pick(objs$GSUR,  "GSUR"))
    X <- m2(X,    .pick(objs$GINV,  "GINV"))

    for (s in c("base","pol")) {
      X[, (paste0("FS_", s)) :=
           get(paste0("INV_", s)) -
           get(paste0("TB_",  s)) -
           (get(paste0("HSAV_", s)) * get(paste0("HDY_", s))) -
           get(paste0("GSUR_", s)) -
           get(paste0("GINV_", s))]
    }
    FS_out <- X[, .(n, t, base = FS_base, pol = FS_pol)]
    data.table::setnames(FS_out, c("base","pol"), c(base_scn, pol_scn))
    FS_out <- add_macroregions_additive(FS_out, EU28, scenarios = cfg$scenarios)
    FS_out <- add_var_cols(FS_out, base = base_scn, pol = pol_scn)
    out[["FS_t"]] <- FS_out
  }

  # --- Domestic Savings DS_t --------------------------------------------------
  HSAV <- rs[["HSAVR_t"]]; HDY <- rs[["HDY_VAL_t"]]
  GSUR <- rs[["GSUR_VAL_t"]]; GINV <- rs[["GINV_VAL_t"]]
  objs2 <- list(HSAV=HSAV, HDY=HDY, GSUR=GSUR, GINV=GINV)
  if (all(!vapply(objs2, is.null, TRUE))) {
    for (nm in names(objs2)) objs2[[nm]] <- norm_key_types(objs2[[nm]])
    keys <- unique(data.table::rbindlist(lapply(objs2, function(D) D[, .(n,t)]), use.names = TRUE))
    data.table::setkey(keys, n, t)
    m2 <- function(L, R) merge(L, R, by = c("n","t"), all = TRUE)

    .pick <- function(D, prefix) {
      out <- D[, c("n","t", base_scn, pol_scn), with = FALSE]
      data.table::setnames(out, c(base_scn, pol_scn), paste0(prefix, c("_base","_pol")))
      out
    }

    DS <- m2(keys, .pick(HSAV,"HSAV"))
    DS <- m2(DS,  .pick(HDY, "HDY"))
    DS <- m2(DS,  .pick(GSUR,"GSUR"))
    DS <- m2(DS,  .pick(GINV,"GINV"))

    for (s in c("base","pol")) {
      DS[, (paste0("DS_", s)) :=
           get(paste0("GSUR_", s)) +
           get(paste0("GINV_", s)) +
           (get(paste0("HSAV_", s)) * get(paste0("HDY_", s)))]
    }
    DS_out <- DS[, .(n, t, base = DS_base, pol = DS_pol)]
    data.table::setnames(DS_out, c("base","pol"), c(base_scn, pol_scn))
    DS_out <- add_macroregions_additive(DS_out, EU28, scenarios = cfg$scenarios)
    DS_out <- add_var_cols(DS_out, base = base_scn, pol = pol_scn)
    out[["DS_t"]] <- DS_out
  }
  # --- Household CPI: P_HH_CPI_t ---------------------------------------------
  P_CPI <- rs[["P_CPI_t"]]
  if (!is.null(P_CPI) && "au" %in% names(P_CPI)) {
    P_HH_CPI <- P_CPI[au == "CP", c("n","t", base_scn, pol_scn), with = FALSE]
    P_HH_CPI <- add_var_cols(P_HH_CPI, base = base_scn, pol = pol_scn)
    out[["P_HH_CPI_t"]] <- P_HH_CPI
  }
  
  # --- K/L ratios (country & sector) ------------------------------------------
  KDT <- rs[["K_t"]]; LDT <- rs[["L_t"]]
  if (!is.null(KDT) && !is.null(LDT)) {
    KDT <- norm_key_types(KDT); LDT <- norm_key_types(LDT)
    keys_KL <- intersect(intersect(names(KDT), names(LDT)), c("n","i","t"))
    KL <- merge(KDT, LDT, by = keys_KL, all = FALSE, suffixes = c("_K","_L"))
    for (s in c(base_scn, pol_scn)) {
      num <- paste0(s, "_K"); den <- paste0(s, "_L")
      if (all(c(num, den) %in% names(KL))) {
        KL[, (s) := fifelse(get(den) == 0 | is.na(get(den)), NA_real_, get(num) / get(den))]
      }
    }
    KL <- KL[, c("n", "i", "t", base_scn, pol_scn), with = FALSE]
    KL <- add_var_cols(KL, base = base_scn, pol = pol_scn)
    out[["KLratio_t"]] <- KL

    # ΣK & ΣL by (n,t), then ratio
    K_cty <- KDT[, lapply(.SD, sum, na.rm=TRUE), .SDcols = c(base_scn, pol_scn), by = .(n,t)]
    L_cty <- LDT[, lapply(.SD, sum, na.rm=TRUE), .SDcols = c(base_scn, pol_scn), by = .(n,t)]
    KL_cty <- merge(K_cty, L_cty, by = c("n","t"), suffixes = c("_K","_L"))
    KL_cty[, (base_scn) := fifelse(get(paste0(base_scn,"_L")) == 0, NA_real_,
                                   get(paste0(base_scn,"_K")) / get(paste0(base_scn,"_L")))]
    KL_cty[, (pol_scn)  := fifelse(get(paste0(pol_scn,"_L"))  == 0, NA_real_,
                                   get(paste0(pol_scn,"_K"))  / get(paste0(pol_scn,"_L")))]
    KL_cty[, c(paste0(base_scn,"_K"), paste0(pol_scn,"_K"),
               paste0(base_scn,"_L"), paste0(pol_scn,"_L")) := NULL]
    KL_cty <- add_var_cols(KL_cty, base = base_scn, pol = pol_scn)
    out[["KLratio_country_t"]] <- KL_cty
  }

  # --- Relative price K/L by (n,i,t): P_KL_t ---------------------------------
  Pinput <- rs[["P_INPUT_t"]]
  if (!is.null(Pinput) && is.data.table(Pinput) && "oc" %in% names(Pinput)) {
    Pinput <- norm_key_types(Pinput)
    P_K <- Pinput[oc == "K", c("n","i","t", base_scn, pol_scn), with = FALSE]
    P_L <- Pinput[oc == "L", c("n","i","t", base_scn, pol_scn), with = FALSE]
    PKL <- merge(P_K, P_L, by = c("n","i","t"), suffixes = c("_K","_L"))
    for (s in c(base_scn, pol_scn)) {
      num <- paste0(s,"_K"); den <- paste0(s,"_L")
      PKL[, (s) := fifelse(get(den) == 0 | is.na(get(den)), NA_real_, get(num)/get(den))]
    }
    PKL <- PKL[, c("n","i","t", base_scn, pol_scn), with = FALSE]
    PKL <- add_var_cols(PKL, base = base_scn, pol = pol_scn)
    out[["P_KL_t"]] <- PKL
  }
  
  # --- Investment by industry: 6-group aggregates ----------------------------
  I_PP <- rs[["I_PP_t"]]
  if (!is.null(I_PP) && is.data.table(I_PP)) {
    # ensure wide
    if ("scenario" %in% names(I_PP) && "value" %in% names(I_PP)) {
      I_PP <- wide_by_scenario(I_PP, scenarios = c(base_scn, pol_scn))
    }
    
    # select columns by *names stored in variables* (data.table way)
    sel <- c("n","i","t", base_scn, pol_scn)
    I_PP_sub <- I_PP[, sel, with = FALSE]   # <- this keeps the real column names
    
    # sanity check (optional)
    miss <- setdiff(sel, names(I_PP_sub))
    if (length(miss)) stop("Missing columns: ", paste(miss, collapse = ", "))
    
    I_PP_G6 <- add_sector_groups_additive(
      I_PP_sub,
      scenarios = c(base_scn, pol_scn),
      append_original = FALSE
    )
    
    # add EU macroregions on the country dimension if you want symmetry with others
    I_PP_G6 <- add_macroregions_additive(I_PP_G6, EU28, scenarios = c(base_scn, pol_scn))
    I_PP_G6 <- add_var_cols(I_PP_G6, base = base_scn, pol = pol_scn)
    out[["I_PP_SECT6_t"]] <- I_PP_G6
  }
  
  # --- Output composition by 6 groups (REAL terms; shares sum to 100) ----------
  # Build gross output in *quantities* by industry:
  GOq_by_i <- NULL
  
  # Preferred: direct quantity table
  if (!is.null(rs[["Q_t"]])) {
    GOq_by_i <- rs[["Q_t"]][, c("n","i","t", base_scn, pol_scn), with = FALSE]
    
    # Fallback: deflate value by price (GO_VAL / P_Q) to get "real" output
  } else if (!is.null(rs[["VA_VAL_t"]]) && !is.null(rs[["P_Q_t"]])) {
    V <- rs[["VA_VAL_t"]][, c("n","i","t", base_scn, pol_scn), with = FALSE]
    P <- rs[["P_Q_t"]][,       c("n","i","t", base_scn, pol_scn), with = FALSE]
    GOq_by_i <- merge(V, P, by = c("n","i","t"), suffixes = c("_V","_P"))
    for (s in c(base_scn, pol_scn)) {
      GOq_by_i[, (s) := fifelse(get(paste0(s,"_P")) == 0 | is.na(get(paste0(s,"_P"))),
                                NA_real_,
                                get(paste0(s,"_V")) / get(paste0(s,"_P")))]
    }
    GOq_by_i <- GOq_by_i[, c("n","i","t", base_scn, pol_scn), with = FALSE]
  }
  
  if (!is.null(GOq_by_i)) {
    GOq_by_i <- norm_key_types(GOq_by_i)
    
    # 1) Aggregate industries to the 6 groups in *real* terms
    GO6q <- add_sector_groups_additive(GOq_by_i,
                                       scenarios = c(base_scn, pol_scn),
                                       append_original = FALSE)
    
    # 2) Add EU macroregions **before** computing shares (shares are not additive)
    GO6q <- add_macroregions_additive(GO6q, EU28, scenarios = c(base_scn, pol_scn))
    
    # 3) Drop total row; compute shares (×100) within each (n,t)
    GO6q <- GO6q[i != "TOT_G6"]
    for (s in c(base_scn, pol_scn)) {
      GO6q[, (s) := 100 * get(s) / sum(get(s), na.rm = TRUE), by = .(n, t)]
    }
    
    # Optional: consistent group order for plotting
    grp_order <- c("primary","high_energy_manufacturing","low_energy_manufacturing",
                   "utilities_construction","market_services","public_personal_services")
    GO6q[, i := factor(as.character(i), levels = grp_order)]
    
    # 4) Add deltas/% changes on the *shares* (base vs policy)
    GO6q <- add_var_cols(GO6q, base = base_scn, pol = pol_scn)
    
    out[["OUT_COMP6_SHARE_REAL_t"]] <- GO6q[]
  } else {
    message("• Skipping OUT_COMP6_SHARE_REAL_t: need either Q_t or (GO_VAL_t & P_Q_t).")
  }
  

  # --- BITRADE by macro regions (USA split) -----------------------------------
  BT_src <- rs[["BITRADE_t"]]
  if (!is.null(BT_src) && is.data.table(BT_src)) {
    BT_src <- norm_key_types(data.table::copy(BT_src))
    if ("n"  %in% names(BT_src))  BT_src <- BT_src[!(n  %chin% c("EU28","NonEU28","WORLD"))]
    if ("n1" %in% names(BT_src))  BT_src <- BT_src[!(n1 %chin% c("EU28","NonEU28","WORLD"))]

    BT_src[, n  := .region_map_vec(n)]
    BT_src[, n1 := .region_map_vec(n1)]

    BT_reg <- BT_src[, lapply(.SD, sum, na.rm = TRUE),
                     .SDcols = c(base_scn, pol_scn), by = .(n, n1, c, t)]

    imp_tot <- BT_reg[, lapply(.SD, sum), .SDcols = c(base_scn, pol_scn), by = .(n, c, t)]
    imp_tot[, n1 := "TOT"]
    data.table::setcolorder(imp_tot, c("n","n1","c","t", base_scn, pol_scn))

    exp_tot <- BT_reg[, lapply(.SD, sum), .SDcols = c(base_scn, pol_scn), by = .(n1, c, t)]
    exp_tot[, n := "TOT"]
    data.table::setcolorder(exp_tot, c("n","n1","c","t", base_scn, pol_scn))

    BT_reg2 <- data.table::rbindlist(list(BT_reg, imp_tot, exp_tot), use.names = TRUE)

    prod_tot <- BT_reg2[, lapply(.SD, sum), .SDcols = c(base_scn, pol_scn), by = .(n, n1, t)]
    prod_tot[, c := "TOT"]
    data.table::setcolorder(prod_tot, c("n","n1","c","t", base_scn, pol_scn))

    BT_reg3 <- data.table::rbindlist(list(BT_reg2, prod_tot), use.names = TRUE)
    BT_reg3[, year := 2014L + as.integer(t)]
    BT_reg3 <- add_var_cols(BT_reg3, base = base_scn, pol = pol_scn)

    order_levels <- c("EEU","NWEU","SEU","USA","CHN","IND","OECD","NonOECD","ROW","TOT")
    BT_reg3[, n  := factor(as.character(n),  levels = order_levels)]
    BT_reg3[, n1 := factor(as.character(n1), levels = order_levels)]

    out[["BITRADE_REG_t"]] <- BT_reg3[]
  }

  Filter(Negate(is.null), out)
}
