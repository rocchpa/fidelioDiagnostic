# ==============================================================================
# ====                          derive_indicators                       ========
# ==============================================================================

# ---- Sector group mapping: generalized by leading letter ---------------------
.sector_group6_vec <- function(ic,
                               high_energy = c("C17","C19","C20","C23","C24")) {
  ic <- as.character(ic)
  
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

# Add 6-group sector aggregates to an (n,i,t,<scenarios...>) wide table.
add_sector_groups_additive <- function(DT, scenarios, append_original = TRUE) {
  stopifnot(data.table::is.data.table(DT), all(scenarios %in% names(DT)))
  if (!"i" %in% names(DT)) return(DT)
  DT <- data.table::copy(DT)
  
  # drop any pre-aggregated industry rows (e.g., "TOT")
  DT <- DT[i != "TOT"]
  
  # map to 6 groups and aggregate
  G <- cbind(DT[, .(i = .sector_group6_vec(i), n, t)], DT[, ..scenarios])
  G <- G[, lapply(.SD, sum, na.rm = TRUE), .SDcols = scenarios, by = .(n, i, t)]
  
  # optional total across groups (for composition checks)
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


# --- Derived indicators orchestrator ------------------------------------------

#' Compute all derived indicators requested in config
#' @export
derive_all <- function(raw, cfg) {
  wanted <- cfg$derived$include
  if (is.null(wanted) || !length(wanted)) {
    wanted <- c("TB_GDP_t")
  }
  out <- list()
  if ("TB_GDP_t" %in% wanted) out$TB_GDP_t <- derive_TB_GDP(raw)
  Filter(Negate(is.null), out)
}

# legacy (no longer used) helper kept for backwards-compatibility
.pick_base_pol <- function(DT, keys = c("n","t"), base_scn, pol_scn, prefix) {
  if (is.null(DT) || !all(c(base_scn, pol_scn) %in% names(DT))) return(NULL)
  cols <- c(intersect(keys, names(DT)), base_scn, pol_scn)
  out  <- DT[, ..cols]
  data.table::setnames(out, c(base_scn, pol_scn), paste0(prefix, c("_base","_pol")))
  out
}

#region map for BITRADE regionalization (can be moved to YAML)
.region_map_vec <- function(cc) {
  seu     <- c("CYP","ESP","GRC","ITA","MLT","PRT")
  eeu     <- c("BGR","CZE","EST","HRV","HUN","LTU","LVA","POL","ROU","SVN","SVK")
  nweu    <- c("AUT","BEL","DEU","DNK","FIN","FRA","GBR","IRL","LUX","NLD","SWE")
  OECD_NO_USA <- c("CAN","JPN","KOR","AUS","CHE","NOR","TUR","MEX")
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

# ---- derive indicators from *wide* base tables (multi-scenario) --------------

#' Build derived tables from wide (results_by_symbol)
#' @param rs named list of wide data.tables (results_by_symbol)
#' @param cfg config (for scenarios + EU28)
#' @return named list of wide data.tables (all scenarios kept)
#' @export
derive_from_wide <- function(rs, cfg) {
  if (is.null(rs) || !length(rs)) return(list())
  
  scenarios <- cfg$scenarios
  base_scn  <- scenarios[1]
  pol_scns  <- scenarios[-1]
  EU28      <- cfg$groups$EU28
  
  out <- list()
  
  # --- I_TOT_PP_t = sum_i I_PP_t ----------------------------------------------
  I_PP <- rs[["I_PP_t"]]
  if (!is.null(I_PP)) {
    I_PP   <- norm_key_types(I_PP)
    I_TOT  <- I_PP[, lapply(.SD, sum, na.rm = TRUE), .SDcols = scenarios, by = .(n, t)]
    I_TOT  <- add_macroregions_additive(I_TOT, EU28, scenarios = scenarios)
    I_TOT  <- add_var_cols_multi(I_TOT, base = base_scn, policies = pol_scns, cfg = cfg)
    out[["I_TOT_PP_t"]] <- I_TOT
  }
  
  # --- TB_t = X - M using value sums ------------------------------------------
  TB_X <- sum_value_by(
    priceDT   = rs[["P_USE_t"]],
    qtyDT     = rs[["USE_PP_t"]],
    by        = c("n","t"),
    filter    = list(au = "X"),
    scenarios = scenarios
  )
  TB_M <- sum_value_by(
    priceDT   = rs[["P_Mcif_t"]],
    qtyDT     = rs[["M_TOT_t"]],
    by        = c("n","t"),
    scenarios = scenarios
  )
  if (!is.null(TB_X) && !is.null(TB_M)) {
    TB <- merge(TB_X, TB_M, by = c("n","t"), all = TRUE, suffixes = c("_X","_M"))
    for (s in scenarios) {
      x <- paste0(s,"_X"); m <- paste0(s,"_M")
      if (all(c(x, m) %in% names(TB))) TB[, (s) := get(x) - get(m)]
    }
    TB <- TB[, c("n","t", scenarios), with = FALSE]
    TB <- add_macroregions_additive(TB, EU28, scenarios = scenarios)
    TB <- add_var_cols_multi(TB, base = base_scn, policies = pol_scns, cfg = cfg)
    out[["TB_t"]] <- TB
  }
  
  # --- Real TB from BITRADE (no pre-agg rows) → TBr_t -------------------------
  BT <- rs[["BITRADE_t"]]
  if (!is.null(BT) && is.data.table(BT)) {
    BT <- norm_key_types(BT)
    if ("n"  %in% names(BT)) BT <- BT[!(n  %in% c("EU28","NonEU28","WORLD"))]
    if ("n1" %in% names(BT)) BT <- BT[!(n1 %in% c("EU28","NonEU28","WORLD"))]
    if ("c"  %in% names(BT)) BT <- BT[c != "TOT"]
    
    meas_cols <- intersect(names(BT), scenarios)
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
      TBr    <- add_macroregions_additive(TB_cty, EU28, scenarios = scenarios)
      TBr    <- add_var_cols_multi(TBr, base = base_scn, policies = pol_scns, cfg = cfg)
      out[["TBr_t"]] <- TBr
    }
  }
  
  # --- TB/GDP (from aggregated wide TB & GDP) ---------------------------------
  TB  <- out[["TB_t"]]
  GDP <- rs[["GDPr_t"]]
  if (!is.null(TB) && !is.null(GDP)) {
    TB  <- norm_key_types(TB)
    GDP <- norm_key_types(GDP)
    TB_GDP <- merge(TB[,  c("n","t", scenarios), with = FALSE],
                    GDP[, c("n","t", scenarios), with = FALSE],
                    by = c("n","t"), suffixes = c("_TB","_GDP"))
    for (s in scenarios) {
      TB_GDP[, (s) := data.table::fifelse(
        get(paste0(s,"_GDP")) == 0 | is.na(get(paste0(s,"_GDP"))),
        NA_real_, get(paste0(s,"_TB")) / get(paste0(s,"_GDP"))
      )]
    }
    # keep only scenario columns after ratio
    drop_cols <- grep("_(TB|GDP)$", names(TB_GDP), value = TRUE)
    if (length(drop_cols)) TB_GDP[, (drop_cols) := NULL]
    TB_GDP <- add_var_cols_multi(TB_GDP, base = base_scn, policies = pol_scns, cfg = cfg)
    out[["TB_GDP_t"]] <- TB_GDP
  }
  
  # --- Foreign Savings FS_t ---------------------------------------------------
  FS_INV <- sum_value_by(
    priceDT   = rs[["P_I_t"]],
    qtyDT     = rs[["I_PP_t"]],
    by        = c("n","t"),
    scenarios = scenarios
  )
  HSAV <- rs[["HSAVR_t"]]; HDY <- rs[["HDY_VAL_t"]]
  GSUR <- rs[["GSUR_VAL_t"]]; GINV <- rs[["GINV_VAL_t"]]
  
  if (!is.null(FS_INV) && !is.null(out[["TB_t"]]) &&
      !is.null(HSAV)   && !is.null(HDY) &&
      !is.null(GSUR)   && !is.null(GINV)) {
    
    objs <- list(FS_INV = FS_INV, TB = out[["TB_t"]], HSAV = HSAV, HDY = HDY, GSUR = GSUR, GINV = GINV)
    for (nm in names(objs)) objs[[nm]] <- norm_key_types(objs[[nm]])
    
    keys <- Reduce(function(a,b) unique(rbindlist(list(a,b), use.names = TRUE, fill = TRUE)),
                   lapply(objs, function(D) D[, .(n,t)]))
    data.table::setkey(keys, n, t)
    
    m2 <- function(L, R) merge(L, R, by = c("n","t"), all = TRUE)
    
    # bind all sources side-by-side
    X <- keys
    for (nm in names(objs)) {
      X <- m2(X, objs[[nm]][, c("n","t", scenarios), with = TRUE])
    }
    
    # Compute per-scenario FS
    for (s in scenarios) {
      X[, (s) := get(paste0(s, ".x"))]  # placeholder, we will overwrite below if duplicated by merges
    }
    # Disambiguate names after merges: ensure unique accessors
    # Build local aliases
    INVp <- objs$FS_INV; TBp <- objs$TB; HSAVp <- objs$HSAV; HDYp <- objs$HDY; GSURp <- objs$GSUR; GINVp <- objs$GINV
    FS_out <- keys
    for (s in scenarios) {
      FS_out <- merge(FS_out, INVp[,  .(n,t, v = get(s))], by = c("n","t"), all.x = TRUE); data.table::setnames(FS_out, "v", paste0("INV_", s))
      FS_out <- merge(FS_out, TBp[,   .(n,t, v = get(s))], by = c("n","t"), all.x = TRUE); data.table::setnames(FS_out,  "v", paste0("TB_",  s))
      FS_out <- merge(FS_out, HSAVp[, .(n,t, v = get(s))], by = c("n","t"), all.x = TRUE); data.table::setnames(FS_out, "v", paste0("HSAV_", s))
      FS_out <- merge(FS_out, HDYp[,  .(n,t, v = get(s))], by = c("n","t"), all.x = TRUE); data.table::setnames(FS_out,  "v", paste0("HDY_",  s))
      FS_out <- merge(FS_out, GSURp[, .(n,t, v = get(s))], by = c("n","t"), all.x = TRUE); data.table::setnames(FS_out, "v", paste0("GSUR_", s))
      FS_out <- merge(FS_out, GINVp[, .(n,t, v = get(s))], by = c("n","t"), all.x = TRUE); data.table::setnames(FS_out, "v", paste0("GINV_", s))
      FS_out[, (s) := get(paste0("INV_", s)) -
               get(paste0("TB_",  s)) -
               (get(paste0("HSAV_", s)) * get(paste0("HDY_", s))) -
               get(paste0("GSUR_", s)) -
               get(paste0("GINV_", s))]
    }
    # keep only (n,t, scenarios)
    keep <- c("n","t", scenarios)
    FS_out <- FS_out[, keep, with = FALSE]
    FS_out <- add_macroregions_additive(FS_out, EU28, scenarios = scenarios)
    FS_out <- add_var_cols_multi(FS_out, base = base_scn, policies = pol_scns, cfg = cfg)
    out[["FS_t"]] <- FS_out
  }
  
  # --- Domestic Savings DS_t ---------------------------------------------------
  HSAV <- rs[["HSAVR_t"]]; HDY <- rs[["HDY_VAL_t"]]
  GSUR <- rs[["GSUR_VAL_t"]]; GINV <- rs[["GINV_VAL_t"]]
  if (!is.null(HSAV) && !is.null(HDY) && !is.null(GSUR) && !is.null(GINV)) {
    HSAV <- norm_key_types(HSAV); HDY <- norm_key_types(HDY)
    GSUR <- norm_key_types(GSUR); GINV <- norm_key_types(GINV)
    
    keys <- Reduce(function(a,b) unique(rbindlist(list(a,b), use.names = TRUE, fill = TRUE)),
                   lapply(list(HSAV, HDY, GSUR, GINV), function(D) D[, .(n,t)]))
    data.table::setkey(keys, n, t)
    
    DS_out <- keys
    for (s in scenarios) {
      DS_out <- merge(DS_out, HSAV[, .(n,t, v = get(s))], by = c("n","t"), all.x = TRUE); data.table::setnames(DS_out, "v", paste0("HSAV_", s))
      DS_out <- merge(DS_out, HDY[,  .(n,t, v = get(s))], by = c("n","t"), all.x = TRUE); data.table::setnames(DS_out,  "v", paste0("HDY_",  s))
      DS_out <- merge(DS_out, GSUR[, .(n,t, v = get(s))], by = c("n","t"), all.x = TRUE); data.table::setnames(DS_out, "v", paste0("GSUR_", s))
      DS_out <- merge(DS_out, GINV[, .(n,t, v = get(s))], by = c("n","t"), all.x = TRUE); data.table::setnames(DS_out, "v", paste0("GINV_", s))
      DS_out[, (s) := get(paste0("GSUR_", s)) +
               get(paste0("GINV_", s)) +
               (get(paste0("HSAV_", s)) * get(paste0("HDY_",  s)))]
    }
    keep <- c("n","t", scenarios)
    DS_out <- DS_out[, keep, with = FALSE]
    DS_out <- add_macroregions_additive(DS_out, EU28, scenarios = scenarios)
    DS_out <- add_var_cols_multi(DS_out, base = base_scn, policies = pol_scns, cfg = cfg)
    out[["DS_t"]] <- DS_out
  }
  
  # --- Household CPI: P_HH_CPI_t ----------------------------------------------
  P_CPI <- rs[["P_CPI_t"]]
  if (!is.null(P_CPI) && "au" %in% names(P_CPI)) {
    P_HH_CPI <- P_CPI[au == "CP", c("n","t", scenarios), with = FALSE]
    P_HH_CPI <- add_var_cols_multi(P_HH_CPI, base = base_scn, policies = pol_scns, cfg = cfg)
    out[["P_HH_CPI_t"]] <- P_HH_CPI
  }
  
  # --- K/L ratios (country & sector) ------------------------------------------
  KDT <- rs[["K_t"]]; LDT <- rs[["L_t"]]
  if (!is.null(KDT) && !is.null(LDT)) {
    KDT <- norm_key_types(KDT); LDT <- norm_key_types(LDT)
    keys_KL <- intersect(intersect(names(KDT), names(LDT)), c("n","i","t"))
    KL <- merge(KDT, LDT, by = keys_KL, all = FALSE, suffixes = c("_K","_L"))
    for (s in scenarios) {
      num <- paste0(s, "_K"); den <- paste0(s, "_L")
      if (all(c(num, den) %in% names(KL))) {
        KL[, (s) := fifelse(get(den) == 0 | is.na(get(den)), NA_real_, get(num) / get(den))]
      }
    }
    KL <- KL[, c("n", "i", "t", scenarios), with = FALSE]
    KL <- add_var_cols_multi(KL, base = base_scn, policies = pol_scns, cfg = cfg)
    out[["KLratio_t"]] <- KL
    
    # ΣK & ΣL by (n,t), then ratio
    K_cty <- KDT[, lapply(.SD, sum, na.rm=TRUE), .SDcols = scenarios, by = .(n,t)]
    L_cty <- LDT[, lapply(.SD, sum, na.rm=TRUE), .SDcols = scenarios, by = .(n,t)]
    KL_cty <- merge(K_cty, L_cty, by = c("n","t"), suffixes = c("_K","_L"))
    for (s in scenarios) {
      KL_cty[, (s) := fifelse(get(paste0(s,"_L")) == 0 | is.na(get(paste0(s,"_L"))),
                              NA_real_, get(paste0(s,"_K")) / get(paste0(s,"_L")))]
    }
    KL_cty[, grep("_(K|L)$", names(KL_cty), value = TRUE) := NULL]
    KL_cty <- add_var_cols_multi(KL_cty, base = base_scn, policies = pol_scns, cfg = cfg)
    out[["KLratio_country_t"]] <- KL_cty
  }
  
  # --- Relative price K/L by (n,i,t): P_KL_t ----------------------------------
  Pinput <- rs[["P_INPUT_t"]]
  if (!is.null(Pinput) && is.data.table(Pinput) && "oc" %in% names(Pinput)) {
    Pinput <- norm_key_types(Pinput)
    P_K <- Pinput[oc == "K", c("n","i","t", scenarios), with = FALSE]
    P_L <- Pinput[oc == "L", c("n","i","t", scenarios), with = FALSE]
    PKL <- merge(P_K, P_L, by = c("n","i","t"), suffixes = c("_K","_L"))
    for (s in scenarios) {
      num <- paste0(s,"_K"); den <- paste0(s,"_L")
      PKL[, (s) := fifelse(get(den) == 0 | is.na(get(den)), NA_real_, get(num)/get(den))]
    }
    PKL <- PKL[, c("n","i","t", scenarios), with = FALSE]
    PKL <- add_var_cols_multi(PKL, base = base_scn, policies = pol_scns, cfg = cfg)
    out[["P_KL_t"]] <- PKL
  }
  
  # --- Investment by industry: 6-group aggregates -----------------------------
  I_PP <- rs[["I_PP_t"]]
  if (!is.null(I_PP) && is.data.table(I_PP)) {
    if ("scenario" %in% names(I_PP) && "value" %in% names(I_PP)) {
      I_PP <- wide_by_scenario(I_PP, scenarios = scenarios)
    }
    sel <- c("n","i","t", scenarios)
    I_PP_sub <- I_PP[, sel, with = FALSE]
    
    I_PP_G6 <- add_sector_groups_additive(
      I_PP_sub, scenarios = scenarios, append_original = FALSE
    )
    I_PP_G6 <- add_macroregions_additive(I_PP_G6, EU28, scenarios = scenarios)
    I_PP_G6 <- add_var_cols_multi(I_PP_G6, base = base_scn, policies = pol_scns, cfg = cfg)
    out[["I_PP_SECT6_t"]] <- I_PP_G6
  }
  
  # --- Output composition by 6 groups (REAL terms; shares sum to 100) ---------
  GOq_by_i <- NULL
  if (!is.null(rs[["Q_t"]])) {
    GOq_by_i <- rs[["Q_t"]][, c("n","i","t", scenarios), with = FALSE]
  } else if (!is.null(rs[["VA_VAL_t"]]) && !is.null(rs[["P_Q_t"]])) {
    V <- rs[["VA_VAL_t"]][, c("n","i","t", scenarios), with = FALSE]
    P <- rs[["P_Q_t"]][,       c("n","i","t", scenarios), with = FALSE]
    GOq_by_i <- merge(V, P, by = c("n","i","t"), suffixes = c("_V","_P"))
    for (s in scenarios) {
      GOq_by_i[, (s) := fifelse(get(paste0(s,"_P")) == 0 | is.na(get(paste0(s,"_P"))),
                                NA_real_, get(paste0(s,"_V")) / get(paste0(s,"_P")))]
    }
    GOq_by_i <- GOq_by_i[, c("n","i","t", scenarios), with = FALSE]
  }
  
  if (!is.null(GOq_by_i)) {
    GOq_by_i <- norm_key_types(GOq_by_i)
    
    # 1) Aggregate industries to the 6 groups in *real* terms
    GO6q <- add_sector_groups_additive(GOq_by_i, scenarios = scenarios, append_original = FALSE)
    
    # 2) Add EU macroregions **before** computing shares
    GO6q <- add_macroregions_additive(GO6q, EU28, scenarios = scenarios)
    
    # 3) Drop total row; compute shares (×100) within each (n,t)
    GO6q <- GO6q[i != "TOT_G6"]
    for (s in scenarios) {
      GO6q[, (s) := 100 * get(s) / sum(get(s), na.rm = TRUE), by = .(n, t)]
    }
    
    # Optional: consistent group order for plotting
    grp_order <- c("primary","high_energy_manufacturing","low_energy_manufacturing",
                   "utilities_construction","market_services","public_personal_services")
    GO6q[, i := factor(as.character(i), levels = grp_order)]
    
    # 4) Add deltas/% changes on the *shares* (base vs each policy)
    GO6q <- add_var_cols_multi(GO6q, base = base_scn, policies = pol_scns, cfg = cfg)
    
    out[["OUT_COMP6_SHARE_REAL_t"]] <- GO6q[]
  } else {
    message("• Skipping OUT_COMP6_SHARE_REAL_t: need either Q_t or (VA_VAL_t & P_Q_t).")
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
                     .SDcols = scenarios, by = .(n, n1, c, t)]
    
    imp_tot <- BT_reg[, lapply(.SD, sum), .SDcols = scenarios, by = .(n, c, t)]
    imp_tot[, n1 := "TOT"]; data.table::setcolorder(imp_tot, c("n","n1","c","t", scenarios))
    
    exp_tot <- BT_reg[, lapply(.SD, sum), .SDcols = scenarios, by = .(n1, c, t)]
    exp_tot[, n := "TOT"]; data.table::setcolorder(exp_tot, c("n","n1","c","t", scenarios))
    
    BT_reg2 <- data.table::rbindlist(list(BT_reg, imp_tot, exp_tot), use.names = TRUE)
    
    prod_tot <- BT_reg2[, lapply(.SD, sum), .SDcols = scenarios, by = .(n, n1, t)]
    prod_tot[, c := "TOT"]; data.table::setcolorder(prod_tot, c("n","n1","c","t", scenarios))
    
    BT_reg3 <- data.table::rbindlist(list(BT_reg2, prod_tot), use.names = TRUE)
    BT_reg3[, year := 2014L + as.integer(t)]
    BT_reg3 <- add_var_cols_multi(BT_reg3, base = base_scn, policies = pol_scns, cfg = cfg)
    
    order_levels <- c("EEU","NWEU","SEU","USA","CHN","IND","OECD","NonOECD","ROW","TOT")
    BT_reg3[, n  := factor(as.character(n),  levels = order_levels)]
    BT_reg3[, n1 := factor(as.character(n1), levels = order_levels)]
    
    out[["BITRADE_REG_t"]] <- BT_reg3[]
  }
  
  Filter(Negate(is.null), out)
}
