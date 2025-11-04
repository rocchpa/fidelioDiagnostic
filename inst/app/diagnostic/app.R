# =========================
# fidelioDiagnostics — Diagnostic app (multi-scenario ready)
# =========================
library(shiny)
library(data.table)
library(ggplot2)
library(DT)

# ---- Package helpers ----
load_symbol             <- fidelioDiagnostics:::load_symbol
load_bundle             <- fidelioDiagnostics:::load_bundle
load_manifest           <- fidelioDiagnostics:::load_manifest
resolve_outputs_dir     <- fidelioDiagnostics:::resolve_outputs_dir
outputs_dir_from_config <- fidelioDiagnostics:::outputs_dir_from_config

# scenario helpers
base_scn      <- fidelioDiagnostics:::base_scn
policy_scns   <- fidelioDiagnostics:::policy_scns
scenario_cols <- fidelioDiagnostics:::scenario_cols
load_config   <- fidelioDiagnostics:::load_config

cat("[APP] getwd(): ", getwd(), "\n")
cat("[APP] outputs/derived resolved to: ",
    normalizePath(file.path("outputs","derived"), winslash="/", mustWork = FALSE), "\n")

# =========================
# Minimal additions: robust config + outputs resolution
# =========================

# safe infix (needed early because get_results() uses it)
`%||%` <- function(x, y) if (is.null(x) || (is.character(x) && !nzchar(x))) y else x

# search upwards for project.yml if option/env are not set
.find_up <- function(start = getwd(), names = c("project.yml", "config/project.yml"), max_up = 8) {
  cur <- normalizePath(start, winslash = "/", mustWork = FALSE)
  for (i in 0:max_up) {
    for (nm in names) {
      cand <- file.path(cur, nm)
      if (file.exists(cand)) return(cand)
    }
    parent <- normalizePath(file.path(cur, ".."), winslash = "/", mustWork = FALSE)
    if (identical(parent, cur)) break
    cur <- parent
  }
  NULL
}

.read_last_ctx <- function(outputs_dir) {
  if (!nzchar(outputs_dir)) return(NULL)
  f <- file.path(outputs_dir, "last_run_context.rds")
  if (!file.exists(f)) return(NULL)
  ctx <- try(readRDS(f), silent = TRUE)
  if (inherits(ctx, "try-error")) return(NULL)
  ctx
}

.get_cfg <- function() {
  # 1) explicit option/env
  cfg_path <- getOption("fidelioDiagnostics.config", Sys.getenv("FIDELIO_DIAG_CONFIG", ""))
  if (nzchar(cfg_path) && file.exists(cfg_path)) {
    cat("[APP] Config via option/env: ", normalizePath(cfg_path, winslash="/"), "\n", sep = "")
    return(load_config(cfg_path))
  }
  # 2) upward search
  up <- .find_up()
  if (!is.null(up)) {
    cat("[APP] Config via upward search: ", normalizePath(up, winslash="/"), "\n", sep = "")
    return(load_config(up))
  }
  # 3) package default
  cfg_try <- try(load_config(), silent = TRUE)
  if (!inherits(cfg_try, "try-error") && !is.null(cfg_try$project$id)) {
    cat("[APP] Config via package default. Project id: ", cfg_try$project$id, "\n", sep = "")
    return(cfg_try)
  }
  stop(
    "[APP] Could not locate the project YAML.\n",
    "Set one of:\n",
    "  options(fidelioDiagnostics.config='C:/path/to/project.yml')  OR\n",
    "  Sys.setenv(FIDELIO_DIAG_CONFIG='C:/path/to/project.yml')\n"
  )
}

.resolve_outputs_derived <- function(cfg) {
  cand <- character(0)
  d1 <- try(outputs_dir_from_config(cfg), silent = TRUE)
  if (!inherits(d1, "try-error") && is.character(d1) && length(d1) == 1) cand <- c(cand, d1)
  if (is.character(cfg$paths$outputs) && length(cfg$paths$outputs) == 1) {
    cand <- c(cand, file.path(cfg$paths$outputs, "derived"))
  }
  out_opt <- getOption("fidelioDiagnostics.outputs", Sys.getenv("FIDELIO_DIAG_OUTPUTS", ""))
  if (nzchar(out_opt)) cand <- c(cand, out_opt)
  
  cand <- unique(cand)
  for (p in cand) {
    if (dir.exists(p)) {
      ctx <- .read_last_ctx(p)
      if (!is.null(ctx) && nzchar(ctx$outputs_dir) && dir.exists(ctx$outputs_dir)) {
        return(normalizePath(ctx$outputs_dir, winslash="/"))
      }
      return(normalizePath(p, winslash="/"))
    }
  }
  if (length(cand)) return(normalizePath(cand[1], winslash="/", mustWork = FALSE))
  file.path("outputs", "derived")
}

# =========================
# Config (project id in title)
# =========================
.cfg <- try(.get_cfg(), silent = TRUE)  # << CHANGED: use robust getter
.project_id <- tryCatch({
  if (!inherits(.cfg, "try-error") && !is.null(.cfg$project$id) && nzchar(.cfg$project$id)) .cfg$project$id else NULL
}, error = function(e) NULL)

# =========================
# App constants (set per app file)
# =========================
APP_KIND <- "diagnostic_app"   # or "results_app"

# =========================
# Bundle picker (by project id)
# =========================
.pick_bundle_for_project <- function(dir, project_id, app_kind = APP_KIND) {
  p1 <- sprintf("^bundle_%s_%s_.*\\.(rds|qs)$", app_kind, project_id)
  p2 <- sprintf("^bundle_.*_%s_.*\\.(rds|qs)$", project_id)
  p3 <- sprintf("^results_bundle_%s_.*\\.(rds|qs)$", project_id)
  fs <- list.files(dir, full.names = TRUE)
  cand <- fs[grepl(p1, basename(fs), ignore.case = TRUE)]
  if (!length(cand)) cand <- fs[grepl(p2, basename(fs), ignore.case = TRUE)]
  if (!length(cand)) cand <- fs[grepl(p3, basename(fs), ignore.case = TRUE)]
  if (!length(cand)) return(NULL)
  cand <- cand[order(file.info(cand)$mtime, decreasing = TRUE)]
  tools::file_path_sans_ext(basename(cand[1]))
}

# =========================
# Data loading — auto-detect bundle by project id, backfill per-symbol
# =========================
get_results <- function() {
  # << CHANGED: make dir resolution robust and independent of CWD
  cfg <- .get_cfg()
  dir <- .resolve_outputs_derived(cfg)
  cat("\n[", APP_KIND, "] outputs dir: ", normalizePath(dir, winslash="/"), "\n", sep = "")
  
  # Symbols used in diagnostic views (nation, industry, bilateral)
  wanted <- c(
    # Nation level
    "GDPr_t","HDY_VAL_t","HDYr_t","I_TOT_PP_t","DS_t","FS_t","GSUR_VAL_t","GINV_VAL_t",
    "TBr_t","TB_GDP_t","HSAVR_t","U_t","KLratio_country_t","ir_t","P_HH_CPI_t",
    # Industry level
    "I_PP_t","K_t","L_t","GHG_t","KLratio_t","P_Q_t","P_KL_t",
    "I_PP_SECT6_t","OUT_COMP6_SHARE_REAL_t",
    # Bilateral
    "BITRADE_REG_t"
  )
  
  # 1) Pick bundle for this project id
  project_id <- cfg$project$id %||% ""
  bundle_name <- if (nzchar(project_id)) .pick_bundle_for_project(dir, project_id) else NULL
  if (!is.null(bundle_name)) cat("[", APP_KIND, "] Using bundle: ", bundle_name, "\n", sep = "")
  
  # 2) Try bundles in order (picked one, then APP_KIND)
  try_names <- unique(na.omit(c(bundle_name, APP_KIND)))
  b <- list()
  for (nm in try_names) {
    tmp <- try(load_bundle(nm, dir = dir), silent = TRUE)
    if (!inherits(tmp, "try-error") && length(tmp)) { b <- tmp; break }
  }
  if (!length(b)) {
    cat("[", APP_KIND, "] No bundle loaded. Will backfill per-symbol files.\n", sep = "")
    b <- list()
  }
  
  # 3) Backfill per-symbol for anything missing
  missing <- setdiff(wanted, names(b))
  if (length(missing)) {
    cat("[", APP_KIND, "] Backfilling per-symbol files for: ", paste(missing, collapse = ", "), "\n", sep = "")
    for (s in missing) {
      files <- list.files(dir, pattern = paste0("^", s, "\\.(parquet|feather|fst|rds|csv)$"))
      cat("  ·", s, "files in dir:", if (length(files)) paste(files, collapse = ", ") else "(none)", "\n")
      b[[s]] <- try(load_symbol(s, dir = dir), silent = TRUE)
      if (inherits(b[[s]], "try-error")) b[[s]] <- NULL
    }
  }
  
  # 4) Drop NULL/empty & report
  b <- Filter(function(x) !is.null(x) && is.data.frame(x) && nrow(x) > 0, b)
  have <- intersect(names(b), wanted)
  miss <- setdiff(wanted, have)
  cat("[", APP_KIND, "] Loaded symbols: ", paste(have, collapse = ", "), "\n", sep = "")
  if (length(miss)) cat("[", APP_KIND, "] Missing or empty: ", paste(miss, collapse = ", "), "\n", sep = "")
  
  b
}

results_by_symbol <- get_results()
results_by_symbol <- Filter(function(x) !is.null(x) && nrow(x) > 0, results_by_symbol)
available_syms <- names(results_by_symbol)

# =========================
# Meta (labels, groups, descriptions)
# =========================
.meta <- data.table::rbindlist(list(
  # Nation level
  data.table(
    symbol = c("GDPr_t","HDY_VAL_t","HDYr_t","I_TOT_PP_t","DS_t","FS_t","GSUR_VAL_t","GINV_VAL_t",
               "TBr_t","TB_GDP_t","HSAVR_t","U_t","KLratio_country_t","ir_t","P_HH_CPI_t"),
    label  = c("Real GDP","HH disposable income (nominal)","HH disposable income (real)","Total investment",
               "Domestic savings","Foreign savings","Government surplus","Government investment",
               "Trade balance (real)","Trade balance to GDP ratio",
               "Household saving rate","Unemployment","Capital–labor ratio (country)","Interest rate","CPI (households)"),
    group  = "Nation level",
    desc   = c(
      "Real GDP at constant prices.",
      "Nominal disposable income of households.",
      "Real disposable income of households.",
      "Economy-wide gross investment.",
      "Domestic savings (national).",
      "Foreign savings (net capital inflow).",
      "Government budget surplus (nominal).",
      "Public investment expenditure.",
      "Real trade balance (exports − imports).",
      "Trade balance divided by GDP (dimensionless).",
      "Household saving rate (share of income).",
      "Unemployment rate.",
      "Country-level capital to labor ratio.",
      "Short-term policy interest rate (model).",
      "Household consumer price index."
    ),
    keep = TRUE
  ),
  # Industry level
  data.table(
    symbol = c("I_PP_t","K_t","L_t","GHG_t","KLratio_t","P_Q_t","P_KL_t",
               "I_PP_SECT6_t","OUT_COMP6_SHARE_REAL_t"),
    label  = c("Investment by sector","Capital demand","Labor demand","Emissions",
               "Capital–labor ratio (sector)","Output price","K/L price",
               "Investment by 6-sector group","Output shares by 6-sector groups (real)"),
    group  = "Industry level",
    desc   = c(
      "Gross investment by sector.",
      "Sectoral demand for capital services.",
      "Sectoral labor demand.",
      "Greenhouse gas emissions by sector.",
      "Capital to labor ratio at sector level.",
      "Sectoral output price index.",
      "Price of the composite K–L nest.",
      "Gross investment aggregated into six sector groups.",
      "Share of real output by six sector groups (sums to 1)."
    ),
    keep = TRUE
  ),
  # Bilateral trade
  data.table(
    symbol = "BITRADE_REG_t",
    label  = "Bilateral trade flows (macro regions)",
    group  = "Bilateral trade",
    desc   = "Bilateral trade matrix across macro regions (origin n → destination n1).",
    keep   = TRUE
  )
))
.meta <- .meta[symbol %in% available_syms & keep == TRUE]
.meta[, group := factor(group, levels = c("Nation level","Industry level","Bilateral trade"))]

choices_grouped <- lapply(split(.meta, .meta$group), function(d)
  stats::setNames(d$symbol, d$label)
)
label_of <- function(sym) .meta[symbol == sym, label][1]
desc_of  <- function(sym) .meta[symbol == sym, desc ][1]

# =========================
# Helpers
# =========================
# (kept your original helper section as-is for backward compatibility)
`%||%` <- function(x, y) if (is.null(x)) y else x

macro_first <- function(vals) {
  vals <- as.character(vals)
  known <- c("EU28","NonEU28","WORLD")
  rest  <- setdiff(vals, known)
  factor(vals, levels = c(known[known %in% vals], sort(rest)))
}

add_year <- function(DT) {
  if ("t" %in% names(DT)) DT[, year := 2014L + as.integer(t)]
  DT
}

to_long_levels <- function(DT, scenarios = NULL) {
  if (is.null(DT)) return(NULL)
  if (is.null(scenarios)) scenarios <- scenario_cols(DT)
  id_cols <- setdiff(names(DT), c(scenarios, "delta", "pct"))
  melt(DT, id.vars = id_cols, measure.vars = scenarios,
       variable.name = "scenario", value.name = "value")
}

# Build long Δ/% using delta_<pol> / pct_<pol> columns
to_long_var <- function(DT, use_pct = TRUE, only_policies = NULL) {
  cfg <- .cfg
  b   <- base_scn(cfg)
  pols <- policy_scns(cfg)
  if (!is.null(only_policies)) pols <- intersect(pols, only_policies)
  if (length(pols) == 0L) return(NULL)
  
  suffix <- if (isTRUE(use_pct)) "pct_" else "delta_"
  cols   <- paste0(suffix, pols)
  cols   <- intersect(cols, names(DT))
  if (length(cols) == 0L) return(NULL)
  
  id_cols <- setdiff(names(DT), c(cols, "pct","delta"))  # ignore legacy
  L <- melt(DT, id.vars = id_cols, measure.vars = cols,
            variable.name = "measure", value.name = "value")
  L[, scenario := sub("^.*?_", "", measure)]
  L[, measure := NULL]
  
  if (isTRUE(use_pct)) {
    mx <- suppressWarnings(max(abs(L$value), na.rm = TRUE))
    frac_like <- is.finite(mx) && mx <= 1.001
    if (isTRUE(frac_like)) L[, value := 100 * value]
  }
  L[]
}

shallow_copy <- function(x) {
  if ("shallow" %in% getNamespaceExports("data.table")) data.table::shallow(x) else data.table::copy(x)
}

# =========================
# UI
# =========================
ui <- fluidPage(
  tags$head(tags$style(HTML("
    .container-fluid { padding-top: 6px; }
    .selectize-dropdown .optgroup-header { font-weight: 600; }
    .var-help { background: #f6f8fa; border: 1px solid #e1e4e8; padding: 10px 12px; border-radius: 6px; margin-top: 10px; }
  "))),
  titlePanel(
    if (is.null(.project_id)) "FIDELIO diagnostics" else paste0("FIDELIO diagnostics — ", .project_id)
  ),
  sidebarLayout(
    sidebarPanel(
      selectizeInput("sym", "Variable:", choices = choices_grouped, selected = .meta$symbol[1]),
      uiOutput("scenario_filter"),
      uiOutput("dynamic_filters"),
      radioButtons("view", "Show:",
                   c("Table" = "table", "Plot levels" = "plot_lvl", "Plot Δ / %" = "plot_var"),
                   selected = "plot_lvl"),
      checkboxInput("asPercent", "Show Δ as %", TRUE),
      
      hr(),
      div(class = "sticky-help", uiOutput("var_help_box"))
    ),
    mainPanel(
      conditionalPanel("input.view == 'table'", DTOutput("tbl")),
      conditionalPanel("input.view != 'table'", plotOutput("plt", height = "650px"))
    )
  )
)

# =========================
# Server
# =========================
server <- function(input, output, session) {
  
  symDT <- reactive({ results_by_symbol[[input$sym]] })
  
  # ---- Scenario filter (union of config + present columns)
  output$scenario_filter <- renderUI({
    DT <- symDT()
    if (is.null(DT)) return(NULL)
    
    cfg  <- .cfg
    b    <- base_scn(cfg)
    pols <- policy_scns(cfg)
    
    wide_scns <- scenario_cols(DT)
    pol_from_var <- gsub("^(delta|pct)_", "", grep("^(delta|pct)_", names(DT), value = TRUE))
    pol_from_var <- unique(pol_from_var)
    
    all_scns <- unique(c(b, pols, wide_scns, pol_from_var))
    all_scns <- all_scns[nzchar(all_scns)]
    
    selectizeInput(
      "scn_pick", "Scenarios:",
      choices  = setNames(all_scns, all_scns),
      selected = all_scns,
      multiple = TRUE,
      options  = list(plugins = list("remove_button"))
    )
  })
  
  # ---- Dynamic filters for keys (skip scenario/time/derived)
  output$dynamic_filters <- renderUI({
    DT <- symDT()
    if (is.null(DT)) return(NULL)
    
    scns     <- scenario_cols(DT)
    key_cols <- setdiff(names(DT), c(scns, "delta","pct","t","year",
                                     grep("^delta_", names(DT), value = TRUE),
                                     grep("^pct_",   names(DT), value = TRUE)))
    show_keys <- intersect(c("n","n1","i","c","au","oc"), key_cols)
    if (length(show_keys) == 0) return(NULL)
    
    pickers <- vector("list", length(show_keys))
    for (ix in seq_along(show_keys)) {
      k <- show_keys[ix]
      vals <- sort(unique(as.character(DT[[k]])))
      if (length(vals) <= 1 || all(!nzchar(vals))) next
      pickers[[ix]] <- selectizeInput(
        inputId = paste0("key_", k),
        label   = paste0("Filter ", k, ":"),
        choices = c("(all)" = "", vals), selected = "",
        options = list(plugins = list("remove_button")),
        multiple = TRUE
      )
    }
    do.call(tagList, pickers)
  })
  
  observe({
    DT <- symDT(); if (is.null(DT)) return()
    scns     <- scenario_cols(DT)
    key_cols <- setdiff(names(DT), c(scns, "delta","pct","t","year",
                                     grep("^delta_", names(DT), value = TRUE),
                                     grep("^pct_",   names(DT), value = TRUE)))
    show_keys <- intersect(c("n","n1","i","c","au","oc"), key_cols)
    for (k in show_keys) {
      vals <- sort(unique(as.character(DT[[k]])))
      if (length(vals) <= 1 || all(!nzchar(vals))) next
      if (k == "n") vals <- levels(macro_first(vals))
      updateSelectizeInput(session, paste0("key_", k),
                           choices = c("(all)" = "", vals), server = TRUE)
    }
  })
  
  # ---- Filtered slice (keys only; scenario selection handled at plotting stage)
  filteredDT_raw <- reactive({
    DT <- results_by_symbol[[input$sym]]
    if (is.null(DT)) return(NULL)
    DT <- shallow_copy(DT)
    if ("n" %in% names(DT)) DT[, n := macro_first(n)]
    if (!"year" %in% names(DT) && "t" %in% names(DT)) DT[, year := 2014L + as.integer(t)]
    for (k in intersect(c("n","n1","i","c","au","oc"), names(DT))) {
      pick <- input[[paste0("key_", k)]]
      if (!is.null(pick) && length(pick) > 0 && any(nchar(pick) > 0)) {
        DT <- DT[get(k) %in% pick]
      }
    }
    if ("pct" %in% names(DT)) DT[, pct_plot := if (isTRUE(input$asPercent)) 100 * pct else pct]
    DT[]
  })
  
  filteredDT <- shiny::bindCache(
    filteredDT_raw, input$sym, input$asPercent,
    input$key_n, input$key_i, input$key_c, input$key_n1, input$key_au, input$key_oc
  )
  
  # ---- Variable help / context box
  # helpers (add once, near label_of/desc_of)
  note_of <- function(sym) {
    # put your custom per-variable guidance here (HTML allowed)
    switch(sym,
           "OUT_COMP6_SHARE_REAL_t" = paste(
             "Interpretation:",
             "Each point shows the sector share changes between base year and final year.",
             "The x (y) axis shows the variation in the baseline (policy) scenario.",
             "Points on the diagonal: same variation in baseline and policy scenario",
             "Above: policy increases the sector's share vs baseline; below: decreases."
           ),
           "BITRADE_REG_t" = paste(
             "Interpretation:",
             "Facets form a matrix of bilateral flows (rows = exporters, columns = importers)."
           ),
           "TB_GDP_t" = "Interpretation: ratio of trade balance to GDP; positive = surplus/GDP, negative = deficit/GDP.",
           "I_PP_SECT6_t" = "Interpretation: gross investment aggregated into 6 sector groups.",
           NULL # default = no extra note
    )
  }
  
  output$var_help_box <- renderUI({
    if (is.null(input$sym)) return(NULL)
    HTML(sprintf(
      '<div class="var-help">
       <b>%s</b><br/>
       <i>%s</i>%s
     </div>',
      label_of(input$sym),
      desc_of(input$sym),
      {
        nt <- note_of(input$sym)
        if (is.null(nt) || !nzchar(nt)) "" else paste0("<br/>", nt)
      }
    ))
  })
  
  # ---- Table
  output$tbl <- renderDT({
    req(input$view == "table")
    datatable(filteredDT(), options = list(pageLength = 20, scrollX = TRUE))
  })
  
  # ---- Plot
  output$plt <- renderPlot({
    
    cfg <- .cfg
    b   <- base_scn(cfg)
    pols_cfg <- policy_scns(cfg)
    
    # Which scenarios to show?
    scn_pick <- input$scn_pick
    pol_pick <- intersect(scn_pick, pols_cfg)
    
    pair_title <- function() {
      if (length(pol_pick) == 0) return("Policy vs baseline")
      if (length(pol_pick) == 1) return(paste(pol_pick, "vs", b))
      paste(paste(pol_pick, collapse = " + "), "vs", b)
    }
    
    # ---- SPECIAL: BITRADE_REG_t (faceted matrix)
    if (input$sym == "BITRADE_REG_t") {
      BT <- filteredDT(); req(!is.null(BT), NROW(BT) > 0)
      if (input$view == "plot_var") {
        L <- to_long_var(BT, use_pct = isTRUE(input$asPercent), only_policies = pol_pick)
        req(!is.null(L), NROW(L) > 0)
        ord <- c("EEU","NWEU","SEU","USA","CHN","IND","OECD","NonOECD","ROW","TOT")
        if ("n"  %in% names(L))  L[,  n := factor(as.character(n),  levels = ord)]
        if ("n1" %in% names(L)) L[, n1 := factor(as.character(n1), levels = ord)]
        xcol <- if ("year" %in% names(L)) "year" else "t"
        ylab <- if (isTRUE(input$asPercent)) "Deviation wrt baseline (%)" else "Change (level)"
        p <- ggplot(L, aes_string(x = xcol, y = "value", color = "scenario", linetype = "scenario", group = "scenario")) +
          geom_hline(yintercept = 0, linewidth = 0.3) +
          geom_line(linewidth = 0.8) +
          facet_grid(n1 ~ n, scales = "free_y") +
          labs(x = "Year", y = ylab, title = paste("Bilateral trade (macro regions):", pair_title()),
               color = "Policy", linetype = "Policy") +
          theme_minimal(base_size = 11) +
          theme(legend.position = "bottom",
                panel.border = element_rect(color = "black", fill = NA, linewidth = 0.4),
                panel.background = element_rect(fill = "grey97", color = NA))
        return(p)
      } else {
        Scns <- scenario_cols(BT)
        use_scns <- if (length(scn_pick)) intersect(Scns, scn_pick) else Scns
        L <- to_long_levels(BT, scenarios = use_scns); req(!is.null(L), NROW(L) > 0)
        ord <- c("EEU","NWEU","SEU","USA","CHN","IND","OECD","NonOECD","ROW","TOT")
        if ("n"  %in% names(L))  L[,  n := factor(as.character(n),  levels = ord)]
        if ("n1" %in% names(L)) L[, n1 := factor(as.character(n1), levels = ord)]
        xcol <- if ("year" %in% names(L)) "year" else "t"
        p <- ggplot(L, aes_string(x = xcol, y = "value", color = "scenario", linetype = "scenario", group = "scenario")) +
          geom_line() + geom_point(size = 0.6) +
          facet_grid(n1 ~ n, scales = "free_y") +
          labs(x = "Year", y = label_of(input$sym),
               title = paste("Bilateral trade (macro regions) — Levels"),
               color = "Scenario", linetype = "Scenario") +
          theme_minimal(base_size = 11) +
          theme(legend.position = "bottom",
                panel.border = element_rect(color = "black", fill = NA, linewidth = 0.4),
                panel.background = element_rect(fill = "grey97", color = NA))
        return(p)
      }
    }
    
    # ---- SPECIAL: OUT_COMP6_SHARE_REAL_t — Δ-share scatter (baseline vs ONE policy)
    if (input$sym == "OUT_COMP6_SHARE_REAL_t" && length(pols_cfg) >= 1) {
      DT <- filteredDT(); req(!is.null(DT), NROW(DT) > 0)
      if (!"year" %in% names(DT) && "t" %in% names(DT)) DT[, year := 2014L + as.integer(t)]
      p1 <- if (length(pol_pick) >= 1) pol_pick[1] else pols_cfg[1]
      
      L <- to_long_levels(DT, scenarios = intersect(scenario_cols(DT), c(b, p1))); req(NROW(L) > 0)
      yr0 <- L[, min(year, na.rm = TRUE)]
      yrT <- L[, max(year, na.rm = TRUE)]
      End <- L[year %in% c(yr0, yrT)]
      
      End_w <- data.table::dcast(End, n + i + scenario ~ year, value.var = "value")
      End_w <- End_w[!is.na(get(as.character(yr0))) & !is.na(get(as.character(yrT)))]
      End_w[, delta := get(as.character(yrT)) - get(as.character(yr0))]
      SC <- data.table::dcast(End_w[, .(n, i, scenario, delta)], n + i ~ scenario, value.var = "delta")
      
      if (all(c(b, p1) %in% names(SC))) {
        mx <- SC[, max(abs(c(get(b), get(p1))), na.rm = TRUE)]
        if (is.finite(mx) && mx <= 1.001) {
          SC[, (b)  := 100 * get(b)]
          SC[, (p1) := 100 * get(p1)]
        }
      } else return(NULL)
      
      if ("n" %in% names(SC)) SC[, n := macro_first(n)]
      SC <- SC[is.finite(get(b)) & is.finite(get(p1))]
      req(NROW(SC) > 0)
      
      p <- ggplot(SC, aes(x = .data[[b]], y = .data[[p1]], color = i)) +
        geom_abline(slope = 1, intercept = 0, linewidth = 0.6, linetype = 2, alpha = 0.8) +
        geom_hline(yintercept = 0, linewidth = 0.4, linetype = 3, alpha = 0.7) +
        geom_vline(xintercept = 0, linewidth = 0.4, linetype = 3, alpha = 0.7) +
        geom_point(size = 2, alpha = 0.9) +
        coord_equal() +
        facet_wrap(~ n) +
        labs(
          x = paste0(b,   " ", yr0, "→", yrT),
          y = paste0(p1, " ", yr0, "→", yrT),
          color = "Sector group (6)",
          title = paste0("Policy vs baseline change in output shares (", yr0, "→", yrT, ")"),
          subtitle = "Above diagonal: policy share increases vs baseline"
        ) +
        theme_minimal(base_size = 11) +
        theme(
          legend.position = "bottom",
          panel.border     = element_rect(color = "black", fill = NA, linewidth = 0.4),
          panel.background = element_rect(fill = "grey97", color = NA)
        )
      return(p)
    }
    
    # ---- Generic plots
    DT <- filteredDT(); req(!is.null(DT), NROW(DT) > 0)
    if (!data.table::is.data.table(DT)) data.table::setDT(DT)
    xcol <- if ("year" %in% names(DT)) "year" else if ("t" %in% names(DT)) "t" else setdiff(names(DT), c("delta","pct"))[1]
    
    if (input$view == "plot_lvl") {
      Scns <- scenario_cols(DT)
      use_scns <- intersect(Scns, input$scn_pick)
      L <- to_long_levels(DT, scenarios = use_scns); req(!is.null(L), NROW(L) > 0)
      gcols <- setdiff(setdiff(names(L), c("scenario","value")), xcol)
      if (length(gcols) == 0) {
        L[, grp := "all"]
      } else {
        L[, grp := do.call(paste, c(.SD, list(sep = "|"))), .SDcols = gcols]
      }
      L[, grp2 := paste(grp, scenario, sep = "|")]
      p <- ggplot(L, aes_string(x = xcol, y = "value",
                                group = "grp2", color = "scenario", linetype = "scenario")) +
        geom_line() + geom_point(size = 0.8) +
        labs(x = xcol, y = label_of(input$sym),
             color = "Scenario", linetype = "Scenario",
             title = paste("Levels —", label_of(input$sym))) +
        theme_minimal()
      facet_key <- NULL
      for (cand in c("n","i","c","n1","au","oc")) if (cand %in% gcols) { facet_key <- cand; break }
      if (!is.null(facet_key)) p <- p + facet_wrap(as.formula(paste("~", facet_key)), scales = "free_y")
      return(p)
      
    } else {
      # Δ / % over policy columns
      pol_from_var <- gsub("^(delta|pct)_", "", grep("^(delta|pct)_", names(DT), value = TRUE))
      pol_pick <- intersect(input$scn_pick, policy_scns(cfg))
      pol_pick <- intersect(pol_pick, unique(pol_from_var))
      if (length(pol_pick) == 0) pol_pick <- unique(pol_from_var)
      L <- to_long_var(DT, use_pct = isTRUE(input$asPercent), only_policies = pol_pick)
      req(!is.null(L), NROW(L) > 0)
      
      keys <- setdiff(names(L), c("scenario","value","t","year"))
      facet_key <- NULL
      for (cand in c("n","i","c","n1","au","oc")) if (cand %in% keys) { facet_key <- cand; break }
      group_cols <- setdiff(keys, c(xcol, facet_key))
      if (length(group_cols) == 0L) { L[, grp := "all"] } else {
        L[, grp := do.call(paste, c(.SD, list(sep = "|"))), .SDcols = group_cols]
      }
      ylab <- if (isTRUE(input$asPercent)) "Change vs baseline (%)" else "Change vs baseline (level)"
      p <- ggplot(L, aes_string(x = xcol, y = "value", color = "scenario", linetype = "scenario", group = "interaction(scenario, grp)")) +
        geom_hline(yintercept = 0, linetype = 2) +
        geom_line(alpha = 0.95) + geom_point(size = 0.7, alpha = 0.9) +
        labs(x = xcol, y = ylab,
             title = paste(pair_title(), "—", label_of(input$sym)),
             color = "Policy", linetype = "Policy") +
        theme_minimal()
      if (!is.null(facet_key)) p <- p + facet_wrap(as.formula(paste("~", facet_key)), scales = "free_y")
      return(p)
    }
  }) %>% shiny::bindCache(
    input$sym, input$view, input$asPercent, input$scn_pick,
    input$key_n, input$key_i, input$key_c, input$key_n1, input$key_au, input$key_oc
  )
}

shinyApp(ui, server)
