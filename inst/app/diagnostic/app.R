library(shiny)
library(data.table)
library(ggplot2)
library(DT)

# Bind internal helpers from the package namespace
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

# ---- Helpers to read base and promote to derived-shape ----
read_base_symbol <- function(name, dir_base = file.path("outputs","base")) {
  f_parq <- file.path(dir_base, paste0(name, ".parquet"))
  f_feat <- file.path(dir_base, paste0(name, ".feather"))
  if (file.exists(f_parq)) {
    if (!requireNamespace("arrow", quietly = TRUE)) return(NULL)
    return(data.table::as.data.table(arrow::read_parquet(f_parq)))
  }
  if (file.exists(f_feat)) {
    if (requireNamespace("arrow", quietly = TRUE)) {
      return(data.table::as.data.table(arrow::read_feather(f_feat)))
    } else if (requireNamespace("feather", quietly = TRUE)) {
      return(data.table::as.data.table(feather::read_feather(f_feat)))
    } else return(NULL)
  }
  NULL
}

promote_base_to_derived <- function(tbl, key_cols, cfg = NULL) {
  # expect tbl long: keys + scenario + value
  need <- c(key_cols, "scenario", "value")
  if (!all(need %in% names(tbl))) return(NULL)

  if (is.null(cfg)) cfg <- load_config()
  b  <- base_scn(cfg)
  ps <- policy_scns(cfg)
  if (length(ps) < 1L) return(NULL)        # no policy scenario available
  p  <- ps[[1]]                             # PR1: first policy only

  # long -> wide
  W <- data.table::dcast(
    data.table::as.data.table(tbl),
    as.formula(paste(paste(key_cols, collapse = " + "), "~ scenario")),
    value.var = "value"
  )

  # need both columns present
  if (!all(c(b, p) %in% names(W))) return(NULL)

  # compute delta and pct safely
  W <- W[!is.na(.SD[[b]]) & !is.na(.SD[[p]])]
  W[, delta := .SD[[p]] - .SD[[b]]]
  W[, pct   := data.table::fifelse(abs(.SD[[b]]) > .Machine$double.eps,
                                   delta / .SD[[b]], NA_real_)]
  W[]
}

# ---- Load bundle first, then fallback to manifest ----
get_results <- function() {
  dir <- file.path("outputs","derived")
  b <- try(load_bundle("diagnostic_app", dir = dir), silent = TRUE)
  if (!inherits(b, "try-error") && !is.null(b)) return(b)

  # Fallback: read per symbol via manifest, using the list in YAML/docs
  wanted <- c("GDPr_t","HDY_VAL_t","HDYr_t","I_TOT_PP_t","DS_t","FS_t","GSUR_VAL_t","GINV_VAL_t",
              "TBr_t","TB_GDP_t","HSAVR_t","U_t","KLratio_country_t","ir_t","P_HH_CPI_t",
              "I_PP_t","K_t","L_t","GHG_t","KLratio_t","P_Q_t","P_KL_t",
              "I_PP_SECT6_t","OUT_COMP6_SHARE_REAL_t","BITRADE_REG_t")
  setNames(lapply(wanted, function(s) load_symbol(s, dir = dir)), wanted)
}

results_by_symbol <- get_results()
# Keep only successfully loaded (non-NULL) tables
results_by_symbol <- Filter(function(x) !is.null(x) && nrow(x) > 0, results_by_symbol)

available_syms <- names(results_by_symbol)

# ---- Your existing meta (trimmed to what exists) ----
meta <- data.table::rbindlist(list(
  # ---------- Nation level (n,t)
  data.table(symbol = c("GDPr_t","HDY_VAL_t","HDYr_t","I_TOT_PP_t","DS_t","FS_t","GSUR_VAL_t",
                        "GINV_VAL_t","TBr_t","TB_GDP_t","HSAVR_t","U_t","KLratio_country_t","ir_t","P_HH_CPI_t"),
             label  = c("Real GDP","Household disposable income","Household real disposable income","Total investment",
                        "Domestic savings","Foreign savings","Government surplus","Government investment",
                        "Trade balance (real)","Trade balance to GDP ratio",
                        "Household saving rate","Unemployment","Capital–labor ratio (country)","Interest rate","HH CPI"),
             group  = "Nation level", keep = TRUE),

  # ---------- Industry level (n,i,t)
  data.table(symbol = c("I_PP_t","K_t","L_t","GHG_t","KLratio_t","P_Q_t","P_KL_t",
                        "I_PP_SECT6_t","OUT_COMP6_SHARE_REAL_t"),
             label  = c("Investment by sector","Capital demand","Labor demand","Emissions",
                        "Capital–labor ratio (sector)","Output price","K/L price",
                        "Investment by 6-sector group","Output shares by 6-sector groups (real)"),
             group  = "Industry level", keep = TRUE),

  # ---------- Bilateral trade
  data.table(symbol = "BITRADE_REG_t",
             label  = "Bilateral trade flows (macro regions)",
             group  = "Bilateral trade", keep = TRUE)
))

# keep only those actually available & marked keep
meta <- meta[symbol %in% available_syms & keep == TRUE]
meta[, group := factor(group, levels = c("Nation level","Industry level","Bilateral trade"))]

# =========================
# Helpers
# =========================

# Show macro regions first in facets/legends
macro_first <- function(vals) {
  vals <- as.character(vals)
  known <- c("EU28","NonEU28","WORLD")
  rest  <- setdiff(vals, known)
  factor(vals, levels = c(known[known %in% vals], sort(rest)))
}

# Convert t -> calendar year (base 2014 -> t=1 = 2015). No-op if no 't'.
add_year <- function(DT) {
  if ("t" %in% names(DT)) DT[, year := 2014 + as.numeric(t)]
  DT
}

# Long-format helper for level plots
to_long <- function(DT, scenarios = NULL) {
  if (is.null(DT)) return(NULL)
  if (is.null(scenarios)) scenarios <- scenario_cols(DT)
  id_cols <- setdiff(names(DT), c(scenarios, "delta", "pct"))
  melt(DT, id.vars = id_cols, measure.vars = scenarios,
       variable.name = "scenario", value.name = "value")
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
  "))),
  titlePanel("FIDELIO diagnostics"),
  sidebarLayout(
    sidebarPanel(
      selectizeInput(
        "sym", "Variable:",
        choices = lapply(split(meta, meta$group), function(d) stats::setNames(d$symbol, d$label)),
        selected = meta$symbol[1]
      ),
      uiOutput("dynamic_filters"),
      radioButtons(
        "view", "Show:",
        c("Table" = "table", "Plot levels" = "plot_lvl", "Plot Δ / %" = "plot_var"),
        selected = "plot_lvl"
      ),
      checkboxInput("asPercent", "Show pct as %", TRUE)
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

  # Selected table for current symbol
  symDT <- reactive({
    results_by_symbol[[input$sym]]
  })

  # ---------- Dynamic filters (built from keys present; skip scenarios/time) ----------
  output$dynamic_filters <- renderUI({
    DT <- symDT()
    if (is.null(DT)) return(NULL)
    scns     <- scenario_cols(DT)
    key_cols <- setdiff(names(DT), c(scns, "delta","pct","t","year"))
    show_keys <- intersect(c("n","n1","i","c","au","oc"), key_cols)
    if (length(show_keys) == 0) return(NULL)

    pickers <- lapply(show_keys, function(k) {
      selectizeInput(
        inputId = paste0("key_", k),
        label   = paste0("Filter ", k, ":"),
        choices = c("(all)" = ""), selected = "",
        options = list(plugins = list("remove_button")),
        multiple = TRUE
      )
    })
    do.call(tagList, pickers)
  })

  # Populate the choices (after UI exists)
  observe({
    DT <- symDT(); if (is.null(DT)) return()
    scns     <- scenario_cols(DT)
    key_cols <- setdiff(names(DT), c(scns, "delta","pct","t","year"))
    show_keys <- intersect(c("n","n1","i","c","au","oc"), key_cols)

    for (k in show_keys) {
      vals <- sort(unique(as.character(DT[[k]])))
      if (k == "n") vals <- levels(macro_first(vals))
      updateSelectizeInput(session, paste0("key_", k),
                           choices = c("(all)" = "", vals), server = TRUE)
    }
  })

  # ---- filtered slice (define raw, then cache once) ----
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

    if ("pct" %in% names(DT)) {
      DT[, pct_plot := if (isTRUE(input$asPercent)) 100 * pct else pct]
    }

    DT[]
  })

  # cache based on the true dependencies of the slice
  filteredDT <- shiny::bindCache(
    filteredDT_raw,
    input$sym, input$asPercent,
    input$key_n, input$key_i, input$key_c, input$key_n1, input$key_au, input$key_oc
  )

  # ---------- Table view ----------
  output$tbl <- renderDT({
    req(input$view == "table")
    datatable(filteredDT(), options = list(pageLength = 20, scrollX = TRUE))
  })

  # ---------- Plot view ----------
  output$plt <- renderPlot({

    cfg <- load_config()
    b   <- base_scn(cfg)
    p1  <- policy_scns(cfg)[1]
    title_pair <- if (!is.na(p1)) paste(p1, "vs", b) else "Policy vs baseline"

    # ---- SPECIAL PLOT for BITRADE_REG_t -----------------------------------------
    if (input$sym == "BITRADE_REG_t") {
      BT <- filteredDT()
      ord <- c("EEU","NWEU","SEU","USA","CHN","IND","OECD","NonOECD","ROW","TOT")
      if ("n"  %in% names(BT))  BT[,  n := factor(as.character(n),  levels = ord)]
      if ("n1" %in% names(BT)) BT[, n1 := factor(as.character(n1), levels = ord)]

      req(!is.null(BT), NROW(BT) > 0)

      # year/delta/pct already precomputed in derived
      xcol <- if ("year" %in% names(BT)) "year" else "t"
      yvar <- if (input$view == "plot_var") {
        if ("pct_plot" %in% names(BT)) "pct_plot" else "pct"
      } else "delta"
      ylab <- if (yvar %in% c("pct","pct_plot")) "Deviation wrt baseline (%)" else "Change (level)"

      p <- ggplot(BT, aes_string(x = xcol, y = yvar, color = "c", group = "c")) +
        geom_hline(yintercept = 0, color = "grey30", linewidth = 0.3) +
        geom_line(linewidth = 0.8) +
        facet_grid(n1 ~ n, scales = "free_y") +
        labs(x = "Year", y = ylab, color = "Commodity",
             title = paste("Bilateral trade (macro regions):", title_pair)) +
        theme_minimal(base_size = 11) +
        theme(
          legend.position = "bottom",
          panel.border = element_rect(color = "black", fill = NA, linewidth = 0.4),
          panel.background = element_rect(fill = "grey97", color = NA)
        )

      return(p)
    }
    # ---- END SPECIAL PLOT for BITRADE_REG_t -------------------------------------

    # ---- SPECIAL PLOT for OUT_COMP6_SHARE_REAL_t — Δ-share scatter --------------
    if (input$sym == "OUT_COMP6_SHARE_REAL_t") {
      DT <- filteredDT()
      req(!is.null(DT), NROW(DT) > 0)

      if (!"year" %in% names(DT) && "t" %in% names(DT)) DT[, year := 2014L + as.integer(t)]
      req("year" %in% names(DT))

      # Long format over scenarios
      L <- to_long(DT)
      req(!is.null(L), NROW(L) > 0)

      # Endpoint years
      yr0 <- L[, min(year, na.rm = TRUE)]
      yrT <- L[, max(year, na.rm = TRUE)]

      # Keep only endpoints
      End <- L[year %in% c(yr0, yrT)]

      # Change (final - initial) by (n,i,scenario)
      End_w <- data.table::dcast(
        End, n + i + scenario ~ year, value.var = "value"
      )
      End_w <- End_w[!is.na(get(as.character(yr0))) & !is.na(get(as.character(yrT)))]
      End_w[, delta := get(as.character(yrT)) - get(as.character(yr0))]

      # Cast scenarios wide with dynamic names
      SC <- data.table::dcast(
        End_w[, .(n, i, scenario, delta)],
        n + i ~ scenario, value.var = "delta"
      )

      # Work out available scenario columns
      sc_cols <- intersect(names(SC), scenario_cols(SC))
      if (!(b %in% sc_cols)) return(NULL)
      pol_col <- if (!is.na(p1) && p1 %in% sc_cols) p1 else setdiff(sc_cols, b)[1]
      if (is.na(pol_col)) return(NULL)

      # If shares are fractions (0–1), convert to pp
      max_abs <- SC[, max(abs(c(get(b), get(pol_col))), na.rm = TRUE)]
      frac_like <- is.finite(max_abs) && max_abs <= 1.001
      if (isTRUE(frac_like)) {
        SC[, (b)        := 100 * get(b)]
        SC[, (pol_col)  := 100 * get(pol_col)]
        ylab_pp <- paste0("Δ share (pp, ", yr0, "→", yrT, ")")
      } else {
        ylab_pp <- paste0("Δ share (units, ", yr0, "→", yrT, ")")
      }

      if ("n" %in% names(SC)) SC[, n := macro_first(n)]
      SC <- SC[is.finite(get(b)) & is.finite(get(pol_col))]
      req(NROW(SC) > 0)

      p <- ggplot(SC, aes(x = .data[[b]], y = .data[[pol_col]], color = i)) +
        geom_abline(slope = 1, intercept = 0, linewidth = 0.6, linetype = 2, alpha = 0.8) +
        geom_hline(yintercept = 0, linewidth = 0.4, linetype = 3, alpha = 0.7) +
        geom_vline(xintercept = 0, linewidth = 0.4, linetype = 3, alpha = 0.7) +
        geom_point(size = 2, alpha = 0.9) +
        coord_equal() +
        facet_wrap(~ n) +
        labs(
          x = paste0(b,   " ", yr0, "→", yrT),
          y = paste0(pol_col, " ", yr0, "→", yrT),
          color = "Sector group (6)",
          title = paste0("Policy vs baseline change in output shares (", yr0, "→", yrT, ")"),
          subtitle = "Points above the diagonal: policy increases the share vs baseline; below: reduces it",
          caption = paste("Policy vs baseline =", title_pair)
        ) +
        theme_minimal(base_size = 11) +
        theme(
          legend.position   = "bottom",
          panel.border      = element_rect(color = "black", fill = NA, linewidth = 0.4),
          panel.background  = element_rect(fill = "grey97", color = NA)
        )

      return(p)
    }
    # ---- END SPECIAL PLOT for OUT_COMP6_SHARE_REAL_t -----------------------------

    # -------- Generic plots --------
    req(input$view != "table")
    DT <- filteredDT()
    req(!is.null(DT), NROW(DT) > 0)

    if (!data.table::is.data.table(DT)) data.table::setDT(DT)

    scns <- scenario_cols(DT)
    keys <- setdiff(names(DT), c(scns, "delta","pct","t","year"))

    # x-axis: prefer 'year', then 't', else first key
    xcol <- if ("year" %in% names(DT)) "year" else if ("t" %in% names(DT)) "t" else keys[1]

    if (input$view == "plot_lvl") {
      L <- to_long(DT)
      req(!is.null(L), NROW(L) > 0)
      if (!data.table::is.data.table(L)) data.table::setDT(L)

      # group lines by all keys except x and scenario
      gcols <- setdiff(setdiff(names(L), c("scenario","value")), xcol)
      if (length(gcols) == 0) {
        L[, grp := "all"]
      } else {
        L[, grp := do.call(paste, c(.SD, list(sep = "|"))), .SDcols = gcols]
      }
      L[, grp2 := paste(grp, scenario, sep = "|")]

      p <- ggplot(L, aes_string(x = xcol, y = "value",
                                group = "grp2", color = "scenario",
                                linetype = "scenario")) +
        geom_line() +
        geom_point(size = 0.8) +
        labs(x = xcol, y = meta[symbol == input$sym, label][1],
             color = "Scenario", linetype = "Scenario",
             title = paste("Levels —", meta[symbol == input$sym, label][1])) +
        theme_minimal()

      # facet by the “largest” key among n > i > c > n1 > au > oc
      facet_key <- NULL
      for (cand in c("n","i","c","n1","au","oc")) if (cand %in% gcols) { facet_key <- cand; break }
      if (!is.null(facet_key)) p <- p + facet_wrap(as.formula(paste("~", facet_key)), scales = "free_y")

      p

    } else {
      # Δ or % plot  (use the pre-scaled pct_plot when available)
      yvar <- if (isTRUE(input$asPercent)) {
        if ("pct_plot" %in% names(DT)) "pct_plot" else "pct"
      } else "delta"

      ylab <- if (yvar %in% c("pct","pct_plot")) "Change (%)" else "Change (level)"

      # choose a facet variable automatically: n > i > c > n1 > au > oc
      facet_key <- NULL
      for (cand in c("n","i","c","n1","au","oc")) if (cand %in% keys) { facet_key <- cand; break }

      # lines grouped by all keys except x & chosen facet
      group_cols <- setdiff(keys, c(xcol, facet_key))
      if (length(group_cols) == 0L) {
        DT[, grp := "all"]; color_col <- NULL
      } else {
        DT[, grp := do.call(paste, c(.SD, list(sep = "|"))), .SDcols = group_cols]
        color_col <- group_cols[1]
      }

      p <- ggplot(DT, aes_string(x = xcol, y = yvar, group = "grp")) +
        geom_hline(yintercept = 0, linetype = 2) +
        geom_line(alpha = 0.9) +
        geom_point(size = 0.7, alpha = 0.9) +
        labs(x = xcol, y = ylab,
             title = paste(title_pair, "—", meta[symbol == input$sym, label][1])) +
        theme_minimal()

      if (!is.null(color_col)) p <- p + aes_string(color = color_col) + labs(color = color_col)
      if (!is.null(facet_key)) p <- p + facet_wrap(as.formula(paste("~", facet_key)), scales = "free_y")

      p
    }
  }) %>% shiny::bindCache(
    input$sym, input$view, input$asPercent,
    input$key_n, input$key_i, input$key_c, input$key_n1, input$key_au, input$key_oc
  )
}

shinyApp(ui, server)
