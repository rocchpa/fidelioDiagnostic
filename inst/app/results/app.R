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
# Data loading
# =========================
get_results <- function() {
  dir <- file.path("outputs","derived")
  wanted <- c("GDPr_t","TBr_t","TB_GDP_t","I_PP_SECT6_t","OUT_COMP6_SHARE_REAL_t","BITRADE_REG_t")

  cat("\n[results_app] Looking in:", normalizePath(dir, winslash = "/"), "\n")

  # 1) Try bundle
  b <- try(load_bundle("results_app", dir = dir), silent = TRUE)
  if (!inherits(b, "try-error") && !is.null(b)) {
    cat("[results_app] Bundle found. Symbols inside:\n  -", paste(names(b), collapse = "\n  - "), "\n")
  } else {
    cat("[results_app] No bundle found (or load error).\n")
    b <- list()
  }

  # 2) Backfill per-symbol files for anything missing from bundle
  missing <- setdiff(wanted, names(b))
  if (length(missing)) {
    cat("[results_app] Backfilling per-symbol files for:", paste(missing, collapse = ", "), "\n")
    for (s in missing) {
      files <- list.files(dir, pattern = paste0("^", s, "\\.(parquet|feather|fst|rds|csv)$"))
      cat("  ·", s, "files in dir:", if (length(files)) paste(files, collapse = ", ") else "(none)", "\n")
      b[[s]] <- try(load_symbol(s, dir = dir), silent = TRUE)
      if (inherits(b[[s]], "try-error")) b[[s]] <- NULL
    }
  }

  # 3) Report row counts
  for (s in intersect(names(b), wanted)) {
    n <- try(nrow(b[[s]]), silent = TRUE)
    cat("  · rows(", s, ") = ", if (inherits(n, "try-error") || is.null(n)) "ERR/NULL" else n, "\n", sep = "")
  }

  # 4) Drop NULL/empty
  b <- Filter(function(x) !is.null(x) && is.data.frame(x) && nrow(x) > 0, b)

  # 5) Final report
  have <- intersect(names(b), wanted)
  miss <- setdiff(wanted, have)
  cat("[results_app] Loaded symbols:", paste(have, collapse = ", "), "\n")
  if (length(miss)) cat("[results_app] Missing or empty:", paste(miss, collapse = ", "), "\n")

  b
}

results_by_symbol <- get_results()
results_by_symbol <- Filter(function(x) !is.null(x) && nrow(x) > 0, results_by_symbol)
available_syms <- names(results_by_symbol)

# =========================
# Meta (labels & groups)
# =========================
meta <- data.table::rbindlist(list(
  data.table(
    symbol = c("GDPr_t","TBr_t","TB_GDP_t"),
    label  = c("Real GDP","Trade balance (real)","Trade balance to GDP ratio"),
    group  = "Nation level", keep = TRUE
  ),
  data.table(
    symbol = c("I_PP_SECT6_t","OUT_COMP6_SHARE_REAL_t"),
    label  = c("Investment by 6-sector group","Output shares by 6-sector groups (real)"),
    group  = "Industry level", keep = TRUE
  ),
  data.table(
    symbol = "BITRADE_REG_t",
    label  = "Bilateral trade flows (macro regions)",
    group  = "Bilateral trade", keep = TRUE
  )
))
meta <- meta[symbol %in% available_syms & keep == TRUE]
meta[, group := factor(group, levels = c("Nation level","Industry level","Bilateral trade"))]

choices_grouped <- lapply(split(meta, meta$group), function(d)
  stats::setNames(d$symbol, d$label)
)
label_of <- function(sym) meta[symbol == sym, label][1]

# =========================
# Helpers
# =========================
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
  titlePanel("FIDELIO results"),
  sidebarLayout(
    sidebarPanel(
      selectizeInput("sym", "Variable:", choices = choices_grouped, selected = meta$symbol[1]),
      uiOutput("dynamic_filters"),
      radioButtons("view", "Show:",
                   c("Table" = "table", "Plot levels" = "plot_lvl", "Plot Δ / %" = "plot_var"),
                   selected = "plot_lvl"),
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

  symDT <- reactive({ results_by_symbol[[input$sym]] })

  # Dynamic filters (skip scenario/time)
  output$dynamic_filters <- renderUI({
    DT <- symDT()
    if (is.null(DT)) return(NULL)

    scns     <- scenario_cols(DT)
    key_cols <- setdiff(names(DT), c(scns, "delta","pct","t","year"))
    show_keys <- intersect(c("n","n1","i","c","au","oc"), key_cols)
    if (length(show_keys) == 0) return(NULL)

    pickers <- list()
    for (k in show_keys) {
      vals <- sort(unique(as.character(DT[[k]])))
      if (length(vals) <= 1 || all(!nzchar(vals))) next
      pickers[[k]] <- selectizeInput(
        inputId = paste0("key_", k),
        label   = paste0("Filter ", k, ":"),
        choices = c("(all)" = "", vals), selected = "",
        options = list(plugins = list("remove_button")),
        multiple = TRUE
      )
    }
    if (length(pickers) == 0) return(NULL)
    do.call(tagList, pickers)
  })

  observe({
    DT <- symDT(); if (is.null(DT)) return()
    scns     <- scenario_cols(DT)
    key_cols <- setdiff(names(DT), c(scns, "delta","pct","t","year"))
    show_keys <- intersect(c("n","n1","i","c","au","oc"), key_cols)
    for (k in show_keys) {
      vals <- sort(unique(as.character(DT[[k]])))
      if (length(vals) <= 1 || all(!nzchar(vals))) next
      if (k == "n") vals <- levels(macro_first(vals))
      updateSelectizeInput(session, paste0("key_", k),
                           choices = c("(all)" = "", vals), server = TRUE)
    }
  })

  # Filtered slice
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

  # Table
  output$tbl <- renderDT({
    req(input$view == "table")
    datatable(filteredDT(), options = list(pageLength = 20, scrollX = TRUE))
  })

  # Plot
  output$plt <- renderPlot({

    cfg <- load_config()
    b   <- base_scn(cfg)
    p1  <- policy_scns(cfg)[1]
    title_pair <- if (!is.na(p1)) paste(p1, "vs", b) else "Policy vs baseline"

    # ---- SPECIAL: BITRADE_REG_t ----
    if (input$sym == "BITRADE_REG_t") {
      BT <- filteredDT()
      ord <- c("EEU","NWEU","SEU","USA","CHN","IND","OECD","NonOECD","ROW","TOT")
      if ("n"  %in% names(BT))  BT[,  n := factor(as.character(n),  levels = ord)]
      if ("n1" %in% names(BT)) BT[, n1 := factor(as.character(n1), levels = ord)]
      req(!is.null(BT), NROW(BT) > 0)

      xcol <- if ("year" %in% names(BT)) "year" else "t"
      yvar <- if (input$view == "plot_var") {
        if ("pct_plot" %in% names(BT)) "pct_plot" else "pct"
      } else "delta"
      ylab <- if (yvar %in% c("pct","pct_plot")) "Deviation wrt baseline (%)" else "Change (level)"

      p <- ggplot(BT, aes_string(x = xcol, y = yvar)) +
        geom_hline(yintercept = 0, color = "grey30", linewidth = 0.3) +
        geom_line(linewidth = 0.8) +
        facet_grid(n1 ~ n, scales = "free_y") +
        labs(x = "Year", y = ylab,
             title = paste("Bilateral trade (macro regions):", title_pair)) +
        theme_minimal(base_size = 11) +
        theme(
          legend.position = "none",
          panel.border = element_rect(color = "black", fill = NA, linewidth = 0.4),
          panel.background = element_rect(fill = "grey97", color = NA)
        )
      return(p)
    }

    # ---- SPECIAL: OUT_COMP6_SHARE_REAL_t — Δ-share scatter ----
    if (input$sym == "OUT_COMP6_SHARE_REAL_t") {
      DT <- filteredDT(); req(!is.null(DT), NROW(DT) > 0)
      if (!"year" %in% names(DT) && "t" %in% names(DT)) DT[, year := 2014L + as.integer(t)]
      L <- to_long(DT); req(!is.null(L), NROW(L) > 0)

      yr0 <- L[, min(year, na.rm = TRUE)]
      yrT <- L[, max(year, na.rm = TRUE)]
      End <- L[year %in% c(yr0, yrT)]

      End_w <- data.table::dcast(End, n + i + scenario ~ year, value.var = "value")
      End_w <- End_w[!is.na(get(as.character(yr0))) & !is.na(get(as.character(yrT)))]
      End_w[, delta := get(as.character(yrT)) - get(as.character(yr0))]

      SC <- data.table::dcast(End_w[, .(n, i, scenario, delta)], n + i ~ scenario, value.var = "delta")

      # dynamic scenario columns
      sc_cols <- intersect(names(SC), scenario_cols(SC))
      if (!(b %in% sc_cols)) return(NULL)
      pol_col <- if (!is.na(p1) && p1 %in% sc_cols) p1 else setdiff(sc_cols, b)[1]
      if (is.na(pol_col)) return(NULL)

      # fraction-like -> pp
      max_abs <- SC[, max(abs(c(get(b), get(pol_col))), na.rm = TRUE)]
      frac_like <- is.finite(max_abs) && max_abs <= 1.001
      if (isTRUE(frac_like)) {
        SC[, (b)       := 100 * get(b)]
        SC[, (pol_col) := 100 * get(pol_col)]
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

    # ---- Generic plots ----
    req(input$view != "table")
    DT <- filteredDT(); req(!is.null(DT), NROW(DT) > 0)
    if (!data.table::is.data.table(DT)) data.table::setDT(DT)

    scns <- scenario_cols(DT)
    keys <- setdiff(names(DT), c(scns, "delta","pct","t","year"))

    xcol <- if ("year" %in% names(DT)) "year" else if ("t" %in% names(DT)) "t" else keys[1]

    if (input$view == "plot_lvl") {
      L <- to_long(DT); req(!is.null(L), NROW(L) > 0)
      if (!data.table::is.data.table(L)) data.table::setDT(L)
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
      p
    } else {
      yvar <- if (isTRUE(input$asPercent)) { if ("pct_plot" %in% names(DT)) "pct_plot" else "pct" } else "delta"
      ylab <- if (yvar %in% c("pct","pct_plot")) "Change (%)" else "Change (level)"
      facet_key <- NULL
      for (cand in c("n","i","c","n1","au","oc")) if (cand %in% keys) { facet_key <- cand; break }
      group_cols <- setdiff(keys, c(xcol, facet_key))
      if (length(group_cols) == 0L) { DT[, grp := "all"]; color_col <- NULL } else {
        DT[, grp := do.call(paste, c(.SD, list(sep = "|"))), .SDcols = group_cols]
        color_col <- group_cols[1]
      }
      p <- ggplot(DT, aes_string(x = xcol, y = yvar, group = "grp")) +
        geom_hline(yintercept = 0, linetype = 2) +
        geom_line(alpha = 0.9) + geom_point(size = 0.7, alpha = 0.9) +
        labs(x = xcol, y = ylab,
             title = paste(title_pair, "—", label_of(input$sym))) +
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
