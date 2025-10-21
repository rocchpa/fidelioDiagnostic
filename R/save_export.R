# ==============================================================================
# ====                          save_export                             ========
# ==============================================================================

# ---- tiny safe infix ----------------------------------------------------------
`%||%` <- function(x, y) if (is.null(x)) y else x

# ---- internal helpers ---------------------------------------------------------
.sanitize_id <- function(x) {
  x <- gsub("[^A-Za-z0-9._-]+", "-", x)
  x <- gsub("-+", "-", x)
  x <- gsub("(^-|-$)", "", x)
  tolower(x)
}
.project_id <- function(cfg) {
  pid <- try(cfg$project$id, silent = TRUE)
  if (!inherits(pid, "try-error") && !is.null(pid) && nzchar(pid)) {
    return(.sanitize_id(pid))
  }
  gd <- try(cfg$paths$gdx_dir, silent = TRUE)
  if (!inherits(gd, "try-error") && !is.null(gd) && nzchar(gd)) {
    return(.sanitize_id(basename(gd)))
  }
  "project"
}
.ts_stamp <- function() {
  format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
}

# --- apply symbol-specific filters (keep/drop lists by column) -----------------
apply_filters <- function(DT, filt) {
  if (is.null(filt) || !length(filt)) return(DT)
  D <- data.table::as.data.table(DT)
  if (!is.null(filt$keep)) {
    for (nm in names(filt$keep)) if (nm %in% names(D))
      D <- D[get(nm) %chin% filt$keep[[nm]]]
  }
  if (!is.null(filt$drop)) {
    for (nm in names(filt$drop)) if (nm %in% names(D))
      D <- D[!(get(nm) %chin% filt$drop[[nm]])]
  }
  D[]
}

# --- normalize any table for a combined CSV (pad missing dims; add 'symbol') ---
norm_for_csv <- function(DT, symbol, prefer = c("baseline","ff55","delta","pct")) {
  dims_all <- c("n","n1","i","c","au","oc","t")
  D <- data.table::as.data.table(DT)
  for (k in dims_all) if (!k %in% names(D)) D[, (k) := NA]
  meas <- intersect(prefer, names(D))
  D[, symbol := symbol]
  data.table::setcolorder(D, c("symbol", dims_all, meas))
  D[]
}

# --- main entry: save according to config -------------------------------------
# objs: named list of data.tables  (your results_by_symbol)
# cfg : parsed YAML config list
# subdir: optional subdirectory under outputs (rarely needed)
#' @export
# ------------------------------------------------------------------------------
# Save artifacts to outputs/<subdir>.
# - objs: named list of data.frames/data.tables keyed by symbol name
# - subdir: "base" or "derived" (or NULL for outputs root)
# - ts_shared: optional run-level timestamp (so all files share the same stamp)
# - write_list_from: which subdir is allowed to write the consolidated list RDS
#                    (default "derived"); guarded so it happens once per run.
# Requires helpers:
#   - .project_id(cfg)
#   - .ts_stamp()
#   - .pipeline_flag_set(key)   # once-guard for the current run
#   - resolve_outputs_dir(cfg)  # optional; falls back to cfg$paths$outputs
# ------------------------------------------------------------------------------
save_artifacts <- function(objs, cfg,
                           subdir = c("base", "derived"),
                           ts_shared = NULL,
                           write_list_from = "derived") {
  
  stopifnot(is.list(objs))
  
  # --- resolve output root dir safely ---
  out_root <- tryCatch(resolve_outputs_dir(cfg), error = function(e) NULL)
  if (is.null(out_root)) out_root <- cfg$paths$outputs
  if (!dir.exists(out_root)) dir.create(out_root, recursive = TRUE, showWarnings = FALSE)
  
  # normalize subdir
  subdir <- if (is.null(subdir)) NULL else match.arg(subdir)
  outdir <- if (is.null(subdir)) out_root else file.path(out_root, subdir)
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  
  # run-level timestamp (shared) + project id
  pid <- .project_id(cfg)
  ts  <- if (!is.null(ts_shared)) ts_shared else .ts_stamp()
  
  # ----- 0) per-subdir once-guard (prevents double-writes in same run) -----
  # (you can remove this if you want to allow re-save within a run)
  if (!is.null(subdir)) {
    if (.pipeline_flag_set(paste0("save_artifacts:", subdir))) {
      message("[SKIP] save_artifacts(", subdir, "): already saved in this run.")
      return(invisible(outdir))
    }
  }
  
  # ----------------------------------------------------------------------------
  # 1) OPTIONAL: per-symbol files (disabled by default)
  # ----------------------------------------------------------------------------
  per_symbol <- isTRUE(cfg$save$per_symbol)
  fmts <- cfg$save$formats
  wrote_any_per_symbol <- FALSE
  
  if (per_symbol) {
    if (is.null(fmts) || !length(fmts)) fmts <- "csv" # default only when per_symbol=TRUE
    fmts <- tolower(unique(fmts))
    
    for (nm in names(objs)) {
      dt <- objs[[nm]]
      # skip empty or NULL safely
      nrows <- tryCatch(NROW(dt), error = function(e) 0L)
      if (is.null(dt) || nrows == 0L) next
      
      base <- file.path(outdir, nm)
      
      if ("feather" %in% fmts) {
        arrow::write_feather(dt, paste0(base, ".feather"))
        wrote_any_per_symbol <- TRUE
      }
      if ("parquet" %in% fmts) {
        arrow::write_parquet(dt, paste0(base, ".parquet"))
        wrote_any_per_symbol <- TRUE
      }
      if ("fst" %in% fmts) {
        fst::write_fst(dt, paste0(base, ".fst"))
        wrote_any_per_symbol <- TRUE
      }
      if ("csv" %in% fmts) {
        data.table::fwrite(dt, paste0(base, ".csv"))
        wrote_any_per_symbol <- TRUE
      }
      if ("rds" %in% fmts) {
        saveRDS(dt, paste0(base, ".rds"))
        wrote_any_per_symbol <- TRUE
      }
    }
  }
  
  # ----------------------------------------------------------------------------
  # 2) CONSOLIDATED list RDS (one file per run), written only from `write_list_from`
  # ----------------------------------------------------------------------------
  if (identical(subdir, write_list_from) && isTRUE(cfg$save$list_rds$enabled)) {
    if (!.pipeline_flag_set("list-rds")) {
      bundles_dir <- file.path(out_root, "derived")  # keep list RDS under derived/
      dir.create(bundles_dir, recursive = TRUE, showWarnings = FALSE)
      
      base_name <- cfg$save$list_rds$basename %||% "results_by_symbol"
      versioned <- file.path(bundles_dir, sprintf("%s_%s_%s.rds", base_name, pid, ts))
      latest    <- file.path(bundles_dir, sprintf("%s_%s_latest.rds", base_name, pid))
      
      saveRDS(objs, versioned)
      file.copy(from = versioned, to = latest, overwrite = TRUE, copy.mode = TRUE)
      
      message("Wrote list RDS: ", basename(versioned), " and ", basename(latest))
    } else {
      message("[SKIP] list RDS: already written in this run.")
    }
  }
  
  # ----------------------------------------------------------------------------
  # 3) Manifest (only if we actually wrote per-symbol files)
  # ----------------------------------------------------------------------------
  if (isTRUE(wrote_any_per_symbol)) {
    make_manifest(objs, outdir)
  }
  
  # ----------------------------------------------------------------------------
  # 4) Bundles: intentionally NOT here anymore (call once at end of run_pipeline)
  # ----------------------------------------------------------------------------
  # (no-op)
  
  invisible(outdir)
}

# --- build a simple manifest (symbol, format, path, nrows) --------------------
make_manifest <- function(objs, outdir) {
  rows <- lapply(names(objs), function(nm) {
    cand <- c("parquet","feather","fst","rds","csv")
    files <- file.path(outdir, paste0(nm, ".", cand))
    keep  <- file.exists(files)
    if (!any(keep)) return(NULL)
    data.table::data.table(
      symbol = nm,
      format = cand[keep],
      path   = files[keep],
      nrows  = nrow(objs[[nm]])
    )
  })
  man <- data.table::rbindlist(rows, use.names = TRUE, fill = TRUE)
  if (!is.null(man) && nrow(man)) {
    data.table::fwrite(man, file.path(outdir, "manifest.csv"))
    saveRDS(man, file.path(outdir, "manifest.rds"))
  }
  man
}

# --- write selective bundles (and optional combined CSV) ----------------------
make_bundles <- function(objs, out_root, bundles_spec, cfg, ts_shared = NULL) {
  if (is.null(bundles_spec) || !length(bundles_spec)) {
    message("[BUNDLES] No bundles_spec provided; skipping.")
    return(invisible(NULL))
  }
  
  # prevent double writes within one run
  if (.pipeline_flag_set("write-bundles")) {
    message("[SKIP] make_bundles(): already wrote bundles in this run.")
    return(invisible(NULL))
  }
  
  pid <- .project_id(cfg)
  ts  <- if (!is.null(ts_shared)) ts_shared else .ts_stamp()
  
  # bundles in .../outputs/derived
  bundles_dir <- file.path(out_root, "derived")
  dir.create(bundles_dir, recursive = TRUE, showWarnings = FALSE)
  
  for (bn in names(bundles_spec)) {
    spec <- bundles_spec[[bn]]
    
    # Flexible selection:
    # - shorthand: results_app: ["GDPr_t","TBr_t",...]
    # - or named:  results_app: { include: [...], filters: {...}, csv_combine: true }
    # - or with 'select:' instead of 'include:'
    if (is.character(spec)) {
      sel_names <- spec
      spec_filters <- list()
    } else {
      sel_names <- (spec$include %||% spec$select %||% character(0))
      spec_filters <- spec$filters %||% list()
    }
    
    # keep only symbols we actually have
    have <- intersect(unique(sel_names), names(objs))
    if (!length(have)) {
      warning("[BUNDLES] Bundle '", bn, "' has no matching symbols in 'objs'; skipping.")
      next
    }
    
    # apply per-symbol filters if provided
    filtered <- lapply(have, function(nm) apply_filters(objs[[nm]], spec_filters[[nm]]))
    names(filtered) <- have
    
    # bundle files: versioned + latest
    versioned <- file.path(bundles_dir, sprintf("bundle_%s_%s_%s.rds", bn, pid, ts))
    latest    <- file.path(bundles_dir, sprintf("bundle_%s_%s_latest.rds", bn, pid))
    saveRDS(filtered, versioned)
    file.copy(from = versioned, to = latest, overwrite = TRUE, copy.mode = TRUE)
    
    message("[BUNDLES] Wrote ", basename(versioned), " and ", basename(latest))
    
    # optional combined CSV
    if (isTRUE(spec$csv_combine)) {
      shape    <- tolower(spec$csv_shape %||% "wide")  # reserved for future use
      pieces   <- mapply(norm_for_csv, filtered, names(filtered), SIMPLIFY = FALSE)
      csv_name <- spec$csv_basename %||% bn
      csv_path <- file.path(bundles_dir, sprintf("%s_%s_%s.csv", csv_name, pid, ts))
      data.table::fwrite(data.table::rbindlist(pieces, use.names = TRUE, fill = TRUE), csv_path)
      message("[BUNDLES] Wrote ", basename(csv_path))
    }
  }
  
  invisible(NULL)
}
