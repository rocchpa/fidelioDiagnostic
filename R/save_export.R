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
  # fallback: derive from gdx_dir basename
  gd <- try(cfg$paths$gdx_dir, silent = TRUE)
  if (!inherits(gd, "try-error") && !is.null(gd) && nzchar(gd)) {
    return(.sanitize_id(basename(gd)))
  }
  "project"
}
.ts_stamp <- function() {
  # yyyy-mm-dd_HH-MM-SS (local time)
  format(Sys.time(), "%Y-%m-%d_%H-%M-%S")
}

#' Save a named list of data.tables according to config
#' @export
save_artifacts <- function(objs, cfg, subdir = NULL) {
  outdir <- cfg$paths$outputs
  if (!is.null(subdir)) outdir <- file.path(outdir, subdir)
  dir.create(outdir, recursive = TRUE, showWarnings = FALSE)
  
  fmts <- cfg$save$formats
  if (is.null(fmts) || !length(fmts)) fmts <- "csv"
  
  for (nm in names(objs)) {
    dt <- objs[[nm]]
    if (is.null(dt) || !nrow(dt)) next
    base <- file.path(outdir, nm)
    
    if ("feather" %in% fmts) {
      arrow::write_feather(dt, paste0(base, ".feather"))
    }
    if ("parquet" %in% fmts) {
      arrow::write_parquet(dt, paste0(base, ".parquet"))
    }
    if ("fst" %in% fmts) {
      fst::write_fst(dt, paste0(base, ".fst"))
    }
    if ("csv" %in% fmts) {
      data.table::fwrite(dt, paste0(base, ".csv"))
    }
    if ("rds" %in% fmts) {
      saveRDS(dt, paste0(base, ".rds"))
    }
  }
  man <- make_manifest(objs, outdir)                 # index for lazy loading in Shiny
  make_bundles(objs, outdir, cfg$save$bundles, cfg)  # selective bundles + combined CSVs
  
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

# --- apply symbol-specific filters (keep/drop lists by column) ----------------
apply_filters <- function(DT, filt) {
  if (is.null(filt) || !length(filt)) return(DT)
  D <- data.table::copy(DT)
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

# --- normalize any table for a combined CSV (pad missing dims; add 'symbol') --
norm_for_csv <- function(DT, symbol, prefer = c("baseline","ff55","delta","pct")) {
  dims_all <- c("n","n1","i","c","au","oc","t")
  D <- data.table::as.data.table(DT)
  for (k in dims_all) if (!k %in% names(D)) D[, (k) := NA]
  meas <- intersect(prefer, names(D))
  D[, symbol := symbol]
  data.table::setcolorder(D, c("symbol", dims_all, meas))
  D[]
}

# --- write selective bundles (and optional combined CSV) ----------------------
make_bundles <- function(objs, outdir, bundles_spec, cfg) {
  if (is.null(bundles_spec) || !length(bundles_spec)) return(invisible(NULL))
  
  pid <- .project_id(cfg)
  ts  <- .ts_stamp()
  
  for (bn in names(bundles_spec)) {
    spec <- bundles_spec[[bn]]
    inc  <- spec$include %||% character(0)
    sel  <- intersect(inc, names(objs))
    if (!length(sel)) next
    
    # apply symbol-specific filters (if any)
    filtered <- lapply(sel, function(nm) {
      DT <- objs[[nm]]
      filt <- spec$filters[[nm]]
      apply_filters(DT, filt)
    })
    names(filtered) <- sel
    
    # --- save RDS bundle with project+timestamp suffix AND a latest copy -------
    versioned <- file.path(outdir, sprintf("bundle_%s_%s_%s.rds", bn, pid, ts))
    latest    <- file.path(outdir, sprintf("bundle_%s_%s_latest.rds", bn, pid))
    
    saveRDS(filtered, versioned)
    file.copy(from = versioned, to = latest, overwrite = TRUE, copy.mode = TRUE)
    
    # optional combined CSV (keep naming consistent with single underscores)
    if (isTRUE(spec$csv_combine)) {
      shape <- tolower(spec$csv_shape %||% "wide")
      pieces <- mapply(norm_for_csv, filtered, names(filtered), SIMPLIFY = FALSE)
      csv_name <- spec$csv_basename %||% bn
      csv_path <- file.path(outdir, sprintf("%s_%s_%s.csv", csv_name, pid, ts))
      data.table::fwrite(data.table::rbindlist(pieces, use.names = TRUE, fill = TRUE), csv_path)
    }
    
  }
  
  invisible(NULL)
}
