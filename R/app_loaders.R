#' @keywords internal
resolve_project_root <- function(root = NULL) {
  # Priority: explicit arg > option > env var > current getwd()
  cand <- c(root, getOption("fidelio.project", ""), Sys.getenv("FIDELIO_PROJECT", ""), getwd())
  cand <- cand[nzchar(cand)]
  
  for (start in cand) {
    cur <- normalizePath(start, winslash = "/", mustWork = TRUE)
    repeat {
      # Heuristics for project root
      has_cfg  <- file.exists(file.path(cur, "config", "project.yml"))
      has_rproj <- length(Sys.glob(file.path(cur, "*.Rproj"))) > 0
      if (has_cfg || has_rproj) return(cur)
      
      parent <- dirname(cur)
      if (identical(parent, cur)) break
      cur <- parent
    }
  }
  
  stop("Cannot resolve project root. Set options(fidelio.project='...') or FIDELIO_PROJECT.")
}


#' @keywords internal
outputs_dir_from_config <- function(root = NULL, config_path = NULL) {
  cfg_file <- if (!is.null(config_path)) {
    normalizePath(config_path, winslash = "/", mustWork = TRUE)
  } else {
    root <- resolve_project_root(root)
    file.path(root, "config", "project.yml")
  }
  if (!file.exists(cfg_file)) stop("Config file not found: ", cfg_file)
  
  cfg <- load_config(cfg_file)  # your existing reader (returns normalized paths)
  out <- file.path(cfg$paths$outputs, "derived")  # cfg$paths$outputs is already absolute in your setup
  normalizePath(out, winslash = "/", mustWork = TRUE)
}



#' @keywords internal
resolve_outputs_dir <- function(dir = NULL) {
  # Priority: explicit arg > option > env var > default relative folder
  cand <- c(
    dir,
    getOption("fidelio.outputs", ""),
    Sys.getenv("FIDELIO_OUTPUTS", ""),
    file.path(getwd(), "outputs", "derived")
  )
  cand <- normalizePath(cand[nzchar(cand)], winslash = "/", mustWork = FALSE)
  hit  <- cand[file.exists(cand)]
  if (!length(hit)) stop(
    "Cannot find outputs directory. Set options(fidelio.outputs='...') ",
    "or env var FIDELIO_OUTPUTS, or pass 'outputs_dir' to launch_app()."
  )
  hit[[1]]
}

#' @keywords internal
load_manifest <- function(dir = NULL) {
  dir <- resolve_outputs_dir(dir)
  man_r <- file.path(dir, "manifest.rds")
  man_c <- file.path(dir, "manifest.csv")
  if (file.exists(man_r)) return(readRDS(man_r))
  if (file.exists(man_c)) return(data.table::fread(man_c))
  stop("No manifest.rds or manifest.csv found in: ", dir)
}

#' @keywords internal
load_symbol <- function(symbol, dir = NULL,
                        prefer = c("parquet","feather","fst","rds","csv")) {
  dir <- resolve_outputs_dir(dir)
  man <- load_manifest(dir)
  row <- man[symbol == !!symbol][match(prefer, format, nomatch = 0L) > 0][1]
  if (!nrow(row)) stop("Symbol not found in manifest: ", symbol)
  p <- file.path(dir, row$filename %||% row$path)  # support either column name
  if (grepl("\\.parquet$", p)) return(arrow::read_parquet(p))
  if (grepl("\\.feather$", p)) return(arrow::read_feather(p))
  if (grepl("\\.fst$", p))     return(fst::read_fst(p, as.data.table = TRUE))
  if (grepl("\\.rds$", p))     return(readRDS(p))
  if (grepl("\\.csv$", p))     return(data.table::fread(p))
  stop("Unknown format: ", p)
}

#' @keywords internal
load_bundle <- function(name, dir = NULL) {
  dir <- resolve_outputs_dir(dir)
  p <- file.path(dir, paste0("bundle_", name, ".rds"))
  if (file.exists(p)) return(readRDS(p))
  NULL
}
