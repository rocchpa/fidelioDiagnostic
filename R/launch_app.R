# ==============================================================================
# ====                            launch_app.R                              ====
# ==============================================================================

#' Launch one of the packaged Shiny apps
#' @param app "diagnostic" or "results"
#' @param outputs_dir Optional absolute path to outputs/derived.
#' @param project_root Optional project root; if set we read config/project.yml there.
#' @param config_path Optional explicit path to config/project.yml.
#' @export
launch_app <- function(app = c("diagnostic","results"),
                       outputs_dir = NULL,
                       project_root = NULL,
                       config_path  = NULL) {
  
  app <- match.arg(app)
  
  # Locate the app within the installed package
  pkg <- utils::packageName()
  app_dir <- system.file("app", app, package = pkg)
  if (!nzchar(app_dir) || !dir.exists(app_dir)) {
    stop("App '", app, "' not found in this package (", pkg, ").")
  }
  
  # If the user passed project_root, advertise it so resolve_project_root() works
  if (!is.null(project_root) && nzchar(project_root)) {
    options(fidelio.project = normalizePath(project_root, winslash = "/", mustWork = TRUE))
    on.exit(options(fidelio.project = NULL), add = TRUE)
  }
  
  # Resolve outputs/derived (preferred: from config; fallback: option/env)
  if (is.null(outputs_dir)) {
    outputs_dir <- tryCatch(
      outputs_dir_from_config(root = project_root, config_path = config_path),
      error = function(e) {
        # Fallback to option/env; return "" if not set
        getOption("fidelio.outputs", Sys.getenv("FIDELIO_OUTPUTS", ""))
      }
    )
  }
  
  if (!nzchar(outputs_dir) || !dir.exists(outputs_dir)) {
    stop(
      "Could not resolve outputs/derived.\n",
      "Try one of:\n",
      " - launch_app(app, project_root = '...')  # uses config/project.yml\n",
      " - launch_app(app, config_path  = '.../config/project.yml')\n",
      " - launch_app(app, outputs_dir  = '.../outputs/derived')\n",
      " - options(fidelio.project = '...') or options(fidelio.outputs = '...')\n",
      " - set FIDELIO_PROJECT or FIDELIO_OUTPUTS environment variables."
    )
  }
  
  # Set the option the loaders use
  outputs_dir <- normalizePath(outputs_dir, winslash = "/", mustWork = TRUE)
  options(fidelio.outputs = outputs_dir)
  on.exit(options(fidelio.outputs = NULL), add = TRUE)
  
  # Lightweight sanity check so the user gets a helpful nudge
  man_r <- file.path(outputs_dir, "manifest.rds")
  man_c <- file.path(outputs_dir, "manifest.csv")
  any_manifest <- file.exists(man_r) || file.exists(man_c)
  
  # Also check if at least one bundle exists (exact, _latest, or versioned)
  has_any_bundle <- length(list.files(
    outputs_dir,
    pattern = "^bundle_.*\\.(rds)$",
    full.names = FALSE
  )) > 0
  
  if (!any_manifest && !has_any_bundle) {
    warning(
      "No manifest or bundles found in: ", outputs_dir, "\n",
      "Run `run_pipeline()` to generate artifacts before launching the app."
    )
  }
  
  message(
    "[fidelioDiagnostics] Launching '", app, "' app\n",
    "  - package: ", pkg, "\n",
    "  - app dir: ", app_dir, "\n",
    "  - outputs: ", outputs_dir, "\n",
    "  - manifest: ", if (any_manifest) "found" else "missing", "\n"
  )
  
  shiny::runApp(app_dir, display.mode = "normal")
}
