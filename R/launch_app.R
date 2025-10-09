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
  app_dir <- system.file("app", app, package = utils::packageName())
  if (!nzchar(app_dir)) stop("App not found in this package: ", app)
  
  # Resolve outputs path (no hard-coding needed)
  if (is.null(outputs_dir)) {
    outputs_dir <- tryCatch(
      outputs_dir_from_config(root = project_root, config_path = config_path),
      error = function(e) {
        # Fallback to option/env var if config discovery fails
        getOption("fidelio.outputs", Sys.getenv("FIDELIO_OUTPUTS", ""))
      }
    )
  }
  if (!nzchar(outputs_dir) || !dir.exists(outputs_dir)) {
    stop(
      "Could not resolve outputs/derived.\n",
      "Try one of:\n",
      " - launch_app(app, project_root = '...')  # use config/project.yml\n",
      " - launch_app(app, config_path  = '.../config/project.yml')\n",
      " - launch_app(app, outputs_dir  = '.../outputs/derived')\n",
      " - options(fidelio.project = '...') or options(fidelio.outputs = '...')\n",
      " - set FIDELIO_PROJECT or FIDELIO_OUTPUTS environment variables."
    )
  }
  
  # Pass to the app via an option the loaders already read
  options(fidelio.outputs = normalizePath(outputs_dir, winslash = "/", mustWork = TRUE))
  on.exit(options(fidelio.outputs = NULL), add = TRUE)
  
  shiny::runApp(app_dir, display.mode = "normal")
}
