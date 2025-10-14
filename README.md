
<!-- README.md is generated from README.Rmd. Please edit that file -->

# fidelioDiagnostics

`fidelioDiagnostics` is an R package that:

- extracts **FIDELIO** model results from GDX files,
- converts them to wide tables and computes **derived indicators**
  (nation, industry, trade),
- saves outputs per symbol (Parquet/Feather/…) plus a **manifest** and
  optional **bundles**,
- and ships two Shiny apps: an internal **diagnostic** app and a
  shareable **results** app.

------------------------------------------------------------------------

## How to use

Follow these steps to set up and run the `fidelioDiagnostics` package
locally.

### 1) Clone the repository

From your terminal or RStudio:

``` bash
git clone https://github.com/rocchpa/fidelioDiagnostic.git
cd fidelioDiagnostics
```

------------------------------------------------------------------------

### 2) Add the GDX files

Inside the repository folder, create a subfolder under `data-raw/gdx/`
containing your GDX files.  
These files are **not stored in GitHub**, as they are large model
outputs.

Example layout:

    data-raw/
      └── gdx/
           └── eta_cpi_025_beta_infl_1/
                ├── results_all_baseline.gdx
                ├── results_all_ff55.gdx
                └── ...

> The default folder name used in the config is
> `eta_cpi_025_beta_infl_1`,  
> but you can use any name and update it in `config/project.yml`.

------------------------------------------------------------------------

### 3) Open the R project

Open the RStudio project file to set the working environment:

    fidelioDiagnostic.Rproj

------------------------------------------------------------------------

### 4) Initial configuration

Load the package, load the config, and point `gdxrrw` to your GAMS
installation.

``` r
library(fidelioDiagnostics)
library(gdxrrw)

# Load configuration and print paths
cfg <- load_config()

# Link to your GAMS installation (example path; adapt as needed)
igdx("C:/GAMS/51")
```

> Each user must provide their own correct GAMS path in `igdx()`.

------------------------------------------------------------------------

### 5) Create the derived objects

Run the full data processing pipeline to extract, reshape and compute
all datasets used by the apps:

``` r
res <- run_pipeline()
```

This will generate:

- per-symbol files under `outputs/derived/` (format per config),
- a manifest for lazy loading (`outputs/derived/manifest.rds`),
- optional bundles (e.g. `bundle_diagnostic_app.rds`,
  `bundle_results_app.rds`) if configured.

------------------------------------------------------------------------

### 6) Launch the applications

Internal diagnostic app (full dataset):

``` r
launch_app("diagnostic")
```

Results app (light, shareable):

``` r
launch_app("results")
```

Both apps read from the outputs generated in step 5.

------------------------------------------------------------------------

## Configure

Edit `config/project.yml`. Minimal example:

``` yaml
paths:
  gdx_dir: "data-raw/gdx/eta_cpi_025_beta_infl_1"
  outputs: "outputs"

scenarios: ["baseline","ff55"]

groups:
  EU28: ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
         "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU",
         "SVK","SVN","ESP","SWE","GBR"]

save:
  formats: ["parquet","feather"]
  bundles:
    diagnostic_app:
      include: ["GDPr_t","HDY_VAL_t","HDYr_t","I_TOT_PP_t","DS_t","FS_t","GSUR_VAL_t",
                "GINV_VAL_t","TBr_t","TB_GDP_t","HSAVR_t","U_t","KLratio_country_t",
                "ir_t","P_HH_CPI_t","I_PP_t","K_t","L_t","GHG_t","KLratio_t",
                "P_Q_t","P_KL_t","I_PP_SECT6_t","OUT_COMP6_SHARE_REAL_t","BITRADE_REG_t"]
    results_app:
      include: ["GDPr_t","TBr_t","TB_GDP_t","I_PP_SECT6_t","OUT_COMP6_SHARE_REAL_t","BITRADE_REG_t"]
      filters:
        BITRADE_REG_t:
          keep:
            c: ["TOT"]
      csv_combine: true
      csv_basename: "results_bundle"
      csv_shape: "wide"
```

------------------------------------------------------------------------

## Developer quick guide (devtools)

### What each command does

- `devtools::document()`  
  Generate **NAMESPACE** and \*\*man/\*.Rd\*\* from roxygen comments
  (`#' @export`, `@param`, …).

- `devtools::load_all()`  
  Load the package **from source** into the current session (no
  install). Fast feedback loop.

- `devtools::install()`  
  Build & install into your user library (use in fresh R sessions or
  scripts).

- `devtools::check()`  
  Run CRAN-like checks (R CMD check, docs, examples, namespace,
  DESCRIPTION, etc.).

- `devtools::build()`  
  Create a source tarball (`.tar.gz`) you can share/install elsewhere.

- `devtools::test()`  
  Run **testthat** tests under `tests/`.

- `devtools::build_vignettes()` / `devtools::clean_vignettes()`  
  Build/clean vignettes if you ship them.

- `devtools::build_readme()`  
  Knit `README.Rmd` → `README.md`.

> **Tip:** If you want roxygen to manage `NAMESPACE`, delete any
> manually-created `NAMESPACE` and run `devtools::document()`.

### Common recipes

#### 1) Fast inner dev loop

    # edit code in R/*.R
    devtools::load_all()

    # if you changed roxygen tags/exports/docs:
    devtools::document()
    devtools::load_all()

    # try your functions/pipeline/apps (same session)
    fidelioDiagnostics::run_pipeline()
    fidelioDiagnostics::launch_app("diagnostic")  # or "results"

#### 2) Install & use in a fresh session

    devtools::document()
    devtools::install(upgrade = "never", dependencies = FALSE, build_vignettes = FALSE)

    # restart the R session, then:
    .rs.restartR(); library(fidelioDiagnostics)
    fidelioDiagnostics::launch_app("results")

#### 3) Pre-commit / pre-share sanity check

    devtools::document()
    devtools::check()

#### 4) Build a distributable (source tarball)

    devtools::document()
    devtools::check()
    devtools::build()   # creates fidelioDiagnostics_<version>.tar.gz

#### 5) Shiny app dev loop (recommended)

    # keep a console open at the package root
    devtools::document()   # only when roxygen/exports changed
    devtools::load_all()
    fidelioDiagnostics::launch_app("diagnostic")   # or "results"

#### 6) Run the pipeline (reads config/project.yml)

    # no install needed if you only edited YAML
    fidelioDiagnostics::run_pipeline()

**Decision mini-tree**

- Changed **function code only** → `devtools::load_all()`
- Changed **roxygen/exports/docs** → `devtools::document()` then
  `devtools::load_all()`
- Need to **use in another/new session** →
  `devtools::install(upgrade = "never", dependencies = FALSE, build_vignettes = FALSE)`
  then restart + `library(fidelioDiagnostics)`
- About to **share** or something is flaky → `devtools::check()`
- Need an **archive** → `devtools::build()`

## Re-render this README

    devtools::build_readme()

> Commit both `README.Rmd` and the generated `README.md` (plus any
> figures under `man/figures/`).
