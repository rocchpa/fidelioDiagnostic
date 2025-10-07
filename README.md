
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

## Installation

``` r
# install the dev version from GitHub (edit owner/org as needed)
# install.packages("remotes")
remotes::install_github("your-org/fidelioDiagnostics")
```

For local development:

``` r
# from package root
# install.packages("devtools")
devtools::load_all()      # develop interactively
devtools::document()      # update docs/NAMESPACE
devtools::install()       # install locally
```

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
  formats: ["parquet","feather"]   # per-symbol files
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
            c: ["TOT"]             # keep only commodity total
      csv_combine: true
      csv_basename: "results_bundle"
      csv_shape: "wide"
```

------------------------------------------------------------------------

## Quick start

``` r
library(fidelioDiagnostics)

# 1) Load config and print paths
cfg <- load_config()
print_runtime_info(cfg)

# 2) Run the pipeline: extract → wide → aggregates → derived → save
res <- run_pipeline()

# 3) Outputs (by default):
# - outputs/derived/<symbol>.parquet (and/or .feather/.csv/.rds per config)
# - outputs/derived/manifest.rds (index for lazy loading in apps)
# - outputs/derived/bundle_<name>.rds for configured bundles
# - outputs/derived/results_bundle.csv if enabled for the results bundle
```

------------------------------------------------------------------------

## Launch the apps

``` r
# Internal diagnostics (full set)
launch_app("diagnostic")

# Slim results app (subset; uses manifest or bundle_results_app.rds)
launch_app("results")
```

Both apps **lazy-load** tables using `outputs/derived/manifest.rds`. If
present, they can also load targeted `bundle_*.rds`.

------------------------------------------------------------------------

## What’s produced

- **Nation (n,t)**: `GDPr_t`, `HDY_VAL_t`, `HDYr_t`, `I_TOT_PP_t`,
  `DS_t`, `FS_t`, `GSUR_VAL_t`, `GINV_VAL_t`, `TBr_t`, `TB_GDP_t`,
  `HSAVR_t`, `U_t`, `KLratio_country_t`, `ir_t`, `P_HH_CPI_t`.
- **Industry (n,i,t)**: `I_PP_t`, `K_t`, `L_t`, `GHG_t`, `KLratio_t`,
  `P_Q_t`, `P_KL_t`, `I_PP_SECT6_t`, `OUT_COMP6_SHARE_REAL_t`.
- **Bilateral trade (n,n1,c,t)**: `BITRADE_REG_t` (with totals rows
  appended).

Tables are wide by scenario (`baseline`, `ff55`) and include `delta` and
`pct` where relevant.

------------------------------------------------------------------------

## Re-render this README

``` r
devtools::build_readme()
```

> Commit both `README.Rmd` and the generated `README.md` (plus any
> figures under `man/figures/`).
