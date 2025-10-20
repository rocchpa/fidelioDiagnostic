
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

Points 1 to 3 requires the R library `gdxrrw`, available only with GAMS.
Point 4 can be run independently, but it requires the .rds files to feed
the Shiny apps.

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

### 2) Add the GDX files or the RDS files

GDX files are used to extract new results from the Fidelio gdx output.
RDS files are used to run the Shiny apps.

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

If you don’t have GAMS and you want to run directly the Shiny apps,
create a folder under `outputs/derived/` containing your .rds files.

Example layout:

    outputs/
      └── derived/
           ├── bundle_diagnostic_app.rds
           ├── bundle_results_app.rds
           └── ...

------------------------------------------------------------------------

### 3) Open the R project

Open the RStudio project file to set the working environment:

    fidelioDiagnostic.Rproj

------------------------------------------------------------------------

### 4) Initial configuration

Depending on whether you are using the package **for the first time** or
**after installation**, follow one of the two options below.

#### **Option A — First-time setup (new user or fresh clone)**

When you clone the repository for the first time, the package is not yet
installed on your machine.  
Follow these steps **once** to install it and its dependencies.

``` r
# 1. Install devtools (if not already installed)
install.packages("devtools")

# 2. Install all dependencies listed in DESCRIPTION
devtools::install_deps(dependencies = TRUE)

# 3. Generate documentation (only needed if NAMESPACE/man are not included)
devtools::document()

# 4. Install the package locally
devtools::install(upgrade = "never", dependencies = FALSE, build_vignettes = FALSE)

# 5. Load it
library(fidelioDiagnostics)
```

#### **Option B —Subsequent uses (package already installed)**

If the package is already installed, simply load it and, if needed,
point `gdxrrw` to your GAMS installation. Library `gdxrrw` should be
loaded only if you are going to run the full data processing pipeline to
extract the dataset frin te gdx files.

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

Before running either **results extraction** or **results
visualization**, the package must be configured through the file:

config/project.yml

This file tells the package where to find input data, which scenarios to
process, what variables to extract from the model, and how to save the
outputs. You can edit it with any text editor or directly within
RStudio.

------------------------------------------------------------------------

### What you need to configure

**1. Project identification**  
- `project.id` – short, file-system-friendly identifier for the run
(used in filenames).  
Example: project: id: “ff55_3scn”

**2. Paths**  
- `paths.gdx_dir` – path to the folder containing your **FIDELIO GDX**
files (or subfolders).  
- `paths.outputs` – path to the folder where all derived data and
bundles will be written.  
Example: paths: gdx_dir: “data-raw/gdx/eta_cpi_025_beta_infl_1” outputs:
“outputs”

**3. Scenarios**  
- `scenarios` – list of scenario names to be processed (these must match
the scenario identifiers used in your GDX/manifests).  
Example: scenarios: - baseline - ff55 - ff55_2

**4. Regional groups (optional)**  
- `groups` – define sets of countries or regions for aggregation or
plotting.  
Example: groups: EU28:
\[“AUT”,“BEL”,“BGR”,“HRV”,“CYP”,“CZE”,“DNK”,“EST”,“FIN”,“FRA”,“DEU”,“GRC”,“HUN”,“IRL”,“ITA”,“LVA”,“LTU”,“LUX”,“MLT”,“NLD”,“POL”,“PRT”,“ROU”,“SVK”,“SVN”,“ESP”,“SWE”,“GBR”\]

**5. Variables to extract**  
- `extract.include` – list of model **symbols** to read from the GDX
files (e.g., `GDPr_t`, `U_t`, `BITRADE_t`, `GHG_t`). Add or remove items
here to control what gets imported.

**6. Aggregation settings**  
- `aggregations.additive_symbols` – list of variables that can be safely
**summed** (e.g., activity levels like GDP, emissions, trade). Avoid
listing non-additive variables (prices, indices, ratios).

**7. Output formats and bundles**  
- `save.formats` – which output file types to generate (e.g., `parquet`,
`feather`, optionally `csv`, `fst`, `rds`).  
- `save.bundles` – define custom bundles for apps/exports: -
`diagnostic_app.include` – variables for the diagnostics dashboard.  
- `results_app.include` – variables for the results dashboard.  
Optional bundle options:  
- `filters` – selection rules to reduce size (e.g., under
`BITRADE_REG_t`, keep only `c: ["TOT"]`).  
- `csv_combine`, `csv_basename`, `csv_shape` – CSV preferences per
bundle.

**8. Derived indicators**  
- `derive.include_from_base` – base variables to promote to derived if
missing.  
- Optionally set `derive.keys` to control long→wide reshaping per
symbol.

**9. Final CSV export (optional)**  
- `export_csv` – automatically generate a single shareable CSV from a
selected bundle. Key options:  
- `enabled`: whether to export.  
- `bundle`: which bundle to export (e.g., `results_app`).  
- `out_basename`: base filename in `outputs/derived/`.  
- `model_name`: model label for metadata.  
- `pct_as_percent`: express percentage changes as `%`.  
- `include_dim_names`: include dimension names in headers.  
- `unit_overrides`: assign custom units to variables.  
Example: export_csv: enabled: true bundle: “results_app” out_basename:
“results_bundle_template” model_name: “FIDELIO” pct_as_percent: true
include_dim_names: true unit_overrides: GDPr_t: “real (base=2014)”
TB_GDP_t: “ratio” I_PP_SECT6_t: “PP units”

------------------------------------------------------------------------

### Typical workflow

1)  Set the project id (i.e. acronym for current analysis)
2)  Set your GDX folder and scenarios in `paths.gdx_dir` and
    `scenarios`.  
3)  Check that all required variables are listed under
    `extract.include`.  
4)  Adjust bundles under `save.bundles` to match what each app
    expects.  
5)  (Optional) Define `groups` and `aggregations.additive_symbols` if
    you will aggregate.  
6)  (Optional) Enable `export_csv` if you need a ready-to-share CSV.

Once the configuration file is correctly set, you can run the pipeline
or launch the Shiny apps; the package will automatically use this
configuration.

------------------------------------------------------------------------

### Example of `config/project.yml`:

``` yaml
project:
  id: "ff55_3scn"   # short, file-system-friendly
  # If omitted, we'll fall back to the basename of paths$gdx_dir

paths:
  gdx_dir: "data-raw/gdx/eta_cpi_025_beta_infl_1"
  outputs: "outputs"

scenarios:
  - baseline
  - ff55
  - ff55_2
  # add more later, e.g.
  # - cbam
  # - nze

groups:
  EU28: ["AUT","BEL","BGR","HRV","CYP","CZE","DNK","EST","FIN","FRA","DEU","GRC",
         "HUN","IRL","ITA","LVA","LTU","LUX","MLT","NLD","POL","PRT","ROU","SVK",
         "SVN","ESP","SWE","GBR"]

extract:
  include: ["GDPr_t","HSAVR_t","HDY_VAL_t","GSUR_VAL_t","GINV_VAL_t",
            "Q_t","I_PP_t","P_I_t","K_t","L_t","U_t","P_CPI_t","ir_t",
            "P_INPUT_t","P_Q_t","USE_PP_t","M_TOT_t","P_USE_t","P_Mcif_t",
            "BITRADE_t","GHG_t"]

aggregations:
  additive_symbols: ["GDPr_t","HDY_VAL_t","GSUR_VAL_t","GINV_VAL_t",
                     "Q_t","I_PP_t","K_t","L_t","U_t",
                     "USE_PP_t","M_TOT_t","BITRADE_t","GHG_t",
                     "P_USE_t","P_Mcif_t","P_I_t","P_Q_t","P_INPUT_t"]

save:
  formats: ["parquet","feather"]   # add "csv","fst","rds" if you like

  # ---- selective bundles belong UNDER save: ----
  bundles:
    diagnostic_app:
      include: ["GDPr_t","HDY_VAL_t","HDYr_t","I_TOT_PP_t","DS_t","FS_t","GSUR_VAL_t",
                "GINV_VAL_t","TBr_t","TB_GDP_t","HSAVR_t","U_t","KLratio_country_t",
                "ir_t","P_HH_CPI_t","I_PP_t","K_t","L_t","GHG_t","KLratio_t","P_Q_t",
                "P_KL_t","I_PP_SECT6_t","OUT_COMP6_SHARE_REAL_t","BITRADE_REG_t"]

    results_app:
      include: ["GDPr_t","TBr_t","TB_GDP_t","I_PP_SECT6_t","OUT_COMP6_SHARE_REAL_t","BITRADE_REG_t"]
      filters:
        BITRADE_REG_t:
          keep:
            c: ["TOT"]           # keep only commodity total
      csv_combine: true
      csv_basename: "results_bundle"
      csv_shape: "wide"


derive:
  # list base symbols to promote to derived if not present
  include_from_base: ["GDPr_t"]
  # optional: tell the promoter which keys to use when coercing long->wide
  # keys:
  #   GDPr_t: ["n","t"]


export_csv:
  enabled: true
  bundle: "results_app"
  out_basename: "results_bundle_template"  # becomes outputs/derived/<name>.csv
  model_name: "FIDELIO"
  pct_as_percent: true
  include_dim_names: true
  unit_overrides:
    GDPr_t: "real (base=2014)"
    TB_GDP_t: "ratio"
    I_PP_SECT6_t: "PP units"
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
