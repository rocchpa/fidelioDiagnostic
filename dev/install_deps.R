# dev/install_deps.R

core <- c(
  "data.table","yaml","tibble","stringr","glue","fst","arrow"
)

suggested <- c(
  "dplyr","tidyr","openxlsx","readxl","writexl","countrycode",
  "OECD","eurostat","xts","rstudioapi","rprojroot","diffr",
  "DT","ggplot2","shiny","testthat","targets","future"
)

# Choose your GDX backend (uncomment one)
core <- c(core, "gdxtools")
# core <- c(core, "gdxrrw")

need <- setdiff(c(core, suggested), rownames(installed.packages()))
if (length(need)) install.packages(need)

# If you need gdxtools from GitHub:
if (!requireNamespace("gdxtools", quietly = TRUE)) {
  if (!requireNamespace("remotes", quietly = TRUE)) install.packages("remotes")
  remotes::install_github("lolow/gdxtools")
}

message("Dependencies checked.")
