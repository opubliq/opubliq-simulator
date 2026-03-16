#!/usr/bin/env Rscript
# Load libraries
library(cancensus)
library(readr)
library(dplyr)

# Define vectors
pop_vector <- "v_CA21_1"
gender_vectors <- c("v_CA21_9", "v_CA21_10")
age_vectors <- c(
  "v_CA21_14", "v_CA21_32", "v_CA21_50", "v_CA21_71", "v_CA21_89",
  "v_CA21_107", "v_CA21_125", "v_CA21_143", "v_CA21_161", "v_CA21_179",
  "v_CA21_197", "v_CA21_215", "v_CA21_233", "v_CA21_254", "v_CA21_272",
  "v_CA21_290", "v_CA21_308", "v_CA21_329"
)
language_vectors <- c("v_CA21_930", "v_CA21_927")

# Get Quebec regions
regions <- list_census_regions("CA21")
qc_regions <- regions %>%
  filter(level == "CSD", PR_UID == "24")

# Create a directory for temporary files
dir.create("temp_data", showWarnings = FALSE)

# Loop through regions
num_regions <- nrow(qc_regions)
for (i in 1:num_regions) {
  region_code <- qc_regions$region[i]
  cat(sprintf("Processing region %d of %d: %s\n", i, num_regions, region_code))
  
  tryCatch({
    census_data <- get_census(
      dataset = "CA21",
      regions = list(CSD = region_code),
      vectors = c(pop_vector, gender_vectors, age_vectors, language_vectors),
      level = "CSD"
    )
    
    # Save to a temporary file
    write_csv(census_data, file.path("temp_data", paste0("region_", region_code, ".csv")))
    
  }, error = function(e) {
    message(paste("Error fetching data for region", region_code, ":", e))
  })
}

# Combine all temporary files
all_files <- list.files("temp_data", full.names = TRUE)
all_data <- lapply(all_files, read_csv) %>%
  bind_rows()

# Write to final CSV
write_csv(all_data, "strate_weights.csv")

# Clean up temporary files
unlink("temp_data", recursive = TRUE)
