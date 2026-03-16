#!/usr/bin/env Rscript
# Load libraries
library(cancensus)
library(readr)
library(dplyr)

# Define the regions
region_mapping <- list(
  montreal = c("2466", "2465"),
  quebec = c("2423"),
  couronne = c("2458", "2467", "2464", "2473", "2471", "2457", "2460", "2456", "2472", "2459", "2461", "2470", "2474", "2463", "2455", "2468", "2469"),
  regions = c(
    "2481", "2443", "2494", "2437", "2425", "2475", "2449", "2447",
    "2454", "2439", "2446", "2410", "2434", "2462", "2445", "2482",
    "2429", "2493", "2453", "2478", "2436", "2422", "2477", "2499",
    "2452", "2431", "2489", "2486", "2497", "2419", "2426", "2451",
    "2479", "2412", "2476", "2433", "2442", "2491", "2468", "2421",
    "2496", "2488", "2480", "2492", "2450", "2432", "2441", "2418",
    "2430", "2414", "2408", "2438", "2483", "2487", "2413", "2427",
    "2444", "2409", "2417", "2407", "2405", "2403", "2402", "2428",
    "2485", "2448", "2415", "2490", "2484", "2440", "2406", "2416",
    "2435", "2401", "2404", "2495", "2498", "2411", "2420"
  )
)

# Define vectors
vectors_to_fetch <- c(
  "v_CA21_1", # Population
  "v_CA21_9", "v_CA21_10", # Gender
  "v_CA21_14", "v_CA21_32", "v_CA21_50", "v_CA21_71", "v_CA21_89",
  "v_CA21_107", "v_CA21_125", "v_CA21_143", "v_CA21_161", "v_CA21_179",
  "v_CA21_197", "v_CA21_215", "v_CA21_233", "v_CA21_254", "v_CA21_272",
  "v_CA21_290", "v_CA21_308", "v_CA21_329", # Age
  "v_CA21_930", "v_CA21_927" # Language
)

# Initialize an empty dataframe
all_data <- data.frame()

# Loop through each region category
for (region_name in names(region_mapping)) {
  region_codes <- region_mapping[[region_name]]
  
  cat(sprintf("Processing region category: %s\n", region_name))
  
  tryCatch({
    census_data <- get_census(
      dataset = "CA21",
      regions = list(CD = region_codes),
      vectors = vectors_to_fetch,
      level = "CD"
    )
    
    # Add the strate_region column
    census_data$strate_region <- region_name
    
    all_data <- rbind(all_data, census_data)
    
  }, error = function(e) {
    message(paste("Error fetching data for", region_name, ":", e))
  })
}

# Write to CSV
write_csv(all_data, "strate_weights.csv")
