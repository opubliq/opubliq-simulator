#!/usr/bin/env Rscript
# Génère ingestion/weights/strate_weights_final.csv au format strate_weights Supabase :
#   strate_age_group | strate_langue | strate_region | strate_genre | weight
#
# strate_age_group ∈ {18-34, 35-54, 55+}   (note: 18-34 inclut 20-34 car cancensus
#                                            n'isole pas 18-19 — simplification acceptable)
# strate_langue    ∈ {francophone, anglo_autre}
# strate_region    ∈ {montreal, quebec, couronne, regions}
# strate_genre     ∈ {homme, femme}

library(cancensus)
library(dplyr)
library(readr)

# ---------------------------------------------------------------------------
# 1. Mapping CDs → strate_region
# ---------------------------------------------------------------------------
region_mapping <- list(
  montreal = c("2466","2465"),
  quebec   = c("2423"),
  couronne = c("2458","2467","2464","2473","2471","2457","2460","2456",
               "2472","2459","2461","2470","2474","2463","2455","2468","2469"),
  regions  = c(
    "2481","2443","2494","2437","2425","2475","2449","2447","2454","2439",
    "2446","2410","2434","2462","2445","2482","2429","2493","2453","2478",
    "2436","2422","2477","2499","2452","2431","2489","2486","2497","2419",
    "2426","2451","2479","2412","2476","2433","2442","2491","2421","2496",
    "2488","2480","2492","2450","2432","2441","2418","2430","2414","2408",
    "2438","2483","2487","2413","2427","2444","2409","2417","2407","2405",
    "2403","2402","2428","2485","2448","2415","2490","2484","2440","2406",
    "2416","2435","2401","2404","2495","2498","2411","2420"
  )
)

# ---------------------------------------------------------------------------
# 2. Vecteurs : âge par genre (5 ans) + langue maternelle (total)
# ---------------------------------------------------------------------------
# Triplets (vecteur, strate_genre, strate_age_group_canonique)
age_spec <- rbind(
  # Homme
  data.frame(v="v_CA21_90",  genre="homme", ag="18-34"),
  data.frame(v="v_CA21_108", genre="homme", ag="18-34"),
  data.frame(v="v_CA21_126", genre="homme", ag="18-34"),
  data.frame(v="v_CA21_144", genre="homme", ag="35-54"),
  data.frame(v="v_CA21_162", genre="homme", ag="35-54"),
  data.frame(v="v_CA21_180", genre="homme", ag="35-54"),
  data.frame(v="v_CA21_198", genre="homme", ag="35-54"),
  data.frame(v="v_CA21_216", genre="homme", ag="55+"),
  data.frame(v="v_CA21_234", genre="homme", ag="55+"),
  data.frame(v="v_CA21_255", genre="homme", ag="55+"),
  data.frame(v="v_CA21_273", genre="homme", ag="55+"),
  data.frame(v="v_CA21_291", genre="homme", ag="55+"),
  data.frame(v="v_CA21_309", genre="homme", ag="55+"),
  data.frame(v="v_CA21_327", genre="homme", ag="55+"),
  # Femme
  data.frame(v="v_CA21_91",  genre="femme", ag="18-34"),
  data.frame(v="v_CA21_109", genre="femme", ag="18-34"),
  data.frame(v="v_CA21_127", genre="femme", ag="18-34"),
  data.frame(v="v_CA21_145", genre="femme", ag="35-54"),
  data.frame(v="v_CA21_163", genre="femme", ag="35-54"),
  data.frame(v="v_CA21_181", genre="femme", ag="35-54"),
  data.frame(v="v_CA21_199", genre="femme", ag="35-54"),
  data.frame(v="v_CA21_217", genre="femme", ag="55+"),
  data.frame(v="v_CA21_235", genre="femme", ag="55+"),
  data.frame(v="v_CA21_256", genre="femme", ag="55+"),
  data.frame(v="v_CA21_274", genre="femme", ag="55+"),
  data.frame(v="v_CA21_292", genre="femme", ag="55+"),
  data.frame(v="v_CA21_310", genre="femme", ag="55+"),
  data.frame(v="v_CA21_328", genre="femme", ag="55+"),
  stringsAsFactors = FALSE
)

lang_vec_fr <- "v_CA21_1186"  # Mother tongue: French
lang_vec_en <- "v_CA21_1183"  # Mother tongue: English
all_vectors <- c(age_spec$v, lang_vec_fr, lang_vec_en)

# ---------------------------------------------------------------------------
# 3. Fetch via cancensus
# ---------------------------------------------------------------------------
raw_list <- lapply(names(region_mapping), function(rname) {
  cat(sprintf("Fetching %s...\n", rname))
  d <- get_census("CA21",
                  regions = list(CD = region_mapping[[rname]]),
                  vectors = all_vectors,
                  level   = "CD")
  d$strate_region <- rname
  d
})
raw <- bind_rows(raw_list)

# Trouver le nom exact de chaque colonne vecteur dans le df
find_col <- function(df, vec_id) {
  grep(paste0("^", vec_id, ":"), names(df), value = TRUE)[1]
}

col_fr <- find_col(raw, lang_vec_fr)
col_en <- find_col(raw, lang_vec_en)
age_cols <- sapply(age_spec$v, find_col, df = raw)  # named vector: id → col name

# Verify no NAs
stopifnot(!any(is.na(age_cols)), !is.na(col_fr), !is.na(col_en))

# ---------------------------------------------------------------------------
# 4. Agréger par strate_region (somme de chaque vecteur par région)
# ---------------------------------------------------------------------------
# On fait l'agrégation manuellement pour éviter les problèmes de noms de col
agg_list <- lapply(names(region_mapping), function(rname) {
  sub <- raw %>% filter(strate_region == rname)
  result <- list(strate_region = rname)
  for (cname in c(age_cols, col_fr, col_en)) {
    result[[cname]] <- sum(sub[[cname]], na.rm = TRUE)
  }
  as.data.frame(result, check.names = FALSE)
})
agg <- bind_rows(agg_list)

# ---------------------------------------------------------------------------
# 5. Construire les lignes strate_weights
# ---------------------------------------------------------------------------
rows <- list()

for (rname in names(region_mapping)) {
  r <- agg %>% filter(strate_region == rname)

  n_fr <- as.numeric(r[[col_fr]])
  n_en <- as.numeric(r[[col_en]])
  lang_total <- n_fr + n_en
  frac_fr <- if (lang_total > 0) n_fr / lang_total else 0.80
  frac_en <- 1 - frac_fr

  for (i in seq_len(nrow(age_spec))) {
    col_ag <- age_cols[[i]]
    n_ag   <- as.numeric(r[[col_ag]])
    genre  <- age_spec$genre[i]
    ag     <- age_spec$ag[i]

    rows[[length(rows)+1]] <- data.frame(
      strate_age_group = ag, strate_langue = "francophone",
      strate_region = rname, strate_genre = genre, n = n_ag * frac_fr,
      stringsAsFactors = FALSE)
    rows[[length(rows)+1]] <- data.frame(
      strate_age_group = ag, strate_langue = "anglo_autre",
      strate_region = rname, strate_genre = genre, n = n_ag * frac_en,
      stringsAsFactors = FALSE)
  }
}

long <- bind_rows(rows) %>%
  group_by(strate_age_group, strate_langue, strate_region, strate_genre) %>%
  summarise(n = sum(n, na.rm = TRUE), .groups = "drop")

total_n <- sum(long$n)
long <- long %>%
  mutate(weight = round(n / total_n * 100, 2)) %>%
  select(strate_age_group, strate_langue, strate_region, strate_genre, weight)

cat(sprintf("\nRows: %d  |  Sum weight: %.2f\n", nrow(long), sum(long$weight)))
print(long %>% arrange(strate_region, strate_age_group, strate_genre, strate_langue), n = Inf)

write_csv(long, "ingestion/weights/strate_weights_final.csv")
cat("Saved to ingestion/weights/strate_weights_final.csv\n")
