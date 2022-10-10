##################################################################
##                     Libraries and stuff                      ##
##################################################################
library(usethis)
library(devtools)
library(ggplot2)
rm(list=ls())
load_all()
future::plan(future::multisession, workers=parallel::detectCores()-1)  # you must re-install the whole package after each edit to use future_maps, otherwise it doesn't use the correct functions

##################################################################
##                          Section 1:                          ##
##           Connect to the databases and init globals          ##
##################################################################
source(system.file("scripts/0.init.R", package = "acsprojectns"))


##################################################################
##                          Section 2:                          ##
##                 Define the IDs of the cohort                 ##
##################################################################
acs_codes           <- get_codes(file_path = system.file("icd10_codes/acs_codes.csv",                       package = "acsprojectns"), append_character = "-")
bnssg_ccg_codes     <- get_codes(file_path = system.file("ccg_provider_codes/bnssg_ccg_provider_codes.csv", package = "acsprojectns"))

# Patients who had an ACS event
acs_cohort <- get_hospital_admissions(
                               sus_apc_obj            = db_conn_struct$sus_apc,
                               ccg_provider_code_list = bnssg_ccg_codes$ccg_provider_code,
                               level                  = "spell",
                               codes_list             = acs_codes$icd_codes,
                               search_strategy        = "primary_diagnosis",
                               search_strat_adm_method= "emergency",
                               return_all_codes       = FALSE,
                               datetime_window        = lubridate::interval(acs_date_window_min, acs_date_window_max, tzone = "GMT"),
                               verbose                = FALSE)

# Check that there are no duplicate id:spell combinations
assertthat::are_equal(nrow(acs_cohort), nrow(unique(acs_cohort %>% dplyr::select(.data$pseudo_nhs_id, .data$spell_interval))))

# Extract the month and year into a column
acs_cohort <- acs_cohort %>% dplyr::mutate(date_month = format(lubridate::int_start(.data$spell_interval), "%Y-%m"))

# Extract the most important ICD-10 code if multiple within spell (the ICD-10 text files should be in order of importance)
acs_cohort <- acs_cohort %>% dplyr::mutate(icd10_code = purrr::map_chr(acs_cohort$spell_codes, ~ .x[match(acs_codes$icd_codes, .x)[which.min(is.na(match(acs_codes$icd_codes, .x)))]]))

# Join in the text descriptions of the ACS event
acs_cohort <- acs_cohort %>% dplyr::left_join(acs_codes %>% dplyr::select(.data$icd_codes, .data$description_simple), by=c("icd10_code" = "icd_codes"))
acs_cohort <- acs_cohort %>% dplyr::mutate(description_simple = factor(.data$description_simple, levels = rev(c("STEMI","NSTEMI","Other_MI","Acute_IHD","Unstable_angina"))),
                                           description_v_simp = dplyr::case_when(.data$description_simple == "STEMI" ~ "STEMI",
                                                                                 TRUE ~ "NSTEMI"))

# Plot the ACS counts per month
ggplot(acs_cohort, aes(x=lubridate::ym(date_month), fill=description_simple)) +
  geom_bar(stat="count", colour="black") +
  theme_minimal() +
  scale_fill_viridis_d(begin=0.2, end=0.8) +
  labs(title = "Acute Coronary Syndrome Admissions", subtitle = "July 2018 - December 2021", fill = "ACS type" ) +
  ylab("Count") +
  xlab("Date") +
  scale_x_date(date_labels="%b %Y")

# Plot the ACS counts per month
ggplot(acs_cohort, aes(x=lubridate::ym(date_month), fill=description_v_simp)) +
  geom_bar(stat="count", colour="black") +
  theme_minimal() +
  scale_fill_viridis_d(begin=0.2, end=0.8) +
  labs(title = "Acute Coronary Syndrome Admissions", subtitle = "July 2018 - December 2021", fill = "ACS type" ) +
  ylab("Count") +
  xlab("Date") +
  scale_x_date(date_labels="%b %Y")
