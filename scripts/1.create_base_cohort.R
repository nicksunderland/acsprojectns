############################################################################
############################################################################
###                                                                      ###
###                            PROJECT TITLE:                            ###
###            ACS, PCI, AND THE ISCHAEMIA:BLEEDING TRADE-OFF            ###
###                                                                      ###
############################################################################
############################################################################
# comments with bannerCommenter::banner()

##################################################################
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
acs_cohort <- data.frame(
  id = get_hospital_admissions(sus_apc_obj            = db_conn_struct$sus_apc,
                               ccg_provider_code_list = bnssg_ccg_codes$ccg_provider_code,
                               level                  = "episode",
                               codes_list             = acs_codes$icd_codes,
                               search_strategy        = "primary_diagnosis",
                               search_strat_adm_method= "emergency",
                               return_all_codes       = FALSE,
                               datetime_window        = lubridate::interval(acs_date_window_min, acs_date_window_max, tzone = "GMT"),
                               verbose                = FALSE) %>%
    dplyr::distinct(pseudo_nhs_id) %>%
    dplyr::pull(pseudo_nhs_id)
)

# The pseudo NHS IDs as a flat doubles vectors
ids <- acs_cohort %>% purrr::reduce(c) %>% as.double()

##################################################################
##                          Section 3:                          ##
##       Pull data from the data sources for each patient       ##
##################################################################
acs_cohort <- dplyr::left_join(acs_cohort, load_mortality_v2           (db_conn_struct, ids), by = c("id" = "pseudo_nhs_id"))
acs_cohort <- dplyr::left_join(acs_cohort, load_demographics_v2        (db_conn_struct, ids), by = c("id" = "pseudo_nhs_id"))
acs_cohort <- dplyr::left_join(acs_cohort, load_admissions_v2          (db_conn_struct, ids), by = c("id" = "pseudo_nhs_id"))
acs_cohort <- dplyr::left_join(acs_cohort, load_emergency_department_v2(db_conn_struct, ids), by = c("id" = "pseudo_nhs_id"))
acs_cohort <- dplyr::left_join(acs_cohort, load_gp_prescriptions_v2    (db_conn_struct, ids), by = c("id" = "pseudo_nhs_id"))
acs_cohort <- dplyr::left_join(acs_cohort, load_swd_activity_v2        (db_conn_struct, ids), by = c("id" = "pseudo_nhs_id"))
acs_cohort <- dplyr::left_join(acs_cohort, load_swd_attributes_v2      (db_conn_struct, ids), by = c("id" = "pseudo_nhs_id"))
acs_cohort <- dplyr::left_join(acs_cohort, load_swd_measurements_v2    (db_conn_struct, ids), by = c("id" = "pseudo_nhs_id"))

# Disconnect from the databases
sapply(db_conn_struct, function(x) x$finalize())

##----------------------------------------------------------------
##                          Index event                          -
##----------------------------------------------------------------
idx_acs_event = furrr::future_map_dfr(.x = acs_cohort$hospital_admissions,
                                      .f = function(x) acsprojectns::query_icd10_codes(
                                                    x,
                                                    codes             = acs_codes$icd_codes,
                                                    search_strat_diag = "primary",
                                                    search_strat_date = "min",
                                                    time_window       = lubridate::interval(acs_date_window_min, acs_date_window_max, tzone = "GMT"),
                                                    return_vals       = c("code", "diagnosis_datetime", "spell_interval")),
                                      .options  = furrr::furrr_options(packages = "acsprojectns",
                                                                       globals  = c("acs_codes", "acs_date_window_min", "acs_date_window_max")),
                                      .progress = TRUE) %>%
                dplyr::group_by(dplyr::row_number()) %>%
                dplyr::group_nest(.key = "idx_acs_event") %>%
                dplyr::select(.data$idx_acs_event)

acs_cohort <- acs_cohort %>%
  dplyr::bind_cols(idx_acs_event) %>%
  dplyr::relocate(.data$idx_acs_event, .after = .data$id)

##----------------------------------------------------------------
##                    Save the base ACS cohort                   -
##----------------------------------------------------------------
# Save the acs_cohort, as generating takes a while
usethis::use_data(acs_cohort, internal = FALSE, overwrite = TRUE)
