##################################################################
##                     Libraries and stuff                      ##
##################################################################
library(usethis)
library(devtools)
library(lubridate)
rm(list=ls())
load_all()
future::plan(future::multisession, workers=parallel::detectCores()-1)  # you must re-install the whole package after each edit to use future_maps, otherwise it doesn't use the correct functions


##----------------------------------------------------------------
##                    Load the base ACS cohort                   -
##----------------------------------------------------------------
load("data/acs_cohort.rda")
#load("data/feature_matrix.rda")

##----------------------------------------------------------------
##                        Init globals                           -
##----------------------------------------------------------------
source(system.file("scripts/0.init.R", package = "acsprojectns"))

##################################################################
##                  Create the feature matrix                   ##
##     Simplify the data into variables to use in the model     ##
##################################################################
feature_matrix <- data.frame("id" = acs_cohort$id)

##----------------------------------------------------------------
##                      The index ACS event                      -
##----------------------------------------------------------------
feature_matrix["idx_acs_datetime"] = furrr::future_map_dfr(acs_cohort$idx_acs_event, ~ setNames(.x$diagnosis_datetime, "idx_acs_datetime"))
feature_matrix["idx_acs_spell_interval"] = furrr::future_map_dfr(acs_cohort$idx_acs_event, ~ setNames(.x$spell_interval, "idx_acs_spell_interval"))
feature_matrix["idx_acs_code"] = furrr::future_map_dfr(acs_cohort$idx_acs_event, ~ setNames(.x$code, "idx_acs_code"))

##----------------------------------------------------------------
##                          Demographics                         -
##----------------------------------------------------------------
{
feature_matrix["sex"]        = furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$sex, "sex"))
feature_matrix["dob_est"]    = furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$dob_estimate, "dob_est"))
feature_matrix[c("ethnicity", "ethnicity_group", "imd_decile", "income_decile", "employment_decile",
                 "gp_practice_code", "area_ward_name",
                 "lower_layer_super_output_area_code", "lower_layer_super_output_area_name", "lower_layer_super_output_area_16plus_population", "lower_layer_super_output_area_pct_ethnic_minority",
                 "middle_layer_super_output_area_name",
                 "air_quality_indicator", "urban_rural_classification")]  = cbind(
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$ethnicity,                            "ethnicity")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$ethnicity_group,                      "ethnicity_group")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$index_of_multiple_deprivation_decile, "index_of_multiple_deprivation_decile")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$income_decile,                        "income_decile")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$employment_decile,                    "employment_decile")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$gp_practice_code,                     "gp_practice_code")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$area_ward_name,                       "area_ward_name")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$lower_layer_super_output_area_code,   "lower_layer_super_output_area_code")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$lower_layer_super_output_area_name,   "lower_layer_super_output_area_name")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$lower_layer_super_output_area_16plus_population,  "lower_layer_super_output_area_16plus_population")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$lower_layer_super_output_area_pct_ethnic_minority,"lower_layer_super_output_area_pct_ethnic_minority")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$middle_layer_super_output_area_name,  "middle_layer_super_output_area_name")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$air_quality_indicator,                "air_quality_indicator")),
  furrr::future_map_dfr(acs_cohort$demographics, ~ setNames(.x$urban_rural_classification,           "urban_rural_classification")))
feature_matrix[c("imd_quartile")] = dplyr::ntile(feature_matrix$imd_decile, 4)
feature_matrix["age"] =
  furrr::future_map2_int(.x = feature_matrix$dob_est,
                         .y = feature_matrix$idx_acs_datetime,
                         .f = function(x, y) as.integer(lubridate::as.period(lubridate::interval(x, y))$year))
feature_matrix["age_category"] = dplyr::case_when(feature_matrix$age < 40 ~ "<40",
                                                                feature_matrix$age >= 40 & feature_matrix$age <= 49 ~ "40-49",
                                                                feature_matrix$age >= 50 & feature_matrix$age <= 59 ~ "50-59",
                                                                feature_matrix$age >= 60 & feature_matrix$age <= 69 ~ "60-69",
                                                                feature_matrix$age >= 70 & feature_matrix$age <= 79 ~ "70-79",
                                                                feature_matrix$age >= 80 & feature_matrix$age <= 89 ~ "80-89",
                                                                feature_matrix$age >= 90 ~ "90+",
                                                                TRUE ~ "error") %>% as.factor()
feature_matrix <- feature_matrix %>% dplyr::relocate(c(.data$age, .data$age_category), .after = .data$dob_est)
}

##----------------------------------------------------------------
##                        SWD data existence                     -
##----------------------------------------------------------------
{
feature_matrix["swd_date_range"] = lubridate::interval(
  furrr::future_map_dfr(.x = acs_cohort$swd_attributes,
                        .f = function(x) acsprojectns::query_swd_attributes(
                                      x,
                                      time_window       = lubridate::interval(as.POSIXct("1900-01-01"), as.POSIXct("2050-01-01")),
                                      search_strat_date = "min",
                                      return_vals       = c("attribute_period_date")),
                        .options = furrr::furrr_options(packages = "acsprojectns",
                                                        globals  = c()),
                        .progress = TRUE) %>% dplyr::pull(),
  furrr::future_map_dfr(.x = acs_cohort$swd_attributes,
                        .f = function(x) acsprojectns::query_swd_attributes(
                                      x,
                                      time_window       = lubridate::interval(as.POSIXct("1900-01-01"), as.POSIXct("2050-01-01")),
                                      search_strat_date = "max",
                                      return_vals       = c("attribute_period_date")),
                        .options = furrr::furrr_options(packages = "acsprojectns",
                                                        globals  = c()),
                        .progress = TRUE) %>% dplyr::pull(),
  tz = "GMT")
feature_matrix["swd_att_data_exists"] = lubridate::int_overlaps(feature_matrix$swd_date_range,
                                                            lubridate::interval(acs_date_window_min, acs_date_window_max)) %>%
                                        replace(is.na(.), FALSE)
feature_matrix["swd_pri_act_data_exists"] = furrr::future_map2_dfr(.x = acs_cohort$swd_activity,
                         .y = feature_matrix$idx_acs_datetime,
                         .f = function(x, y) acsprojectns::query_swd_activity(
                           x,
                           provider    = c("primary_care_contact"),
                           setting     = NA,
                           urgency     = NA,
                           speciality  = NA,
                           time_window = lubridate::interval(y, acs_date_window_max),
                           return_vals = c("count")),
                         .options = furrr::furrr_options(packages = "acsprojectns",
                                                         globals  = c("acs_date_window_max")),
                         .progress = TRUE) %>% dplyr::pull() > 0
feature_matrix = feature_matrix %>%
  dplyr::mutate(swd_data_cat = dplyr::case_when( .data$swd_att_data_exists &  .data$swd_pri_act_data_exists ~ "swd_att_act",
                                                 .data$swd_att_data_exists & !.data$swd_pri_act_data_exists ~ "swd_att",
                                                !.data$swd_att_data_exists &  .data$swd_pri_act_data_exists ~ "swd_act",
                                                !.data$swd_att_data_exists & !.data$swd_pri_act_data_exists ~ "swd_none",
                                                TRUE ~ "error"))

# Plot the ACS cohort breakdown
{
# # Library
# library(plotly)
#
# # Change these to create different Sankey plotd
# category   = "ethnicity_group"
# plot_title = "Ethnicity group split of the cohort"
#
# links = feature_matrix %>%
#         dplyr::group_by(target  = as.character(!!sym(category))) %>%
#         dplyr::summarise(source = "acs_cohort",
#                          value  = dplyr::n())
# nodes = data.frame(name = c(links$target, links$source) %>% unique()) %>%
#         dplyr::left_join(links, by = c("name" = "target"))
# nodes[which(nodes$name=="acs_cohort"), 3] = sum(nodes$value, na.rm=T)
# labels = nodes %>% dplyr::select(-.data$source) %>% tidyr::unite("label", name:value, sep=" n=", remove = FALSE) %>% dplyr::pull(.data$label)
# nodes  = nodes$name
# links$IDsource <- match(links$source, nodes)-1
# links$IDtarget <- match(links$target, nodes)-1
#
# fig <- plot_ly(
#   type        = "sankey",
#   domain      = list(x = c(0,1), y = c(0,1)),
#   orientation = "h",
#   valueformat = ".0f",
#   valuesuffix = "TWh",
#   node        = list(label     = labels,
#                      #color    = json_data$data[[1]]$node$color,
#                      pad       = 15,
#                      thickness = 15,
#                      line      = list(color = "black", width = 0.5)),
#   link        = list(source    = links$IDsource,
#                      target    = links$IDtarget,
#                      value     = links$value,
#                      label     = rep("foo", length(links$value)))
# )
#
# fig <- fig %>% layout(
#   title = plot_title,
#   font  = list(size = 10),
#   xaxis = list(showgrid = F, zeroline = F),
#   yaxis = list(showgrid = F, zeroline = F)
# )
# fig
}


# Filter the cohort for only those with SWD data
message("Cohort breakdown by whether or not SWD data is available")
data.frame("Group" = c("Total", "SWD attribute data", "SWD attribute & activity data", "Only SWD attribute data", "Only SWD activity data", "No SWD data"),
           "Count" = c(nrow(feature_matrix),
                       sum(feature_matrix$swd_att_data_exists),
                       sum(feature_matrix$swd_att_data_exists | feature_matrix$swd_pri_act_data_exists),
                       sum(feature_matrix$swd_att_data_exists & !feature_matrix$swd_pri_act_data_exists),
                       sum(!feature_matrix$swd_att_data_exists & feature_matrix$swd_pri_act_data_exists),
                       sum(!feature_matrix$swd_att_data_exists & !feature_matrix$swd_pri_act_data_exists)))
acs_cohort     <- acs_cohort     %>% dplyr::filter(feature_matrix$swd_att_data_exists)
feature_matrix <- feature_matrix %>% dplyr::filter(.data$swd_att_data_exists)
}

##----------------------------------------------------------------
##                    Comorbidities & history                    -
##----------------------------------------------------------------
# -- Hypertension
{
hypertension_codes <- get_codes(file_path = system.file("icd10_codes/hypertension_codes.csv", package = "acsprojectns"), append_character = "-")
feature_matrix[c("hypertension")] =
  cbind( # bind together columns of co-morbidity checks
    furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                           .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                           .f = function(x, y) acsprojectns::query_swd_attributes(
                                         x,
                                         time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                         search_strat_date = "max",
                                         return_vals       = c("hypertension", "hypertension_qof")),
                           .options = furrr::furrr_options(packages = "acsprojectns"),
                           .progress = TRUE),
    furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                           .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                           .f = function(x, y) acsprojectns::query_icd10_codes(
                                         x,
                                         codes             = hypertension_codes$icd_codes,
                                         search_strat_diag = "all",
                                         search_strat_date = "min",
                                         time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                         return_vals       = c("any_exist")),
                           .options  = furrr::furrr_options(packages = "acsprojectns"),
                           .progress = TRUE)
  ) %>% # pipe the df into pmap
  purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Any diabetes
{
t1dm_codes     <- get_codes(file_path = system.file("icd10_codes/diabetes_type1_codes.csv", package = "acsprojectns"), append_character = "-")
t2dm_codes     <- get_codes(file_path = system.file("icd10_codes/diabetes_type2_codes.csv", package = "acsprojectns"), append_character = "-")
otherdm_code   <- get_codes(file_path = system.file("icd10_codes/diabetes_other_codes.csv", package = "acsprojectns"), append_character = "-")
diabetes_codes <- rbind(t1dm_codes, t2dm_codes, otherdm_code)
feature_matrix[c("diabetes")] =
  cbind( # bind together columns of co-morbidity checks
    furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                           .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                           .f = function(x, y) acsprojectns::query_swd_attributes(
                                         x,
                                         time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                         search_strat_date = "max",
                                         return_vals       = c("type_1_diabetes", "type_2_diabetes")),
                           .options = furrr::furrr_options(packages = "acsprojectns"),
                           .progress = TRUE),
    furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                           .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                           .f = function(x, y) acsprojectns::query_icd10_codes(
                                         x,
                                         codes             = diabetes_codes$icd_codes,
                                         search_strat_diag = "all",
                                         search_strat_date = "min",
                                         time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                         return_vals       = c("any_exist")),
                           .options  = furrr::furrr_options(packages = "acsprojectns"),
                           .progress = TRUE)
  ) %>% # pipe the df into pmap
  purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Chronic kidney disease
{
  ckd_codes <- get_codes(file_path = system.file("icd10_codes/ckd_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("ckd")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_swd_attributes(
                                           x,
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_date = "max",
                                           return_vals       = c("chronic_kidney_disease", "chronic_kidney_disease_qof")),
                             .options = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c()),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = ckd_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("ckd_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Hypercholesterolaemia
{
  hypercholesterolaemia_codes <- get_codes(file_path = system.file("icd10_codes/hypercholesterolaemia_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("hypercholesterolaemia")] =
    furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                           .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                           .f = function(x, y) acsprojectns::query_icd10_codes(
                                         x,
                                         codes             = hypercholesterolaemia_codes$icd_codes,
                                         search_strat_diag = "all",
                                         search_strat_date = "min",
                                         time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                         return_vals       = c("any_exist")),
                            .options  = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c("hypercholesterolaemia_codes")),
                            .progress = TRUE)
}

# -- Smoking
{
  smoking_codes <- get_codes(file_path = system.file("icd10_codes/smoking_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("smoking")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_swd_attributes(
                                           x,
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_date = "max",
                                           return_vals       = c("smoking_status")),
                             .options = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c()),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = smoking_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("smoking_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Obesity
{
  obesity_codes <- get_codes(file_path = system.file("icd10_codes/obesity_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("obesity")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_swd_attributes(
                                           x,
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_date = "max",
                                           return_vals       = c("obesity", "obesity_qof", "bmi")),
                             .options = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c()),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = obesity_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("obesity_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    dplyr::mutate(bmi = dplyr::if_else(.data$bmi >= 30, TRUE, FALSE, missing = NA)) %>%
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Heart failure
{
  heart_failure_codes <- get_codes(file_path = system.file("icd10_codes/heart_failure_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("heart_failure")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_swd_attributes(
                                           x,
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_date = "max",
                                           return_vals       = c("heart_failure", "heart_failure_qof")),
                             .options = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c()),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = heart_failure_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("heart_failure_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Ischaemic cardiomyopathy
{
  isch_cardiomyopathy_codes <- get_codes(file_path = system.file("icd10_codes/ischaemic_cardiomyopathy_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("isch_cardiomyopathy")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_swd_attributes(
                                           x,
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_date = "max",
                                           return_vals       = c("ischaemic_heart_disease_mi")),
                             .options = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c()),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = isch_cardiomyopathy_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("isch_cardiomyopathy_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Non-ischaemic cardiomyopathy
{
  nonisch_cardiomyopathy_codes <- get_codes(file_path = system.file("icd10_codes/nonischaemic_cardiomyopathy_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("nonisch_cardiomyopathy")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_swd_attributes(
                                           x,
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_date = "max",
                                           return_vals       = c("ischaemic_heart_disease_non_mi")),
                             .options = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c()),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = nonisch_cardiomyopathy_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("nonisch_cardiomyopathy_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Atrial fibrillation
{
  af_codes <- get_codes(file_path = system.file("icd10_codes/atrial_fibrillation_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("atrial_fibrillation")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_swd_attributes(
                                           x,
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_date = "max",
                                           return_vals       = c("atrial_fibrillation", "atrial_fibrillation_qof")),
                             .options = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c()),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = af_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("af_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Peripheral vascular disease
{
  vasc_disease_codes <- get_codes(file_path = system.file("icd10_codes/vascular_disease_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("vasc_disease")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_swd_attributes(
                                           x,
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_date = "max",
                                           return_vals       = c("vascular_disease", "vascular_disease_qof")),
                             .options = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c()),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = vasc_disease_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("vasc_disease_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Anaemia
{
  anaemia_codes <- get_codes(file_path = system.file("icd10_codes/anaemia_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("anaemia")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_swd_attributes(
                                           x,
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_date = "max",
                                           return_vals       = c("anaemia_other", "anaemia_iron_deficiency")),
                             .options = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c()),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = anaemia_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("anaemia_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Bleeding history
{
  bleeding_codes <- get_codes(file_path = system.file("icd10_codes/bleeding_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("bleeding_previous_any")] =
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = bleeding_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("bleeding_codes")),
                             .progress = TRUE)

  feature_matrix[c("bleeding_previous_gi")] =
    furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                           .y = feature_matrix$idx_acs_datetime,
                           .f = function(x, y) acsprojectns::query_icd10_codes(
                                         x,
                                         codes             = bleeding_codes$icd_codes[bleeding_codes$group == "Gastrointestinal bleeding"], #Gastrointestinal bleeding   Intracranial bleeding   Other bleeding
                                         search_strat_diag = "all",
                                         search_strat_date = "min",
                                         time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                         return_vals       = c("any_exist")),
                           .options  = furrr::furrr_options(packages = "acsprojectns",
                                                            globals  = c("bleeding_codes")),
                           .progress = TRUE)

  feature_matrix[c("bleeding_previous_ich")] =
    furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                           .y = feature_matrix$idx_acs_datetime,
                           .f = function(x, y) acsprojectns::query_icd10_codes(
                                         x,
                                         codes             = bleeding_codes$icd_codes[bleeding_codes$group == "Intracranial bleeding"], #Gastrointestinal bleeding   Intracranial bleeding   Other bleeding
                                         search_strat_diag = "all",
                                         search_strat_date = "min",
                                         time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                         return_vals       = c("any_exist")),
                           .options  = furrr::furrr_options(packages = "acsprojectns",
                                                            globals  = c("bleeding_codes")),
                           .progress = TRUE)

  feature_matrix[c("bleeding_previous_other")] =
    furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                           .y = feature_matrix$idx_acs_datetime,
                           .f = function(x, y) acsprojectns::query_icd10_codes(
                                         x,
                                         codes             = bleeding_codes$icd_codes[bleeding_codes$group == "Other bleeding"], #Gastrointestinal bleeding   Intracranial bleeding   Other bleeding
                                         search_strat_diag = "all",
                                         search_strat_date = "min",
                                         time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                         return_vals       = c("any_exist")),
                           .options  = furrr::furrr_options(packages = "acsprojectns",
                                                            globals  = c("bleeding_codes")),
                           .progress = TRUE)
}

# -- Bleeding diathesis
{
  bleed_diathesis_codes <- get_codes(file_path = system.file("icd10_codes/bleeding_diathesis_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("bleed_diathesis")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_swd_attributes(
                                           x,
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_date = "max",
                                           return_vals       = c("coagulopathy")),
                             .options = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c()),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = bleed_diathesis_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("bleed_diathesis_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Blood transfusion
{
  transfusion_diag_codes <- get_codes(file_path = system.file("icd10_codes/blood_transfusion_codes.csv", package = "acsprojectns"), append_character = "-")
  transfusion_opcs_codes <- get_codes(file_path = system.file("opcs_codes/blood_transfusion_procedure_codes.csv", package = "acsprojectns"))
  feature_matrix[c("blood_transfusion")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = feature_matrix$idx_acs_datetime - lubridate::duration(1, units = "seconds"),
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = transfusion_diag_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("transfusion_diag_codes")),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = feature_matrix$idx_acs_datetime - lubridate::duration(1, units = "seconds"),
                             .f = function(x, y) acsprojectns::query_opcs_codes(
                                           x,
                                           codes             = transfusion_opcs_codes$opcs_codes,
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_adm_method = "emergency",
                                           return_vals       = c("code", "procedure_datetime")),
                             .options = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c("transfusion_opcs_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Brain AVM
{
  brain_avm_codes <- get_codes(file_path = system.file("icd10_codes/brain_avm_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("brain_avm")] =
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = brain_avm_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("brain_avm_codes")),
                             .progress = TRUE)
}

# -- Cancer
{
  cancer_codes <- get_codes(file_path = system.file("icd10_codes/cancer_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("cancer_prior_5y")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_swd_attributes(
                                           x,
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_date = "max",
                                           return_vals       = c("lung_cancer_within_5_years_flag", "breast_cancer_within_5_years_flag", "bowel_cancer_within_5_years_flag", "prostate_cancer_within_5_years_flag", "leukaemia_lymphoma_cancer_within_5_years_flag", "cervical_cancer_within_5_years_flag",  "ovarian_cancer_within_5_years_flag", "melanoma_cancer_within_5_years_flag", "headneck_cancer_within_5_years_flag", "giliver_cancer_within_5_years_flag", "other_cancer_within_5_years_flag", "metastatic_cancer_within_5_years_flag", "bladder_cancer_within_5_years_flag", "kidney_cancer_within_5_years_flag")),
                             .options = furrr::furrr_options(packages = "acsprojectns",
                                                             globals  = c()),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = cancer_codes$icd_codes,
                                           search_strat_diag = "primary",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(y - lubridate::duration(5, units = "years"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("cancer_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return

# -- Cancer, definitely 12 months
  feature_matrix[c("cancer_prior_1y")] =
    furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                           .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                           .f = function(x, y) acsprojectns::query_icd10_codes(
                             x,
                             codes             = cancer_codes$icd_codes,
                             search_strat_diag = "primary",
                             search_strat_date = "min",
                             time_window       = lubridate::interval(y - lubridate::duration(1, units = "years"), y, tzone = "GMT"),
                             return_vals       = c("any_exist")),
                           .options  = furrr::furrr_options(packages = "acsprojectns",
                                                            globals  = c("cancer_codes")),
                           .progress = TRUE)
}

# -- Portal hypertension and cirrhosis
{
  liver_codes <- get_codes(file_path = system.file("icd10_codes/portal_htn_cirrhosis_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("liver_disease")] =
    cbind( # bind together columns of co-morbidity checks
      furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_swd_attributes(
                                           x,
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           search_strat_date = "max",
                                           return_vals       = c("alcoholic_liver_disease", "non_alcoholic_fatty_liver_disease", "other_liver_disease")),
                             .options = furrr::furrr_options(packages = "acsprojectns"),
                             .progress = TRUE),
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval),
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = liver_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("liver_codes")),
                             .progress = TRUE)
    ) %>% # pipe the df into pmap
    purrr::pmap_lgl(., ~ any(.x %in% c(TRUE, 1))) # test if they are TRUE or flag 1 (true), condense to one column and return
}

# -- Thrombocytopenia
{
  thrombocytopenia_codes <- get_codes(file_path = system.file("icd10_codes/thrombocytopenia_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("thrombocytopenia")] =
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = thrombocytopenia_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(as.POSIXct("1900-01-01"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("thrombocytopenia_codes")),
                             .progress = TRUE)
}

# -- Trauma and injuries
{
  trauma_codes <- get_codes(file_path = system.file("icd10_codes/trauma_injury_codes.csv", package = "acsprojectns"), append_character = "-")
  feature_matrix[c("trauma_30d")] =
      furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                             .y = feature_matrix$idx_acs_datetime,
                             .f = function(x, y) acsprojectns::query_icd10_codes(
                                           x,
                                           codes             = trauma_codes$icd_codes,
                                           search_strat_diag = "all",
                                           search_strat_date = "min",
                                           time_window       = lubridate::interval(y - lubridate::duration(30, units = "days"), y, tzone = "GMT"),
                                           return_vals       = c("any_exist")),
                             .options  = furrr::furrr_options(packages = "acsprojectns",
                                                              globals  = c("trauma_codes")),
                             .progress = TRUE)
}

##----------------------------------------------------------------
##                Index ACS admission procedures                 -
##----------------------------------------------------------------
# -- PCI
{
pci_codes <- get_codes(file_path = system.file("opcs_codes/pci_procedure_codes.csv", package = "acsprojectns"))
feature_matrix[c("pci_code", "pci_datetime")] =
  furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                         .y = feature_matrix$idx_acs_spell_interval,
                         .f = function(x, y) acsprojectns::query_opcs_codes(
                                        x,
                                        codes             = pci_codes$opcs_codes,
                                        search_strat_date = "min",
                                        time_window       = y,
                                        return_vals       = c("code", "procedure_datetime")),
                        .options = furrr::furrr_options(packages = "acsprojectns",
                                                        globals  = c("pci_codes")),
                        .progress = TRUE)

# Needs to be lubridate::int_start(feature_matrix$idx_acs_spell_interval) as some diagnoses are made after the start but the PCI occurred prior
feature_matrix[c("pci_during_admission")] = !is.na(feature_matrix$pci_code)
feature_matrix[c("time_to_pci_hrs")] = lubridate::interval(lubridate::int_start(feature_matrix$idx_acs_spell_interval), feature_matrix$pci_datetime) %/% lubridate::duration(1, "hours")



}

# -- CABG
{
cabg_codes <- get_codes(file_path = system.file("opcs_codes/cabg_procedure_codes.csv", package = "acsprojectns"))
feature_matrix[c("cabg_code", "cabg_datetime")] =
  furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                         .y = feature_matrix$idx_acs_spell_interval,
                         .f = function(x, y) acsprojectns::query_opcs_codes(
                                       x,
                                       codes             = cabg_codes$opcs_codes,
                                       search_strat_date = "min",
                                       time_window       = y,
                                       return_vals       = c("code", "procedure_datetime")),
                         .options = furrr::furrr_options(packages = "acsprojectns",
                                                         globals  = c("cabg_codes")),
                         .progress = TRUE)
# Needs to be lubridate::int_start(feature_matrix$idx_acs_spell_interval) as some diagnoses are made after the start but the PCI occurred prior
feature_matrix[c("cabg_during_admission")] = !is.na(feature_matrix$cabg_code)
feature_matrix[c("time_to_cabg_hrs")] = lubridate::interval(lubridate::int_start(feature_matrix$idx_acs_spell_interval), feature_matrix$cabg_datetime) %/% lubridate::duration(1, "hours")
}

##----------------------------------------------------------------
##                      Post-event treatments                    -
##----------------------------------------------------------------

# Measurements SWD dataset info:
# first cholesterol in SWD appears on 01-03-22
# first bp reading  in SWD appears on 01-10-19
# first hba1c       in SWD appears on 01-10-19

# -- Blood pressures, HbA1c, and Cholesterol levels
{
  feature_matrix[c("bp_readings_in_4m", "cholesterol_tests_in_4m", "hba1c_tests_in_4m")] =
    furrr::future_map2_dfr(.x = acs_cohort$swd_measurements,
                           .y = feature_matrix$idx_acs_datetime,
                           .f = function(x, y) acsprojectns::query_swd_measurements(
                                         x,
                                         measurements = c("systolic_bp", "cholesterol", "hba1c"),
                                         time_window  = lubridate::interval(y, y + lubridate::duration(4, units = "months"), tzone = "GMT"),
                                         return_vals  = c("count")),
                           .options  = furrr::furrr_options(packages = "acsprojectns",
                                                            globals  = c()),
                           .progress = TRUE)

  feature_matrix[c("sbp_mean_4m_post_acs", "dbp_mean_4m_post_acs", "hba1c_mean_4m_post_acs")] =
    furrr::future_map2_dfr(.x = acs_cohort$swd_measurements,
                           .y = feature_matrix$idx_acs_datetime,
                           .f = function(x, y) acsprojectns::query_swd_measurements(
                                         x,
                                         measurements = c("systolic_bp", "diastolic_bp", "hba1c"),
                                         time_window  = lubridate::interval(y, y + lubridate::duration(4, units = "months"), tzone = "GMT"),
                                         return_vals  = c("mean")),
                           .options  = furrr::furrr_options(packages = "acsprojectns",
                                                            globals  = c()),
                           .progress = TRUE)

  feature_matrix[c("bp_group")] =
    feature_matrix %>%
    dplyr::mutate(bp_control_group = dplyr::case_when( ((.data$sbp_mean_4m_post_acs >= 140 | .data$dbp_mean_4m_post_acs >= 90) & .data$age  <80)   ~ "High BP",
                                                       ((.data$sbp_mean_4m_post_acs >= 150 | .data$dbp_mean_4m_post_acs >= 90) & .data$age >=80)   ~ "High BP",
                                                        (.data$sbp_mean_4m_post_acs <  140 & .data$dbp_mean_4m_post_acs <  90  & .data$age  <80)   ~ "Controlled BP",
                                                        (.data$sbp_mean_4m_post_acs <  150 & .data$dbp_mean_4m_post_acs <  90  & .data$age >=80)   ~ "Controlled BP",
                                                        (is.na(.data$sbp_mean_4m_post_acs) | is.na(.data$dbp_mean_4m_post_acs) | is.na(.data$age)) ~ "Not measured",
                                                         TRUE ~ "error"),
                  bp_control_group = factor(.data$bp_control_group, levels = c("Not measured", "Controlled BP", "High BP"))) %>%
    dplyr::select(.data$bp_control_group)

  feature_matrix[c("hba1c_group")] =
    feature_matrix %>%
    dplyr::mutate(hba1c_group = dplyr::case_when(.data$hba1c_mean_4m_post_acs <= 38                                       ~ "Normal",
                                                 .data$hba1c_mean_4m_post_acs >  38 & .data$hba1c_mean_4m_post_acs <= 47  ~ "Prediabetes",
                                                 .data$hba1c_mean_4m_post_acs >  47                                       ~ "Diabetes",
                                                 is.na(.data$hba1c_mean_4m_post_acs)                                      ~ "Not measured",
                                                 TRUE ~ "error"),
                  hba1c_group = factor(.data$hba1c_group, levels = c("Not measured", "Normal", "Prediabetes", "Diabetes"))) %>%
    dplyr::select(.data$hba1c_group)


}


# -- cardiac rehabilitation attendance
{
feature_matrix[c("cardiac_rehab_visits", "cardiac_rehab_date_min", "cardiac_rehab_date_max")] =
  furrr::future_map2_dfr(.x = acs_cohort$swd_activity,
                         .y = feature_matrix$idx_acs_datetime,
                         .f = function(x, y) acsprojectns::query_swd_activity(
                                       x,
                                       provider    = NA,
                                       setting     = NA,
                                       urgency     = NA,
                                       speciality  = c("cardiac rehabilitation service"),
                                       time_window = lubridate::interval(y, lubridate::now(tzone = "GMT")),
                                       return_vals = c("count", "date_min", "date_max")),
                        .options = furrr::furrr_options(packages = "acsprojectns",
                                                        globals = c()),
                        .progress = TRUE)
feature_matrix["any_cardiac_rehab"] = purrr::map_lgl(feature_matrix$cardiac_rehab_visits, ~ .x > 0)
}


##----------------------------------------------------------------
##                          Outcome data                         -
##----------------------------------------------------------------
# -- deaths
{
feature_matrix[c("date_of_death", "death_1a", "death_1b", "death_1c", "death_2")] =
  furrr::future_map_dfr(.x = acs_cohort$mortality,
                        .f = function(x) acsprojectns::query_mortality(
                                      x,
                                      codes             = c(NA),
                                      search_strat_diag = "all",
                                      time_window       = lubridate::interval(acs_date_window_min, acs_date_window_max, tzone = "GMT"),
                                      return_vals       = c("date_of_death", "death_1a_icd_codes", "death_1b_icd_codes", "death_1c_icd_codes", "death_2_icd_codes")),
                        .options  = furrr::furrr_options(packages = "acsprojectns",
                                                         globals = c()),
                        .progress = TRUE)

acs_codes                    <- get_codes(file_path = system.file("icd10_codes/acs_codes.csv",                         package = "acsprojectns"), append_character = "-")
heart_failure_codes          <- get_codes(file_path = system.file("icd10_codes/heart_failure_codes.csv",               package = "acsprojectns"), append_character = "-")
isch_cardiomyopathy_codes    <- get_codes(file_path = system.file("icd10_codes/ischaemic_cardiomyopathy_codes.csv",    package = "acsprojectns"), append_character = "-")
nonisch_cardiomyopathy_codes <- get_codes(file_path = system.file("icd10_codes/nonischaemic_cardiomyopathy_codes.csv", package = "acsprojectns"), append_character = "-")
vasc_disease_codes           <- get_codes(file_path = system.file("icd10_codes/vascular_disease_codes.csv",            package = "acsprojectns"), append_character = "-")
isch_stroke_codes            <- get_codes(file_path = system.file("icd10_codes/ischaemic_stroke_codes.csv",            package = "acsprojectns"))
bleeding_death_codes         <- get_codes(file_path = system.file("icd10_codes/bleeding_codes.csv",                    package = "acsprojectns"), append_character = "-")
cv_death_codes               <- rbind(acs_codes, heart_failure_codes, isch_cardiomyopathy_codes, nonisch_cardiomyopathy_codes, vasc_disease_codes, isch_stroke_codes)

feature_matrix[c("status_death")]        = purrr::map_lgl (feature_matrix$date_of_death, ~ !is.na(.x))
feature_matrix[c("status_cv_death")]     = purrr::pmap_lgl(list(feature_matrix$death_1a, feature_matrix$death_1b, feature_matrix$death_1c),
                                                           function(a,b,c){
                                                             any(c(a,b,c) %in% cv_death_codes$icd_codes)
                                                           })
feature_matrix[c("status_non_cv_death")] = purrr::pmap_lgl(list(feature_matrix$death_1a, feature_matrix$death_1b, feature_matrix$death_1c),
                                                           function(a,b,c){
                                                             if(all(is.na(c(a,b,c))) | any(c(a,b,c) %in% cv_death_codes$icd_codes)){
                                                               return(FALSE)
                                                             }else{
                                                               return(TRUE)
                                                             }
                                                            })
feature_matrix[c("status_bleed_death")]  = purrr::pmap_lgl(list(feature_matrix$death_1a, feature_matrix$death_1b, feature_matrix$death_1c),
                                                           function(a,b,c){
                                                             any(c(a,b,c) %in% bleeding_death_codes$icd_codes)
                                                           })
}

# -- subsequent ACS
{
acs_codes <- get_codes(file_path = system.file("icd10_codes/acs_codes.csv", package = "acsprojectns"), append_character = "-")
feature_matrix[c("sub_acs_code", "sub_acs_datetime")] =
  furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                         .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval) + lubridate::duration(1, units = "seconds"),
                         .f = function(x, y) acsprojectns::query_icd10_codes(
                                       x,
                                       codes             = acs_codes$icd_codes,
                                       search_strat_diag = "primary",
                                       search_strat_date = "min",
                                       search_strat_adm_method = "emergency",
                                       time_window       = lubridate::interval(y, acs_date_window_max, tzone = "GMT"),
                                       return_vals       = c("code", "diagnosis_datetime")),
                         .options  = furrr::furrr_options(packages = "acsprojectns",
                                                          globals = c("acs_codes", "acs_date_window_max")),
                         .progress = TRUE)
}

# -- subsequent emergency PCI
{
pci_codes <- get_codes(file_path = system.file("opcs_codes/pci_procedure_codes.csv", package = "acsprojectns"))
feature_matrix[c("sub_pci_code", "sub_pci_datetime")] =
  furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                         .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval) + lubridate::duration(1, units = "seconds"),
                         .f = function(x, y) acsprojectns::query_opcs_codes(
                                       x,
                                       codes             = pci_codes$opcs_codes,
                                       search_strat_date = "min",
                                       time_window       = lubridate::interval(y, acs_date_window_max, tzone = "GMT"),
                                       search_strat_adm_method = "emergency",
                                       return_vals       = c("code", "procedure_datetime")),
                         .options = furrr::furrr_options(packages = "acsprojectns",
                                                         globals = c("pci_codes", "acs_date_window_max")),
                         .progress = TRUE)
}

# -- subsequent emergency CABG
{
cabg_codes <- get_codes(file_path = system.file("opcs_codes/cabg_procedure_codes.csv", package = "acsprojectns"))
feature_matrix[c("sub_cabg_code", "sub_cabg_datetime")] =
  furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                         .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval) + lubridate::duration(1, units = "seconds"),
                         .f = function(x, y) acsprojectns::query_opcs_codes(
                                       x,
                                       codes             = cabg_codes$opcs_codes,
                                       search_strat_date = "min",
                                       search_strat_adm_method = "emergency",
                                       time_window       = lubridate::interval(y, acs_date_window_max, tzone = "GMT"),
                                       return_vals       = c("code", "procedure_datetime")),
                         .options = furrr::furrr_options(packages = "acsprojectns",
                                                         globals = c("cabg_codes", "acs_date_window_max")),
                         .progress = TRUE)
}

# -- subsequent ischaemic stroke admission
{
isch_stroke_codes <- get_codes(file_path = system.file("icd10_codes/ischaemic_stroke_codes.csv", package = "acsprojectns"))
feature_matrix[c("sub_isch_stroke_code", "sub_isch_stroke_datetime")] =
  furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                         .y = feature_matrix$idx_acs_datetime,
                         .f = function(x, y) acsprojectns::query_icd10_codes(
                                       x,
                                       codes             = isch_stroke_codes$icd_codes, #[bleeding_codes$group == "Gastrointestinal bleeding" | bleeding_codes$group == "Intracranial bleeding" ],
                                       search_strat_diag = "primary",
                                       search_strat_date = "min",
                                       search_strat_adm_method = "emergency",
                                       time_window       = lubridate::interval(y, acs_date_window_max, tzone = "GMT"),
                                       return_vals       = c("code", "diagnosis_datetime")),
                        .options = furrr::furrr_options(packages = "acsprojectns",
                                                        globals = c("isch_stroke_codes", "acs_date_window_max")),
                        .progress = TRUE)
}

# -- subsequent heart failure admission
{
heart_failure_codes <- get_codes(file_path = system.file("icd10_codes/heart_failure_codes.csv", package = "acsprojectns"), append_character = "-")
feature_matrix[c("sub_hf_adm_code", "sub_hf_adm_datetime")] =
    furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                           .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval) + lubridate::duration(1, units = "seconds"),
                           .f = function(x, y) acsprojectns::query_icd10_codes(
                                         x,
                                         codes             = heart_failure_codes$icd_codes,
                                         search_strat_diag = "primary",
                                         search_strat_date = "min",
                                         search_strat_adm_method = "emergency",
                                         time_window       = lubridate::interval(y, acs_date_window_max, tzone = "GMT"),
                                         return_vals       = c("code", "diagnosis_datetime")),
                           .options  = furrr::furrr_options(packages = "acsprojectns",
                                                            globals = c("heart_failure_codes", "acs_date_window_max")),
                           .progress = TRUE)
}

# -- subsequent bleeding event admissions
{
bleeding_codes <- get_codes(file_path = system.file("icd10_codes/bleeding_codes.csv", package = "acsprojectns"), append_character = "-")
feature_matrix[c("sub_bleed_all_code", "sub_bleed_all_datetime")] =
  furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                         .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval) + lubridate::duration(1, units = "seconds"),
                         .f = function(x, y) acsprojectns::query_icd10_codes(
                                       x,
                                       codes             = bleeding_codes$icd_codes,
                                       search_strat_diag = "primary",
                                       search_strat_date = "min",
                                       search_strat_adm_method = "emergency",
                                       time_window       = lubridate::interval(y, acs_date_window_max, tzone = "GMT"),
                                       return_vals       = c("code", "diagnosis_datetime")),
                         .options  = furrr::furrr_options(packages = "acsprojectns",
                                                          globals = c("bleeding_codes", "acs_date_window_max")),
                         .progress = TRUE)

feature_matrix[c("sub_bleed_gi_code", "sub_bleed_gi_datetime")] =
  furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                         .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval) + lubridate::duration(1, units = "seconds"),
                         .f = function(x, y) acsprojectns::query_icd10_codes(
                                       x,
                                       codes             = bleeding_codes$icd_codes[bleeding_codes$group == "Gastrointestinal bleeding"],
                                       search_strat_diag = "primary",
                                       search_strat_date = "min",
                                       search_strat_adm_method = "emergency",
                                       time_window       = lubridate::interval(y, acs_date_window_max, tzone = "GMT"),
                                       return_vals       = c("code", "diagnosis_datetime")),
                         .options  = furrr::furrr_options(packages = "acsprojectns",
                                                          globals = c("bleeding_codes", "acs_date_window_max")),
                         .progress = TRUE)

feature_matrix[c("sub_bleed_ich_code", "sub_bleed_ich_datetime")] =
  furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                         .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval) + lubridate::duration(1, units = "seconds"),
                         .f = function(x, y) acsprojectns::query_icd10_codes(
                                       x,
                                       codes             = bleeding_codes$icd_codes[bleeding_codes$group == "Intracranial bleeding"],
                                       search_strat_diag = "primary",
                                       search_strat_date = "min",
                                       search_strat_adm_method = "emergency",
                                       time_window       = lubridate::interval(y, acs_date_window_max, tzone = "GMT"),
                                       return_vals       = c("code", "diagnosis_datetime")),
                         .options  = furrr::furrr_options(packages = "acsprojectns",
                                                          globals = c("bleeding_codes", "acs_date_window_max")),
                         .progress = TRUE)

feature_matrix[c("sub_bleed_other_code", "sub_bleed_other_datetime")] =
  furrr::future_map2_dfr(.x = acs_cohort$hospital_admissions,
                         .y = lubridate::int_end(feature_matrix$idx_acs_spell_interval) + lubridate::duration(1, units = "seconds"),
                         .f = function(x, y) acsprojectns::query_icd10_codes(
                                       x,
                                       codes             = bleeding_codes$icd_codes[bleeding_codes$group == "Other bleeding"],
                                       search_strat_diag = "primary",
                                       search_strat_date = "min",
                                       search_strat_adm_method = "emergency",
                                       time_window       = lubridate::interval(y, acs_date_window_max, tzone = "GMT"),
                                       return_vals       = c("code", "diagnosis_datetime")),
                         .options  = furrr::furrr_options(packages = "acsprojectns",
                                                          globals = c("bleeding_codes", "acs_date_window_max")),
                         .progress = TRUE)
}




# Get days to event data for survival analysis
{
feature_matrix <- feature_matrix %>%
    dplyr::mutate(days_to_stroke       = lubridate::interval(.data$idx_acs_datetime, .data$sub_isch_stroke_datetime)      %/% lubridate::days(1),
                  days_to_bleed        = lubridate::interval(.data$idx_acs_datetime, .data$sub_bleed_all_datetime)        %/% lubridate::days(1),
                  days_to_gi_bleed     = lubridate::interval(.data$idx_acs_datetime, .data$sub_bleed_gi_datetime)         %/% lubridate::days(1),
                  days_to_ich_bleed    = lubridate::interval(.data$idx_acs_datetime, .data$sub_bleed_ich_datetime)        %/% lubridate::days(1),
                  days_to_other_bleed  = lubridate::interval(.data$idx_acs_datetime, .data$sub_bleed_other_datetime)      %/% lubridate::days(1),
                  days_to_acs          = lubridate::interval(.data$idx_acs_datetime, .data$sub_acs_datetime)              %/% lubridate::days(1),
                  days_to_pci          = lubridate::interval(.data$idx_acs_datetime, .data$sub_pci_datetime)              %/% lubridate::days(1),
                  days_to_cabg         = lubridate::interval(.data$idx_acs_datetime, .data$sub_cabg_datetime)             %/% lubridate::days(1),
                  days_to_hf           = lubridate::interval(.data$idx_acs_datetime, .data$sub_hf_adm_datetime)           %/% lubridate::days(1),
                  days_to_death        = dplyr::if_else((lubridate::interval(.data$idx_acs_datetime, .data$date_of_death) %/% lubridate::days(1)) >= 0,
                                                         lubridate::interval(.data$idx_acs_datetime, .data$date_of_death) %/% lubridate::days(1), NA_real_),
                  days_to_cv_death     = dplyr::if_else(.data$status_cv_death,     .data$days_to_death, NA_real_),
                  days_to_non_cv_death = dplyr::if_else(.data$status_non_cv_death, .data$days_to_death, NA_real_),
                  days_to_bleed_death  = dplyr::if_else(.data$status_bleed_death,  .data$days_to_death, NA_real_)) %>%

    dplyr::mutate(days_follow_up       = purrr::pmap_dbl(list(.data$days_to_stroke,
                                                              .data$days_to_bleed,
                                                              .data$days_to_acs,
                                                              .data$days_to_pci,
                                                              .data$days_to_cabg,
                                                              .data$days_to_hf,
                                                              .data$days_to_death,
                                                              data.frame("days_worth_of_swd_data" = lubridate::interval(.data$idx_acs_datetime, lubridate::int_end(.data$swd_date_range)) %/% lubridate::days(1))),
                                                              ~ max(..., na.rm=TRUE)),
                  days_follow_up       = dplyr::if_else(.data$days_follow_up > lubridate::interval(.data$idx_acs_datetime, acs_date_window_max) %/% lubridate::days(1),
                                                        lubridate::interval(.data$idx_acs_datetime, acs_date_window_max) %/% lubridate::days(1),
                                                        .data$days_follow_up)) %>%

    dplyr::mutate(surv_nace_days         = purrr::pmap_dbl(list(.data$days_to_stroke,       .data$days_to_bleed, .data$days_to_acs, .data$days_to_pci, .data$days_to_cabg, .data$days_to_hf, .data$days_to_death, .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_macce_days        = purrr::pmap_dbl(list(.data$days_to_stroke,       .data$days_to_acs, .data$days_to_pci, .data$days_to_cabg, .data$days_to_hf, .data$days_to_cv_death, .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_stroke_days       = purrr::pmap_dbl(list(.data$days_to_stroke,       .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_bleed_days        = purrr::pmap_dbl(list(.data$days_to_bleed,        .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_gi_bleed_days     = purrr::pmap_dbl(list(.data$days_to_gi_bleed,     .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_ich_bleed_days    = purrr::pmap_dbl(list(.data$days_to_ich_bleed,    .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_other_bleed_days  = purrr::pmap_dbl(list(.data$days_to_other_bleed,  .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_acs_days          = purrr::pmap_dbl(list(.data$days_to_acs,          .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_pci_days          = purrr::pmap_dbl(list(.data$days_to_pci,          .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_cabg_days         = purrr::pmap_dbl(list(.data$days_to_cabg,         .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_hf_days           = purrr::pmap_dbl(list(.data$days_to_hf,           .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_death_days        = purrr::pmap_dbl(list(.data$days_to_death,        .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_cv_death_days     = purrr::pmap_dbl(list(.data$days_to_cv_death,     .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_non_cv_death_days = purrr::pmap_dbl(list(.data$days_to_non_cv_death, .data$days_follow_up), ~ min(..., na.rm=TRUE)),
                  surv_bleed_death_days  = purrr::pmap_dbl(list(.data$days_to_bleed_death,  .data$days_follow_up), ~ min(..., na.rm=TRUE)),

                  status_nace         = purrr::pmap_lgl(list(.data$days_to_stroke, .data$days_to_bleed, .data$days_to_acs, .data$days_to_pci, .data$days_to_cabg, .data$days_to_hf, .data$days_to_death), ~ !all(is.na(c(...)))),
                  status_macce        = purrr::pmap_lgl(list(.data$days_to_stroke, .data$days_to_acs, .data$days_to_pci, .data$days_to_cabg, .data$days_to_hf, .data$days_to_cv_death), ~ !all(is.na(c(...)))),
                  status_bleed        = purrr::map_lgl (     .data$days_to_bleed,        ~ !is.na(.x)),
                  status_gi_bleed     = purrr::map_lgl (     .data$days_to_gi_bleed,     ~ !is.na(.x)),
                  status_ich_bleed    = purrr::map_lgl (     .data$days_to_ich_bleed,    ~ !is.na(.x)),
                  status_other_bleed  = purrr::map_lgl (     .data$days_to_other_bleed,  ~ !is.na(.x)),
                  status_acs          = purrr::map_lgl (     .data$days_to_acs,          ~ !is.na(.x)),
                  status_pci          = purrr::map_lgl (     .data$days_to_pci,          ~ !is.na(.x)),
                  status_cabg         = purrr::map_lgl (     .data$days_to_cabg,         ~ !is.na(.x)),
                  status_hf           = purrr::map_lgl (     .data$days_to_hf,           ~ !is.na(.x)),
                  status_stroke       = purrr::map_lgl (     .data$days_to_stroke,       ~ !is.na(.x)),
                  status_death        = purrr::map_lgl (     .data$days_to_death,        ~ !is.na(.x)))
}

##----------------------------------------------------------------
##                    Save the feature_matrix                    -
##----------------------------------------------------------------
# Save the acs_cohort, as generating takes a while
usethis::use_data(feature_matrix, internal = FALSE, overwrite = TRUE)




