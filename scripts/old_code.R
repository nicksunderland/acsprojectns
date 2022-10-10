
# -- Antiplatelet and anticoagulant medications
{

  #Time series graphs of prescribing habits for ACS
  antiplatelet_medication_timeseries = data.frame();
  window_span = 3
  month_step  = 3
  start_month = -3
  end_month   = 24

  # These are static and don't need to be in the loop - does the patient exist in the SWD attributes at the start of data collection
  exists_at_attributes_start =
    !is.na(furrr::future_map_dfr(.x = acs_cohort$swd_attributes,
                                 .f = function(x) acsprojectns::query_swd_attributes(
                                   x,
                                   time_window       = lubridate::interval(as.POSIXct("2019-09-30"), as.POSIXct("2019-11-30")),
                                   search_strat_date = "min",
                                   return_vals       = c("attribute_period_date")),
                                 .options = furrr::furrr_options(packages = "acsprojectns"),
                                 .progress = TRUE))

  date_of_death = furrr::future_map_dfr(.x = acs_cohort$mortality,
                                        .f = function(x) acsprojectns::query_mortality(
                                          x,
                                          codes             = c(NA),
                                          search_strat_diag = "all",
                                          time_window       = lubridate::interval(acs_date_window_min, acs_date_window_max, tzone = "GMT"),
                                          return_vals       = c("date_of_death")),
                                        .options  = furrr::furrr_options(packages = "acsprojectns"),
                                        .progress = TRUE)

  for(i in seq(start_month, end_month, month_step)){
    print(glue::glue("\nMonth: {i}\tTimespan: {window_span}-months"))

    # The meds for this timepoint - returns a (df) logical matrix for each patient (rows) and each medication's existence (columns)
    med_matrix <- furrr::future_map2_dfr(.x = acs_cohort$gp_prescriptions,
                                         .y = feature_matrix$idx_acs_datetime,
                                         .f = function(x, y) acsprojectns::query_gp_medications(
                                           x,
                                           medications = c("warfarin", "apixaban", "rivaroxaban", "edoxaban", "dabigatran", "aspirin", "clopidogrel", "ticagrelor"),
                                           processing_strategy = "for_each",
                                           time_window = lubridate::interval(y + lubridate::duration(i-(window_span/2), units = "months"), y + lubridate::duration(i+(window_span/2), units = "months")),
                                           return_vals  = c("exists")),
                                         .options = furrr::furrr_options(packages = "acsprojectns"),
                                         .progress = TRUE)

    #TODO: need to work out how to assess in the same way as I did for the lipid therapy (see below)
    # Is there any medication data
    #######med_matrix["medications_exist"] = purrr::pmap_lgl(med_matrix, ~!all(is.na(c(...))))

    # Where or not the patient is in the SWD
    # Unfortunately the only thing that is updated regularly, regardless of activity, and therefore confirms the patient to be still local
    # is the primary_care_attributes table. Whilst the GP activity, including prescriptions, in the SWD exist from 2018-07-01, the GP
    # attributes actually starts 2019-10-01.
    # So, we have to assume that all the people with attributes data between 2018-07-01 and 2019-10-01 all existed in the SWD, if they also exist
    # at the beginning of when the attributes start being collected i.e. 2019-10-01.
    # After 2019-10-01 we can start checking at each timepoint whether the patient does actually still exist in the Bristol area

    # In this current loop, in the time window defined below, does the patient exists in the SWD attributes?
    med_matrix["exists_in_swd_attributes"] =
      !is.na(furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                                    .y = feature_matrix$idx_acs_datetime,
                                    .f = function(x, y) acsprojectns::query_swd_attributes(
                                      x,
                                      time_window       = lubridate::interval(y + lubridate::duration(i-(window_span/2), units = "months"), y + lubridate::duration(i+(window_span/2), units = "months")),
                                      search_strat_date = "min",
                                      return_vals       = c("attribute_period_date")),
                                    .options = furrr::furrr_options(packages = "acsprojectns"),
                                    .progress = TRUE))

    # The grey zone is the time between when we started recording prescription data, and started recording monthly attributes data
    # If we are in the grey zone and the patient exists at the beginning of the attributes period, then assume they existed prior
    med_matrix["grey_zone"] =
      ifelse((feature_matrix$idx_acs_datetime + lubridate::duration(i+(window_span/2), units = "months") < as.POSIXct("2019-10-01")) &
               (feature_matrix$idx_acs_datetime + lubridate::duration(i-(window_span/2), units = "months") > as.POSIXct("2018-07-01")) &
               exists_at_attributes_start,
             TRUE, FALSE)

    # Data available if they definitely exist in the SWD attributes table, OR, they are grey zone 'positive'
    med_matrix["data_available"] = med_matrix$exists_in_swd_attributes | med_matrix$grey_zone

    # Join the ids and the time period in
    med_matrix["time_months"] = i
    med_matrix["id"]          = acs_cohort$id

    # As we progress through time (the loops) we need to drop those patients that reach the end of the study period
    med_matrix["reached_end"] = (feature_matrix$idx_acs_datetime + lubridate::duration(i+(window_span/2), units = "months")) > acs_date_window_max

    # As well as dropping the people that die
    med_matrix["dead"] = date_of_death$date_of_death < (feature_matrix$idx_acs_datetime + lubridate::duration(i-(window_span/2), units = "months"))
    med_matrix["dead"] = tidyr::replace_na(med_matrix$dead, FALSE)

    # Filter them out and store the number remaining
    med_matrix <- med_matrix %>%
      dplyr::relocate(c(.data$id, .data$time_months)) %>%
      dplyr::filter(!.data$reached_end,
                    !.data$dead) %>%
      dplyr::mutate(num_at_risk = nrow(.))

    # Append the data to the timeseries
    antiplatelet_medication_timeseries <- rbind(antiplatelet_medication_timeseries, med_matrix)
  }

  # Save for later, as generating takes a while
  usethis::use_data(antiplatelet_medication_timeseries, internal = FALSE, overwrite = TRUE)

  # Load it back
  load("data/antiplatelet_medication_timeseries.rda")

  # Create the timeseries to plot
  antiplatelet_medication_timeseries <- antiplatelet_medication_timeseries %>%
    dplyr::rowwise() %>%
    dplyr::mutate(num_antiplatelets  = sum(.data$aspirin_exists,.data$clopidogrel_exists,.data$ticagrelor_exists),
                  num_anticoagulants = sum(.data$warfarin_exists,.data$apixaban_exists,.data$rivaroxaban_exists,.data$edoxaban_exists,.data$dabigatran_exists),
                  strategy           = dplyr::case_when(
                    .data$num_antiplatelets==1 & .data$num_anticoagulants >0 ~ "SAPT+AC",
                    .data$num_antiplatelets >1 & .data$num_anticoagulants >0 ~ "DAPT+AC",
                    .data$num_antiplatelets==0 & .data$num_anticoagulants >0 ~ "AC",
                    .data$num_antiplatelets==1 & .data$num_anticoagulants==0 ~ "SAPT",
                    .data$num_antiplatelets >1 & .data$num_anticoagulants==0 ~ "DAPT",
                    .data$num_antiplatelets==0 & .data$num_anticoagulants==0 & .data$data_available   ~ "None",
                    .data$num_antiplatelets==0 & .data$num_anticoagulants==0 & !.data$data_available  ~ "Missing",
                    TRUE ~ "error")) %>%
    dplyr::mutate(strategy = factor(.data$strategy , levels=c("DAPT+AC", "SAPT+AC", "DAPT", "AC", "SAPT", "None", "Missing")))

  average_strategy <- antiplatelet_medication_timeseries %>%
    dplyr::group_by(.data$time_months) %>%
    dplyr::mutate(total_n = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(.data$time_months, .data$strategy) %>%
    dplyr::mutate(pct = dplyr::n() / .data$total_n) %>%
    dplyr::summarise(pct = mean(.data$pct))


  # Plot a stacked colour chart
  ggplot(average_strategy, aes(x = time_months, y = pct, fill = strategy)) +
    geom_area()

  # Look at those people with apparent under treatment at XX months
  assess_treatment_at_months = 2
  treatment_description = data.frame("med_strategy_2m"      = c("DAPT+AC", "SAPT+AC", "DAPT", "AC", "SAPT", "None", "Missing"),
                                     "med_strategy_2m_desc" = c("treated", "treated", "treated", "under_treated", "under_treated", "under_treated", "missing"))
  strategy_at_2months = antiplatelet_medication_timeseries %>%
    dplyr::filter(.data$time_months==assess_treatment_at_months) %>%
    dplyr::left_join(treatment_description, by=c("strategy" = "med_strategy_2m")) %>%
    dplyr::rename("med_strategy_2m" = .data$strategy) %>%
    dplyr::right_join(data.frame("id"=ids), by=c("id")) %>%
    dplyr::mutate(med_strategy_2m      = tidyr::replace_na(.data$med_strategy_2m, "likely died or out of area"),
                  med_strategy_2m_desc = tidyr::replace_na(.data$med_strategy_2m_desc, "likely died or out of area")) %>%
    dplyr::select(.data$id, .data$med_strategy_2m, .data$med_strategy_2m_desc)
  strategy_at_2months$med_strategy_2m      <- as.factor(strategy_at_2months$med_strategy_2m)
  strategy_at_2months$med_strategy_2m_desc <- as.factor(strategy_at_2months$med_strategy_2m_desc)
  feature_matrix <- feature_matrix %>%
    dplyr::left_join(strategy_at_2months, by="id")

}


# -- Lipid medications
{
#Time series graphs of prescribing habits for ACS
lipid_medication_timeseries = data.frame();
window_span = 3
month_step  = 3
start_month = -3
end_month   = 12

# These are static and don't need to be in the loop - does the patient exist in the SWD attributes at the start of data collection
exists_at_attributes_start =
  !is.na(furrr::future_map_dfr(.x = acs_cohort$swd_attributes,
                               .f = function(x) acsprojectns::query_swd_attributes(
                                 x,
                                 time_window       = lubridate::interval(as.POSIXct("2019-09-30"), as.POSIXct("2019-11-30")),
                                 search_strat_date = "min",
                                 return_vals       = c("attribute_period_date")),
                               .options = furrr::furrr_options(packages = "acsprojectns"),
                               .progress = TRUE))

date_of_death = furrr::future_map_dfr(.x = acs_cohort$mortality,
                                      .f = function(x) acsprojectns::query_mortality(
                                        x,
                                        codes             = c(NA),
                                        search_strat_diag = "all",
                                        time_window       = lubridate::interval(acs_date_window_min, acs_date_window_max, tzone = "GMT"),
                                        return_vals       = c("date_of_death")),
                                      .options  = furrr::furrr_options(packages = "acsprojectns"),
                                      .progress = TRUE)

for(i in seq(start_month, end_month, month_step)){
  print(glue::glue("\nMonth: {i}\tTimespan: {window_span}-months\n"))

  # Where or not the patient is in the SWD
  # Unfortunately the only thing that is updated regularly, regardless of activity, and therefore confirms the patient to be still local
  # is the primary_care_attributes table. Whilst the GP activity, including prescriptions, in the SWD exist from 2018-07-01, the GP
  # attributes actually starts 2019-10-01.
  # So, we have to assume that all the people with attributes data between 2018-07-01 and 2019-10-01 all existed in the SWD, if they also exist
  # at the beginning of when the attributes start being collected i.e. 2019-10-01.
  # After 2019-10-01 we can start checking at each timepoint whether the patient does actually still exist in the Bristol area

  # In this current loop, in the time window defined below, does the patient exists in the SWD attributes?
  exists_in_swd_attributes =
    furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
                           .y = feature_matrix$idx_acs_datetime,
                           .f = function(x, y) acsprojectns::query_swd_attributes(
                             x,
                             time_window       = lubridate::interval(y + lubridate::duration(i-(window_span/2), units = "months"), y + lubridate::duration(i+(window_span/2), units = "months")),
                             search_strat_date = "min",
                             return_vals       = c("attribute_period_date")),
                           .options = furrr::furrr_options(packages = "acsprojectns"),
                           .progress = TRUE) %>%
    dplyr::rename_with(.cols = c("attribute_period_date"), ~ c("exists_in_swd_attributes")) %>%
    purrr::map_df(~ !is.na(.x))

  # The grey zone is the time between when we started recording prescription data, and started recording monthly attributes data
  # If we are in the grey zone and the patient exists at the beginning of the attributes period, then assume they existed prior
  grey_zone =
    ifelse((feature_matrix$idx_acs_datetime + lubridate::duration(i+(window_span/2), units = "months") < as.POSIXct("2019-10-01")) &
             (feature_matrix$idx_acs_datetime + lubridate::duration(i-(window_span/2), units = "months") > as.POSIXct("2018-07-01")) &
             exists_at_attributes_start,
           TRUE, FALSE)

  # The meds for this timepoint - returns a (df) logical matrix for each patient (rows) and each medication's existence (columns)
  med_matrix <- furrr::future_map2_dfr(.x = acs_cohort$gp_prescriptions,
                                       .y = feature_matrix$idx_acs_datetime,
                                       .f = function(x, y) acsprojectns::query_gp_medications(
                                         x,
                                         medications = c("inclisiran", "ezetimibe", "atorvastatin", "rosuvastatin", "simvastatin", "fluvastatin", "pravastatin"),
                                         processing_strategy = "for_each",
                                         time_window = lubridate::interval(y + lubridate::duration(i-(window_span/2), units = "months"), y + lubridate::duration(i+(window_span/2), units = "months")),
                                         return_vals  = c("av_dose")),
                                       .options = furrr::furrr_options(packages = "acsprojectns"),
                                       .progress = TRUE)

  # Is there any medication data
  medications_exist = purrr::pmap_lgl(med_matrix, ~!all(is.na(c(...))))

  # Data available if they definitely exist in the SWD attributes table, OR, they are grey zone 'positive' OR we found medication data
  data_exists = medications_exist | exists_in_swd_attributes | grey_zone

  # As we progress through time (the loops) we need to drop those patients that reach the end of the study period
  reached_end = (feature_matrix$idx_acs_datetime + lubridate::duration(i+(window_span/2), units = "months")) > acs_date_window_max

  # As well as dropping the people that die
  dead = (date_of_death$date_of_death < (feature_matrix$idx_acs_datetime + lubridate::duration(i-(window_span/2), units = "months"))) %>%
    replace(is.na(.), FALSE)

  # The filter to remove unwanted data
  censored = !data_exists | (data_exists & (reached_end | dead))

  med_matrix <- cbind("id"   = acs_cohort$id,
                      "time" = i,
                      "censored" = unname(censored),
                      med_matrix)

  # Append the data to the timeseries
  lipid_medication_timeseries <- rbind(lipid_medication_timeseries, med_matrix)
}

# Save for later, as generating takes a while
usethis::use_data(lipid_medication_timeseries, internal = FALSE, overwrite = TRUE)

# Load it back
load("data/lipid_medication_timeseries.rda")

}








#
#
#
# feature_matrix[c("egfr_mean_4m_post_acs")] =
#   furrr::future_map2_dfr(.x = acs_cohort$swd_measurements,
#                          .y = feature_matrix$idx_acs_datetime,
#                          .f = function(x, y) acsprojectns::query_swd_measurements(
#                            x,
#                            measurements = c("egfr"),
#                            time_window  = lubridate::interval(y, y + lubridate::duration(4, units = "months"), tzone = "GMT"),
#                            return_vals  = c("mean")),
#                          .options  = furrr::furrr_options(packages = "acsprojectns"),
#                          .progress = TRUE)
#
# feature_matrix[c("egfr_group")] =
#   feature_matrix %>%
#   dplyr::mutate(egfr_group = dplyr::case_when(.data$egfr_mean_4m_post_acs >89                                      ~ "eGFR >=90",
#                                               .data$egfr_mean_4m_post_acs >=60 & .data$egfr_mean_4m_post_acs <= 89 ~ "eGFR 60-89",
#                                               .data$egfr_mean_4m_post_acs >=45 & .data$egfr_mean_4m_post_acs <  60 ~ "eGFR 45-59",
#                                               .data$egfr_mean_4m_post_acs >=30 & .data$egfr_mean_4m_post_acs <  44 ~ "eGFR 30-44",
#                                               .data$egfr_mean_4m_post_acs >=15 & .data$egfr_mean_4m_post_acs <  29 ~ "eGFR 15-29",
#                                               .data$egfr_mean_4m_post_acs <15                                      ~ "eGFR <15",
#                                               is.na(.data$egfr_mean_4m_post_acs)                                   ~ "Not measured",
#                                               TRUE ~ "error"),
#                 egfr_group = factor(.data$egfr_group, levels = c("eGFR <15", "eGFR 15-29", "eGFR 30-44", "eGFR 45-59", "eGFR 60-89", "eGFR >=90", "Not measured"))) %>%
#   dplyr::select(.data$egfr_group)
# #
#
#
#   # People getting BP tested after their ACS event, and the mean level
#   feature_matrix %>%
#     dplyr::mutate(date_bin = lubridate::floor_date(.data$idx_acs_datetime, unit = "3 months")) %>%
#     dplyr::group_by(.data$date_bin, .data$total_chol_group) %>%
#     dplyr::mutate(n = dplyr::n()) %>%
#     ggplot2::ggplot(aes(x=date_bin, y=.data$n, fill=.data$bp_group)) +
#     geom_bar(position = "fill", stat = "identity") +
#     geom_text(aes(y = 0, label = n), vjust = 0, nudge_y = .05) +
#     ggtitle("BP measured within 4 months, by mean BP category (NICE)") +
#     labs(x = "Date", y = "%", fill = "Group")
#
#   # People getting HbA1c tested after their ACS event, and the mean level
#   feature_matrix %>%
#     dplyr::mutate(date_bin = lubridate::floor_date(.data$idx_acs_datetime, unit = "3 months")) %>%
#     dplyr::group_by(.data$date_bin, .data$total_chol_group) %>%
#     dplyr::mutate(n = dplyr::n()) %>%
#     ggplot2::ggplot(aes(x=date_bin, y=.data$n, fill=.data$hba1c_group)) +
#     geom_bar(position = "fill", stat = "identity") +
#     geom_text(aes(y = 0, label = n), vjust = 0, nudge_y = .05) +
#     ggtitle("Number of patients getting HbA1c measured within 4 months, by mean HbA1c category") +
#     labs(x = "Date", y = "%", fill = "Group")
#
#   # People getting cholesterol tested after their ACS event, and the mean level
#   feature_matrix %>%
#     dplyr::mutate(date_bin = lubridate::floor_date(.data$idx_acs_datetime, unit = "3 months")) %>%
#     dplyr::group_by(.data$date_bin, .data$total_chol_group) %>%
#     dplyr::mutate(n = dplyr::n()) %>%
#     ggplot2::ggplot(aes(x=.data$date_bin, y=.data$n, fill=.data$total_chol_group)) +
#     geom_bar(position = "fill", stat = "identity") +
#     geom_text(aes(y = 0, label = n), vjust = 0, nudge_y = .05) +
#     ggtitle("Number of patients getting cholesterol measured, by mean cholesterol category") +
#     labs(x = "Date", y = "%", fill = "Group")
#
#   # People getting eGFR tested after their ACS event, and the mean level
#   feature_matrix %>%
#     dplyr::mutate(date_bin = lubridate::floor_date(.data$idx_acs_datetime, unit = "3 months")) %>%
#     dplyr::group_by(.data$date_bin, .data$egfr_group) %>%
#     dplyr::mutate(n = dplyr::n()) %>%
#     ggplot2::ggplot(aes(x=.data$date_bin, y=.data$n, fill=.data$egfr_group)) +
#     geom_bar(position = "fill", stat = "identity") +
#     geom_text(aes(y = 0, label = n), vjust = 0, nudge_y = .05) +
#     ggtitle("Number of patients getting eGFR measured, by CKD category") +
#     labs(x = "Date", y = "%", fill = "Group")
# }






#
#
#
# }
#
#
# # -- Look at the temporal pattern of the cost occurring before and after event
# {
#
#   #Time series graphs of prescribing habits for ACS
#   cost_timeseries = data.frame();
#   window_span = 1
#   month_step  = 1
#   start_month = -3
#   end_month   = 24
#
#   # These are static and don't need to be in the loop - does the patient exist in the SWD attributes at the start of data collection
#   exists_at_attributes_start =
#     !is.na(furrr::future_map_dfr(.x = acs_cohort$swd_attributes,
#                                  .f = function(x) acsprojectns::query_swd_attributes(
#                                                x,
#                                                time_window       = lubridate::interval(as.POSIXct("2019-09-30"), as.POSIXct("2019-11-30")),
#                                                search_strat_date = "min",
#                                                return_vals       = c("attribute_period_date")),
#                                  .options = furrr::furrr_options(packages = "acsprojectns"),
#                                  .progress = TRUE))
#
#   date_of_death = furrr::future_map_dfr(.x = acs_cohort$mortality,
#                                         .f = function(x) acsprojectns::query_mortality(
#                                                       x,
#                                                       codes             = c(NA),
#                                                       search_strat_diag = "all",
#                                                       time_window       = lubridate::interval(acs_date_window_min, acs_date_window_max, tzone = "GMT"),
#                                                       return_vals       = c("date_of_death")),
#                                         .options  = furrr::furrr_options(packages = "acsprojectns"),
#                                         .progress = TRUE)
#
#   for(i in seq(start_month, end_month, month_step)){
#     print(glue::glue("\nMonth: {i}\tTimespan: {window_span}-months"))
#
#
#     # Where or not the patient is in the SWD
#     # Unfortunately the only thing that is updated regularly, regardless of activity, and therefore confirms the patient to be still local
#     # is the primary_care_attributes table. Whilst the GP activity, including prescriptions, in the SWD exist from 2018-07-01, the GP
#     # attributes actually starts 2019-10-01.
#     # So, we have to assume that all the people with attributes data between 2018-07-01 and 2019-10-01 all existed in the SWD, if they also exist
#     # at the beginning of when the attributes start being collected i.e. 2019-10-01.
#     # After 2019-10-01 we can start checking at each timepoint whether the patient does actually still exist in the Bristol area
#
#     # In this current loop, in the time window defined below, does the patient exists in the SWD attributes?
#     exists_in_swd_attributes =
#       furrr::future_map2_dfr(.x = acs_cohort$swd_attributes,
#                                     .y = feature_matrix$idx_acs_datetime,
#                                     .f = function(x, y) acsprojectns::query_swd_attributes(
#                                                   x,
#                                                   time_window       = lubridate::interval(y + lubridate::duration(i, units = "months"), y + lubridate::duration(i+window_span, units = "months")),
#                                                   search_strat_date = "min",
#                                                   return_vals       = c("attribute_period_date")),
#                                     .options = furrr::furrr_options(packages = "acsprojectns"),
#                                     .progress = TRUE) %>%
#       dplyr::rename_with(.cols = c("attribute_period_date"), ~ c("exists_in_swd_attributes")) %>%
#       purrr::map_df(~ !is.na(.x))
#
#     # The grey zone is the time between when we started recording prescription data, and started recording monthly attributes data
#     # If we are in the grey zone and the patient exists at the beginning of the attributes period, then assume they existed prior
#     grey_zone =
#       ifelse((feature_matrix$idx_acs_datetime + lubridate::duration(i+window_span, units = "months") < as.POSIXct("2019-10-01")) &
#              (feature_matrix$idx_acs_datetime + lubridate::duration(i, units = "months") > as.POSIXct("2018-07-01")) &
#               exists_at_attributes_start,
#              TRUE, FALSE)
#
#     # Data available if they definitely exist in the SWD attributes table, OR, they are grey zone 'positive'
#     data_available = exists_in_swd_attributes | grey_zone
#
#     # As we progress through time (the loops) we need to drop those patients that reach the end of the study period
#     reached_end = (feature_matrix$idx_acs_datetime + lubridate::duration(i+window_span, units = "months")) > acs_date_window_max
#
#     # As well as dropping the people that die
#     dead = (date_of_death$date_of_death < (feature_matrix$idx_acs_datetime + lubridate::duration(i, units = "months"))) %>%
#       replace(is.na(.), FALSE)
#
#     # The costs for this timepoint
#     primary_care_cost <- furrr::future_map2_dfr(
#       .x = acs_cohort$swd_activity,
#       .y = feature_matrix$idx_acs_datetime,
#       .f = function(x, y) acsprojectns::query_swd_activity(
#         x,
#         provider    = "primary_care_contact",
#         setting     = NA,
#         urgency     = NA,
#         speciality  = NA,
#         time_window = lubridate::interval(y + lubridate::duration(i, units = "months"), y + lubridate::duration(i+window_span, units = "months")),
#         return_vals = c("cost", "count")),
#       .options = furrr::furrr_options(packages = "acsprojectns"),
#       .progress = TRUE) %>%
#       dplyr::rename_with(.cols = c("cost", "count"), ~ c("primary_care_cost", "primary_care_count"))
#
#     secondary_care_cost <- furrr::future_map2_dfr(
#       .x = acs_cohort$swd_activity,
#       .y = feature_matrix$idx_acs_datetime,
#       .f = function(x, y) acsprojectns::query_swd_activity(
#         x,
#         provider    = "secondary",
#         setting     = NA,
#         urgency     = NA,
#         speciality  = NA,
#         time_window = lubridate::interval(y + lubridate::duration(i, units = "months"), y + lubridate::duration(i+window_span, units = "months")),
#         return_vals = c("cost", "count")),
#       .options = furrr::furrr_options(packages = "acsprojectns"),
#       .progress = TRUE) %>%
#       dplyr::rename_with(.cols = c("cost", "count"), ~ c("secondary_care_cost", "secondary_care_count"))
#
#     community_care_cost <- furrr::future_map2_dfr(
#       .x = acs_cohort$swd_activity,
#       .y = feature_matrix$idx_acs_datetime,
#       .f = function(x, y) acsprojectns::query_swd_activity(
#         x,
#         provider    = "community",
#         setting     = NA,
#         urgency     = NA,
#         speciality  = NA,
#         time_window = lubridate::interval(y + lubridate::duration(i, units = "months"), y + lubridate::duration(i+window_span, units = "months")),
#         return_vals = c("cost", "count")),
#       .options = furrr::furrr_options(packages = "acsprojectns"),
#       .progress = TRUE) %>%
#       dplyr::rename_with(.cols = c("cost", "count"), ~ c("community_care_cost", "community_care_count"))
#
#     mental_care_cost <- furrr::future_map2_dfr(
#       .x = acs_cohort$swd_activity,
#       .y = feature_matrix$idx_acs_datetime,
#       .f = function(x, y) acsprojectns::query_swd_activity(
#         x,
#         provider    = "mental_health",
#         setting     = NA,
#         urgency     = NA,
#         speciality  = NA,
#         time_window = lubridate::interval(y + lubridate::duration(i, units = "months"), y + lubridate::duration(i+window_span, units = "months")),
#         return_vals = c("cost", "count")),
#       .options = furrr::furrr_options(packages = "acsprojectns"),
#       .progress = TRUE) %>%
#       dplyr::rename_with(.cols = c("cost", "count"), ~ c("mental_care_cost", "mental_care_count"))
#
#     all_care_cost <- furrr::future_map2_dfr(
#       .x = acs_cohort$swd_activity,
#       .y = feature_matrix$idx_acs_datetime,
#       .f = function(x, y) acsprojectns::query_swd_activity(
#         x,
#         provider    = NA,
#         setting     = NA,
#         urgency     = NA,
#         speciality  = NA,
#         time_window = lubridate::interval(y + lubridate::duration(i, units = "months"), y + lubridate::duration(i+window_span, units = "months")),
#         return_vals = c("cost", "count")),
#       .options = furrr::furrr_options(packages = "acsprojectns"),
#       .progress = TRUE) %>%
#       dplyr::rename_with(.cols = c("cost", "count"), ~ c("all_care_cost", "all_care_count"))
#
#     cost_matrix <- cbind("id"   = acs_cohort$id,
#                          "time" = i,
#                          "data_available" = unname(data_available),
#                          "reached_end" = unname(reached_end),
#                          "dead" = unname(dead),
#                          primary_care_cost,
#                          secondary_care_cost,
#                          community_care_cost,
#                          mental_care_cost,
#                          all_care_cost)
#
#     # Append the data to the timeseries
#     cost_timeseries <- rbind(cost_timeseries, cost_matrix)
#   }
#
#   # Save for later, as generating takes a while
#   usethis::use_data(cost_timeseries, internal = FALSE, overwrite = TRUE)
#
#   # Load it back
#   load("data/cost_timeseries.rda")
#
#
# #Time series graphs of cost
# ggplot(data = cost_timeseries %>%
#                 dplyr::filter(.data$censored) %>%
#                 dplyr::left_join(feature_matrix$lipid_strategy_0m_desc, by = "id"),
#        mapping = aes(x=time, y=all_care_cost, color=lipid_strategy_0m_desc)) +
#   stat_summary(geom    = "line",
#                fun     = ~median(.x)) +
#   stat_summary(geom    = "ribbon",
#                fun.min = ~quantile(.x, 0.25),
#                fun.max = ~quantile(.x, 0.75),
#                alpha   = 0.3,
#                linetype= 0) +
#   labs(title="Dynamic healthcare activity costs - Total cost",
#        x="Time post index event (months)", y="Median patient cost per month (£)") +
#   scale_colour_manual(name='Lipid strategy', values=c("censored_by_0months"='grey', "none"='red', "sub_optimum_dose" = "orange", "optimum_dose" = "green")) +
#   scale_fill_manual  (name='Lipid strategy', values=c("censored_by_0months"='grey', "none"='red', "sub_optimum_dose" = "orange", "optimum_dose" = "green")) +
#   scale_y_continuous(trans='log10')
#
# }
#
#
#
#
#
#
#
#
# # Look at bleeding risk factors and the survival from bleed events post PCI / ACS
# {
#   # Age category
#   feature_matrix["age_gt75"] = dplyr::if_else(feature_matrix$age >= 75, TRUE, FALSE, missing = NA)
#
#   # Anaemia
#   anaemia_codes <- get_codes(file_path = system.file("icd10_codes/anaemia_codes.csv",          package = "acsprojectns"), append_character = "-")
#   feature_matrix["anaemia_history"] =
#     !is.na(
#       furrr::future_map2_dfr(cohort$hospital_admissions, feature_matrix$idx_event_interval, ~ query_icd10_codes(
#                                     .x,
#                                     codes             = anaemia_codes$icd_codes,
#                                     search_strat_diag = "all",
#                                     search_strat_date = "max",
#                                     time_window       = lubridate::interval(lubridate::int_start(.y) - lubridate::duration(10, "years"), lubridate::int_end(.y)),
#                                     return_vals        = "code"),
#                             .options = furrr::furrr_options(packages = "acsprojectns"),
#                             .progress = TRUE))
#
#   # Anticoagulants
#   feature_matrix[c("any_anticoagulant")] =
#     furrr::future_map2_dfr(cohort$gp_prescriptions, feature_matrix$idx_event_datetime, ~ query_gp_medications(
#                                     .x,
#                                     medications = c("warfarin", "apixaban", "rivaroxaban", "edoxaban", "dabigatran"),
#                                     time_window = lubridate::interval(.y, lubridate::now(tzone = "GMT")),
#                                     return_vals  = c("any_exist")),
#                           .options = furrr::furrr_options(packages = "acsprojectns"),
#                           .progress = TRUE)
#
#   # Potent P2Y12
#   feature_matrix[c("any_potent_p2y12")] =
#     furrr::future_map2_dfr(cohort$gp_prescriptions, feature_matrix$idx_event_datetime, ~ query_gp_medications(
#                                     .x,
#                                     medications = c("prasugrel", "ticagrelor"),
#                                     time_window = lubridate::interval(.y, lubridate::now(tzone = "GMT")),
#                                     return_vals  = c("any_exist")),
#                           .options = furrr::furrr_options(packages = "acsprojectns"),
#                           .progress = TRUE)
#
#
#   #Some simple stats
#   foo <- feature_matrix %>%
#     dplyr::mutate(bleed_flag = dplyr::if_else((!is.na(.data$bleed_code) | !is.na(.data$bleed_code)), TRUE, FALSE, missing = NA),
#                   duration_months = .data$swd_data_date_range %/% lubridate::dmonths(1),
#                   duration_months = base::replace(.data$duration_months, .data$duration_months==0, 1)) %>%
#     dplyr::filter(!is.na(.data$duration_months)) %>%
#     dplyr::group_by(bleed_flag) %>%
#     dplyr::mutate(total_num_bleed_group = dplyr::n()) %>%
#     dplyr::ungroup() %>%
#     dplyr::group_by(bleed_flag, any_anticoagulant) %>%
#     dplyr::mutate(pct_anticoag = dplyr::n() / total_num_bleed_group) %>%
#     dplyr::ungroup() %>%
#     dplyr::group_by(bleed_flag, any_potent_p2y12) %>%
#     dplyr::mutate(pct_p2y12 = dplyr::n() / total_num_bleed_group) %>%
#     dplyr::ungroup() %>%
#     dplyr::group_by(bleed_flag, any_anticoagulant, any_potent_p2y12) %>%
#     dplyr::mutate(n = dplyr::n(),
#                   pct_bleed_group = n / total_num_bleed_group) %>%
#     dplyr::summarise(n = mean(n),
#                      pct_bleed_group = mean(pct_bleed_group))
#
# }
#
#
#
#
#
#
#
#
#
#
#
#
#
#
# # <<<<<<< HEAD
# # =======
# # # Look at survival from NACE - Net Adverse Clinical Events
# # {
# # #TODO: need to check attribute data as to whether they still exist in the dataset - then censor / update the max follow up date
# #
# # foo <- feature_matrix %>%
# #     dplyr::mutate(days_to_stroke= lubridate::interval(.data$idx_acs_datetime, .data$sub_isch_stroke_datetime) %/% lubridate::days(1),
# #                   days_to_bleed = lubridate::interval(.data$idx_acs_datetime, .data$sub_bleed_all_datetime) %/% lubridate::days(1),
# #                   days_to_acs   = lubridate::interval(.data$idx_acs_datetime, .data$sub_acs_datetime) %/% lubridate::days(1),
# #                   days_to_pci   = lubridate::interval(.data$idx_acs_datetime, .data$sub_pci_datetime) %/% lubridate::days(1),
# #                   days_to_cabg  = lubridate::interval(.data$idx_acs_datetime, .data$sub_cabg_datetime) %/% lubridate::days(1),
# #                   days_to_hf    = lubridate::interval(.data$idx_acs_datetime, .data$sub_hf_adm_datetime) %/% lubridate::days(1),
# #                   days_to_death = dplyr::if_else((lubridate::interval(.data$idx_acs_datetime, .data$death_date) %/% lubridate::days(1)) >= 0,
# #                                                   lubridate::interval(.data$idx_acs_datetime, .data$death_date) %/% lubridate::days(1), NA_real_),
# #                   days_follow_up= dplyr::case_when(!is.na(days_to_death) ~ days_to_death,
# #                                                    TRUE ~ lubridate::interval(.data$idx_acs_datetime, acs_date_window_max) %/% lubridate::days(1))) %>%
# #     dplyr::mutate(surv_nace_days= purrr::pmap_dbl(    list(.data$days_to_stroke,
# #                                                        .data$days_to_bleed,
# #                                                        .data$days_to_acs,
# #                                                        .data$days_to_pci,
# #                                                        .data$days_to_cabg,
# #                                                        .data$days_to_hf,
# #                                                        .data$days_to_death,
# #                                                        .data$days_follow_up), ~ min(..., na.rm=TRUE)),
# #                   status_nace   = purrr::pmap_lgl(list(.data$days_to_stroke,
# #                                                        .data$days_to_bleed,
# #                                                        .data$days_to_acs,
# #                                                        .data$days_to_pci,
# #                                                        .data$days_to_cabg,
# #                                                        .data$days_to_hf,
# #                                                        .data$days_to_death), ~ !all(is.na(c(...)))),
# #                 surv_stroke_days= purrr::pmap_dbl(    list(.data$days_to_stroke,
# #                                                        .data$days_follow_up), ~ min(..., na.rm=TRUE)),
# #                 status_stroke   = purrr::map_lgl(      .data$days_to_stroke, ~ !is.na(.x)),
# #                 surv_bleed_days = purrr::pmap_dbl(    list(.data$days_to_bleed,
# #                                                        .data$days_follow_up), ~ min(..., na.rm=TRUE)),
# #                 status_bleed   = purrr::map_lgl(       .data$days_to_bleed, ~ !is.na(.x)),
# #                 surv_acs_days   = purrr::pmap_dbl(    list(.data$days_to_acs,
# #                                                        .data$days_follow_up), ~ min(..., na.rm=TRUE)),
# #                 status_acs     = purrr::map_lgl(       .data$days_to_acs, ~ !is.na(.x)),
# #                 surv_pci_days   = purrr::pmap_dbl(    list(.data$days_to_pci,
# #                                                        .data$days_follow_up), ~ min(..., na.rm=TRUE)),
# #                 status_pci     = purrr::map_lgl(       .data$days_to_pci, ~ !is.na(.x)),
# #                 surv_cabg_days   = purrr::pmap_dbl(   list(.data$days_to_cabg,
# #                                                        .data$days_follow_up), ~ min(..., na.rm=TRUE)),
# #                 status_cabg     = purrr::map_lgl(      .data$days_to_cabg, ~ !is.na(.x)),
# #                 surv_hf_days     = purrr::pmap_dbl(   list(.data$days_to_hf,
# #                                                        .data$days_follow_up), ~ min(..., na.rm=TRUE)),
# #                 status_hf     = purrr::map_lgl(        .data$days_to_hf, ~ !is.na(.x)),
# #                 surv_death_days     = purrr::pmap_dbl(   list(.data$days_to_death,
# #                                                        .data$days_follow_up), ~ min(..., na.rm=TRUE)),
# #                 status_death     = purrr::map_lgl(      .data$days_to_death, ~ !is.na(.x)))
# #
# # # Plot some survival
# # library(survival)
# # library(survminer)
# #
# # # multiple curves - https://github.com/kassambara/survminer/issues/195
# # nace_fit <- survfit(Surv(foo$surv_nace_days, foo$status_nace), ~ 1)
# # stroke_fit <- survfit(Surv(foo$surv_stroke_days, foo$status_stroke) ~ 1)
# # bleed_fit <- survfit(Surv(foo$surv_bleed_days, foo$status_bleed) ~ 1)
# # acs_fit <- survfit(Surv(foo$surv_acs_days, foo$status_acs) ~ 1)
# # pci_fit <- survfit(Surv(foo$surv_pci_days, foo$status_pci) ~ 1)
# # cabg_fit <- survfit(Surv(foo$surv_cabg_days, foo$status_cabg) ~ 1)
# # hf_fit <- survfit(Surv(foo$surv_hf_days, foo$status_hf) ~ 1)
# # death_fit <- survfit(Surv(foo$surv_death_days, foo$status_death) ~ 1)
# #
# # fits = list(nace = nace_fit, stroke = stroke_fit, bleed = bleed_fit, acs = acs_fit, pci = pci_fit, cabg = cabg_fit, hf= hf_fit, death=death_fit)
# #
# # survminer::ggsurvplot(fits,
# #                       data = foo,
# #                       combine = TRUE,
# #                       risk.table = TRUE,
# #                       tables.theme = theme_cleantable(),
# #                       ylim = c(0.0, 1.0),
# #                       title = "ACS patients - survival from NACE",
# #                       xlab = "Days",
# #                       ylab = "Survival (%)")
# # }
# #
# #
# #
# #
# # >>>>>>> 4e6b7bd18d1249fdf2969739d3390b14b6fb66ba
#
#
# #################################################################
# ##                    Useful things to keep                    ##
# #################################################################
#
# pci_codes       <- get_codes(file_path = system.file("opcs_codes/pci_procedure_codes.csv",              package = "acsprojectns"))
#
# # Patients who had a PCI procedure
# pci_cohort <- data.frame(id = get_hospital_admissions(
#   sus_apc_obj            = db_conn_struct$sus_apc,
#   ccg_provider_code_list = bnssg_ccg_codes$ccg_provider_code,
#   level                  = "episode",
#   codes_list             =  pci_codes$opcs_codes,
#   search_strategy        = "procedures",
#   return_all_codes       = FALSE,
#   datetime_window        = lubridate::interval(date_window_min, date_window_max, tzone = "GMT"),
#   verbose                = FALSE) %>%
#     dplyr::distinct(pseudo_nhs_id) %>%
#     dplyr::pull(pseudo_nhs_id))
#
#
# # Did this when there was two index events, ACS and PCI; so not needed now
# feature_matrix["idx_event"] =
#   dplyr::case_when(feature_matrix$idx_acs_datetime <= feature_matrix$idx_pci_datetime ~ "ACS",
#                    feature_matrix$idx_acs_datetime > feature_matrix$idx_pci_datetime ~ "PCI",
#                    (is.na(feature_matrix$idx_pci_datetime) & !is.na(feature_matrix$idx_acs_datetime))  ~ "ACS",
#                    (is.na(feature_matrix$idx_acs_datetime) & !is.na(feature_matrix$idx_pci_datetime)) ~ "PCI",
#                    TRUE ~ NA_character_)
# feature_matrix["idx_event_datetime"] =
#   dplyr::case_when(feature_matrix$idx_acs_datetime <= feature_matrix$idx_pci_datetime ~ feature_matrix$idx_acs_datetime,
#                    feature_matrix$idx_acs_datetime > feature_matrix$idx_pci_datetime ~ feature_matrix$idx_pci_datetime,
#                    (is.na(feature_matrix$idx_pci_datetime) & !is.na(feature_matrix$idx_acs_datetime)) ~ feature_matrix$idx_acs_datetime,
#                    (is.na(feature_matrix$idx_acs_datetime) & !is.na(feature_matrix$idx_pci_datetime)) ~ feature_matrix$idx_pci_datetime,
#                    TRUE ~ as.POSIXct(NA))
# feature_matrix["idx_event_interval"] =
#   dplyr::case_when(feature_matrix$idx_acs_datetime <= feature_matrix$idx_pci_datetime ~ feature_matrix$idx_acs_spell_interval,
#                    feature_matrix$idx_acs_datetime > feature_matrix$idx_pci_datetime ~ feature_matrix$idx_pci_interval,
#                    (is.na(feature_matrix$idx_pci_datetime) & !is.na(feature_matrix$idx_acs_datetime)) ~ feature_matrix$idx_acs_spell_interval,
#                    (is.na(feature_matrix$idx_acs_datetime) & !is.na(feature_matrix$idx_pci_datetime)) ~ feature_matrix$idx_pci_interval,
#                    TRUE ~ lubridate::interval(as.POSIXct(NA), as.POSIXct(NA)))

