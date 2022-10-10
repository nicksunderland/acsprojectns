# Looking at changing medication patterns over time
library(usethis)
library(devtools)
library(ggplot2)
library(plotly)
rm(list=ls())
load_all()
load("data/acs_cohort.rda")

cohort   = acs_cohort[c("id", "idx_acs_event", "swd_attributes")] %>%
  tidyr::unnest(cols = c("idx_acs_event", "swd_attributes")) %>%
  dplyr::group_by(.data$id) %>%
  dplyr::mutate(swd_date_range = dplyr::if_else(!is.na(attribute_period_date),
                                                lubridate::interval(max(.data$attribute_period_date, na.rm=T), min(.data$attribute_period_date, na.rm=T), tzone="GMT"),
                                                lubridate::interval(NA,NA))) %>%
  dplyr::filter(!is.na(.data$swd_date_range)) %>%
  dplyr::slice_head(n=1) %>%
  dplyr::select(id             = .data$id,
                idx_event_date = .data$diagnosis_datetime,
                swd_date_range = .data$swd_date_range)

# Antiplatelets
{
meds = acs_cohort[c("id", "gp_prescriptions")] %>%
  tidyr::unnest(cols = "gp_prescriptions") %>%
  dplyr::filter(!is.na(.data$medication_name)) %>%
  dplyr::select(id                = .data$id,
                prescription_date = .data$episode_start_datetime,
                medication_name   = .data$medication_name) %>%
  dplyr::mutate(data_col          = TRUE)

#Timeseries controls
t_units     = "months"
window_span = 3
step        = 3
t_start     = -3
t_end       = 18
search_meds = c("warfarin", "apixaban", "rivaroxaban", "edoxaban", "dabigatran", "aspirin", "clopidogrel", "prasugrel", "ticagrelor")

# Create the timeseries to plot
antiplatelets_timeseries <- create_medication_timeseries(cohort, meds, t_start, t_end, step, window_span, t_units, search_meds)
antiplatelets_timeseries <- antiplatelets_timeseries %>%
  dplyr::rowwise() %>%
  dplyr::mutate(num_antiplatelets  = sum(.data$aspirin,.data$clopidogrel, .data$prasugrel, .data$ticagrelor, na.rm=T),
                num_anticoagulants = sum(.data$warfarin,.data$apixaban,.data$rivaroxaban,.data$edoxaban,.data$dabigatran, na.rm=T),
                strategy           = dplyr::case_when(
                  .data$num_antiplatelets==1 & .data$num_anticoagulants >0 ~ "SAPT+AC",
                  .data$num_antiplatelets >1 & .data$num_anticoagulants >0 ~ "DAPT+AC",
                  .data$num_antiplatelets==0 & .data$num_anticoagulants >0 ~ "AC",
                  .data$num_antiplatelets==1 & .data$num_anticoagulants==0 ~ "SAPT",
                  .data$num_antiplatelets >1 & .data$num_anticoagulants==0 ~ "DAPT",
                  .data$num_antiplatelets==0 & .data$num_anticoagulants==0 &  .data$in_swd_date_range  ~ "None",
                  .data$num_antiplatelets==0 & .data$num_anticoagulants==0 & !.data$in_swd_date_range  ~ "Missing",
                  TRUE ~ "error")) %>%
  dplyr::ungroup() %>%
  dplyr::filter(.data$strategy != "Missing") %>%
  dplyr::mutate(strategy = factor(.data$strategy , levels=c("DAPT+AC", "SAPT+AC", "DAPT", "AC", "SAPT", "None", "Missing")))

# Plot a stacked colour chart
average_strategy <- antiplatelets_timeseries %>%
  dplyr::group_by(.data$time_step) %>%
  dplyr::mutate(total_n = dplyr::n()) %>%
  dplyr::ungroup() %>%
  dplyr::group_by(.data$time_step, .data$strategy) %>%
  dplyr::mutate(pct = dplyr::n() / .data$total_n) %>%
  dplyr::summarise(pct = mean(.data$pct))
ggplot(average_strategy, aes(x = time_step, y = pct, fill = strategy)) +
  geom_area()

# Plot a Sankeys chart
plot_sankey_timeseries(antiplatelets_timeseries %>% dplyr::select(.data$id, .data$time_step, group=.data$strategy),
                       plot_title = "Antiplatelet prescribing after ACS")

}



# Statins
{
  meds = acs_cohort[c("id", "gp_prescriptions")] %>%
    tidyr::unnest(cols = "gp_prescriptions") %>%
    dplyr::filter(!is.na(.data$medication_name)) %>%
    dplyr::select(id                = .data$id,
                  prescription_date = .data$episode_start_datetime,
                  medication_name   = .data$medication_name,
                  data_col          = .data$medication_dose)

  #Timeseries controls
  t_units     = "months"
  window_span = 3
  step        = 3
  t_start     = -3
  t_end       = 12
  search_meds = c("inclisiran", "ezetimibe", "atorvastatin", "rosuvastatin", "simvastatin", "fluvastatin", "pravastatin")

  # Create the timeseries to plot
  # Create the timeseries to plot
  lipid_medication_timeseries <- create_medication_timeseries(cohort_df=cohort, meds_df=meds, t_start, t_end, step, window_span, t_units, search_meds)
  lipid_medication_timeseries <- lipid_medication_timeseries %>%
    dplyr::rowwise() %>%
    dplyr::mutate(any_statin     = dplyr::if_else(all(is.na(c(.data$atorvastatin,
                                                              .data$rosuvastatin,
                                                              .data$simvastatin,
                                                              .data$pravastatin,
                                                              .data$fluvastatin))), FALSE, TRUE),
                  any_ezetimibe  = dplyr::if_else(is.na(.data$ezetimibe), FALSE, TRUE),
                  any_inclisiran = dplyr::if_else(is.na(.data$inclisiran), FALSE, TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(strategy = dplyr::case_when(
      # Missing data
      !.data$in_swd_date_range  ~ "Censored",
      # No lipid therapy
      !.data$any_ezetimibe & !.data$any_statin & !.data$any_inclisiran ~ "None",
      # Any inclisiran
      .data$any_inclisiran ~ "Inclisiran +/- other",
      # Only ezetimibe therapy
      .data$any_ezetimibe & !.data$any_statin ~ "Ezetimibe only",
      # High dose statin therapy with ezetimibe
      ((!is.na(.data$atorvastatin) & .data$atorvastatin >=40) |
         (!is.na(.data$rosuvastatin) & .data$rosuvastatin >=20) |
         (!is.na(.data$simvastatin)  & .data$simvastatin  >=80)) &
        .data$any_ezetimibe ~ "High dose statin + ezetimibe",
      # High dose statin therapy without ezetimibe - # high dose = atorva 40-80, rosuvastatin 20-40, simva 80
      ((!is.na(.data$atorvastatin) & .data$atorvastatin >=40) |
         (!is.na(.data$rosuvastatin) & .data$rosuvastatin >=20) |
         (!is.na(.data$simvastatin)  & .data$simvastatin  >=80)) &
        !.data$any_ezetimibe ~ "High dose statin",
      # Moderate dose statin therapy with ezetimibe
      ((!is.na(.data$atorvastatin) & .data$atorvastatin  <40) |
         (!is.na(.data$rosuvastatin) & .data$rosuvastatin  <20) |
         (!is.na(.data$fluvastatin)  & .data$fluvastatin   >40) |
         (!is.na(.data$pravastatin)  & .data$pravastatin  >=40) |
         (!is.na(.data$simvastatin)  & .data$simvastatin  >=20 & .data$simvastatin <80)) &
        .data$any_ezetimibe ~ "Moderate dose statin + ezetimibe",
      # Moderate dose statin therapy without ezetimibe - # mod dose = atorva 10-20, rosuvastatin 5-10, simva 20-40, prava 40-80, lovastat 40, fluva 80, pitva 2-4
      ((!is.na(.data$atorvastatin) & .data$atorvastatin  <40) |
         (!is.na(.data$rosuvastatin) & .data$rosuvastatin  <20) |
         (!is.na(.data$fluvastatin)  & .data$fluvastatin   >40) |
         (!is.na(.data$pravastatin)  & .data$pravastatin  >=40) |
         (!is.na(.data$simvastatin)  & .data$simvastatin  >=20 & .data$simvastatin <80)) &
        !.data$any_ezetimibe ~ "Moderate dose statin",
      # Low dose statin therapy with ezetimibe
      ((!is.na(.data$simvastatin) & .data$simvastatin  <20) |
         (!is.na(.data$pravastatin) & .data$pravastatin  <40) |
         (!is.na(.data$fluvastatin) & .data$fluvastatin <=40)) &
        .data$any_ezetimibe ~ "Low dose statin + ezetimibe",
      # Low dose statin therapy without ezetimibe - # low dose = simva 10, prava 10-20, lova 20, fluva 20-40, pitva 1            https://www.google.com/url?sa=i&url=https%3A%2F%2Fwww.uspharmacist.com%2Farticle%2Flipidlowering-therapies-a-review-of-current-and-future-options&psig=AOvVaw1oUjYp0LCmvmyKdwPQM3UM&ust=1649363828741000&source=images&cd=vfe&ved=0CAoQjRxqFwoTCMDY-pqlgPcCFQAAAAAdAAAAABAD
      ((!is.na(.data$simvastatin) & .data$simvastatin  <20) |
         (!is.na(.data$pravastatin) & .data$pravastatin  <40) |
         (!is.na(.data$fluvastatin) & .data$fluvastatin <=40)) &
        !.data$any_ezetimibe ~ "Low dose statin",
      # Something went wrong
      TRUE ~ "error")) %>%
    dplyr::filter(.data$strategy != "Censored") %>%
    dplyr::mutate(strategy = factor(.data$strategy , levels=c("Inclisiran +/- other",
                                                              "High dose statin + ezetimibe",
                                                              "High dose statin",
                                                              "Moderate dose statin + ezetimibe",
                                                              "Moderate dose statin",
                                                              "Low dose statin + ezetimibe",
                                                              "Low dose statin",
                                                              "Ezetimibe only",
                                                              "None",
                                                              "Censored")))

  average_lipid_strategy <- lipid_medication_timeseries %>%
    dplyr::group_by(.data$time_step) %>%
    #got to be careful here to exclude the patients who have reached the end of the time window or already died
    dplyr::filter(.data$in_swd_date_range) %>% # the patient has not reached the end of the study time window
    dplyr::mutate(total_n = dplyr::n()) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(.data$time_step, .data$strategy) %>%
    dplyr::mutate(pct = dplyr::n() / .data$total_n) %>%
    dplyr::summarise(pct = mean(.data$pct))

  # Plot a stacked colour chart
  ggplot(average_lipid_strategy, aes(x = time_step, y = pct, fill = strategy)) +
    geom_area()

  # Plot a Sankeys chart
  plot_sankey_timeseries(lipid_medication_timeseries %>% dplyr::select(.data$id, .data$time_step, group=.data$strategy),
                         plot_title = "Statin prescribing after ACS")



}

