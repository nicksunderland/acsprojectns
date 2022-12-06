# Deal with the questions from the ICB meeting on this ACS work
library(devtools)
library(icdb)
library(dplyr)
library(tidyr)
library(ggplot2)
rm(list=ls())
load_all()

# Connect to the server
svr <- icdb::server("XSW")

# Some cohort parameters
start <- "2018-07-01"
end   <- "2021-12-31"
adm   <- acsprojectns::get_codes(file_path = system.file("admission_method_codes/admission_method_codes.csv", package = "acsprojectns"))
ccg   <- acsprojectns::get_codes(file_path = system.file("ccg_provider_codes/bnssg_ccg_provider_codes.csv", package = "acsprojectns"))

# The PCN for each practice
dat_pcn <- svr$MODELLING_SQL_AREA$swd_attribute %>%
  distinct(practice_code, primary_care_network) %>%
  mutate(practice_code = toupper(practice_code)) %>%
  select(pcn=primary_care_network, prac_code=practice_code) %>%
  run() %>%
  mutate(pcn = as.factor(pcn))

# Get the first ACS episode within the time window
dat_sus <- svr$ABI$vw_APC_SEM_001 %>%
  select(AIMTC_Pseudo_NHS,
         AIMTC_Age,
         Sex,
         EthnicGroup,
         AIMTC_PCTRegGP,
         AIMTC_PracticeCodeOfRegisteredGP,
         AdmissionMethod_HospitalProviderSpell,
         AIMTC_ProviderSpell_Start_Date,
         AIMTC_ProviderSpell_End_Date,
         DiagnosisPrimary_ICD) %>%
  filter(AIMTC_ProviderSpell_Start_Date > start,
         AIMTC_ProviderSpell_End_Date   < end,
         AdmissionMethod_HospitalProviderSpell %in% !!adm$admission_method_code[grep("emergency", adm$admission_method_description)],
         AIMTC_PCTRegGP %in% !!ccg$ccg_provider_code) %>%
  codes_from("icd10/acs.yaml", DiagnosisPrimary_ICD) %>%
  run() %>%
  group_by(AIMTC_Pseudo_NHS) %>%
  arrange(AIMTC_ProviderSpell_Start_Date, .by_group=T) %>%
  fill(c(AIMTC_Age, Sex, EthnicGroup), .direction="downup") %>%
  slice_min(AIMTC_ProviderSpell_Start_Date, with_ties = FALSE) %>%
  ungroup() %>%
  mutate(DiagnosisPrimary_ICD = drop_detail(.data$DiagnosisPrimary_ICD, level=2)) %>%
  select(nhs_number = AIMTC_Pseudo_NHS,
         age        = AIMTC_Age,
         sex        = Sex,
         ethnicity  = EthnicGroup,
         prac_code  = AIMTC_PracticeCodeOfRegisteredGP,
         epi_start  = AIMTC_ProviderSpell_Start_Date,
         epi_end    = AIMTC_ProviderSpell_End_Date,
         primary_diagnosis = DiagnosisPrimary_ICD)

# The SWD date range
dat_attr <- svr$MODELLING_SQL_AREA$primary_care_attributes %>%
  select(nhs_number, attribute_period, sex, ethnicity) %>%
  filter(nhs_number %in% !!sus$nhs_number) %>%
  run() %>%
  group_by(nhs_number) %>%
  mutate(swd_min = min(attribute_period, na.rm=T),
         swd_max = max(attribute_period, na.rm=T)) %>%
  arrange(attribute_period, .by_group=T) %>%
  fill(c(sex, ethnicity), .direction="downup") %>%
  slice_max(attribute_period, with_ties = FALSE) %>%
  ungroup() %>%
  select(nhs_number, sex, ethnicity, swd_min, swd_max)

# Create the cohort
ethnicity_codes <- get_codes(file_path = system.file("ethnicity_codes/ethnicity_codes.csv", package = "acsprojectns"))
sex_codes       <- get_codes(file_path = system.file("sex_codes/sex_codes.csv", package = "acsprojectns"))
cohort <- dat_sus %>%
  left_join(dat_pcn, by="prac_code") %>%
  left_join(dat_attr, by="nhs_number") %>%
  mutate(sex_code       = do.call(dplyr::coalesce, across(contains("sex"))),
         ethnicity_code = do.call(dplyr::coalesce, across(contains("ethnicity")))) %>%
  left_join(sex_codes, by="sex_code") %>%
  left_join(ethnicity_codes, by="ethnicity_code") %>%
  select(nhs_number, age, sex, ethnicity, ethnicity_group, prac_code, epi_start, epi_end, primary_diagnosis, swd_min, swd_max)

# The prescriptions
med_filter_string <- "(?i)([A-z ]+)(\\d+\\.?\\d*)?([A-z]+)?[ ]*(?:[A-z]*)"
search_meds       <- c("inclisiran", "ezetimibe", "atorvastatin", "rosuvastatin", "simvastatin", "fluvastatin", "pravastatin")
med_end           <- as.Date(end) + lubridate::duration(1, units="years")
dat_meds <- svr$MODELLING_SQL_AREA$swd_activity %>%
  select(nhs_number, arr_date, dep_date, pod_l1, spec_l1b) %>%
  filter(nhs_number %in% !!sus$nhs_number,
         arr_date > start,
         dep_date < med_end,
         pod_l1 == "primary_care_prescription") %>%
  show_query() %>%
  run() %>%
  filter(grepl(paste0(search_meds, collapse = "|"), spec_l1b, ignore.case=T)) %>%
  dplyr::mutate(med_name  = trimws(stringr::str_match(spec_l1b, pattern = med_filter_string)[,2]),
                med_dose  = stringr::str_match(spec_l1b, pattern = med_filter_string)[,3],
                med_units = stringr::str_match(spec_l1b, pattern = med_filter_string)[,4]) %>%
  mutate(lipid_strat = case_when(is.na(med_name)                               ~ "None",
                                 med_name=="inclisiran" |
                                 (med_name=="atorvastatin" & med_dose >=40) |
                                 (med_name=="rosuvastatin" & med_dose >=20) |
                                 (med_name=="simvastatin"  & med_dose >=80)    ~ "High intensity",
                                 TRUE                                          ~ "Low or Medium Intensity"),
         lipid_strat = factor(lipid_strat, levels=c("None","Low or Medium Intensity","High intensity"))) %>%
  select(nhs_number, prescription_date = arr_date, med_name, med_dose, med_units)

#############################################
## ANALYSIS
strategy_over_time <- purrr::map_df(.x = 1:12,
                                    .f = function(win, win_unit="months", dat_sus, dat_attr, dat_meds){

  cohort_with_swd_coverage <- cohort %>%
    # filter out people that do not have swd data covering the period of interest
    filter(across(c(swd_min, swd_max, epi_start, epi_end), ~!is.na(.x)),
           lubridate::interval(epi_end, epi_end+lubridate::duration(win,win_unit)) %within% lubridate::interval(swd_min, swd_max))


  avg_med_strat_in_window <- cohort %>%
    # Join the medication data
    left_join(dat_meds, by="nhs_number") %>%
    # Filter out the prescriptions that are out of range
    filter(prescription_date %within% lubridate::interval(epi_end, epi_end+lubridate::duration(win,win_unit))) %>%
    # Group by each patient
    group_by(nhs_number) %>%
    # Get the lipid strategy for this time period for each patient
    summarise(strat_mode = tail(names(sort(table(strategy))), 1))


  window_cohort_med_strat <- cohort_with_swd_coverage %>%
    # Join in the medication strategy to the cohort
    left_join(avg_med_strat_in_window, by="nhs_number") %>%
    # Add the time period
    mutate(period = paste0(x, "_", win_unit))

  return(window_cohort_med_strat)

}, dat_sus=dat_sus, dat_attr=dat_attr, dat_meds=dat_meds)


# Bar chart of average strategy use over time
strategy_over_time %>%
  group_by(period) %>%
  mutate(n_period=n()) %>%
  group_by(period, strat_mode) %>%
  summarise(pct = n()/n_period) %>%
  ggplot(aes(x=period, y=pct, fill=strat_mode)) +
  geom_col()

# Bar chart of average strategy per PCN
strategy_over_time %>%
  group_by(period, pcn) %>%
  mutate(n_per_pcn=n()) %>%
  group_by(period, pcn, strat_mode) %>%
  summarise(pct = n()/n_per_pcn) %>%
  ggplot(aes(x=forcats::fct_reorder(pcn, pct, .desc=T), y=pct, fill=strat_mode)) +
  geom_col() +
  coord_flip() +
  facet_wrap(~period)










# Target graphs
# Number (and %) of ACS patients per PCN
# Number (and %) of ACS patients on statins in year after ACS

#
#
# # What proportion of heart attack patients in this cohort are current smokers?
# smokers <- svr$MODELLING_SQL_AREA$primary_care_attributes() %>%
#   select(nhs_number, attribute_period, smoking) %>%
#   filter(nhs_number %in% !!cohort$AIMTC_Pseudo_NHS) %>%
#   show_query() %>%
#   run()
#
# # Does deprivation impact outcomes and costs?
# imd_centile
#

# Are outcomes different in areas with higher adjusted event rates (e.g. Weston)?


#   Is the pattern of spend that is observed in the follow up costs (e.g. 4/12% patients = 25/50% costs) also observed at the index event?
#   Do people with high index costs also have high follow up costs?
#   What are the most frequent diagnoses for the subsequent hospital admissions following a heart attack admission?
#   Regarding secondary prevention uptake and outcomes:
#   How many people attend a cardiac rehabilitation programme?
#   Who attends cardiac rehabilitation programmes and who does not?
#   How do outcomes differ between those patients that attended a cardiac rehabilitation programme and those that did not?
#   How do patients that do and do not have follow up checks (BP, HbA1c, cholesterol) differ in terms of the following [measures].
# How do patients that do and do not receive high-intensity lipid lowering therapy differ in terms of the following [measures].
# Measures:
#   • MSOA age-adjusted ACS rate
# • Age
# • Sex
# • Ethnicity
# • English as first spoken language
# • Deprivation
# • Cambridge score




# Look at who has SWD data by practice
ggplot(data    = cohort %>% group_by(pcn) %>% summarise(n=n(), pct_swd=sum(in_swd)/n()) %>% ungroup(),
       mapping = aes(x=forcats::fct_reorder(pcn, pct_swd, .desc=T), y=pct_swd)) +
  geom_bar(stat="identity", fill="lightblue") +
  geom_text(aes(label=n), vjust=0.4, hjust=2) +
  coord_flip() +
  ylab('Percent of ACS patients with follow up SWD data') +
  scale_y_continuous(labels = scales::percent)

