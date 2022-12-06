# Deal with the questions from the ICB meeting on this ACS work
library(devtools)
library(icdb)
library(dplyr)
library(tidyr)
library(ggplot2)
library(viridis)
library(bigsnpr)
library(kableExtra)
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
  select(nhs_number, attribute_period, sex, ethnicity, lsoa) %>%
  filter(nhs_number %in% !!dat_sus$nhs_number) %>%
  run() %>%
  group_by(nhs_number) %>%
  mutate(swd_min = min(attribute_period, na.rm=T),
         swd_max = max(attribute_period, na.rm=T)) %>%
  arrange(attribute_period, .by_group=T) %>%
  fill(c(sex, ethnicity, lsoa), .direction="downup") %>%
  slice_max(attribute_period, with_ties = FALSE) %>%
  ungroup() %>%
  select(nhs_number, sex, ethnicity, lsoa, swd_min, swd_max)

# LSOA : MSOA mapping
dat_msoa <- svr$MODELLING_SQL_AREA$swd_LSOA_descriptions %>%
  select(lsoa = `LSOA code`,
         lsoa_name = `LSOA name`) %>%
  run() %>%
  mutate(msoa = substr(.data$lsoa_name, 1, nchar(.data$lsoa_name)-1)) %>%
  select(lsoa, msoa)

# Cambridge score
dat_camb <- svr$MODELLING_SQL_AREA$New_Cambridge_Score %>%
  select(nhs_number, attribute_period, segment) %>%
  filter(nhs_number %in% !!dat_sus$nhs_number) %>%
  group_by(nhs_number) %>%
  slice_max(attribute_period) %>%
  dplyr::select(nhs_number, segment) %>%
  icdb::run()

# IMD score
dat_imd <- svr$Analyst_SQL_Area$tbl_BNSSG_Datasets_LSOA_IMD_2019 %>%
  select(imd = `Index of Multiple Deprivation (IMD) Decile`,
         lsoa = `LSOA Code`) %>%
  icdb::run()

# Create the cohort
ethnicity_codes <- get_codes(file_path = system.file("ethnicity_codes/ethnicity_codes.csv", package = "acsprojectns"))
sex_codes       <- get_codes(file_path = system.file("sex_codes/sex_codes.csv", package = "acsprojectns"))
cohort <- dat_sus %>%
  left_join(dat_pcn,  by="prac_code") %>%
  left_join(dat_attr, by="nhs_number") %>%
  left_join(dat_imd,  by="lsoa") %>%
  left_join(dat_msoa, by="lsoa") %>%
  left_join(dat_camb, by="nhs_number") %>%
  mutate(sex_code       = do.call(dplyr::coalesce, across(contains("sex"))),
         ethnicity_code = do.call(dplyr::coalesce, across(contains("ethnicity")))) %>%
  left_join(sex_codes, by="sex_code") %>%
  left_join(ethnicity_codes, by="ethnicity_code") %>%
  select(nhs_number, age, sex, ethnicity, ethnicity_group, lsoa, msoa, prac_code, pcn, imd, segment, epi_start, epi_end, primary_diagnosis, swd_min, swd_max)

# The prescriptions
med_filter_string <- "(?i)([A-z ]+)(\\d+\\.?\\d*)?([A-z]+)?[ ]*(?:[A-z]*)"
search_meds       <- c("inclisiran", "ezetimibe", "atorvastatin", "rosuvastatin", "simvastatin", "fluvastatin", "pravastatin")
med_end           <- as.Date(end) + lubridate::duration(1, units="years")
dat_meds <- svr$MODELLING_SQL_AREA$swd_activity %>%
  select(nhs_number, arr_date, dep_date, pod_l1, spec_l1b) %>%
  filter(nhs_number %in% !!dat_sus$nhs_number,
         arr_date > start,
         dep_date < med_end,
         pod_l1 == "primary_care_prescription") %>%
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
                                 TRUE                                          ~ "Low or Medium Intensity")) %>%
  select(nhs_number, prescription_date = arr_date, med_name, med_dose, med_units, lipid_strat)

#############################################
## ANALYSIS
strategy_over_time <- purrr::map_df(.x = seq(3,12,3),
                                    .f = function(win, win_unit="months", dat_sus, dat_attr, dat_meds){

  cohort_with_swd_coverage <- cohort %>%
    # filter out people that do not have swd data covering the period of interest
    filter(if_any(c(swd_min, swd_max, epi_start, epi_end), ~!is.na(.x)),
           lubridate::interval(epi_end, epi_end+lubridate::duration(win,win_unit)) %within% lubridate::interval(swd_min, swd_max))


  avg_med_strat_in_window <- cohort %>%
    # Join the medication data
    left_join(dat_meds, by="nhs_number") %>%
    # Filter out the prescriptions that are out of range
    filter(prescription_date %within% lubridate::interval(epi_end, epi_end+lubridate::duration(win,win_unit))) %>%
    # Group by each patient
    group_by(nhs_number) %>%
    # Get the lipid strategy for this time period for each patient
    summarise(strat_mode = tail(names(sort(table(lipid_strat))), 1)) %>%
    ungroup()


  window_cohort_med_strat <- cohort_with_swd_coverage %>%
    # Join in the medication strategy to the cohort
    left_join(avg_med_strat_in_window, by="nhs_number") %>%
    # Add the time period and replace the NAs with 'None'
    mutate(period     = as.factor(paste0(win, "_", win_unit)),
           pcn        = as.factor(pcn),
           prac_code  = as.factor(prac_code),
           strat_mode = replace_na(strat_mode, "None"),
           strat_mode = factor(strat_mode, levels=c("None","Low or Medium Intensity","High intensity")))

  return(window_cohort_med_strat)

}, dat_sus=dat_sus, dat_attr=dat_attr, dat_meds=dat_meds)

#############################################
## ANALYSIS
strategy_over_time %>%
  filter(period=="3_months") %>%
  group_by(strat_mode) %>%
  summarise(n=n(),
            sex_male  = sum(sex=="male", na.rm=T)/n,
            age       = mean(age, na.rm=T),
            eth_white = sum(ethnicity_group=="White", na.rm=T)/n,
            eth_black = sum(ethnicity_group=="Black", na.rm=T)/n,
            eth_asian = sum(ethnicity_group=="Asian", na.rm=T)/n,
            pct_mixed = sum(ethnicity_group=="Mixed", na.rm=T)/n,
            pct_other = sum(ethnicity_group=="Other", na.rm=T)/n,
            pct_unkwn = sum(is.na(ethnicity_group))/n,
            imd       = mean(imd, na.rm=T),
            segment   = mean(segment, na.rm=T)) %>%
  kbl() %>%
  kable_minimal()

# Bar chart of average strategy use over time
strategy_over_time %>%
  group_by(period) %>%
  mutate(n_period=n()) %>%
  group_by(period, strat_mode) %>%
  summarise(pct = n()/n_period[[1]]) %>%
  ggplot(aes(x=forcats::fct_rev(period), y=pct, fill=forcats::fct_rev(strat_mode))) +
  geom_col() +
  coord_flip()

# Bar chart of average strategy per PCN
strategy_over_time %>%
  group_by(period, pcn) %>%
  mutate(n_per_group=n()) %>%
  group_by(period, pcn, strat_mode) %>%
  summarise(pct = n()/n_per_group[[1]], n=n_per_group[[1]]) %>%
  mutate(pcn = factor(pcn, levels=filter(.,period=="3_months",strat_mode=="None") %>% arrange(pct) %>% pull(pcn) %>% as.character(.))) %>%
  filter(!is.na(pcn)) %>%
  ggplot(aes(x=pcn, y=pct, fill=forcats::fct_rev(strat_mode))) +
  geom_col() +
  geom_text(data = . %>% group_by(pcn) %>% slice(n=1) %>% select(n, strat_mode),
            aes(y=1, label=n), size=2, hjust = 1) +
  labs(title = "Prescribed lipid lowering therapy post heart attack",
       subtitle = "Any single prescription within timeframe; (n=number of PCN heart attack patients)",
       fill="Lipid lowering therapy",
       x = "Primary Care Network",
       y = "Percentage") +
  coord_flip() +
  facet_wrap(~period, nrow=1)

# Bar chart of average strategy per practice
strategy_over_time %>%
  group_by(period, prac_code) %>%
  mutate(n_per_group=n()) %>%
  group_by(period, prac_code, strat_mode) %>%
  summarise(pct = n()/n_per_group[[1]], n=n_per_group[[1]]) %>%
  mutate(prac_code = factor(prac_code, levels=filter(.,period=="3_months",strat_mode=="None") %>% arrange(pct) %>% pull(prac_code) %>% as.character(.))) %>%
  filter(!is.na(prac_code)) %>%
  ggplot(aes(x=prac_code, y=pct, fill=forcats::fct_rev(strat_mode))) +
  geom_col() +
  geom_text(data = . %>% group_by(prac_code) %>% slice(n=1) %>% select(n, strat_mode),
            aes(y=1, label=n), size=2, hjust = 1) +
  coord_flip() +
  labs(title = "Prescribed lipid lowering therapy post heart attack",
       subtitle = "Cohort July 2018 - Dec 2021",
       fill="Lipid lowering therapy",
       xlab = "Percentage",
       ylab= "GP practice")
  facet_wrap(~period, nrow=1)

# Maps
ggplot() +
  geom_polygon(data    = gen_msoa_geo_data(dat=strategy_over_time %>%
                                             filter(period=="3_months") %>%
                                             group_by(msoa) %>%
                                             mutate(n=n()) %>%
                                             group_by(msoa, strat_mode) %>%
                                             filter(strat_mode=="None") %>%
                                             summarise(values=n()/n[[1]]) %>%
                                             select(msoa, values)),
               mapping = aes(x=long, y=lat, group=group, fill=values), alpha=0.9, colour="white", linewidth=0.1) +
  theme_void() +
  coord_map() +
  labs(title = "Percetage of heart attack patients not prescribed lipid lowering\ntherapy at 3 months post event",
       subtitle = "BNSSG middle layer super output areas (July 2018 - Dec 2021)",
       fill="Percentage") +
  scale_color_discrete(guide = "none") +
  scale_fill_viridis_c(option = "plasma", begin = 0.0, end=0.99) +
  theme(plot.title      = element_text(size= 16, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.40, l = 2, unit = "cm")),
        plot.subtitle   = element_text(size= 12, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.43, l = 2, unit = "cm")),
        legend.position = c(0.20, 0.65)
  )







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

