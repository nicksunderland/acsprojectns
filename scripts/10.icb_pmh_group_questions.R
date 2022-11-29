# Deal with the questions from the ICB meeting on this ACS work
library(devtools)
library(icdb)
library(dplyr)
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

# Get the first ACS episode within the time window
sus <- svr$ABI$vw_APC_SEM_001 %>%
  select(AIMTC_Pseudo_NHS,
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
  group_by(AIMTC_Pseudo_NHS) %>%
  slice_min(AIMTC_ProviderSpell_Start_Date, with_ties = FALSE) %>%
  ungroup() %>%
  show_query() %>%
  run() %>%
  select(nhs_number = AIMTC_Pseudo_NHS,
         prac_code  = AIMTC_PracticeCodeOfRegisteredGP,
         epi_start  = AIMTC_ProviderSpell_Start_Date,
         epi_end    = AIMTC_ProviderSpell_End_Date,
         primary_diagnosis = DiagnosisPrimary_ICD) %>%
  mutate(primary_diagnosis = drop_detail(.data$primary_diagnosis, level=2))

# The SWD date range
in_swd <- svr$MODELLING_SQL_AREA$primary_care_attributes %>%
  select(nhs_number, attribute_period) %>%
  filter(nhs_number %in% !!sus$nhs_number) %>%
  run() %>%
  group_by(nhs_number) %>%
  mutate(swd_min = min(attribute_period, na.rm=T),
         swd_max = max(attribute_period, na.rm=T)) %>%
  select(nhs_number, swd_min, swd_max) %>%
  slice(n=1) %>%
  ungroup()

# Flag people who don't have SWD data
cohort <- sus %>%
  left_join(in_swd, by="nhs_number") %>%
  mutate(in_swd = if_else(!is.na(swd_min),
                          lubridate::int_overlaps(lubridate::interval(epi_start, epi_end+lubridate::duration(1,"years")),
                                                  lubridate::interval(swd_min, swd_max)),
                          FALSE))
cat("Number ACS patients: ", as.character(nrow(cohort)))
cat("Number with SWD data:", as.character(sum(cohort$in_swd)), ",", as.character(round(sum(cohort$in_swd)*100/nrow(cohort)),digits=0), "%")

# The PCN for each practice
prac_pcn <- svr$MODELLING_SQL_AREA$swd_attribute %>%
  distinct(practice_code, primary_care_network) %>%
  mutate(practice_code = toupper(practice_code)) %>%
  select(pcn=primary_care_network, prac_code=practice_code) %>%
  run()

# Add PCN to cohort
cohort <- cohort %>%
  left_join(prac_pcn, by="prac_code") %>%
  relocate(pcn, .after=prac_code) %>%
  mutate(across(c(prac_code, pcn, primary_diagnosis), ~as.factor(.x)))

# Look at who has SWD data by practice
ggplot(data    = cohort %>% group_by(pcn) %>% summarise(n=n(), pct_swd=sum(in_swd)/n()) %>% ungroup(),
       mapping = aes(x=forcats::fct_reorder(pcn, pct_swd, .desc=T), y=pct_swd)) +
  geom_bar(stat="identity", fill="lightblue") +
  geom_text(aes(label=n), vjust=0.4, hjust=2) +
  coord_flip() +
  ylab('Percent of ACS patients with follow up SWD data') +
  scale_y_continuous(labels = scales::percent)

# Remove those without SWD data
cohort <- cohort %>% filter(in_swd)

# The prescriptions
med_filter_string <- "(?i)([A-z ]+)(\\d+\\.?\\d*)?([A-z]+)?[ ]*(?:[A-z]*)"
search_meds = c("inclisiran", "ezetimibe", "atorvastatin", "rosuvastatin", "simvastatin", "fluvastatin", "pravastatin")
med_end = as.Date(end) + lubridate::duration(1, units="years")
meds <- svr$MODELLING_SQL_AREA$swd_activity %>%
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
  select(nhs_number,
         prescription_date = arr_date,
         med_name,
         med_dose,
         med_units)

# Filter the meds to within 1 year of ACS
meds <- sus %>%
  left_join(meds, by="nhs_number") %>%
  filter(prescription_date > epi_start,
         prescription_date < epi_end + lubridate::duration(1, "years")) %>%
  select(nhs_number, prescription_date, med_name, med_dose, med_units)

# Percentage of people with lipid therapy after ACS
cohort_meds <- cohort %>%
  left_join(meds, by="nhs_number") %>%
  dplyr::mutate(strategy = dplyr::case_when(
    is.na(med_name)                               ~ "None",
    med_name=="inclisiran"                        ~ "Inclisiran",
    (med_name=="atorvastatin" & med_dose >=40) |
    (med_name=="rosuvastatin" & med_dose >=20) |
    (med_name=="simvastatin"  & med_dose >=80)    ~ "High intensity",
    TRUE                                          ~ "Low or Medium Intensity")) %>%
  group_by(nhs_number) %>%
  mutate(strategy_mode  = tail(names(sort(table(strategy))), 1),
         strategy_mode = factor(strategy_mode, levels=c("None","Low or Medium Intensity","High intensity","Inclisiran")),
         num_s = n(),
         num_scripts = case_when(all(is.na(med_name))~"0",
                                 num_s<3  ~ "1-2",
                                 num_s<7  ~ "3-6",
                                 num_s<13 ~ "7-12",
                                 num_s<25 ~ "13-24",
                                 num_s>=25~ "25+"),
         num_scripts = factor(num_scripts, levels=c("0","1-2","3-6","7-12","13-24","25+"))) %>%
  slice(n=1) %>%
  select(-strategy) %>%
  group_by(pcn) %>%
  mutate(num_none = sum(strategy_mode=="None")) %>%
  ungroup()

# Pie chart of strategies
ggplot(cohort_meds, aes(x=factor(1), fill=strategy_mode))+
  geom_bar(width = 1) +
  coord_polar("y")

# Plot the % people with at least one prescription over the year
ggplot(data    = cohort_meds,
       mapping = aes(x=pcn, fill=forcats::fct_rev(strategy_mode))) +
  geom_bar(position="fill") +
  coord_flip() +
  ylab('Percent of ACS patients with at least one prescription')

# Plot the % people with at least 6 prescriptions over the year
ggplot(data    = cohort_meds,
       mapping = aes(x=pcn, fill=forcats::fct_rev(num_scripts))) +
  geom_bar(position="fill") +
  coord_flip() +
  ylab('Percent of ACS patients with different number of prescriptions')










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
