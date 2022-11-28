# Deal with the questions from the ICB meeting on this ACS work
library(devtools)
library(icdb)
library(dplyr)
library(ggplot2)
rm(list=ls())
load_all()

# Connect to the server
svr <- icdb::server("XSW")

# Set some parameters for the cohort
cohort_start     <- as.POSIXct("2018-07-01", tz = "GMT")
cohort_end       <- as.POSIXct("2021-12-31", tz = "GMT")
acs_codes        <- acsprojectns::get_codes(file_path = system.file("icd10_codes/acs_codes.csv", package = "acsprojectns"), append_character = "-")
bnssg_ccg_codes  <- acsprojectns::get_codes(file_path = system.file("ccg_provider_codes/bnssg_ccg_provider_codes.csv", package = "acsprojectns"))
adm_method_codes <- acsprojectns::get_codes(file_path = system.file("admission_method_codes/admission_method_codes.csv", package = "acsprojectns"))
emergency_codes  <- adm_method_codes$admission_method_code[grep("emergency", adm_method_codes$admission_method_description)]

# Get the cohort of patients
sus <- svr$ABI$vw_APC_SEM_001 %>%
  select(AIMTC_Pseudo_NHS,
         AIMTC_ProviderSpell_Start_Date,
         AIMTC_ProviderSpell_End_Date,
         AdmissionMethod_HospitalProviderSpell,
         AIMTC_PCTRegGP,
         matches("Diagnosis[0-9]*[rdths]*(Primary|Secondary)_ICD")) %>%
  filter(AIMTC_ProviderSpell_Start_Date >= cohort_start,
         AIMTC_ProviderSpell_Start_Date <= cohort_end,
         AdmissionMethod_HospitalProviderSpell %in% !!emergency_codes,
         AIMTC_PCTRegGP %in% !!bnssg_ccg_codes$ccg_provider_code,
         if_any(matches("Diagnosis[0-9]*[rdths]*(Primary)_ICD"), ~ .x %in% !!acs_codes$icd_codes)) %>%
  group_by(AIMTC_Pseudo_NHS) %>%
  slice_min(AIMTC_ProviderSpell_Start_Date, with_ties=F) %>%
  show_query() %>%
  run()


# The PCN for each patient
swd_att <- svr$MODELLING_ANALYTICS$swd_attributes_history %>%
  select(nhs_number, attribute_period, primary_care_network, locality_name, lsoa) %>%
  filter(nhs_number %in% !!cohort$nhs_number) %>%
  group_by(nhs_number) %>%
  mutate(swd_min = min(.data$attribute_period, na.rm=T),
         swd_max = max(.data$attribute_period, na.rm=T)) %>%
  ungroup() %>%
  run()


# Statin prescriptions
swd_drugs <- svr$MODELLING_ANALYTICS$swd_activity %>%
  select(nhs_number, pod_l1) %>%
  filter(nhs_number %in% !!cohort$nhs_number,
         pod_l1 == "primary_care_prescription") %>%
  run() %>%


  search_meds = c("inclisiran", "ezetimibe", "atorvastatin", "rosuvastatin", "simvastatin", "fluvastatin", "pravastatin")

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
