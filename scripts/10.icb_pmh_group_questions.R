# Deal with the questions from the ICB meeting on this ACS work
library(usethis)
library(devtools)
library(icdb)
library(dplyr)
library(ggplot2)
rm(list=ls())
load_all()

# Connect to the server
svr <- Databases(config=system.file("mysql.json", package="acsprojectns"))

# Set some parameters for the cohort
cohort_start <- as.POSIXct("2018-07-01", tz = "GMT")
cohort_end <- as.POSIXct("2021-12-31", tz = "GMT")
acs_codes <- acsprojectns::get_codes(file_path = system.file("icd10_codes/acs_codes.csv", package = "acsprojectns"), append_character = "-")
bnssg_ccg_codes <- acsprojectns::get_codes(file_path = system.file("ccg_provider_codes/bnssg_ccg_provider_codes.csv", package = "acsprojectns"))
admission_method_codes <- acsprojectns::get_codes(file_path = system.file("admission_method_codes/admission_method_codes.csv", package = "acsprojectns"))
emergency_codes <- admission_method_codes$admission_method_code[grep("emergency", admission_method_codes$admission_method_description)]

# Get the cohort of patients
cohort <- svr$ABI$vw_APC_SEM_001() %>%
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



# What proportion of heart attack patients in this cohort are current smokers?
smokers <- svr$MODELLING_SQL_AREA$primary_care_attributes() %>%
  select(nhs_number, attribute_period, smoking) %>%
  filter(nhs_number %in% !!cohort$AIMTC_Pseudo_NHS) %>%
  show_query() %>%
  run()

# Does deprivation impact outcomes and costs?
imd_centile


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
