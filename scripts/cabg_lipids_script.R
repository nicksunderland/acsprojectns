# Lipid control post-coronary artery bypass surgery
# Auditing secondary prevention in the CABG cohort regardless of acuity.

# required external packages
library(devtools)
library(dplyr)
library(tidyr)
library(purrr)
library(lubridate)
load_all()
library(icdb)
icdb::use_cache(TRUE)

####################################
# Aim 1 - identify the CABG cohort #
####################################

# Aim 2 - describe who gets post CABG lipid profiles
# if granular enough by controlled and uncontrolled
# how many get lipids done within 1 year or >3months of discharge
# (exclusion of previous lipid profile done and already controlled)

# Aim 3 - describe who gets lipid lower therapy post CABG
# within 1-2 yrs
# evidence of uptitration?

# Aim 4 - describe outcomes
# mortality, stroke, AF
# split by controlled or uncontrolled

# Aim 5 - costs
# controlled vs uncontrolled lipids


# server setup
if(.Platform$OS.type == "unix") {
  msrv <- icdb::mapped_server(config  = system.file("database_connections", "home", "mysql.yaml", package="acsprojectns"),
                              mapping = system.file("database_connections", "home", "mapping.yaml", package="acsprojectns"))
}else if (.Platform$OS.type == "windows") {
  msrv <- icdb::mapped_server("XSW",
                              mapping = system.file("database_connections", "work", "mapping.yaml", package="acsprojectns"))
}

# the date we started collecting cholesterol data in the SWD
start <- as.Date("2022-01-01")

# today's date; maximize cohort size
end   <- Sys.Date()

# OPCS code for K40-K48 (any CABG) then exclude (K01-K38) and K52- K78 (contemporaneously)
cabg_opcs_codes = c("K401","K402","K403","K404","K408","K409",
                    "K411","K412","K413","K414","K418","K419",
                    "K431","K432","K433","K434","K438","K439",
                    "K441","K442","K448","K449",
                    "K451","K452","K453","K454","K455","K456","K457","K458",
                    "K461","K462","K463","K464","K465","K468","K469",                 "K631")


## Identify the pseudoNHS IDs of people who have had a CABG
cohort_ids <- msrv$sus$apc_spells_procedures |>
  # Time window
  filter(spell_start >= start & spell_end <= end) |>
  # Only bnssg patients - have to have appeared in the SWD attributes history table
  inner_join(msrv$swd$attr_h |> distinct(nhs_number), by="nhs_number") |>
  # Collect to local
  icdb::run() |>
  # shouldn't need to do this at the ccg
  dplyr::mutate(nhs_number = as.character(nhs_number)) |>
  # CABG code exists
  filter(if_any(matches("procedure_[0-9]+_opcs"), ~ . %in% cabg_opcs_codes)) |>
  # Distinct entries
  dplyr::distinct(nhs_number) |>
  # As a plain character vector
  pull() |> as.character()


## The hospital spell
dat_index_spell <- msrv$sus$apc_spells_procedures |>
  # Time window
  filter(spell_start >= start & spell_end <= end) %>%
  # Only cohort NHS numbers
  {if(.Platform$OS.type == "windows") {
    right_join(., data.frame("nhs_number" = cohort_ids), by="nhs_number", copy = TRUE)
  } else {
    filter(., nhs_number %in% cohort_ids)
  }} |>
  # Collect to local
  icdb::run() |>
  # shouldn't need to do this at the ccg
  mutate(nhs_number = as.character(nhs_number)) |>
  # CABG code exists
  filter(if_any(matches("procedure_[0-9]+_opcs"), ~ . %in% cabg_opcs_codes)) |>
  # Distinct entries
  group_by(nhs_number) |>
  slice_min(spell_start, with_ties=F) |>
  # always ungroup stuff
  dplyr::ungroup()


## Age data
dat_age <- dat_index_spell |> select(nhs_number, age = age_on_admission)


## Sex data
# read in the code mappnigs for sex
sex_codes <- read.csv(system.file("sex_codes/sex_codes.csv", package = "acsprojectns"))
# get the sex data from the SUS data, then add in that from the SWD attribute history data
dat_sex <- msrv$sus$apc_spells_primary_diagnosis %>%
  # Only cohort NHS numbers
  {if(.Platform$OS.type == "windows") {
    right_join(., data.frame("nhs_number" = cohort_ids), by="nhs_number", copy = TRUE)
  } else {
    filter(., nhs_number %in% cohort_ids)
  }} |>
  # only nhs number and sex
  select(nhs_number, sex_sus = sex) |>
  # combine with the sex data from the SWD attribute history
  left_join(msrv$swd$attr_h %>%
            # Only cohort NHS numbers
            {if(.Platform$OS.type == "windows") {
              right_join(., data.frame("nhs_number" = cohort_ids), by="nhs_number", copy = TRUE)
            } else {
              filter(., nhs_number %in% cohort_ids)
            }} |>
            select(nhs_number, sex_swd = sex), by="nhs_number") |>
  # collect to local
  run() |>
  # ensure all characters
  mutate(across(dplyr::starts_with("sex"), ~as.character(.x))) |>
  # combine into one column
  pivot_longer(starts_with("sex"), names_to="source", values_to="sex_code") |>
  # re-code according to the mappings
  left_join(sex_codes, by="sex_code") |>
  # group by each patient
  group_by(nhs_number) |>
  # take the mode within each patient, ignoring NAs
  summarise(sex = acsprojectns::mode(sex)) |>
  # shouldn't need to do this at work
  mutate(nhs_number = as.character(nhs_number)) |>
  # ensure all nhs numbers represented
  right_join(dat_index_spell |> select(nhs_number), by="nhs_number")


## Ethnicity
# read in the ethnicity mappings
ethnicity_codes <- read.csv(system.file("ethnicity_codes/ethnicity_codes.csv", package = "acsprojectns"))
# get the ethnicity data from the SUS data, then add in that from the SWD attribute history data
dat_ethnicity <- msrv$sus$apc_spells_primary_diagnosis %>%
  # Only cohort NHS numbers
  {if(.Platform$OS.type == "windows") {
    right_join(., data.frame("nhs_number" = cohort_ids), by="nhs_number", copy = TRUE)
  } else {
    filter(., nhs_number %in% cohort_ids)
  }} |>
  # Just ethnicity
  select(nhs_number, ethnicity_sus = ethnic_group) |>
  # Add SWD attribute data
  left_join(msrv$swd$attr_h  %>%
            # Only cohort NHS numbers
            {if(.Platform$OS.type == "windows") {
              right_join(., data.frame("nhs_number" = cohort_ids), by="nhs_number", copy = TRUE)
            } else {
              filter(., nhs_number %in% cohort_ids)
            }} |>
            select(nhs_number, ethnicity_swd = ethnicity), by="nhs_number") |>
  # collect to local
  icdb::run() |>
  # ensure all characters
  mutate(dplyr::across(dplyr::starts_with("ethnicity"), ~as.character(.x))) |>
  # combine into one column
  pivot_longer(dplyr::starts_with("ethnicity"), names_to="source", values_to="ethnicity_code") |>
  # re-code according to the mappings
  left_join(ethnicity_codes, by="ethnicity_code") |>
  # group by each patient
  group_by(nhs_number) |>
  # take the mode within each patient, ignoring NAs; n.b. use 'ethnicity' rather than
  # 'ethnicity_group' for more detailed breakdown
  summarise(ethnicity = acsprojectns::mode(ethnicity_group)) |>
  # shouldnt need to do this at work
  mutate(nhs_number = as.character(nhs_number)) |>
  # ensure all nhs numbers represented
  right_join(dat_index_spell |> dplyr::select(nhs_number), by="nhs_number")


## Index of Multiple Deprivation
# extract the IMD data from the swd_attributes table
dat_imd <- msrv$swd$swd_attribute %>%
  # Only cohort NHS numbers
  {if(.Platform$OS.type == "windows") {
    right_join(., data.frame("nhs_number" = cohort_ids), by="nhs_number", copy = TRUE)
  } else {
    filter(., nhs_number %in% cohort_ids)
  }} |>
  # just the IMD
  select(nhs_number, wd_imd_decile_19) |>
  # collect to local
  icdb::run()  |>
  # group by each patient
  group_by(nhs_number) |>
  # take the most recent
  slice_head(n=1) |>
  # shouldnt need to do this at work
  mutate(nhs_number = as.character(nhs_number)) |>
  # ensure all nhs numbers represented
  right_join(dat_index_spell |> select(nhs_number), by="nhs_number")


## Frailty
# extract the electronic frailty index data from the swd attributes history table
dat_frailty <- msrv$swd$attr_h %>%
  # Only cohort NHS numbers
  {if(.Platform$OS.type == "windows") {
    right_join(., data.frame("nhs_number" = cohort_ids), by="nhs_number", copy = TRUE)
  } else {
    filter(., nhs_number %in% cohort_ids)
  }} |>
  # just the efi decile
  select(nhs_number, attribute_period, score = efi_category) |>
  # label the score
  mutate(frailty_score = "efi") |>
  # collect to local
  icdb::run() |>
  # add the Cambridge segment data
  bind_rows(msrv$swd$cambridge %>%
            # Only cohort NHS numbers
            {if(.Platform$OS.type == "windows") {
              right_join(., data.frame("nhs_number" = cohort_ids), by="nhs_number", copy = TRUE)
            } else {
              filter(., nhs_number %in% cohort_ids)
            }} |>
            select(nhs_number, attribute_period, score = segment) |>
            mutate(frailty_score = "cambridge_segment") |>
            icdb::run()) |>
  # shouldn't need to do this at work
  mutate(nhs_number = as.character(nhs_number)) |>
  # join with the index ACS event date, so that we can filter for data existing prior to the ACS event
  left_join(dat_index_spell |> select(nhs_number, spell_start), by="nhs_number") |>
  # only look at data prior to the ACS event
  filter(attribute_period < spell_start) |>
  # NAs aren't useful
  filter(!is.na(score)) |>
  # take the most recent of each score
  group_by(nhs_number, frailty_score) |>
  slice_max(attribute_period, with_ties=FALSE) |>
  # select only those we need
  select(nhs_number, frailty_score, score) |>
  # separate efi and Cambridge
  pivot_wider(names_from=frailty_score, values_from=score) |>
  # shouldn't need to do this at work
  mutate(nhs_number = as.character(nhs_number)) |>
  # ensure all nhs numbers represented
  right_join(dat_index_spell |> select(nhs_number), by="nhs_number")


## Lipid data
dat_lipid_profile <- msrv$swd$swd_measurement %>%
  # Only cohort NHS numbers
  {if(.Platform$OS.type == "windows") {
    right_join(., data.frame("nhs_number" = cohort_ids), by="nhs_number", copy = TRUE)
  } else {
    filter(., nhs_number %in% cohort_ids)
  }} |>
  # shouldn't need to do this at work
  mutate(nhs_number = as.character(nhs_number)) |>
  # filter for cholesterol
  filter(measurement_name %in% "cholesterol") |>
  # collect to local
  icdb::run() |>
  # link to CABG date
  left_join(dat_index_spell |> select(nhs_number, spell_start, spell_end), by="nhs_number") |>
  # time relative to CABG
  mutate(lipid_timing_d = time_length(interval(spell_start, measurement_date), unit="days"))


## Prescribing data
# # Low dose statin therapy without ezetimibe - # low dose = simva 10, prava 10-20, lova 20, fluva 20-40, pitva 1            https://www.google.com/url?sa=i&url=https%3A%2F%2Fwww.uspharmacist.com%2Farticle%2Flipidlowering-therapies-a-review-of-current-and-future-options&psig=AOvVaw1oUjYp0LCmvmyKdwPQM3UM&ust=1649363828741000&source=images&cd=vfe&ved=0CAoQjRxqFwoTCMDY-pqlgPcCFQAAAAAdAAAAABAD
# med_filter_string <- "(?i)([A-z ]+)(\\d+\\.?\\d*)?([A-z]+)?[ ]*(?:[A-z]*)"
# subsequent_prescriptions <- msrv$swd$swd_prescription %>%
#   # Only cohort NHS numbers
#   {if(.Platform$OS.type == "windows") {
#     right_join(., data.frame("nhs_number" = cohort_ids), by="nhs_number", copy = TRUE)
#   } else {
#     filter(., nhs_number %in% cohort_ids)
#   }} |>
#   # Time window - start of the cohort window to present; and only prescriptions
#   filter(prescription_date >= acs_date_min) |>
#   # select the needed columns
#   select(nhs_number,
#          event_start=prescription_date,
#          event_end=prescription_date,
#          prescription_name) |>
#   # collect to local
#   icdb::run() |>
#   # shouldnt need to do this at work
#   mutate(nhs_number = as.character(nhs_number)) |>
#   # join the index spell data
#   right_join(dat_index_spell |> select(nhs_number, index_start=spell_start, index_end=spell_end), by="nhs_number") |>
#   # make sure that the spell comes after the end of the index ACS spell
#   filter(event_start > index_end) |>
#   #dplyr::filter(event_start >= index_start) |>
#   # prescription grouping / coding
#   mutate(medication_name   = tolower(trimws(stringr::str_match(prescription_name, pattern = med_filter_string)[,2])),
#                 medication_dose   = stringr::str_match(prescription_name, pattern = med_filter_string)[,3],
#                 medication_units  = stringr::str_match(prescription_name, pattern = med_filter_string)[,4],
#                 event_desc = dplyr::case_when(
#                   grepl("inclisiran", medication_name) ~ "inclisiran",
#                   # High dose statin therapy
#                   (grepl("atorvastatin", medication_name) & medication_dose>=40) |
#                     (grepl("rosuvastatin", medication_name) & medication_dose>=20) |
#                     (grepl("simvastatin",  medication_name) & medication_dose>=80) ~ "high_statin",
#                   # Moderate dose statin therapy
#                   (grepl("atorvastatin", medication_name) & medication_dose<40)  |
#                     (grepl("rosuvastatin", medication_name) & medication_dose<20)  |
#                     (grepl("fluvastatin",  medication_name) & medication_dose>40)  |
#                     (grepl("pravastatin",  medication_name) & medication_dose>=40) |
#                     (grepl("simvastatin",  medication_name) & medication_dose>=20) ~ "subopt_statin",
#                   # Low dose statin therapy
#                   (grepl("fluvastatin",  medication_name) & medication_dose<=40) |
#                     (grepl("pravastatin",  medication_name) & medication_dose<40)  |
#                     (grepl("simvastatin",  medication_name) & medication_dose<20) ~ "subopt_statin",
#
#                   TRUE ~ NA_character_)) |>
#   # for now, just get rid of things that aren't statins
#   dplyr::filter(!is.na(event_desc)) |>
#   # only take id, spell dates and event description
#   dplyr::select(nhs_number, event_desc, event_start, event_end)


## Combined data
dat_lst <- list(dat_age, dat_sex, dat_ethnicity, dat_imd, dat_frailty)
dat_demographics <- reduce(dat_lst, left_join, by='nhs_number')





