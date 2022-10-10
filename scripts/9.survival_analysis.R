##################################################################
##                     Libraries and stuff                      ##
##################################################################
library(usethis)
library(devtools)
library(ggplot2)
library(GGally)
library(wesanderson)
library(zoo)
library(scales)
library(hrbrthemes)
library(viridis)
library(survival)
library(survminer)
rm(list=ls())
load_all()
source(system.file("scripts/0.init.R", package = "acsprojectns"))

# Get all of the ACS data for the area
acs_codes           <- get_codes(file_path = system.file("icd10_codes/acs_codes.csv",                       package = "acsprojectns"), append_character = "-")
bnssg_ccg_codes     <- get_codes(file_path = system.file("ccg_provider_codes/bnssg_ccg_provider_codes.csv", package = "acsprojectns"))

# Patients who had an ACS event
acs_cohort <- get_hospital_admissions(
  sus_apc_obj            = db_conn_struct$sus_apc,
  ccg_provider_code_list = bnssg_ccg_codes$ccg_provider_code,
  level                  = "spell",
  codes_list             = acs_codes$icd_codes,
  search_strategy        = "primary_diagnosis",
  search_strat_adm_method= "emergency",
  return_all_codes       = FALSE,
  datetime_window        = lubridate::interval(acs_date_window_min, acs_date_window_max, tzone = "GMT"),
  verbose                = FALSE)
# Check that there are no duplicate id:spell combinations
assertthat::are_equal(nrow(acs_cohort), nrow(unique(acs_cohort %>% dplyr::select(.data$pseudo_nhs_id, .data$spell_interval))))

# Extract the most important ICD-10 code if multiple within spell (the ICD-10 text files should be in order of importance)
acs_spells <- acs_cohort %>% dplyr::mutate(icd10_code = purrr::map_chr(acs_cohort$spell_codes, ~ .x[match(acs_codes$icd_codes, .x)[which.min(is.na(match(acs_codes$icd_codes, .x)))]]))

# Join in the text descriptions of the ACS event
acs_spells <- acs_spells %>% dplyr::left_join(acs_codes %>% dplyr::select(.data$icd_codes, .data$description_simple), by=c("icd10_code" = "icd_codes"))
acs_spells <- acs_spells %>% dplyr::mutate(description_simple = factor(.data$description_simple, levels = rev(c("STEMI","NSTEMI","Other_MI","Acute_IHD","Unstable_angina"))))

# Get the index event date
acs_spells <- acs_spells %>%
  dplyr::group_by(.data$pseudo_nhs_id) %>%
  dplyr::slice_min(lubridate::int_start(.data$spell_interval)) %>%
  dplyr::rename_at(vars(-.data$pseudo_nhs_id), ~ paste0(.x, "_acs")) %>%
  dplyr::ungroup()
# Ensure unique ids
assertthat::are_equal(nrow(acs_spells), length(unique(acs_spells$pseudo_nhs_id)))
ids = acs_spells$pseudo_nhs_id

# Get the sex and age
demog <- load_demographics_v2(db_conn_struct, ids) %>%
  tidyr::unnest(demographics) %>%
  dplyr::distinct(.data$pseudo_nhs_id, .keep_all=TRUE) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$sex,
                .data$dob_estimate)

# Get the SWD date range
swd_range = get_swd_date_range(db_conn_struct$swd_att_hist, ids)

# Combine
acs_spells <- acs_spells %>%
  dplyr::left_join(demog,     by=c("pseudo_nhs_id")) %>%
  dplyr::left_join(swd_range, by=c("pseudo_nhs_id")) %>%
  dplyr::mutate(age = lubridate::as.period(lubridate::interval(.data$dob_estimate, lubridate::int_start(.data$spell_interval_acs)))$year) %>%
  dplyr::select(-c(.data$dob_estimate,
                   .data$admission_method_acs,
                   .data$ccg_provider_code_acs,
                   .data$spell_codes_acs))
rm("demog", "swd_range", "acs_cohort")

# Define all the diagnoses that we are interested in
admission_event_mappings <- rbind(
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/acs_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "acute_coronary_syndrome"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/heart_failure_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "heart_failure"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/ischaemic_cardiomyopathy_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "ischaemic_cardiomyopathy"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/nonischaemic_cardiomyopathy_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "non_ischaemic_cardiomyopathy"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/ischaemic_stroke_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "ischaemic_stroke"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/bleeding_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "bleeding"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/vascular_disease_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "vascular_disease"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/hypertension_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "hypertension"),
  data.frame("icd_code"   = c(get_codes(file_path = system.file("icd10_codes/diabetes_type1_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
                              get_codes(file_path = system.file("icd10_codes/diabetes_type2_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
                              get_codes(file_path = system.file("icd10_codes/diabetes_other_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes),
             "event_code" = "diabetes"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/ckd_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "chronic_kidney_disease"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/hypercholesterolaemia_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "hypercholesterolaemia"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/smoking_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "smoking"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/obesity_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "obesity"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/atrial_fibrillation_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "atrial_fibrillation"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/anaemia_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "anaemia"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/blood_transfusion_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "blood_transfusion"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/cancer_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "cancer"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/cardiac_arrest_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "cardiac_arrest"),
  data.frame("icd_code"   = get_codes(file_path = system.file("icd10_codes/cardiac_dysrhythmia_codes.csv", package = "acsprojectns"), append_character = "-")$icd_codes,
             "event_code" = "cardiac_dysrhythmia")
  )

  # Define all the procedures that we are interested in
procedure_event_mappings <- rbind(
  data.frame("opcs_code"  = get_codes(file_path = system.file("opcs_codes/pci_procedure_codes.csv", package = "acsprojectns"))$opcs_codes,
             "event_code" = "pci"),
  data.frame("opcs_code"  = get_codes(file_path = system.file("opcs_codes/cabg_procedure_codes.csv", package = "acsprojectns"))$opcs_codes,
             "event_code" = "cabg"),
  data.frame("opcs_code"  = get_codes(file_path = system.file("opcs_codes/blood_transfusion_procedure_codes.csv", package = "acsprojectns"))$opcs_codes,
             "event_code" = "blood_transfusion")
)

# Define cardiovascular death
mortality_event_mappings <- admission_event_mappings %>%
  dplyr::filter(.data$event_code %in% c("acute_coronary_syndrome",
                                        "heart_failure",
                                        "ischaemic_cardiomyopathy",
                                        "non_ischaemic_cardiomyopathy",
                                        "cardiac_dysrhythmia")) %>%
  dplyr::mutate(event_code = "cv_death")

endpoint_mappings = data.frame(matrix(c(
  "cv_death"                    ,T,T,T,F,
  "non_cv_death"                ,F,F,T,F,
  "acute_coronary_syndrome"     ,T,T,T,T,
  "heart_failure"               ,T,T,T,T,
  "ischaemic_stroke"            ,F,T,T,T,
  "bleeding"                    ,F,F,T,T,
  "ischaemic_cardiomyopathy"    ,F,F,F,T,
  "non_ischaemic_cardiomyopathy",F,F,F,T,
  "vascular_disease"            ,F,F,F,T,
  "hypertension"                ,F,F,F,T,
  "diabetes"                    ,F,F,F,T,
  "chronic_kidney_disease"      ,F,F,F,T,
  "hypercholesterolaemia"       ,F,F,F,T,
  "smoking"                     ,F,F,F,T,
  "obesity"                     ,F,F,F,T,
  "atrial_fibrillation"         ,F,F,F,T,
  "anaemia"                     ,F,F,F,T,
  "cancer"                      ,F,F,F,T,
  "cardiac_arrest"              ,F,F,F,T,
  "cardiac_dysrhythmia"         ,F,F,F,T,
  "blood_transfusion"           ,F,F,T,T,
  "pci"                         ,T,T,T,T,
  "cabg"                        ,T,T,T,T), ncol = 5, byrow=TRUE))
colnames(endpoint_mappings) <- c("event_code", "mace", "macce", "nace", "comorbidity")
endpoint_mappings <- endpoint_mappings %>%
  dplyr::mutate(mace = as.logical(mace),
                macce= as.logical(macce),
                nace = as.logical(nace),
                comorbidity = as.logical(comorbidity))
stopifnot(all(endpoint_mappings$event_code %in% c("non_cv_death",
                                                  admission_event_mappings$event_code,
                                                  procedure_event_mappings$event_code,
                                                  mortality_event_mappings$event_code)))

# Get all the hospital admission episodes and the primary diagnoses
all_admission_episodes <-
  get_hospital_admissions(
    sus_apc_obj            = db_conn_struct$sus_apc,
    codes_list             = admission_event_mappings$icd_code,
    ccg_provider_code_list = NA,
    search_strategy        = "primary_diagnosis",
    search_strat_adm_method= "all",
    return_all_codes       = FALSE,
    datetime_window        = lubridate::interval(as.POSIXct("1900-01-01"), Sys.Date()),
    id_list                = ids,
    level                  = "episode",
    verbose                = FALSE) %>%
  tidyr::unnest(.data$episode_codes) %>%
  dplyr::filter(stringr::str_detect(.data$admission_method, "(?i)emergency")) %>%
  dplyr::left_join(admission_event_mappings, by = c("episode_codes"="icd_code")) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$episode_interval,
                .data$event_code)

# Get all the hospital procedure codes
all_procedure_episodes <-
  get_hospital_admissions(
    sus_apc_obj            = db_conn_struct$sus_apc,
    codes_list             = procedure_event_mappings$opcs_code,
    ccg_provider_code_list = NA,
    search_strategy        = "procedures",
    search_strat_adm_method= "all",
    return_all_codes       = FALSE,
    datetime_window        = lubridate::interval(as.POSIXct("1900-01-01"), Sys.Date()),
    id_list                = ids,
    level                  = "episode",
    verbose                = FALSE) %>%
  tidyr::unnest(.data$episode_codes) %>%
  dplyr::left_join(procedure_event_mappings, by = c("episode_codes"="opcs_code")) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$episode_interval,
                .data$event_code)

# Get all the deaths
all_mortality <- load_mortality_v2(db_conn_struct, ids) %>%
  tidyr::unnest(mortality) %>%
  dplyr::filter(!is.na(.data$date_of_death)) %>%
  tidyr::pivot_longer(dplyr::matches("1(a|b|c)"), names_to="component", values_to = "episode_codes") %>%
  tidyr::unnest(.data$episode_codes) %>%
  dplyr::left_join(mortality_event_mappings, by = c("episode_codes"="icd_code")) %>%
  dplyr::mutate(event_code = dplyr::if_else(is.na(.data$event_code), "non_cv_death", .data$event_code),
                episode_interval = lubridate::interval(.data$date_of_death, .data$date_of_death)) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$episode_interval,
                .data$event_code)

# Get all the SWD attributes data
all_swd_attributes <- load_swd_attributes_v2(db_conn_struct, ids) %>%
  tidyr::unnest(swd_attributes) %>%
  dplyr::left_join(acs_spells %>%
                     dplyr::select(.data$pseudo_nhs_id, .data$spell_interval_acs), by="pseudo_nhs_id") %>%
  dplyr::relocate(.data$spell_interval_acs, .after = .data$attribute_period_date) %>%
  dplyr::filter(lubridate::as_datetime(.data$attribute_period_date, tz="GMT") < lubridate::int_start(.data$spell_interval_acs)) %>%
  dplyr::select(-.data$spell_interval_acs) %>%
  dplyr::group_by(.data$pseudo_nhs_id) %>%
  dplyr::slice_max(.data$attribute_period_date, with_ties=F) %>%
  dplyr::ungroup() %>%
  dplyr::mutate(dplyr::across(where(is.character), as.factor),
                dplyr::across(where(is.factor), as.integer),
                dplyr::across(-c("pseudo_nhs_id", "attribute_period_date"), as.numeric)) %>%
  dplyr::mutate(ca = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("cancer"))),
                htn = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("hypertension"))),
                obs = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("obesity"))),
                smo = smoking_status,
                ckd = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("chronic_kidney_disease"))),
                ana = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("anaemia"))),
                str = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("stroke"))),
                liv = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("liver_disease"))),
                afi = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("atrial_fibrillation"))),
                arr = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("arrhythmia"))),
                isc = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("ischaemic_heart_disease"))),
                hf  = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("heart_failure"))),
                mi  = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("ischaemic_heart_disease_mi"))),
                pvd = dplyr::coalesce(!!!dplyr::select(., dplyr::contains("vascular_disease"))),
                dm  = dplyr::coalesce(!!!dplyr::select(., dplyr::matches("type_1_diabetes|type_2_diabetes")))) %>%
  dplyr::select(-c(dplyr::matches("cancer|hypertension|obesity|smoking_status|chronic_kidney_disease|anaemia|stroke|liver_disease|atrial_fibrillation|arrhythmia|ischaemic_heart_disease|heart_failure|ischaemic_heart_disease_mi|vascular_disease|type_1_diabetes|type_2_diabetes"))) %>%
  dplyr::select(pseudo_nhs_id,
                attribute_period_date,
                acute_coronary_syndrome = mi,
                heart_failure = hf,
                ischaemic_stroke = str,
                ischaemic_cardiomyopathy = isc,
                vascular_disease = pvd,
                hypertension = htn,
                diabetes = dm,
                chronic_kidney_disease = ckd,
                smoking = smo,
                obesity = obs,
                atrial_fibrillation = afi,
                anaemia = ana,
                cancer = ca,
                cardiac_dysrhythmia = arr) %>%
  tidyr::pivot_longer(cols      = -c(.data$pseudo_nhs_id, .data$attribute_period_date),
                      names_to  = "event_code",
                      values_to = "event_status") %>%
  dplyr::filter(.data$event_status == 1) %>%
  dplyr::mutate(episode_interval = lubridate::interval(.data$attribute_period_date, .data$attribute_period_date, tzone="GMT")) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$episode_interval,
                .data$event_code)

# Get all the measurements
all_measurements <- load_swd_measurements_v2(db_conn_struct, ids) %>%
  tidyr::unnest(swd_measurements) %>%
  dplyr::filter(!is.na(.data$measurement_datetime)) %>%
  dplyr::mutate(episode_interval = lubridate::interval(.data$measurement_datetime, .data$measurement_datetime, tzone="GMT"),
                event_code = dplyr::case_when(.data$measurement_type  == "hba1c" & as.numeric(.data$measurement_value) < 41.999                    ~ "HbA1c_nondiabetic",
                                              .data$measurement_type  == "hba1c" & dplyr::between(as.numeric(.data$measurement_value), 42, 47.001) ~ "HbA1c_prediabetic",
                                              .data$measurement_type  == "hba1c" & as.numeric(.data$measurement_value) > 47.001                    ~ "HbA1c_diabetic",
                                              .data$measurement_type  == "blood_pressure" &
                                                as.numeric(trimws(sub("\\/.*", "", .data$measurement_value))) < 140 &
                                                as.numeric(trimws(sub(".*\\/", "", .data$measurement_value))) < 90                                 ~ "BP_well_controlled",
                                              .data$measurement_type  == "blood_pressure"                                                          ~ "BP_not_controlled",
                                              TRUE ~ NA_character_)) %>%
  dplyr::filter(!is.na(.data$event_code)) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$episode_interval,
                .data$event_code)

# All events
all_events = rbind(all_admission_episodes,
                   all_procedure_episodes,
                   all_mortality,
                   all_swd_attributes,
                   all_measurements)

# The targets
df_ml_targets = acs_spells %>%
  dplyr::left_join(all_events, by="pseudo_nhs_id") %>%
  dplyr::ungroup() %>%
  dplyr::filter(!(
                    lubridate::int_overlaps(.data$spell_interval_acs, .data$episode_interval) | # events overlap
                    lubridate::date(lubridate::int_end(.data$spell_interval_acs)) == lubridate::date(lubridate::int_start(.data$episode_interval)) #event occurred on the same day as the last day of the acs spell
                  )
                ) %>%
  dplyr::mutate(event_status  = TRUE,
                survival_days = lubridate::interval(lubridate::int_end(.data$spell_interval_acs), lubridate::int_start(.data$episode_interval)) / lubridate::ddays(),
                survival_days = dplyr::if_else(grepl("death", .data$event_code) & .data$survival_days >-2 & .data$survival_days <0, 0, .data$survival_days)) %>% # died just before admission (i.e. died on admission)
  dplyr::group_by(pseudo_nhs_id) %>%
  dplyr::filter(.data$survival_days > 0) %>% # things known after the time of the ACS
  dplyr::slice_min(survival_days, with_ties=F) %>% # first diagnosis
  dplyr::ungroup() %>%
  dplyr::bind_rows(setNames(data.frame(matrix(ncol=ncol(.), nrow=nrow(endpoint_mappings))), colnames(.)) %>%
                     dplyr::mutate(event_code = endpoint_mappings$event_code)) %>%
  tidyr::complete(tidyr::nesting(pseudo_nhs_id, swd_date_range, spell_interval_acs), .data$event_code) %>%
  dplyr::rowwise() %>%
  dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                any(.data$event_code %in% c(endpoint_mappings$event_code[endpoint_mappings$mace],
                                            endpoint_mappings$event_code[endpoint_mappings$macce],
                                            endpoint_mappings$event_code[endpoint_mappings$nace]))) %>%
  dplyr::ungroup() %>%
  tidyr::pivot_wider(id_cols     = c(.data$pseudo_nhs_id),
                     names_from  = .data$event_code,
                     names_glue  = "{.value}_{event_code}_targ",
                     values_from = c(event_status, survival_days),
                     values_fill = list("event_status" = FALSE, "survival_days" = NA_real_)) %>%
  dplyr::right_join(
    dplyr::left_join(acs_spells, all_mortality %>%
                      dplyr::mutate(dod = lubridate::int_start(.data$episode_interval)) %>%
                      dplyr::select(pseudo_nhs_id, dod) %>%
                      dplyr::distinct(pseudo_nhs_id, .keep_all=T),
                    by="pseudo_nhs_id"),
    by="pseudo_nhs_id") %>%
  dplyr::mutate(latest_fu_date = dplyr::coalesce(dod, max(lubridate::int_end(swd_date_range), lubridate::int_end(spell_interval_acs), na.rm=T)),
                dplyr::across(dplyr::starts_with("survival_days"), ~ dplyr::coalesce(., lubridate::interval(lubridate::int_start(.data$spell_interval_acs), latest_fu_date) / lubridate::ddays())),
                dplyr::across(dplyr::starts_with("event_status"),  ~ dplyr::coalesce(., FALSE))) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(event_status_mace_targ   = any(dplyr::across(paste0("event_status_",  endpoint_mappings$event_code[endpoint_mappings$mace],  "_targ"))),
                survival_days_mace_targ  = min(dplyr::across(paste0("survival_days_", endpoint_mappings$event_code[endpoint_mappings$mace],  "_targ")), na.rm=T),
                event_status_macce_targ  = any(dplyr::across(paste0("event_status_",  endpoint_mappings$event_code[endpoint_mappings$macce], "_targ"))),
                survival_days_macce_targ = min(dplyr::across(paste0("survival_days_", endpoint_mappings$event_code[endpoint_mappings$macce], "_targ")), na.rm=T),
                event_status_nace_targ   = any(dplyr::across(paste0("event_status_",  endpoint_mappings$event_code[endpoint_mappings$nace],  "_targ"))),
                survival_days_nace_targ  = min(dplyr::across(paste0("survival_days_", endpoint_mappings$event_code[endpoint_mappings$nace],  "_targ")), na.rm=T),
                event_status_death_targ  = any(dplyr::across(paste0("event_status_",  c("non_cv_death", "cv_death"),  "_targ"))),
                survival_days_death_targ = min(dplyr::across(paste0("survival_days_", c("non_cv_death", "cv_death"),  "_targ")), na.rm=T)) %>%
  dplyr::ungroup() %>%
  dplyr::select(dplyr::starts_with(c("pseudo_nhs_id", "event_status", "survival")))


# The predictors
df_ml_predictors = acs_spells %>%
  dplyr::left_join(all_events, by="pseudo_nhs_id") %>%
  dplyr::ungroup() %>%
  dplyr::filter(!(
                  (lubridate::int_overlaps(.data$spell_interval_acs, .data$episode_interval) | # events overlap
                   lubridate::date(lubridate::int_end(.data$episode_interval)) == lubridate::date(lubridate::int_start(.data$spell_interval_acs))) #event occurred on the same day as the first day of the acs spell
                  & .data$event_code %in% c("acute_coronary_syndrome", "pci", "cabg"))) %>%
  dplyr::mutate(event_status  = TRUE,
                survival_days = lubridate::interval(lubridate::int_end(.data$spell_interval_acs), lubridate::int_start(.data$episode_interval)) / lubridate::ddays(),
                survival_days = dplyr::if_else(grepl("HbA1c|BP_", .data$event_code) & dplyr::between(.data$survival_days, 0, 365), 0, .data$survival_days)) %>% #set to zero so filtering in a bit keeps the row
  dplyr::group_by(pseudo_nhs_id) %>%
  dplyr::filter(.data$survival_days <= 0) %>% # things known before or at the end of the ACS event
  dplyr::slice_max(survival_days, with_ties=F) %>% # most recent diagnosis
  dplyr::ungroup() %>%
  dplyr::bind_rows(setNames(data.frame(matrix(ncol=ncol(.), nrow=nrow(endpoint_mappings))), colnames(.)) %>%
                     dplyr::mutate(event_code = endpoint_mappings$event_code)) %>%
  tidyr::complete(tidyr::nesting(pseudo_nhs_id, swd_date_range, spell_interval_acs), .data$event_code) %>%
  dplyr::rowwise() %>%
  dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                (any(.data$event_code %in% endpoint_mappings$event_code[!grepl("death", endpoint_mappings$event_code, ignore.case=T)]) |
                  grepl("HbA1c|BP_", .data$event_code))) %>%
  dplyr::ungroup() %>%
  tidyr::pivot_wider(id_cols     = c(.data$pseudo_nhs_id),
                     names_from  = .data$event_code,
                     names_glue  = "{.value}_{event_code}_pred",
                     values_from = c(event_status, survival_days),
                     values_fill = list("event_status" = FALSE, "survival_days" = NA_real_)) %>%
  dplyr::right_join(acs_spells, by="pseudo_nhs_id") %>%
  dplyr::mutate(bloodpressure_control_1y    = dplyr::case_when(.data$event_status_BP_well_controlled_pred ~ "Controlled",
                                                    .data$event_status_BP_not_controlled_pred  ~ "Not controlled",
                                                    TRUE ~ "Not measured"),
                diabetic_control_1y = dplyr::case_when(.data$event_status_HbA1c_prediabetic_pred  ~ "HbA1c_prediabetic",
                                                    .data$event_status_HbA1c_diabetic_pred  ~ "HbA1c_diabetic",
                                                    .data$event_status_HbA1c_nondiabetic_pred  ~ "HbA1c_nondiabetic",
                                                    TRUE ~ "Not measured"),
                bloodpressure_control_1y = factor(bloodpressure_control_1y, labels = c("Not controlled", "Controlled", "Not measured")),
                diabetic_control_1y = factor(diabetic_control_1y, labels = c("HbA1c_diabetic", "HbA1c_prediabetic", "HbA1c_nondiabetic", "Not measured"))) %>%
  dplyr::mutate(earliest_fu_date = min(lubridate::int_start(swd_date_range), lubridate::int_start(spell_interval_acs)),
                dplyr::across(dplyr::starts_with("survival_days"), ~ dplyr::coalesce(., lubridate::interval(lubridate::int_start(.data$spell_interval_acs), earliest_fu_date) / lubridate::ddays())),
                dplyr::across(dplyr::starts_with("event_status"),  ~ dplyr::coalesce(., FALSE))) %>%
  dplyr::rowwise() %>%
  dplyr::mutate(event_status_hba1c_checked_first_year = dplyr::if_any(dplyr::contains("HbA1c")),
                event_status_bp_checked_first_year    = dplyr::if_any(dplyr::contains("BP_")),
                event_status_hba1c_checked_first_year = tidyr::replace_na(.data$event_status_hba1c_checked_first_year, FALSE),
                event_status_bp_checked_first_year    = tidyr::replace_na(.data$event_status_bp_checked_first_year, FALSE)) %>%
  dplyr::select(dplyr::starts_with(c("pseudo_nhs_id", "age", "sex", "event_status", "survival", "bloodpressure_control_1y", "diabetic_control_1y")))


# The full dataset
survival_dataset = dplyr::left_join(df_ml_predictors, df_ml_targets, by="pseudo_nhs_id") %>%
  tidyr::pivot_longer(cols = dplyr::ends_with("_targ"),
                      names_to = c(".value", "component"),
                      names_pattern = "(survival_days|event_status)_(.*)_targ") %>%
  dplyr::mutate(component = factor(.data$component),
                age_cat   = cut(.data$age, breaks=seq(0,120,20), labels=c(paste0(seq(0,120,20)[1:5], "-", seq(0,120,20)[2:6]),"100+"), ordered_result=T),
                age_cat   = factor(age_cat, levels = c("0-20","20-40","40-60","60-80","80-100","100+"))) %>%
  dplyr::ungroup()


# Pairs plots
GGally::ggpairs(
  data = survival_dataset %>% dplyr::select("age",
                                            (dplyr::starts_with("event_status_acute") | dplyr::starts_with("survival_days_acute"))),
  mapping = aes(color = event_status_acute_coronary_syndrome_pred),
  upper = list(continuous = "cor",    combo = "box", discrete = "count"),
  lower = list(continuous = "points", combo = "dot", discrete = "facetbar"),
  diag  = list(continuous = "densityDiag",
               discrete   = "barDiag",
               na         = "naDiag",
               mapping    = aes(alpha = 0.75) ),
) +
  theme_bw() +
  scale_fill_manual(values=wes_palette(n=2, name="GrandBudapest1")) +
  scale_color_manual(values=wes_palette(n=2, name="GrandBudapest1"))


# Survival curves
surv_obj <- Surv(time  = survival_dataset$survival_days,
                 event = survival_dataset$event_status)

components_to_plot = c("macce")
splits = c("bloodpressure_control_1y", "age_cat")

# bloodpressure_control_1y   diabetic_control_1y
# [1] acute_coronary_syndrome bleeding                blood_transfusion       cabg                    cv_death
# [6] heart_failure           ischaemic_stroke        non_cv_death            pci                     mace
# [11] macce                   nace                    death

surv_fit <- survfit(formula = as.formula(paste("surv_obj ~ component + ", paste(splits, collapse = "+"))),
                    data    = survival_dataset,
                    subset  = component %in% components_to_plot)

ggsurv <- ggsurvplot(surv_fit,
                     color = splits[[1]],
                     palette = RColorBrewer::brewer.pal(4, "Set2"),#colorRampPalette(RColorBrewer::brewer.pal(8, "Set2"))(15),
                     ylim = c(0.5, 1.0),
                     title = paste("Survival from", components_to_plot, "by", paste(splits, collapse=" + ")),
                     xlab = "Days",
                     ylab = "Survival")
#                      legend = "none")

ggsurv$plot +
  theme_bw() +
  theme(legend.position = "bottom") +
  facet_grid(~factor(age_cat, levels = c("0-20","20-40","40-60","60-80","80-100","100+")))
#
# factor(component, levels=components_to_plot) ~ 1 ) #







# Machine learning
library(caret)

# define the predictors and targets
component      <- "acute_coronary_syndrome"
predictor_cols <- c("age", "sex", colnames(ml_dataset)[grepl("event_status_(.*)_pred$", colnames(ml_dataset))])
target_col     <- c("event_status")

# The dataset
ml_dataset = survival_dataset %>%
  dplyr::filter(.data$component == component) %>%
  dplyr::select(c(predictor_cols, target_col)) %>%
  dplyr::select(where(~dplyr::n_distinct(.x) > 1)) %>%
  dplyr::mutate(dplyr::across(.cols = where(is.character), as.factor),
                dplyr::across(.cols = where(is.logical),   as.factor))

# Update the predictor columns as we filtered out one that are all the same value
predictor_cols <- intersect(predictor_cols, colnames(ml_dataset))

##### DELETE LATER
ml_dataset$sex <- factor(sample(c("male","female"), size=nrow(ml_dataset), replace=T), levels = c("male", "female"))

# create a list of indices for 80% of the rows in the original dataset we can use for training
validation_index   <- createDataPartition(ml_dataset[[target_col]], p=0.80, list=FALSE)

# select 20% of the data for validation
validation_dataset <- dplyr::slice(ml_dataset, -validation_index)

# use the remaining 80% of data to training and testing the models
training_dataset   <- dplyr::slice(ml_dataset, validation_index)

# Plot x against y
GGally::ggpairs(data    = validation_dataset[c(predictor_cols[1:5], target_col)],
                mapping = aes(color = event_status)) +
  theme_bw() +
  scale_fill_manual(values=wes_palette(n=2, name="GrandBudapest1")) +
  scale_color_manual(values=wes_palette(n=2, name="GrandBudapest1"))

# Run algorithms using 10-fold cross validation
control <- trainControl(method="cv", number=10)
metric <- "Accuracy"

# a) linear algorithms
set.seed(7)
fit.lda <- train(event_status~., data=training_dataset, method="lda", metric=metric, trControl=control)
# b) nonlinear algorithms
# CART
set.seed(7)
fit.cart <- train(event_status~., data=training_dataset, method="rpart", metric=metric, trControl=control)
# kNN
set.seed(7)
fit.knn <- train(event_status~., data=training_dataset, method="knn", metric=metric, trControl=control)
# c) advanced algorithms
# SVM
set.seed(7)
fit.svm <- train(event_status~., data=training_dataset, method="svmRadial", metric=metric, trControl=control)
# Random Forest
set.seed(7)
fit.rf <- train(event_status~., data=training_dataset, method="rf", metric=metric, trControl=control)

# summarize accuracy of models
results <- resamples(list(lda=fit.lda, cart=fit.cart, knn=fit.knn, svm=fit.svm, rf=fit.rf))
summary(results)

# compare accuracy of models
dotplot(results)

# summarize Best Model
print(fit.rf)

# estimate skill of LDA on the validation dataset
predictions <- predict(fit.rf, validation_dataset)
confusionMatrix(predictions, validation_dataset$event_status)
