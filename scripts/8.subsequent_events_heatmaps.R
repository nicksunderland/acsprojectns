##################################################################
##                     Libraries and stuff                      ##
##################################################################
library(usethis)
library(devtools)
library(ggplot2)
library(zoo)
library(scales)
library(hrbrthemes)
library(viridis)
rm(list=ls())
load_all()
source(system.file("scripts/0.init.R", package = "acsprojectns"))
future::plan(future::multisession, workers=parallel::detectCores()-1)  # you must re-install the whole package after each edit to use future_maps, otherwise it doesn't use the correct functions

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
  dplyr::mutate(datetime = lubridate::int_start(.data$spell_interval)) %>%
  dplyr::slice_min(.data$datetime) %>%
  dplyr::rename_at(vars(-.data$pseudo_nhs_id), ~ paste0(.x, "_acs"))

# Ensure unique ids
assertthat::are_equal(nrow(acs_spells), length(unique(acs_spells$pseudo_nhs_id)))
ids = acs_spells$pseudo_nhs_id

# Get the sex and age
tmp <- load_demographics_v2(db_conn_struct, ids)
tmp["sex"]     = furrr::future_map_dfr(tmp$demographics, ~ setNames(.x$sex, "sex"))
tmp["dob_est"] = furrr::future_map_dfr(tmp$demographics, ~ setNames(.x$dob_estimate, "dob_est"))
tmp <- tmp %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$sex,
                .data$dob_est)

acs_spells <- dplyr::left_join(acs_spells, tmp, by=c("pseudo_nhs_id"))

acs_spells["age"] = purrr::map2_int(.x = acs_spells$dob_est,
                                    .y = acs_spells$datetime_acs,
                                    .f = function(x, y) as.integer(lubridate::as.period(lubridate::interval(x, y))$year))
acs_spells["age_category"] = cut(acs_spells$age,
                                 breaks=c(0,40,50,60,70,80,90,120),
                                 right=FALSE,
                                 labels=c("<40", "40-49", "50-59", "60-69", "70-79", "80-89", "90+"))
# Ditch the tmp data
rm(tmp)



# Define all the diagnoses that we are interested in
acs_codes                    <- get_codes(file_path = system.file("icd10_codes/acs_codes.csv",                         package = "acsprojectns"), append_character = "-")
pci_codes                    <- get_codes(file_path = system.file("opcs_codes/pci_procedure_codes.csv", package = "acsprojectns"))
cabg_codes                   <- get_codes(file_path = system.file("opcs_codes/cabg_procedure_codes.csv", package = "acsprojectns"))
heart_failure_codes          <- get_codes(file_path = system.file("icd10_codes/heart_failure_codes.csv",               package = "acsprojectns"), append_character = "-")
isch_cardiomyopathy_codes    <- get_codes(file_path = system.file("icd10_codes/ischaemic_cardiomyopathy_codes.csv",    package = "acsprojectns"), append_character = "-")
nonisch_cardiomyopathy_codes <- get_codes(file_path = system.file("icd10_codes/nonischaemic_cardiomyopathy_codes.csv", package = "acsprojectns"), append_character = "-")
isch_stroke_codes            <- get_codes(file_path = system.file("icd10_codes/ischaemic_stroke_codes.csv",            package = "acsprojectns"))
bleeding_codes               <- get_codes(file_path = system.file("icd10_codes/bleeding_codes.csv",                    package = "acsprojectns"), append_character = "-")
vasc_disease_codes           <- get_codes(file_path = system.file("icd10_codes/vascular_disease_codes.csv",            package = "acsprojectns"), append_character = "-")
cv_death_codes               <- rbind(acs_codes %>% dplyr::select(-description_simple), heart_failure_codes, isch_cardiomyopathy_codes, nonisch_cardiomyopathy_codes, vasc_disease_codes, isch_stroke_codes)


# Get all the hospital admissions for the cohort
all_admissions <-
  get_hospital_admissions(
    sus_apc_obj            = db_conn_struct$sus_apc,
    codes_list             = NA,
    ccg_provider_code_list = NA,
    search_strategy        = "primary_diagnosis",
    search_strat_adm_method= "all",
    return_all_codes       = TRUE,
    datetime_window        = lubridate::interval(acs_date_window_min - lubridate::dyears(1), acs_date_window_max + lubridate::dyears(3), tzone = "GMT"),
    id_list                = ids,
    level                  = "spell",
    verbose                = FALSE) %>%
  dplyr::mutate(episode_start_datetime = lubridate::int_start(.data$spell_interval),
                domain = "Hospital admission",
                descriptor = dplyr::case_when(purrr::map_lgl(.data$spell_codes, ~ any(.x %in% acs_codes[["icd_codes"]][acs_codes$description_simple=="STEMI"])) ~ "ACS - STEMI",
                                              purrr::map_lgl(.data$spell_codes, ~ any(.x %in% acs_codes[["icd_codes"]][acs_codes$description_simple!="STEMI"])) ~ "ACS - NSTEMI",
                                              purrr::map_lgl(.data$spell_codes, ~ any(.x %in% heart_failure_codes$icd_codes)) ~ "Heart Failure",
                                              purrr::map_lgl(.data$spell_codes, ~ any(.x %in% isch_stroke_codes$icd_codes)) ~ "Stroke (ischaemic)",
                                              purrr::map_lgl(.data$spell_codes, ~ any(.x %in% bleeding_codes[["icd_codes"]][bleeding_codes$group=="Intracranial bleeding"])) ~ "Bleeding - ICH",
                                              purrr::map_lgl(.data$spell_codes, ~ any(.x %in% bleeding_codes[["icd_codes"]][bleeding_codes$group=="Gastrointestinal bleeding"])) ~ "Bleeding - GI",
                                              purrr::map_lgl(.data$spell_codes, ~ any(.x %in% bleeding_codes[["icd_codes"]][bleeding_codes$group=="Other bleeding"])) ~ "Bleeding - other",
                                              TRUE ~ "Other diagnosis"))  %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$episode_start_datetime,
                .data$domain,
                .data$descriptor)
# Check that there are no duplicate id:spell combinations
assertthat::are_equal(nrow(all_admissions), nrow(unique(all_admissions %>% dplyr::select(.data$pseudo_nhs_id, .data$episode_start_datetime))))

# Get all procedures for the cohort
all_procedures <- load_admissions_v2(db_conn_struct, ids) %>%
  tidyr::unnest(hospital_admissions) %>%
  dplyr::mutate(domain = "Hospital procedure",
                descriptor = dplyr::case_when(purrr::map_lgl(.data$procedure_opcs_codes, ~ any(.x %in% pci_codes$opcs_codes)) &
                                              base::grepl("elective" , .data$admission_method, ignore.case=TRUE)  ~ "PCI - elective",
                                              purrr::map_lgl(.data$procedure_opcs_codes, ~ any(.x %in% pci_codes$opcs_codes)) ~ "PCI - non-elective",
                                              purrr::map_lgl(.data$procedure_opcs_codes, ~ any(.x %in% cabg_codes$opcs_codes)) &
                                              base::grepl("elective" , .data$admission_method, ignore.case=TRUE)  ~ "CABG - elective",
                                              purrr::map_lgl(.data$procedure_opcs_codes, ~ any(.x %in% cabg_codes$opcs_codes)) ~ "CABG - non-elective")) %>%
  dplyr::filter(!is.na(.data$descriptor)) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$episode_start_datetime,
                .data$domain,
                .data$descriptor)
# Check that there are no duplicate id:spell combinations
assertthat::are_equal(nrow(all_procedures), nrow(unique(all_procedures %>% dplyr::select(.data$pseudo_nhs_id, .data$episode_start_datetime))))

# Get all the mortality for the cohort
all_mortality <- load_mortality_v2(db_conn_struct, ids) %>%
  tidyr::unnest(mortality) %>%
  dplyr::mutate(domain = "Death",
                episode_start_datetime = .data$date_of_death,
                descriptor = purrr::pmap_chr(list(.data$death_1a_icd_codes, .data$death_1b_icd_codes, .data$death_1c_icd_codes),
                                          function(a,b,c){
                                            if(any(c(a,b,c) %in% cv_death_codes$icd_codes)){
                                              return("Cardiovascular")
                                            }else if(!all(is.na(c(a,b,c)))){
                                              return("Non-cardiovascular")
                                            }else{
                                              return(NA_character_)
                                            }
                                          })) %>%
  dplyr::filter(!is.na(.data$descriptor)) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$episode_start_datetime,
                .data$domain,
                .data$descriptor)
# Check that there are no duplicate ids
assertthat::are_equal(nrow(all_mortality), length(unique(all_mortality$pseudo_nhs_id)))

# Get all the ED attendances for the cohort
all_ed_attendances <- load_emergency_department_v2(db_conn_struct, ids) %>%
  tidyr::unnest(ed_attendances) %>%
  dplyr::mutate(domain = "Emergency Department",
                descriptor = "ED attendance") %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$episode_start_datetime,
                .data$domain,
                .data$descriptor)
# Check that there are no duplicate id:spell combinations
assertthat::are_equal(nrow(all_ed_attendances), nrow(unique(all_ed_attendances %>% dplyr::select(.data$pseudo_nhs_id, .data$episode_start_datetime))))

# Get all the SWD activity for the cohort
all_swd_activity <- load_swd_activity_v2(db_conn_struct, ids) %>%
  tidyr::unnest(swd_activity) %>%
  dplyr::mutate(episode_start_datetime = lubridate::int_start(.data$spell_interval),
                tmp                    = dplyr::case_when(.data$activity_provider == "primary_care_contact" &
                                                            .data$activity_setting == "gp" ~ "Primary care<>GP appointment",
                                                          .data$activity_provider == "secondary" &
                                                            .data$activity_speciality == "cardiology service" ~ "Hospital outpatients<>Cardiology",
                                                          .data$activity_provider == "secondary" &
                                                            .data$activity_speciality == "cardiac rehabilitation service" ~ "Hospital outpatients<>Cardiac Rehab",),
                domain                 = sub("<>.*", "", tmp),
                descriptor             = sub(".*<>", "", tmp)) %>%
  dplyr::filter(!is.na(.data$descriptor)) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$episode_start_datetime,
                .data$domain,
                .data$descriptor)
# Check that there are no duplicate id:spell combinations
# assertthat::are_equal(nrow(all_swd_activity), nrow(unique(all_swd_activity %>% dplyr::select(.data$pseudo_nhs_id, .data$episode_start_datetime))))


# Get all of the HbA1c / cholesterol / BP data from SWD measurements
all_swd_results <- load_swd_measurements_v2(db_conn_struct, ids) %>%
  tidyr::unnest(swd_measurements) %>%
  dplyr::mutate(domain = "Measurements",
                episode_start_datetime = lubridate::as_datetime(.data$measurement_datetime),
                descriptor = dplyr::case_when(.data$measurement_type == "hba1c" ~ "HbA1c check",
                                              .data$measurement_type == "blood_pressure" ~ "BP check",
                                              .data$measurement_type == "cholesterol" ~ "Cholesterol check")) %>%
  dplyr::filter(!is.na(.data$descriptor)) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$episode_start_datetime,
                .data$domain,
                .data$descriptor)

# Get all of the statin /BB/ ACEi / AP data data from SWD activity -> create statin yes/no etc.
lipid_regexs = c("statin", "ezetimibe", "inclisiran")
diabetes_regexs = c("metformin", "gliclazide", "insulin", "gliptin", "exenatide", "glutide", "gliflozin", "glitazone")
p2y12_regexs = c("clopidogrel", "prasugrel", "ticagrelor")
beta_blocker_regexs = c("bisoprolol", "carvedilol", "metoprolol", "atenolol", "labetalol", "propranolol", "sotalol")
acei_regexs = c("ramipril", "benazepril", "Captopril", "Enalapril", "Fosinopril", "Lisinopril", "Moexipril", "Perindopril", "Quinapril", "Trandolapril")
arb_regexs = c("candesartan", "Azilsartan", "Eprosartan", "Irbesartan", "Losartan", "Olmesartan", "Telmisartan", "Valsartan")
acei_arb_regexs = c(acei_regexs, arb_regexs)
mra_regexs = c("spironolactone", "eplerenone")

all_gp_prescriptions <- load_gp_prescriptions_v2(db_conn_struct, ids) %>%
  tidyr::unnest(gp_prescriptions) %>%
  dplyr::mutate(domain = "Primary care",
                descriptor = dplyr::case_when(base::grepl(paste(lipid_regexs, collapse="|"), .data$medication_name, ignore.case=TRUE) ~ "Lipid therapy",
                                              base::grepl(paste(diabetes_regexs, collapse="|"), .data$medication_name, ignore.case=TRUE) ~ "Diabetic therapy",
                                              base::grepl("aspirin", .data$medication_name, ignore.case=TRUE) ~ "Aspirin therapy",
                                              base::grepl(paste(p2y12_regexs, collapse="|"), .data$medication_name, ignore.case=TRUE) ~ "P2Y12 therapy",
                                              base::grepl(paste(beta_blocker_regexs, collapse="|"), .data$medication_name, ignore.case=TRUE) ~ "Beta-blocker therapy",
                                              base::grepl(paste(acei_arb_regexs, collapse="|"), .data$medication_name, ignore.case=TRUE) ~ "ACEi/ARB therapy",
                                              base::grepl(paste(mra_regexs, collapse="|"), .data$medication_name, ignore.case=TRUE) ~ "MRA therapy")) %>%
  dplyr::filter(!is.na(.data$descriptor)) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$episode_start_datetime,
                .data$domain,
                .data$descriptor)

# Combine asc_spells with all the activity data and filter into the time window around the acs event
pre_win   = -365 #days
post_win  = 365*3 #days
day_intervals = seq(pre_win, post_win, 1)
all_activity <- rbind(all_admissions,
                      all_procedures,
                      all_mortality,
                      all_ed_attendances,
                      all_swd_activity,
                      all_gp_prescriptions,
                      all_swd_results) %>%
  dplyr::right_join(acs_spells, by=c("pseudo_nhs_id")) %>%
  dplyr::mutate(relative_time_days = lubridate::interval(.data$datetime_acs, .data$episode_start_datetime)/lubridate::ddays(1)) %>%
  dplyr::filter(relative_time_days >= pre_win & relative_time_days <= post_win)  %>%
  dplyr::mutate(day_post_acs = day_intervals[findInterval(.data$relative_time_days, day_intervals)]) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$datetime_acs,
                .data$domain,
                .data$descriptor,
                .data$day_post_acs)

# Define the comparative splits e.g. hba1c checked, or not; or age >80 or <=80
age_gteq_80 <- acs_spells %>%
  dplyr::mutate(comparator = .data$age >= 80) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$comparator)

hba1c_window_days = 120 # post ACS
hba1c_checked <- acs_spells %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$datetime_acs) %>%
  dplyr::left_join(all_swd_results %>% dplyr::filter(.data$descriptor == "HbA1c check"), by="pseudo_nhs_id") %>%
  dplyr::filter(lubridate::`%within%`(.data$episode_start_datetime, lubridate::interval(.data$datetime_acs, .data$datetime_acs + lubridate::ddays(hba1c_window_days)))) %>%
  dplyr::distinct(.data$pseudo_nhs_id, .keep_all = TRUE) %>%
  dplyr::right_join(acs_spells %>% dplyr::select(.data$pseudo_nhs_id), by="pseudo_nhs_id") %>%
  dplyr::mutate(comparator = !is.na(.data$descriptor)) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$comparator)


# Secondary care activity specialty by HbA1c check - HEATMAP
x_bin = 31
my_breaks = c(rev(seq(-x_bin/2, pre_win-x_bin,by=-x_bin)), seq(x_bin/2,post_win+x_bin,by=x_bin))
my_labels = as.character(zoo::rollmean(my_breaks, 2))
heatmap_levels = data.frame(matrix(
  c("Hospital admission", "ACS - STEMI",
    "Hospital admission", "ACS - NSTEMI",
    "Hospital admission", "Heart Failure",
    "Hospital admission", "Stroke (ischaemic)",
    "Hospital admission", "Bleeding - ICH",
    "Hospital admission", "Bleeding - GI",
    "Hospital admission", "Bleeding - other",
    "Hospital admission", "Other diagnosis",
    "Hospital procedure", "PCI - elective",
    "Hospital procedure", "PCI - non-elective",
    "Hospital procedure", "CABG - elective",
    "Hospital procedure", "CABG - non-elective",
    "Emergency Department", "ED attendance",
    "Death"             , "Cardiovascular",
    "Death"             , "Non-cardiovascular",
    "Hospital outpatients", "Cardiology",
    "Hospital outpatients", "Cardiac Rehab",
    "Primary care"      , "GP appointment",
    "Primary care"      , "Lipid therapy",
    "Primary care"      , "Diabetic therapy",
    "Primary care"      , "Aspirin therapy",
    "Primary care"      , "P2Y12 therapy",
    "Primary care"      , "Beta-blocker therapy",
    "Primary care"      , "ACEi/ARB therapy",
    "Primary care"      , "MRA therapy",
    "Measurements"      , "HbA1c check",
    "Measurements"      , "BP check",
    "Measurements"      , "Cholesterol check"),
  ncol = 2, byrow=TRUE))
colnames(heatmap_levels) <- c("domain", "descritor")

# Plot the data for overall trends (not split by a comparator)
ggplot(data=all_activity %>%
           dplyr::mutate(time_bins = cut(.data$day_post_acs, breaks=my_breaks, labels=my_labels)) %>%
           dplyr::group_by(domain, descriptor, time_bins) %>%
           dplyr::summarise(total = dplyr::n()) %>%
           dplyr::ungroup() %>%
           # Convert to factor
           dplyr::mutate(domain     = factor(.data$domain,     levels = unique(heatmap_levels$domain)),
                         descriptor = factor(.data$descriptor, levels = unique(heatmap_levels$descritor))),
       aes(x=time_bins, y=descriptor, fill=total)) +
  geom_tile() +
  labs(title="ACS proximate events (ACS event at time=0)",
       subtitle = "BNSSG Acute Coronary Syndrome Patients Jul 2017 - Dec 2021 (n=5743)",
       y="Event",
       x="Time (days)",
       fill="Number of events") +
  scale_y_discrete(limits=rev) +
  scale_fill_viridis_c(option="viridis", begin = 0, end=1, labels = comma,
                       breaks = c(1, 10, 100, 1000, 5000),
                       trans=scales::pseudo_log_trans(sigma = 100, base = exp(1))) +
  theme_minimal() +
  facet_grid(rows = vars(domain), scales = "free", space = "free")
  # facet_wrap(~domain, ncol=1, scales = "free")
  #facet_grid(rows = vars(domain), scales = "fixed", space = "free")







# Plot the data split by a comparator
ggplot(data=all_activity %>%
         dplyr::mutate(time_bins = cut(.data$day_post_acs, breaks=my_breaks, labels=my_labels)) %>%
         #hba1c_checked  age_gteq_80
         dplyr::left_join(hba1c_checked, by="pseudo_nhs_id") %>%

         dplyr::group_by(domain, descriptor, time_bins, comparator) %>%
         dplyr::summarise(total = dplyr::n()) %>%
         dplyr::ungroup() %>%
         # Convert to factor
         dplyr::mutate(domain     = factor(.data$domain,     levels = unique(heatmap_levels$domain)),
                       descriptor = factor(.data$descriptor, levels = unique(heatmap_levels$descritor))),
       aes(x=time_bins, y=descriptor, fill=total)) +
  geom_tile() +
  labs(title="ACS proximate events (-90<day<=365)",
       subtitle = "BNSSG Acute Coronary Syndrome Patients Jul 2017 - Dec 2021 (n=5743)",
       y="Event",
       x="Time (days)",
       fill="Number of events") +
  scale_y_discrete(limits=rev) +
  scale_fill_viridis_c(option="viridis", begin = 0, end=1, labels = comma,
                       breaks = c(1, 10, 100, 1000, 5000),
                       trans=scales::pseudo_log_trans(sigma = 100, base = exp(1))) +
  theme_minimal() +
 # facet_wrap(~domain, ncol=1, scales = "free")
  facet_grid(rows = vars(domain), cols = vars(comparator), scales = "free", space = "free")




