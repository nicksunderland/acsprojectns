#' NBT NIHR i4i Connect application SWD data
#'
#' Email:
#' We would like to: Evaluate the potential use of routine data from the BNSSG System-Wide Dataset,
#' though with relevance to other regions, to identify patients to assess for participation in a
#' clinical trial of this device (for example people with treatment resistant hypertension e.g.
#' persistent hypertension despite at least 3 anti-hypertensive medications) Estimate the potential
#' numbers of participants identified in this way There will be funding available for obtaining and
#' analysing the data, though the i4i Connect grant is relatively small and therefore will only
#' cover a restricted amount of time, to cover a focussed and limited data analysis.
#'
#' Author: Nick Sunderland
#' Date: 2023-01-03
#'
library(devtools)
library(icdb)
library(dplyr)
library(tidyr)
library(BBmisc)
library(lubridate)
library(ggplot2)
library(ggpubr)
library(viridis)
library(kableExtra)
rm(list=ls())
load_all()

# Connect to the ICB server
svr <- icdb::server("XSW")

# All SWD patients with at least one blood pressure measurement
bp <- svr$MODELLING_SQL_AREA$swd_measurement %>%
  select(nhs_number, measurement_name, measurement_date, measurement_value) %>%
  filter(measurement_name == "blood_pressure",
         measurement_value != "unknown",
         !is.na(measurement_value)) %>%
  run() %>%
  mutate(sbp = as.numeric(gsub("([0-9]+).*$", "\\1",  measurement_value)),
         dbp = as.numeric(gsub("^.+/([0-9]+)", "\\1", measurement_value))) %>%
  filter(!is.na(sbp),
         !is.na(dbp),
         sbp>40,
         dbp>20)

# bp <- bp[1:1000,]

# Patient current age
#TODO change this to mean that when calculating the BP cutoff we have the age on the date of the BP
age_bands     <- c(0,40,60,80,110)
age_band_labs <- paste0(age_bands[1:length(age_bands)-1], "-", age_bands[2:length(age_bands)]-1)
age <- svr$MODELLING_SQL_AREA$swd_attribute %>%
  select(nhs_number, age) %>%
  run() %>%
  filter(nhs_number %in% !!bp$nhs_number) %>%
  group_by(nhs_number) %>%
  summarise(age = max(age, na.rm=T))

# Add the ages and compute the BP summary data
blood_pressures <- bp %>%
  left_join(age, by="nhs_number") %>%
  group_by(nhs_number) %>%
  # arrange(measurement_date, .by_group=T) %>%
  # mutate(date_first_sbp_gteq_140 = ifelse(any(sbp>=140), measurement_date[which.first(sbp>=140)], as.POSIXct(NA)),
  #        date_last_sbp_gteq_140  = ifelse(any(sbp>=140), measurement_date[which.last(sbp>=140)],  as.POSIXct(NA)),
  #        date_first_sbp_gteq_150 = ifelse(any(sbp>=150), measurement_date[which.first(sbp>=150)], as.POSIXct(NA)),
  #        date_last_sbp_gteq_150  = ifelse(any(sbp>=150), measurement_date[which.last(sbp>=150)],  as.POSIXct(NA)),
  #        date_first_dbp_gteq_90  = ifelse(any(dbp>=90),  measurement_date[which.first(dbp>=90)],  as.POSIXct(NA)),
  #        date_last_dbp_gteq_90   = ifelse(any(dbp>=90),  measurement_date[which.last(dbp>=90)],   as.POSIXct(NA)),
  #        interval_hypertensive   = case_when(
  #          !is.na(date_first_sbp_gteq_140) & !is.na(date_first_dbp_gteq_90) & !is.na(date_last_sbp_gteq_140) & !is.na(date_last_dbp_gteq_90) & age<80
  #               ~  interval(min(date_first_sbp_gteq_140, date_first_dbp_gteq_90, na.rm=T),
  #                           max(date_last_sbp_gteq_140,  date_last_dbp_gteq_90, na.rm=T),
  #                           tzone="GMT"),
  #          !is.na(date_first_sbp_gteq_150) & !is.na(date_first_dbp_gteq_90) & !is.na(date_last_sbp_gteq_150) & !is.na(date_last_dbp_gteq_90) & age>=80
  #               ~ interval(min(date_first_sbp_gteq_150, date_first_dbp_gteq_90, na.rm=T),
  #                          max(date_last_sbp_gteq_150,  date_last_dbp_gteq_90, na.rm=T),
  #                          tzone="GMT"),
  #          TRUE ~ interval(NA,NA))) %>%
  summarise(age    = mean(age, na.rm=T),
            n_bp   = n(),
            sbp_av = mean(sbp, na.rm=T),
            sbp_hi = max(sbp, na.rm=T),
            sbp_lo = min(sbp, na.rm=T),
            dbp_av = mean(dbp, na.rm=T),
            dbp_hi = max(dbp, na.rm=T),
            dbp_lo = min(dbp, na.rm=T)) %>%
  ungroup()
            # ,
            # interval_hypertensive = first(na.omit(interval_hypertensive)))

# Define the blood pressure medications
diuretics <- c("chlorthalidone", "chlorothiazide", "hydrochlorothiazide", "indapamide", "metolazone", "amiloride", "spironolactone", "triamterene", "bumetanide", "furosemide", "torsemide")
beta_blockers <- c("acebutolol", "atenolol", "betaxolol", "bisoprolol", "metoprolol", "nadolol", "pindolol", "propranolol", "solotol", "timolol")
ace_inhibitors <- c("benazepril", "captopril", "enalapril", "fosinopril", "lisinopril", "moexipril", "perindopril", "quinapril", "ramipril", "trandolapril")
arbs <- c("candesartan", "eprosartan", "irbesartan", "losartan", "telmisartan", "valsartan")
ccbs <- c("amlodipine", "diltiazem", "felodipine", "isradipine", "nicardipine", "nifedipine", "nisoldipine", "verapamil")
alpha_blockers <- c("doxazosin", "prazosin", "terazosin")
a_b_blockers <- c("carvedilol", "labetalol")
central_agonists <- c("methyldopa", "clonidine", "guanfacine")
vasodilators <- c("hydralazine", "minoxidil")
mras <- c("eplerenone","spironolactone")
renin_inhibitors <- c("aliskiren")
blood_pressure_meds <- c(diuretics, beta_blockers, ace_inhibitors, arbs, ccbs, alpha_blockers, a_b_blockers, central_agonists, vasodilators, mras, renin_inhibitors)

# Get the medication data
med_filter_string <- "(?i)([A-z ]+)(\\d+\\.?\\d*)?([A-z]+)?[ ]*(?:[A-z]*)"
med_filter <- paste0("\"spec_l1b\" %LIKE% %", blood_pressure_meds, "%", collapse = " | ")
medications <- svr$MODELLING_SQL_AREA$swd_measurement %>%
  select(nhs_number, measurement_name, measurement_value) %>%
  filter(measurement_name == "blood_pressure",
         measurement_value != "unknown",
         !is.na(measurement_value)) %>%
  distinct(nhs_number) %>%
  # joins much quicker than passing in loads of IDs
  left_join( svr$MODELLING_SQL_AREA$swd_activity %>% select(nhs_number, prescription_date = arr_date, pod_l1, spec_l1b),
             by = "nhs_number") %>%
  filter(pod_l1 == "primary_care_prescription") %>%
  filter(spec_l1b  %LIKE% '%chlorthalidone%' | spec_l1b  %LIKE% '%chlorothiazide%' | spec_l1b  %LIKE% '%hydrochlorothiazide%' | spec_l1b  %LIKE% '%indapamide%' | spec_l1b  %LIKE% '%metolazone%' | spec_l1b  %LIKE% '%amiloride%' | spec_l1b  %LIKE% '%spironolactone%' | spec_l1b  %LIKE% '%triamterene%' | spec_l1b  %LIKE% '%bumetanide%' | spec_l1b  %LIKE% '%furosemide%' | spec_l1b  %LIKE% '%torsemide%' | spec_l1b  %LIKE% '%acebutolol%' | spec_l1b  %LIKE% '%atenolol%' | spec_l1b  %LIKE% '%betaxolol%' | spec_l1b  %LIKE% '%bisoprolol%' | spec_l1b  %LIKE% '%metoprolol%' | spec_l1b  %LIKE% '%nadolol%' | spec_l1b  %LIKE% '%pindolol%' | spec_l1b  %LIKE% '%propranolol%' | spec_l1b  %LIKE% '%solotol%' | spec_l1b  %LIKE% '%timolol%' | spec_l1b  %LIKE% '%benazepril%' | spec_l1b  %LIKE% '%captopril%' | spec_l1b  %LIKE% '%enalapril%' | spec_l1b  %LIKE% '%fosinopril%' | spec_l1b  %LIKE% '%lisinopril%' | spec_l1b  %LIKE% '%moexipril%' | spec_l1b  %LIKE% '%perindopril%' | spec_l1b  %LIKE% '%quinapril%' | spec_l1b  %LIKE% '%ramipril%' | spec_l1b  %LIKE% '%trandolapril%' | spec_l1b  %LIKE% '%candesartan%' | spec_l1b  %LIKE% '%eprosartan%' | spec_l1b  %LIKE% '%irbesartan%' | spec_l1b  %LIKE% '%losartan%' | spec_l1b  %LIKE% '%telmisartan%' | spec_l1b  %LIKE% '%valsartan%' | spec_l1b  %LIKE% '%amlodipine%' | spec_l1b  %LIKE% '%diltiazem%' | spec_l1b  %LIKE% '%felodipine%' | spec_l1b  %LIKE% '%isradipine%' | spec_l1b  %LIKE% '%nicardipine%' | spec_l1b  %LIKE% '%nifedipine%' | spec_l1b  %LIKE% '%nisoldipine%' | spec_l1b  %LIKE% '%verapamil%' | spec_l1b  %LIKE% '%doxazosin%' | spec_l1b  %LIKE% '%prazosin%' | spec_l1b  %LIKE% '%terazosin%' | spec_l1b  %LIKE% '%carvedilol%' | spec_l1b  %LIKE% '%labetalol%' | spec_l1b  %LIKE% '%methyldopa%' | spec_l1b  %LIKE% '%clonidine%' | spec_l1b  %LIKE% '%guanfacine%' | spec_l1b  %LIKE% '%hydralazine%' | spec_l1b  %LIKE% '%minoxidil%' | spec_l1b  %LIKE% '%eplerenone%' | spec_l1b  %LIKE% '%spironolactone%' | spec_l1b  %LIKE% '%aliskiren%') %>%
  select(nhs_number, prescription_date, spec_l1b) %>%
  show_query() %>%
  run() %>%

  filter(nhs_number %in% !!blood_pressures$nhs_number) %>%
  mutate(med_name  = trimws(stringr::str_match(spec_l1b, pattern = med_filter_string)[,2]),
         med_dose  = stringr::str_match(spec_l1b, pattern = med_filter_string)[,3],
         med_units = stringr::str_match(spec_l1b, pattern = med_filter_string)[,4]) %>%

  # check if any NAs generated here

  select(nhs_number, prescription_date, med_name) %>%
  group_by(nhs_number, prescription_date) %>%
  mutate(num_meds = n_distinct(med_name)) %>%
  group_by(nhs_number) %>%
  #arrange(prescription_date, .by_group=T) %>%
  summarise(min_num_meds        = min(num_meds, na.rm=T),
            #date_min_num_meds   = prescription_date[which.min(num_meds)],
            max_num_meds        = max(num_meds, na.rm=T),
            #date_max_num_meds   = prescription_date[which.max(num_meds)],
            avg_num_meds        = mean(num_meds, na.rm=T)) %>%
            # ,
            # interval_gteq3_meds = interval(prescription_date[which.first(num_meds>=3)],
            #                                prescription_date[which.last(num_meds>=3)], tzone="GMT")) %>%
  ungroup()

# Combine the blood pressure data, age, and medication data
# high_bp_and_gteq3_meds = int_overlaps(interval_hypertensive, interval_gteq3_meds),
data <- blood_pressures %>%
  left_join(medications, by="nhs_number") %>%
  mutate(max_num_meds = replace_na(max_num_meds, 0),
         age          = replace_na(age, 70),
         bp_control_cat_any = case_when(
         max_num_meds>3 & age<80  & (sbp_hi>=140 | dbp_hi>=90) ~ "resistant HTN (>=3 meds, any BP)",
      max_num_meds>=3 & age>=80 & (sbp_hi>=150 | dbp_hi>=90) ~ "resistant HTN (>=3 meds, any BP)",
      max_num_meds<3  & age<80  & (sbp_hi>=140 | dbp_hi>=90) ~ "uncontrolled HTN (<=2 meds, any BP)",
      max_num_meds<3  & age>=80 & (sbp_hi>=150 | dbp_hi>=90) ~ "uncontrolled HTN (<=2 meds, any BP)",
      max_num_meds==0 ~ "normal BP (0 meds)",
      max_num_meds<3  ~ "controlled HTN (<=2 meds, any BP)",
      max_num_meds>=3 ~ "controlled HTN (>=3 meds, any BP)",
      TRUE ~ "error"),
    bp_control_cat_any = factor(bp_control_cat_any, levels = c("resistant HTN (>=3 meds, any BP)",
                                                               "uncontrolled HTN (<=2 meds, any BP)",
                                                               "controlled HTN (>=3 meds, any BP)",
                                                               "controlled HTN (<=2 meds, any BP)",
                                                               "normal BP (0 meds)")),
    bp_control_cat_mean = case_when(
      max_num_meds>=3 & age<80  & (sbp_av>=140 | dbp_av>=90) ~ "resistant HTN (>=3 meds, mean BP)",
      max_num_meds>=3 & age>=80 & (sbp_av>=150 | dbp_av>=90) ~ "resistant HTN (>=3 meds, mean BP)",
      max_num_meds<3  & age<80  & (sbp_av>=140 | dbp_av>=90) ~ "uncontrolled HTN (<=2 meds, mean BP)",
      max_num_meds<3  & age>=80 & (sbp_av>=150 | dbp_av>=90) ~ "uncontrolled HTN (<=2 meds, mean BP)",
      max_num_meds==0 & age<80  & (sbp_av<140  | dbp_av<90 ) ~ "normal BP (0 meds, mean BP)",
      max_num_meds==0 & age>=80 & (sbp_av<150  | dbp_av<90 ) ~ "normal BP (0 meds, mean BP)",
      max_num_meds<3  & age<80  & (sbp_av<140  & dbp_av<90 ) ~ "controlled HTN (<=2 meds, mean BP)",
      max_num_meds<3  & age>=80 & (sbp_av<150  & dbp_av<90 ) ~ "controlled HTN (<=2 meds, mean BP)",
      max_num_meds>=3 & age<80  & (sbp_av<140  & dbp_av<90 ) ~ "controlled HTN (>=3 meds, mean BP)",
      max_num_meds>=3 & age>=80 & (sbp_av<150  & dbp_av<90 ) ~ "controlled HTN (>=3 meds, mean BP)",
      TRUE ~ "error"),
    bp_control_cat_mean = factor(bp_control_cat_mean, levels = c("resistant HTN (>=3 meds, mean BP)",
                                                                 "uncontrolled HTN (<=2 meds, mean BP)",
                                                                 "controlled HTN (>=3 meds, mean BP)",
                                                                 "controlled HTN (<=2 meds, mean BP)",
                                                                 "normal BP (0 meds, mean BP)")),
    age_cat = cut(age, right=FALSE, breaks=age_bands, labels=age_band_labs)
  )

# Tables for the data
t1 <- data %>%
  group_by(bp_control_cat_any) %>%
  summarise(n=n()) %>%
  kable()
t2 <- data %>%
  group_by(bp_control_cat_any, age_cat) %>%
  summarise(n=n()) %>%
  kable()

# Plot the bar chats for numbers of patients - ANY BP value
p1 <- ggplot(data    = data,
             mapping = aes(x=bp_control_cat_any, fill=age_cat)) +
  geom_bar() +
  scale_fill_viridis_d() +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(title    = 'SWD blood pressure control data',
       subtitle = 'Categories based on any recorded blood pressure value and any medication prescription*',
       caption  = '*not necessarily related in time',
       x        = 'BP control category',
       y        = 'Count',
       fill     = 'Age')
p1

# Plot the bar chats for numbers of patients - MEAN BP value
p2 <- ggplot(data    = data,
       mapping = aes(x=bp_control_cat_mean, fill=age_cat)) +
  geom_bar() +
  scale_fill_viridis_d() +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
  labs(title    = 'SWD blood pressure control data',
       subtitle = 'Categories based on average blood pressure values and any medication prescriptions',
       x        = 'BP control category',
       y        = 'Count',
       fill     = 'Age')
p2

# # Plot the bar chats for numbers of patients - ANY BP value but split by whether or not the time
# # period in which the patient has hypertensive values, overlaps the period in which they are
# # prescribed 3 meds or more
# p3 <- ggplot(data    = data,
#              mapping = aes(x=bp_control_cat_any)) +
#   geom_bar(fill=high_bp_and_gteq3_meds) +
#   scale_fill_viridis_d() +
#   theme_minimal() +
#   theme(axis.text.x = element_text(angle = 45, hjust = 1)) +
#   labs(title    = 'SWD blood pressure control data',
#        subtitle = 'Categories based on blood pressure values and medication prescriptions*',
#        caption  = '*split by time period overlap of high BP values and >=3 medications',
#        x        = 'BP control category',
#        y        = 'Count',
#        fill     = 'BP:Med overlap')
# p3

# Combine plots
figure <- ggarrange(p1, p2,
                    labels = c("A", "B"),
                    ncol = 1, nrow = 2)

png(file=paste0(system.file("extdata/figures/"), "bp_counts.png"),
    width=600, height=1200)
figure
dev.off()

