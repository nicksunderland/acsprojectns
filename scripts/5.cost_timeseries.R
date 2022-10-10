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
  dplyr::mutate(idx_acs_datetime = lubridate::int_start(.data$spell_interval)) %>%
  dplyr::slice_min(.data$idx_acs_datetime)

# Ensure unique ids
assertthat::are_equal(nrow(acs_spells), length(unique(acs_spells$pseudo_nhs_id)))
ids = acs_spells$pseudo_nhs_id

# Get the sex and age
tmp <- load_demographics_v2(db_conn_struct, acs_spells$pseudo_nhs_id)
tmp["sex"]     = furrr::future_map_dfr(tmp$demographics, ~ setNames(.x$sex, "sex"))
tmp["dob_est"] = furrr::future_map_dfr(tmp$demographics, ~ setNames(.x$dob_estimate, "dob_est"))
tmp <- tmp %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$sex,
                .data$dob_est)

acs_spells <- dplyr::left_join(acs_spells, tmp, by=c("pseudo_nhs_id"))

acs_spells["age"] = purrr::map2_int(.x = acs_spells$dob_est,
                                    .y = acs_spells$idx_acs_datetime,
                                    .f = function(x, y) as.integer(lubridate::as.period(lubridate::interval(x, y))$year))
acs_spells["age_category"] = cut(acs_spells$age,
                                 breaks=c(0,40,50,60,70,80,90,120),
                                 right=FALSE,
                                 labels=c("<40", "40-49", "50-59", "60-69", "70-79", "80-89", "90+"))

# Ditch the tmp data
rm(tmp)

# Get the min and max SWD dates
tmp = load_swd_attributes_v2(db_conn_struct, ids)
acs_spells["min_swd_date"] =
  purrr::map_dfr(.x = tmp$swd_attributes,
                 .f = function(x) acsprojectns::query_swd_attributes(
                               x,
                               time_window       = lubridate::interval(as.POSIXct("1900-01-01"), as.POSIXct("2050-01-01")),
                               search_strat_date = "min",
                               return_vals       = c("attribute_period_date"))) %>% dplyr::pull()
acs_spells["max_swd_date"] =
  purrr::map_dfr(.x = tmp$swd_attributes,
                 .f = function(x) acsprojectns::query_swd_attributes(
                               x,
                               time_window       = lubridate::interval(as.POSIXct("1900-01-01"), as.POSIXct("2050-01-01")),
                               search_strat_date = "max",
                               return_vals       = c("attribute_period_date"))) %>% dplyr::pull()
# Ditch the tmp data
rm(tmp)

# Get the activity data
var_dict  <- create_data_table_variable_dictionary(db_conn_struct$swd_act$table_name)    #create the named list to convert to standardised naming
base_vars <- c(pseudo_nhs_id         = var_dict[["pseudo_nhs_id"]],              #grab the basic variables we'll need
               spell_start_date      = var_dict[["episode_start_date"]],         #datetime_window looking at prescriptions start_date==end_date
               spell_end_date        = var_dict[["episode_end_date"]],           #datetime_window looking at prescriptions start_date==end_date
               activity_provider     = var_dict[["pod_level_1"]],                #the column containing the 'primary_care_contact' or ' secondary' or 'community' etc flag
               activity_setting      = var_dict[["pod_level_2a"]],
               activity_urgency      = var_dict[["pod_level_2b"]],
               activity_specialty    = var_dict[["details_level_1b"]],
               activity_cost         = var_dict[["episode_cost_1"]])             #the column containing the cost of the medication

data <- db_conn_struct$swd_act$data %>%
  dplyr::select(dplyr::all_of(base_vars)) %>%                                  #select the variables to work with
  dplyr::distinct() %>%
  dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                !is.na(.data$spell_start_date) &
                !is.na(.data$spell_end_date) &
                .data$pseudo_nhs_id %in% ids &
                .data$activity_provider %in% c('primary_care_prescription', 'primary_care_contact', 'secondary', 'community', 'mental_health', '999', '111')) %>%
  dplyr::collect() %>%
  dplyr::mutate(pseudo_nhs_id       = as.numeric(.data$pseudo_nhs_id),
                activity_start_date = .data$spell_start_date,
                activity_end_date   = .data$spell_end_date,
                activity_specialty  = dplyr::coalesce(.data$activity_specialty, .data$activity_setting),
                activity_specialty  = replace(.data$activity_specialty, .data$activity_specialty=="ae", "emergency medicine service")) %>%
  dplyr::select(.data$pseudo_nhs_id,
                .data$activity_start_date,
                .data$activity_end_date,
                .data$activity_provider,
                .data$activity_setting,
                .data$activity_urgency,
                .data$activity_specialty,
                .data$activity_cost)


# Combine asc_spells with all activity data and filter into the time window around the acs event
pre_win   = -90 #days
post_win  = 365 #days
day_intervals = seq(pre_win, post_win, 1)

full_data = dplyr::full_join(acs_spells, data, by=c("pseudo_nhs_id")) %>%
  dplyr::mutate(relative_time_days = lubridate::interval(.data$idx_acs_datetime, .data$activity_start_date)/lubridate::ddays(1),
                relative_time_days_min = lubridate::interval(.data$idx_acs_datetime, .data$min_swd_date)/lubridate::ddays(1),
                relative_time_days_max = lubridate::interval(.data$idx_acs_datetime, .data$max_swd_date)/lubridate::ddays(1)) %>%
  dplyr::filter(relative_time_days >= pre_win &
                relative_time_days <= post_win)  %>%
  dplyr::mutate(day_group = day_intervals[findInterval(.data$relative_time_days, day_intervals)],
                sec_act_type = dplyr::case_when( .data$activity_provider!="secondary" ~ NA_character_,
                                                               (.data$activity_provider=="secondary" & .data$activity_setting=="maternity") ~ "Maternity",
                                                               (.data$activity_provider=="secondary" & .data$activity_setting=="ae") ~ "A&E",
                                                               (.data$activity_provider=="secondary" & .data$activity_setting=="op" & .data$activity_urgency=="procedure") ~ "Outpatient procedure",
                                                               (.data$activity_provider=="secondary" & .data$activity_setting=="op") ~ "Outpatient other",
                                                               (.data$activity_provider=="secondary" & .data$activity_setting=="critical care") ~ "Non-elective IP",
                                                               (.data$activity_provider=="secondary" & .data$activity_setting=="ip" & .data$activity_urgency=="non_elective") ~ "Non-elective IP",
                                                               (.data$activity_provider=="secondary" & .data$activity_setting=="ip" & is.na(.data$activity_urgency)) ~ "Non-elective IP",
                                                               (.data$activity_provider=="secondary" & .data$activity_setting=="ip" & .data$activity_urgency=="elective") ~ "Elective IP",
                                                               TRUE ~ "error")) %>%
  # Filter out people who weren't in the attributes
  dplyr::mutate(possible_censor = !(day_group >= relative_time_days_min & day_group <= relative_time_days_max) | any(is.na(c(relative_time_days_min, relative_time_days_max))),
                cost_missing    = is.na(.data$activity_cost),
                remove          = .data$possible_censor & .data$cost_missing) %>%
  dplyr::filter(!remove)




full_data_1 = full_data %>%
  dplyr::group_by(.data$pseudo_nhs_id, .data$day_group) %>%
  dplyr::mutate(pt_day_cost = sum(.data$activity_cost, na.rm=T)) %>%
  dplyr::slice(1) %>%
  dplyr::arrange(.data$day_group) %>%
  dplyr::ungroup() %>%
  # Introduce zeros for those that don't have any activity on that day
  tidyr::pivot_wider(id_cols=c("pseudo_nhs_id", "relative_time_days_min", "relative_time_days_max"),
                     names_from=day_group,
                     values_from=pt_day_cost,
                     values_fill=0) %>%
  # Back to longer
  tidyr::pivot_longer(!c("pseudo_nhs_id", "relative_time_days_min", "relative_time_days_max"),
                      names_to="day_group",
                      values_to="pt_day_cost") %>%
  dplyr::mutate(day_group = as.numeric(.data$day_group)) %>%
  dplyr::group_by(.data$day_group) %>%
  dplyr::summarise(mean_cost_per_pt_per_day = mean(.data$pt_day_cost, na.rm=T)) %>%
  dplyr::ungroup()


fill_bg <- "white"
fill_col <- "#01967B"
color_pt <-  "gray"
colour_line <- "grey"
bar_width <- 2
y_cap <- 200

ggplot(data=full_data_1,
       aes(x=day_group,
           y=mean_cost_per_pt_per_day)) +
  geom_col(fill=fill_col, width = bar_width)+
  xlim(pre_win, post_win-1)+
  ylim(0, y_cap)+
  theme(panel.background = element_rect(fill = fill_bg),
        plot.background = element_rect(fill = fill_bg),
        panel.grid.major.y = element_blank(),
        panel.grid.major.x = element_line(),
        panel.grid.minor = element_blank(),
        axis.ticks = element_blank(),
        axis.line = element_line(color=colour_line),
        legend.position = "top",
        legend.background = element_rect(fill = fill_bg),
        legend.justification = "left",
        legend.key = element_rect(fill = fill_bg),
        plot.title = element_text(face = "bold")) +
  labs(title="Average daily spend per patient by time",
       subtitle = "BNSSG Acute Coronary Syndrome Patients (n=5743)",
       y="Average daily cost per patient (£)",
       x="Time relative to ACS event (days)") +
  annotate("text", x = c(0), y = c(200), label=c("£4555"), size=4, color="red")



# Look at spend over the years
full_data_2 = full_data %>%
  dplyr::mutate(contributing_cost = dplyr::case_when(.data$activity_start_date <  lubridate::ymd(paste0(format(.data$idx_acs_datetime, "%Y-%m"),"-01")) ~ 0.0,
                                                     .data$activity_start_date >  acs_date_window_max ~ 0.0,
                                                     TRUE ~ .data$activity_cost),
                new_this_month = dplyr::if_else(format(.data$idx_acs_datetime, "%Y-%m")==format(.data$activity_start_date, "%Y-%m"),TRUE,FALSE),
                date_month = format(.data$activity_start_date, "%Y-%m"))


# All - barchart across months
ggplot(data=full_data_2 %>% dplyr::group_by(.data$date_month, .data$new_this_month) %>% dplyr::summarise(cost = sum(.data$contributing_cost, na.rm=T)),
       aes(x=lubridate::ym(date_month),
           y=cost,
           fill=new_this_month)) +
  geom_bar(position="stack", stat="identity") +
  theme_minimal() +
  labs(title="Monthly spend",
       subtitle = "BNSSG Acute Coronary Syndrome Patients (n=5743)",
       y="Cost (£)",
       x="Date") +
  scale_fill_discrete(name="", labels = c("Ongoing care", "New patients")) +
  scale_x_date(date_labels="%b %Y") +
  scale_y_continuous(labels = comma)

# By provider category
ggplot(data=full_data_2 %>%
              dplyr::group_by(.data$activity_provider) %>%
              dplyr::summarise(cost = sum(.data$contributing_cost, na.rm=T)) %>%
              dplyr::ungroup() %>%
              dplyr::mutate(cost_pct = cost / sum(.data$cost)),
       aes(x="", y=cost_pct, fill=activity_provider)) +
  geom_bar(width = 1, stat = "identity") +
  coord_polar("y") +
  theme_minimal() +
  scale_fill_viridis_d(begin=0.0, end=0.90) +
  labs(title="Proportion of spend by activity provider",
       subtitle = "BNSSG Acute Coronary Syndrome Patients",
       fill = "Activity provider",
       y="% cost")

# By ACS type
ggplot(data=full_data_2 %>%
         dplyr::group_by(.data$description_simple) %>%
         dplyr::summarise(cost = sum(.data$contributing_cost, na.rm=T)) %>%
         dplyr::ungroup() %>%
         dplyr::mutate(cost_pct = cost / sum(.data$cost)),
       aes(x="", y=cost_pct, fill=description_simple)) +
  geom_bar(width = 1, stat = "identity") +
  coord_polar("y") +
  theme_minimal() +
  scale_fill_viridis_d(option="magma", begin=0.0, end=0.90) +
  labs(title="Proportion of spend by ACS category",
       subtitle = "BNSSG Acute Coronary Syndrome Patients",
       fill = "ACS category",
       y="% cost")

# By Age category
ggplot(data=full_data_2 %>%
         dplyr::group_by(.data$age_category) %>%
         dplyr::summarise(cost = sum(.data$contributing_cost, na.rm=T)) %>%
         dplyr::ungroup() %>%
         dplyr::mutate(cost_pct = cost / sum(.data$cost)),
       aes(x="", y=cost_pct, fill=age_category)) +
  geom_bar(width = 1, stat = "identity") +
  coord_polar("y") +
  theme_minimal() +
  scale_fill_viridis_d(option="plasma", begin=0.0, end=0.90) +
  labs(title="Proportion of spend by age category",
       subtitle = "BNSSG Acute Coronary Syndrome Patients",
       fill = "Age category",
       y="% cost")


# Look at cumulative spend across time from ACS, by provider
ggplot(data=full_data_2 %>%
         dplyr::filter(!.data$activity_provider %in% c("111", "999")) %>%
         dplyr::ungroup() %>%
         dplyr::select(day_group, activity_provider, contributing_cost) %>%
         dplyr::arrange(.data$day_group, .data$activity_provider) %>%
         dplyr::group_by(.data$activity_provider) %>%
         dplyr::mutate(contributing_cost = tidyr::replace_na(.data$contributing_cost, 0.0),
                       cum_sum = cumsum(.data$contributing_cost)) %>%
         dplyr::group_by(.data$activity_provider, .data$day_group) %>%
         dplyr::slice(which.max(.data$cum_sum)),
       aes(x=day_group,
           y=cum_sum,
           group=activity_provider,
           color=activity_provider)) +
  geom_line(size=1.5) +
  theme_minimal() +
  scale_colour_brewer(type = "qual") +
  labs(title="Cumulative spend relative to index ACS event",
       subtitle = "BNSSG Acute Coronary Syndrome Patients Jul 2018 - Dec 2021 (n=5743)",
       y="Cost (£)",
       x="Days from ACS",
       colour="Activity provider") +
  scale_y_continuous(labels = comma)


# Look at normalised cumulative spend across time from ACS, by age
ggplot(data=full_data_2 %>%
         dplyr::filter(!.data$activity_provider %in% c("111", "999")) %>%
         dplyr::ungroup() %>%
         dplyr::select(day_group, activity_provider, age_category, contributing_cost) %>%
         dplyr::arrange(.data$day_group, .data$age_category) %>%
         dplyr::group_by(.data$age_category) %>%
         dplyr::left_join(acs_spells %>% dplyr::group_by(age_category) %>% dplyr::summarise(age_cat_nums=dplyr::n()), by="age_category") %>%
         dplyr::mutate(contributing_cost = tidyr::replace_na(.data$contributing_cost, 0.0),
                       cum_sum = cumsum(.data$contributing_cost),
                       cum_sum_norm = cum_sum / age_cat_nums) %>%
         dplyr::group_by(.data$age_category, .data$day_group) %>%
         dplyr::slice(which.max(.data$cum_sum_norm)),
       aes(x=day_group,
           y=cum_sum_norm,
           group=age_category,
           color=age_category)) +
  geom_line(size=1.5) +
  theme_minimal() +
  scale_colour_brewer(type = "qual") +
  labs(title="Normalised cumulative spend relative to index ACS event",
       subtitle = "BNSSG Acute Coronary Syndrome Patients Jul 2018 - Dec 2021 (n=5743)",
       y="Cost (£) (cumulative, per patient)",
       x="Days from ACS",
       colour="Age category") +
  scale_y_continuous(labels = comma)




#Plots

# All spending
ggplot(data=full_data %>%
            dplyr::group_by(.data$pseudo_nhs_id) %>%
            dplyr::summarise(pt_total_cost = sum(.data$activity_cost, na.rm=T)) %>%
            dplyr::arrange(.data$pt_total_cost) %>%
            dplyr::mutate(cum_pt_cost = cumsum(.data$pt_total_cost),
                          cum_pct_cost = cum_pt_cost / sum(.data$pt_total_cost),
                          cum_centile  = seq(0,100, length=dplyr::n())),
       aes(x=cum_centile, y=cum_pct_cost)) + #, group=cut, fill=cut
  geom_point() +
  scale_color_brewer(palette = 'Set3') +
  scale_y_log10(labels = comma) +
  scale_x_log10() +
  labs(title="Cumulative (-90<days<365) spend by proportion of cohort",
       subtitle = "BNSSG Acute Coronary Syndrome Patients Jul 2018 - Dec 2021 (n=5743)",
       y="Cumulative Cost (£)",
       x="Proportion of cohort (%)",
       colour="Activity provider")

# Index ACS spending
ggplot(data=full_data %>%
         dplyr::filter(.data$activity_provider=="secondary" & .data$day_group==0) %>%
         dplyr::group_by(.data$pseudo_nhs_id, .data$activity_provider) %>%
         dplyr::summarise(pt_sub_cost = sum(.data$activity_cost, na.rm=T)) %>%
         dplyr::group_by(.data$activity_provider) %>%
         dplyr::arrange(.data$pt_sub_cost, .by_group = TRUE) %>%
         dplyr::mutate(cum_pt_act_cost = cumsum(.data$pt_sub_cost),
                       cum_pct_act_cost = cum_pt_act_cost / sum(.data$pt_sub_cost),
                       cum_act_centile  = seq(0,100, length=dplyr::n())),
       aes(x=cum_act_centile, y=cum_pt_act_cost, color=activity_provider)) + #, group=cut, fill=cut
  geom_point() +
  scale_color_brewer(palette = 'Set3') +
  labs(title="Cumulative ACS-event (day=0) spend by proportion of cohort",
       subtitle = "BNSSG Acute Coronary Syndrome Patients Jul 2018 - Dec 2021 (n=5743)",
       y="Cumulative Cost (£)",
       x="Proportion of cohort (%)",
       colour="Activity provider") +
  scale_y_continuous(labels = comma)

# Post ACS spending
ggplot(data=full_data %>%
            dplyr::filter(.data$day_group >0) %>%
            dplyr::group_by(.data$pseudo_nhs_id, .data$activity_provider) %>%
            dplyr::summarise(pt_sub_cost = sum(.data$activity_cost, na.rm=T)) %>%
            dplyr::group_by(.data$activity_provider) %>%
            dplyr::arrange(.data$pt_sub_cost, .by_group = TRUE) %>%
            dplyr::mutate(cum_pt_act_cost = cumsum(.data$pt_sub_cost),
                          cum_pct_act_cost = cum_pt_act_cost / sum(.data$pt_sub_cost),
                          cum_act_centile  = seq(0,100, length=dplyr::n())),
      aes(x=cum_act_centile, y=cum_pt_act_cost, color=activity_provider)) + #, group=cut, fill=cut
  geom_point() +
  scale_color_brewer(palette = 'Set3') +
  labs(title="Cumulative post-ACS (0<day<=365) spend by proportion of cohort",
       subtitle = "BNSSG Acute Coronary Syndrome Patients Jul 2018 - Dec 2021 (n=5743)",
       y="Cumulative Cost (£)",
       x="Proportion of cohort (%)",
       colour="Activity provider") +
  scale_y_continuous(labels = comma)

# Who are the bottom and top spenders in the year after the ACS event (inclusive of ACS event?
ids_top_5pct = full_data %>%
  dplyr::filter(.data$day_group >=0) %>%
  dplyr::group_by(.data$pseudo_nhs_id) %>%
  dplyr::summarise(pt_cost = sum(.data$activity_cost, na.rm=T)) %>%
  dplyr::arrange(.data$pt_cost) %>%
  dplyr::mutate(cum_pt_cost = cumsum(.data$pt_cost)) %>%
  dplyr::ungroup() %>%
  dplyr::slice_tail(prop=0.05) %>%
  dplyr::select(.data$pseudo_nhs_id) %>%
  dplyr::mutate(comp_group="top_5pct")
ids_bot_5pct = full_data %>%
  dplyr::filter(.data$day_group >=0) %>%
  dplyr::group_by(.data$pseudo_nhs_id) %>%
  dplyr::summarise(pt_cost = sum(.data$activity_cost, na.rm=T)) %>%
  dplyr::arrange(.data$pt_cost) %>%
  dplyr::mutate(cum_pt_cost = cumsum(.data$pt_cost)) %>%
  dplyr::ungroup() %>%
  dplyr::slice_head(prop=0.05) %>%
  dplyr::select(.data$pseudo_nhs_id) %>%
  dplyr::mutate(comp_group="bot_5pct")
foo=dplyr::left_join(rbind(ids_top_5pct, ids_bot_5pct), full_data_2, by="pseudo_nhs_id") %>%
  dplyr::filter(.data$day_group>=0) %>%
  dplyr::group_by(.data$pseudo_nhs_id, .data$activity_provider) %>%
  dplyr::mutate(pt_tot_cost = sum(.data$activity_cost, na.rm=T)) %>%
  dplyr::slice(n=1) %>%
  dplyr::group_by(.data$comp_group, .data$activity_provider) %>%
  dplyr::summarise(total_cost = sum(.data$pt_tot_cost, na.rm=T),
                   mean_cost  = mean(.data$pt_tot_cost, na.rm=T),
                   mean_age   = mean(age),
                   pct_male   = sum(sex=="male")/dplyr::n(),
                   pct_stemi  = sum(description_simple=="STEMI")/dplyr::n(),
                   pct_nstemi = sum(description_simple=="NSTEMI")/dplyr::n())

# Do the spends differ between those that had BP checks
load("nhs_bp_map.RData")
foo=dplyr::left_join(full_data_2, df, by="pseudo_nhs_id") %>%
  dplyr::filter(.data$day_group>=0) %>%
  dplyr::group_by(.data$pseudo_nhs_id, .data$activity_provider) %>%
  dplyr::mutate(pt_tot_cost = sum(.data$activity_cost, na.rm=T)) %>%
  dplyr::slice(n=1) %>%
  dplyr::group_by(.data$bloodpressure_control_1y, .data$activity_provider) %>%
  dplyr::summarise(n = dplyr::n(),
                   total_cost = sum(.data$pt_tot_cost, na.rm=T),
                   mean_cost  = mean(.data$pt_tot_cost, na.rm=T),
                   mean_age   = mean(age),
                   pct_male   = sum(sex=="male")/dplyr::n(),
                   pct_stemi  = sum(description_simple=="STEMI")/dplyr::n(),
                   pct_nstemi = sum(description_simple=="NSTEMI")/dplyr::n())

dplyr::left_join(full_data_2, df, by="pseudo_nhs_id") %>%
  dplyr::filter(.data$day_group>=0) %>%
  dplyr::group_by(.data$pseudo_nhs_id) %>%
  dplyr::slice(n=1) %>%
  dplyr::group_by(.data$bloodpressure_control_1y) %>%
  dplyr::summarise(n = dplyr::n())


# Do the spends differ between those that had HbA1c checks
load("nhs_bp_map.RData")
foo=dplyr::left_join(full_data_2, df, by="pseudo_nhs_id") %>%
  dplyr::filter(.data$day_group>=0) %>%
  dplyr::group_by(.data$pseudo_nhs_id, .data$activity_provider) %>%
  dplyr::mutate(pt_tot_cost = sum(.data$activity_cost, na.rm=T)) %>%
  dplyr::slice(n=1) %>%
  dplyr::group_by(.data$diabetic_control_1y, .data$activity_provider) %>%
  dplyr::summarise(n = dplyr::n(),
                   total_cost = sum(.data$pt_tot_cost, na.rm=T),
                   mean_cost  = mean(.data$pt_tot_cost, na.rm=T),
                   mean_age   = mean(age),
                   pct_male   = sum(sex=="male")/dplyr::n(),
                   pct_stemi  = sum(description_simple=="STEMI")/dplyr::n(),
                   pct_nstemi = sum(description_simple=="NSTEMI")/dplyr::n())

dplyr::left_join(full_data_2, df, by="pseudo_nhs_id") %>%
  dplyr::filter(.data$day_group>=0) %>%
  dplyr::group_by(.data$pseudo_nhs_id) %>%
  dplyr::slice(n=1) %>%
  dplyr::group_by(.data$diabetic_control_1y) %>%
  dplyr::summarise(n = dplyr::n())


# Pre ACS spending
ggplot(data=full_data %>%
         dplyr::filter(.data$day_group<0 & .data$day_group>=-90) %>%
         dplyr::group_by(.data$pseudo_nhs_id, .data$activity_provider) %>%
         dplyr::summarise(pt_sub_cost = sum(.data$activity_cost, na.rm=T)) %>%
         dplyr::group_by(.data$activity_provider) %>%
         dplyr::arrange(.data$pt_sub_cost, .by_group = TRUE) %>%
         dplyr::mutate(cum_pt_act_cost = cumsum(.data$pt_sub_cost),
                       cum_pct_act_cost = cum_pt_act_cost / sum(.data$pt_sub_cost),
                       cum_act_centile  = seq(0,100, length=dplyr::n())),
       aes(x=cum_act_centile, y=cum_pt_act_cost, color=activity_provider)) + #, group=cut, fill=cut
  geom_point() +
  scale_color_brewer(palette = 'Set3') +
  labs(title="Cumulative pre-ACS (-90<=day<0) spend by proportion of cohort",
       subtitle = "BNSSG Acute Coronary Syndrome Patients Jul 2017 - Dec 2021 (n=5743)",
       y="Cumulative Cost (£)",
       x="Proportion of cohort (%)",
       colour="Activity provider") +
  scale_y_continuous(labels = comma)

# Secondary care activity type - post ACS
ggplot(data=full_data %>%
            dplyr::filter(.data$activity_provider=="secondary" & .data$day_group >0) %>%
            dplyr::group_by(.data$pseudo_nhs_id, .data$activity_provider, .data$sec_act_type) %>%
            dplyr::summarise(pt_sub_sub_cost = sum(.data$activity_cost, na.rm=T)) %>%
            dplyr::group_by(.data$activity_provider, .data$sec_act_type) %>%
            dplyr::arrange(.data$pt_sub_sub_cost, .by_group = TRUE) %>%
            dplyr::mutate(cum_pt_act_sub_cost = cumsum(.data$pt_sub_sub_cost),
                          cum_pct_act_sub_cost = cum_pt_act_sub_cost / sum(.data$pt_sub_sub_cost),
                          cum_act_sub_centile  = seq(0,100, length=dplyr::n())),
       aes(x=cum_act_sub_centile, y=cum_pt_act_sub_cost, color=sec_act_type)) + #, group=cut, fill=cut
  geom_point() +
  scale_color_brewer(palette = 'Set3') +
  labs(title="Cumulative post-ACS (0<day<=365) secondary care spend by proportion of cohort",
       subtitle = "BNSSG Acute Coronary Syndrome Patients Jul 2017 - Dec 2021 (n=5743)",
       y="Cumulative Cost (£)",
       x="Proportion of cohort (%)",
       colour="Activity provider") +
  scale_y_continuous(labels = comma)

# Secondary care activity specialty - post ACS
ggplot(data=full_data %>%
            dplyr::filter(.data$activity_provider=="secondary" & .data$day_group >0) %>%
            dplyr::mutate(time_bins = cut(.data$day_group,
                                          breaks=c(-90,-30,-0.5,0.5,30,90,120,365),
                                          labels=c("-90 to -30","-30 to <0","index ACS day", ">0 to 30", "30 to 90", "90 to 120", "120 to 365"))) %>%
            dplyr::group_by(.data$activity_specialty) %>%
            dplyr::mutate(tot_order = sum(.data$activity_cost)) %>%
            dplyr::group_by(.data$activity_specialty, time_bins, tot_order, sec_act_type) %>%
            dplyr::summarise(total = sum(.data$activity_cost)) %>%
            dplyr::ungroup() %>%
            dplyr::mutate(activity_specialty = factor(.data$activity_specialty, levels = unique(.data$activity_specialty[order(.data$tot_order, decreasing=T)]), ordered = T)) %>%
            dplyr::filter(as.integer(.data$activity_specialty)<=10),
       aes(x=activity_specialty, y=total, fill=time_bins)) + #, group=cut, fill=cut
  geom_bar(stat="identity", position="stack") +
  labs(title="Total post-ACS (0<day<=365) secondary care spend by speciality (top 10)",
       subtitle = "BNSSG Acute Coronary Syndrome Patients Jul 2017 - Dec 2021 (n=5743)",
       y="Cost (£)",
       x="Speciality",
       fill="Time from ACS (days)") +
  theme(axis.text.x = element_text(angle = 45, vjust=1, hjust=1)) +
  scale_y_continuous(labels = comma) +
  scale_fill_viridis_d(option="magma", begin = 0.2, end=0.8)



# Secondary care activity specialty - post ACS - HEATMAP
x_bin = 30
y_len = 15
my_breaks = c(rev(seq(-x_bin/2, -90-x_bin,by=-x_bin)), seq(x_bin/2,365+x_bin,by=x_bin))
my_labels = as.character(zoo::rollmean(my_breaks, 2))
ggplot(data=full_data %>%
         dplyr::filter(.data$activity_provider=="secondary") %>%
         dplyr::mutate(time_bins = cut(.data$day_group, breaks=my_breaks, labels=my_labels)) %>%
         dplyr::group_by(activity_specialty, time_bins, sec_act_type) %>%
         dplyr::summarise(total = sum(.data$activity_cost)) %>%
         dplyr::ungroup() %>%
         dplyr::mutate(activity_specialty = factor(.data$activity_specialty, levels = unique(.data$activity_specialty[order(.data$total, decreasing=T)]), ordered = T)) %>%
         dplyr::filter(as.integer(.data$activity_specialty)<=y_len),
       aes(x=time_bins, y=activity_specialty, fill=total)) +
  geom_tile() +
  labs(title="ACS secondary care spend by speciality (-90<day<=365)",
       subtitle = "BNSSG Acute Coronary Syndrome Patients Jul 2017 - Dec 2021 (n=5743)",
       y="Speciality",
       x="Time (days)",
       fill="Total cost (£)") +
  scale_fill_viridis_c(option="magma", begin = 0, end=1, labels = comma,
                       breaks = c(1, 1000, 100000, 10000000),
                       trans=scales::pseudo_log_trans(sigma = 100, base = exp(1))) +
  facet_wrap(~ sec_act_type)

# Secondary care activity specialty by HbA1c check - HEATMAP
x_bin = 30
y_len = 15
my_breaks = c(rev(seq(-x_bin/2, -90-x_bin,by=-x_bin)), seq(x_bin/2,365+x_bin,by=x_bin))
my_labels = as.character(zoo::rollmean(my_breaks, 2))
ggplot(data=full_data %>%
         dplyr::filter(.data$activity_provider=="secondary") %>%
         dplyr::mutate(time_bins = cut(.data$day_group, breaks=my_breaks, labels=my_labels)) %>%
         dplyr::group_by(activity_specialty, time_bins, sec_act_type) %>%
         dplyr::summarise(total = sum(.data$activity_cost)) %>%
         dplyr::ungroup() %>%
         dplyr::mutate(activity_specialty = factor(.data$activity_specialty, levels = unique(.data$activity_specialty[order(.data$total, decreasing=T)]), ordered = T)) %>%
         dplyr::filter(as.integer(.data$activity_specialty)<=y_len) %>%

         # need to enter the HbA1c and lipid codes here,
         #
         #
         dplyr::mutate(hba1c_check = sample(c("hba1c", "no hba1c"), nrow(.), replace=TRUE)),
       #
       #
       #


       aes(x=time_bins, y=activity_specialty, fill=total)) +
  geom_tile() +
  labs(title="ACS secondary care spend by speciality (-90<day<=365)",
       subtitle = "BNSSG Acute Coronary Syndrome Patients Jul 2017 - Dec 2021 (n=5743)",
       y="Speciality",
       x="Time (days)",
       fill="Total cost (£)") +
  scale_fill_viridis_c(option="viridis", begin = 0, end=1, labels = comma,
                       breaks = c(1, 1000, 100000, 10000000),
                       trans=scales::pseudo_log_trans(sigma = 100, base = exp(1))) +
  facet_grid(hba1c_check ~ sec_act_type)


