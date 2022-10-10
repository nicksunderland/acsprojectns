##################################################################
##                     Libraries and stuff                      ##
##################################################################
library(usethis)
library(devtools)
library(ggplot2)
rm(list=ls())
load_all()
source(system.file("scripts/0.init.R", package = "acsprojectns"))

# Get the paths of the map geometries
geo_filepaths <- list.files(path = system.file("geojson_files/bnssg", package="acsprojectns"), full.names = T)

# Choose area type
area_type = "msoa"
stopifnot("Area type must be one of: 'ward', 'lsoa', 'msoa'" = area_type %in% c("ward", "lsoa", "msoa"))
region_labels = list(msoa="MSOA11NM", lsoa="LSOA11CD", ward="WARDCD")
geo_plot_data <- data.frame()

# Combine the geometries
for (i in 1:length(geo_filepaths)){
  if(stringr::str_detect(geo_filepaths[i], area_type)){
    geojson_data <- geojsonio::geojson_read(geo_filepaths[i], what = 'sp')
    tidy_df = broom::tidy(geojson_data, region = region_labels[[area_type]])
    geo_plot_data     <- rbind(geo_plot_data, tidy_df)
  }
}
geo_plot_data <- geo_plot_data %>%
  dplyr::rename(msoa_code = id)

# Get all of the ACS data for the area
acs_codes           <- get_codes(file_path = system.file("icd10_codes/acs_codes.csv",                       package = "acsprojectns"), append_character = "-")
bnssg_ccg_codes     <- get_codes(file_path = system.file("ccg_provider_codes/bnssg_ccg_provider_codes.csv", package = "acsprojectns"))

# All ACS spells
acs_admissions <- get_hospital_admissions(
  sus_apc_obj            = db_conn_struct$sus_apc,
  ccg_provider_code_list = bnssg_ccg_codes$ccg_provider_code,
  level                  = "spell",
  codes_list             = acs_codes$icd_codes,
  search_strategy        = "primary_diagnosis",
  search_strat_adm_method= "emergency",
  return_all_codes       = FALSE,
  datetime_window        = lubridate::interval(acs_date_window_min, acs_date_window_max, tzone = "GMT"),
  verbose                = FALSE)

# The pseudo_nhs_id and the associated LSOA code from the SWD attribute history table (this should be more complete than just the attributes table)
var_dict <- create_data_table_variable_dictionary(db_conn_struct$swd_att_hist$table_name)
vars     <- c(pseudo_nhs_id                      = var_dict[["pseudo_nhs_id"]],
              attribute_period_date              = var_dict[["attribute_period_date"]],
              lower_layer_super_output_area_code = var_dict[["lower_layer_super_output_area_code"]])
ids      <- acs_admissions$pseudo_nhs_id
patient_lsoa_codes <- db_conn_struct$swd_att_hist$data %>%                     #a reference to the SQL table
  dplyr::select(dplyr::all_of(vars)) %>%                                 #select the variables to work with
  dplyr::distinct() %>%
  dplyr::filter(!is.na(.data$pseudo_nhs_id) &                                 #leave if no id, as can't do anything with these
                !is.na(.data$attribute_period_date) &
                .data$pseudo_nhs_id %in% ids &
                .data$attribute_period_date <= acs_date_window_max &
                .data$attribute_period_date >= acs_date_window_min) %>%
  dplyr::group_by(.data$pseudo_nhs_id) %>%
  dplyr::slice_max(.data$attribute_period_date, with_ties = FALSE) %>%
  dplyr::select(-.data$attribute_period_date) %>%
  dplyr::collect() %>%
  dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id))

# The descriptions of each LSOA (what MSOA they belong to, their population size, etc.)
var_dict  <- create_data_table_variable_dictionary(db_conn_struct$swd_lsoa$table_name)
vars      <- c(lower_layer_super_output_area_code                = var_dict[["lower_layer_super_output_area_code"]],
               lower_layer_super_output_area_name                = var_dict[["lower_layer_super_output_area_name"]],
               lower_layer_super_output_area_16plus_population   = var_dict[["lower_layer_super_output_area_16plus_population"]],
               lower_layer_super_output_area_pct_ethnic_minority = var_dict[["lower_layer_super_output_area_pct_ethnic_minority"]],
               middle_layer_super_output_area_name               = var_dict[["middle_layer_super_output_area_name"]],
               area_ward_name                                    = var_dict[["area_ward_name"]],
               lsoa_index_of_multiple_deprivation_centile        = var_dict[["lsoa_index_of_multiple_deprivation_centile"]])

lsoa_descriptions_1 <- db_conn_struct$swd_lsoa$data %>%                                  #a reference to the SQL table
  dplyr::select(dplyr::all_of(vars)) %>%                                 #select the variables to work with
  dplyr::distinct() %>%
  dplyr::collect() %>%
  dplyr::mutate(msoa_code = substr(.data$lower_layer_super_output_area_name, 1, nchar(.data$lower_layer_super_output_area_name)-1)) %>%
  dplyr::group_by(.data$msoa_code) %>%
  dplyr::mutate(msoa_population_size      = sum(.data$lower_layer_super_output_area_16plus_population),
                msoa_av_ethnic_population = mean(.data$lower_layer_super_output_area_pct_ethnic_minority),
                msoa_av_imd_centile       = mean(as.numeric(.data$lsoa_index_of_multiple_deprivation_centile)))

# Get other area indicators from the attributes table
var_dict        <- create_data_table_variable_dictionary(db_conn_struct$swd_att$table_name)       #create the named list to convert to standardised naming
base_vars       <- c(lower_layer_super_output_area_code   = var_dict[["lower_layer_super_output_area_code"]],                               #grab the basic variables we'll need
                     index_of_multiple_deprivation_decile = var_dict[["index_of_multiple_deprivation_decile"]],
                     income_decile                        = var_dict[["income_decile"]],
                     employment_decile                    = var_dict[["employment_decile"]],
                     air_quality_indicator                = var_dict[["air_quality_indicator"]],
                     lsoa_av_distance_to_gp_km            = var_dict[["lsoa_av_distance_to_gp_km"]],
                     lsoa_num_takeaways                   = var_dict[["lsoa_num_takeaways"]])

lsoa_descriptions_2 <- db_conn_struct$swd_att$data %>%
  dplyr::select(dplyr::all_of(base_vars)) %>%
  dplyr::collect() %>%
  dplyr::filter(!is.na(.data$lower_layer_super_output_area_code)) %>%
  dplyr::group_by(.data$lower_layer_super_output_area_code) %>%
  dplyr::slice_min(.data$lower_layer_super_output_area_code, with_ties=F) %>%
  dplyr::collect() %>%
  dplyr::mutate(lower_layer_super_output_area_code = toupper(.data$lower_layer_super_output_area_code))

# Merge the patient id and LSOA with the LSOA description data
full_lsoa_descriptions <- dplyr::left_join(lsoa_descriptions_1, lsoa_descriptions_2, by = 'lower_layer_super_output_area_code') %>%
  dplyr::group_by(.data$msoa_code) %>%
  dplyr::mutate(msoa_av_employment_decile = mean(.data$employment_decile, rm.na=T),
                msoa_av_income_decile     = mean(.data$income_decile, rm.na=T),
                msoa_av_air_quality       = mean(.data$air_quality_indicator, rm.na=T),
                msoa_av_km_to_gp          = mean(.data$lsoa_av_distance_to_gp_km, rm.na=T),
                msoa_av_num_takeaways     = mean(.data$lsoa_num_takeaways, rm.na=T)) %>%
  dplyr::ungroup()

# Associate the nhs ids with the lsoa data we've just gathered
id_lsoa_link <- dplyr::left_join(patient_lsoa_codes, full_lsoa_descriptions, by = 'lower_layer_super_output_area_code')

# Work out the incidence per 1000 per year
area_data <- id_lsoa_link %>%
  dplyr::right_join(acs_admissions, by=c("pseudo_nhs_id")) %>%

  # CHECK HOW MANY NAs WE ACTUALLY REMOVE HERE

  dplyr::filter(!is.na(.data$msoa_code)) %>%
  dplyr::group_by(.data$msoa_code) %>%
  dplyr::mutate(msoa_incidence_per1000_peryear = ((dplyr::n() / .data$msoa_population_size)*1000) / (lubridate::interval(acs_date_window_min,acs_date_window_max)/lubridate::years(1))) %>%
  dplyr::slice_head(n=1) %>%
  dplyr::select(.data$msoa_code,
                .data$msoa_incidence_per1000_peryear,
                .data$msoa_av_ethnic_population,
                .data$msoa_av_imd_centile,
                .data$msoa_av_employment_decile,
                .data$msoa_av_income_decile,
                .data$msoa_av_air_quality,
                .data$msoa_av_km_to_gp,
                .data$msoa_av_num_takeaways)




# Now work out the age-adjusted rates across the following age categories
age_breaks     = seq(0,120,20)
{

  # Get the lsoa : msoa code map
  lmlink <- full_lsoa_descriptions[c("lower_layer_super_output_area_code", "msoa_code")]

  # The swd attribute table variable dictionary
  var_dict <- create_data_table_variable_dictionary(db_conn_struct$swd_att_hist$table_name)

  # The variables we want
  vars     <- c(pseudo_nhs_id                      = var_dict[["pseudo_nhs_id"]],
                attribute_period_date              = var_dict[["attribute_period_date"]],
                age                                = var_dict[["age"]],
                lower_layer_super_output_area_code = var_dict[["lower_layer_super_output_area_code"]])

  # Get all the swd ids that ever existed and their last age
  swd_age_msoa <- db_conn_struct$swd_att_hist$data %>%
    dplyr::select(dplyr::all_of(vars)) %>%
    dplyr::distinct() %>%
    dplyr::collect() %>%
    dplyr::group_by(.data$pseudo_nhs_id) %>%
    dplyr::slice_max(.data$attribute_period_date, with_ties = FALSE) %>%
    dplyr::ungroup() %>%
    dplyr::left_join(lmlink, by="lower_layer_super_output_area_code") %>%
    dplyr::select(pseudo_nhs_id, msoa_code, age) %>%
    dplyr::mutate(age_cat = cut(age, breaks=age_breaks, right=FALSE, labels=paste(age_breaks[1:length(age_breaks)-1], age_breaks[2:length(age_breaks)]-1, sep="-"))) %>%
    dplyr::ungroup()


  # The swd attribute table variable dictionary
  var_dict <- create_data_table_variable_dictionary(db_conn_struct$swd_att$table_name)
  # The variables we want
  vars     <- c(age                                = var_dict[["age"]],
                lower_layer_super_output_area_code = var_dict[["lower_layer_super_output_area_code"]])

  # Get all of the swd ids, the ages, and the lsoa code
  msoa_age_cats <- db_conn_struct$swd_att$data %>%
    dplyr::select(dplyr::all_of(vars)) %>%
    dplyr::distinct() %>%
    dplyr::collect() %>%
    dplyr::mutate(age_cat = cut(age, breaks=age_breaks, right=FALSE, labels=paste(age_breaks[1:length(age_breaks)-1], age_breaks[2:length(age_breaks)]-1, sep="-"))) %>%
    dplyr::left_join(lmlink, by="lower_layer_super_output_area_code") %>%
    dplyr::group_by(msoa_code, age_cat) %>%
    dplyr::summarise(msoa_age_n = dplyr::n()) %>%
    dplyr::ungroup() %>%
    tidyr::complete(msoa_code, age_cat, fill=list(msoa_age_n=0))

  # Get the number of acs events in each age category in each mosa
  num_acs_msoa <- acs_admissions %>%
    dplyr::left_join(swd_age_msoa, by=c("pseudo_nhs_id")) %>%
    dplyr::filter(!is.na(.data$msoa_code)) %>%
    dplyr::group_by(msoa_code, age_cat) %>%
    dplyr::summarise(acs_n = dplyr::n())

  # Get the final age adjustment table
  age_adjusted_msoa_acs_rates <- num_acs_msoa %>%
    dplyr::left_join(msoa_age_cats, by=c("msoa_code", "age_cat")) %>%
    dplyr::mutate(age_rate = acs_n / msoa_age_n) %>%
    dplyr::left_join(msoa_age_cats %>%
                       dplyr::group_by(age_cat) %>%
                       dplyr::summarise(tot_age_cat_n = sum(msoa_age_n)), by=c("age_cat")) %>%
    dplyr::mutate(exp_events = age_rate*tot_age_cat_n) %>%
    dplyr::group_by(msoa_code) %>%
    dplyr::summarise(age_adj_rate_1000 = ((sum(exp_events) / sum(tot_age_cat_n))*1000) / (lubridate::interval(acs_date_window_min,acs_date_window_max)/lubridate::years(1)))
}

# Merge the ACS incidence value with the geo data
geo_data = purrr::reduce(list(geo_plot_data, area_data, age_adjusted_msoa_acs_rates), dplyr::left_join, by="msoa_code")


# Plot the maps
library(ggplot2)
library(viridis)
library(bigsnpr)
# Plot the ACS incidence map
scale_breaks  = unique(c(floor(min(trunc(geo_data$msoa_incidence_per1000_peryear*100)/100, na.rm=T)),
                         round(quantile(geo_data$msoa_incidence_per1000_peryear, 0.05, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_incidence_per1000_peryear, 0.10, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_incidence_per1000_peryear, 0.25, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_incidence_per1000_peryear, 0.50, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_incidence_per1000_peryear, 0.75, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_incidence_per1000_peryear, 0.90, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_incidence_per1000_peryear, 0.95, na.rm=T)[[1]], digits=0),
                         ceiling(max(ceiling(geo_data$msoa_incidence_per1000_peryear*100)/100, na.rm=T))))
scale_labels = c()
for(x in seq_along(scale_breaks)){
  if(x!=length(scale_breaks)){
    scale_labels = c(scale_labels, paste0(as.character(scale_breaks[x]),"-",as.character(scale_breaks[x+1])))
  }
}
acs_incidence_plot <- ggplot() +
  geom_polygon(data=geo_data, aes(x=long, y=lat, group=group, fill=cut(msoa_incidence_per1000_peryear, breaks=scale_breaks, labels=scale_labels)), alpha=0.9, colour="white", size=0.1) +
  theme_void() +
  coord_map() +
  labs(title = "Acute Coronary Syndrome Admissions", subtitle = "BNSSG middle layer super output areas (July 2018 - Dec 2021)", fill="Events / 1000 population / year") +
  scale_fill_viridis_d(option = "plasma", begin = 0.0, end=0.8) +
  scale_color_discrete(guide = "none") +
  theme(plot.title      = element_text(size= 16, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.40, l = 2, unit = "cm")),
        plot.subtitle   = element_text(size= 12, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.43, l = 2, unit = "cm")),
        legend.position = c(0.20, 0.65)
  )
acs_incidence_plot

# Plot the ethnic minority population map
scale_breaks  = unique(c(min(trunc(geo_data$msoa_av_ethnic_population*100)/100, na.rm=T),
                         round(quantile(geo_data$msoa_av_ethnic_population, 0.05, na.rm=T)[[1]], digits=2),
                         round(quantile(geo_data$msoa_av_ethnic_population, 0.10, na.rm=T)[[1]], digits=2),
                         round(quantile(geo_data$msoa_av_ethnic_population, 0.25, na.rm=T)[[1]], digits=2),
                         round(quantile(geo_data$msoa_av_ethnic_population, 0.50, na.rm=T)[[1]], digits=2),
                         round(quantile(geo_data$msoa_av_ethnic_population, 0.75, na.rm=T)[[1]], digits=2),
                         round(quantile(geo_data$msoa_av_ethnic_population, 0.90, na.rm=T)[[1]], digits=2),
                         round(quantile(geo_data$msoa_av_ethnic_population, 0.95, na.rm=T)[[1]], digits=2),
                         max(ceiling(geo_data$msoa_av_ethnic_population*100)/100, na.rm=T)))
scale_labels = c()
for(x in seq_along(scale_breaks)){
  if(x!=length(scale_breaks)){
    scale_labels = c(scale_labels, paste0(as.character(scale_breaks[x]),"-",as.character(scale_breaks[x+1])))
  }
}
ethnicity_pct_plot <- ggplot() +
  geom_polygon(data=geo_data, aes(x=long, y=lat, group=group, fill=cut(msoa_av_ethnic_population, breaks=scale_breaks, labels=scale_labels)), alpha=0.9, colour="gray", size=0.1) +
  theme_void() +
  coord_map() +
  labs(title = "Ethnic minority population percentage", subtitle = "BNSSG middle layer super output areas", fill="Percentage (%)") +
  scale_fill_viridis_d() +
  scale_color_discrete(guide = "none") +
  theme(plot.title      = element_text(size= 16, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.40, l = 2, unit = "cm")),
        plot.subtitle   = element_text(size= 12, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.43, l = 2, unit = "cm")),
        legend.position = c(0.20, 0.65)
  )
ethnicity_pct_plot

# Plot average MSOA IMD centile map
scale_breaks  = unique(c(min(trunc(geo_data$msoa_av_imd_centile*100)/100, na.rm=T),
                         round(quantile(geo_data$msoa_av_imd_centile, 0.05, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_imd_centile, 0.10, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_imd_centile, 0.25, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_imd_centile, 0.50, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_imd_centile, 0.75, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_imd_centile, 0.90, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_imd_centile, 0.95, na.rm=T)[[1]], digits=0),
                         max(ceiling(geo_data$msoa_av_imd_centile*100)/100, na.rm=T)))
scale_labels = c()
for(x in seq_along(scale_breaks)){
  if(x!=length(scale_breaks)){
    scale_labels = c(scale_labels, paste0(as.character(scale_breaks[x]),"-",as.character(scale_breaks[x+1])))
  }
}
imd_centile_plot <- ggplot() +
  geom_polygon(data=geo_data, aes(x=long, y=lat, group=group, fill=msoa_av_imd_centile), alpha=0.9, colour="gray", size=0.1) +
  theme_void() +
  coord_map() +
  labs(title = "Average MSOA index of multiple deprivation centile", subtitle = "BNSSG middle layer super output areas", fill="Average deprivation centile \n(low=less, high=more)") +
  scale_fill_viridis_c() +
  theme(plot.title      = element_text(size= 16, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.40, l = 2, unit = "cm")),
        plot.subtitle   = element_text(size= 12, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.43, l = 2, unit = "cm")),
        legend.position = c(0.20, 0.65)
  )
imd_centile_plot

# Plot average MSOA employment decile map
scale_breaks  = unique(c(min(trunc(geo_data$msoa_av_employment_decile*100)/100, na.rm=T),
                         round(quantile(geo_data$msoa_av_employment_decile, 0.05, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_employment_decile, 0.10, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_employment_decile, 0.25, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_employment_decile, 0.50, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_employment_decile, 0.75, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_employment_decile, 0.90, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_employment_decile, 0.95, na.rm=T)[[1]], digits=0),
                         max(ceiling(geo_data$msoa_av_employment_decile*100)/100, na.rm=T)))
scale_labels = c()
for(x in seq_along(scale_breaks)){
  if(x!=length(scale_breaks)){
    scale_labels = c(scale_labels, paste0(as.character(scale_breaks[x]),"-",as.character(scale_breaks[x+1])))
  }
}
employment_decile_plot <- ggplot() +
  geom_polygon(data=geo_data, aes(x=long, y=lat, group=group, fill=msoa_av_employment_decile), alpha=0.9, colour="gray", size=0.1) +
  theme_void() +
  coord_map() +
  labs(title = "Average MSOA employment decile", subtitle = "BNSSG middle layer super output areas", fill="Average employment decile") +
  scale_fill_viridis_c() +
  theme(plot.title      = element_text(size= 16, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.40, l = 2, unit = "cm")),
        plot.subtitle   = element_text(size= 12, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.43, l = 2, unit = "cm")),
        legend.position = c(0.20, 0.65)
  )
employment_decile_plot

# Plot average MSOA income decile map
scale_breaks  = unique(c(min(trunc(geo_data$msoa_av_income_decile*100)/100, na.rm=T),
                         round(quantile(geo_data$msoa_av_income_decile, 0.05, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_income_decile, 0.10, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_income_decile, 0.25, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_income_decile, 0.50, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_income_decile, 0.75, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_income_decile, 0.90, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_income_decile, 0.95, na.rm=T)[[1]], digits=0),
                         max(ceiling(geo_data$msoa_av_income_decile*100)/100, na.rm=T)))
scale_labels = c()
for(x in seq_along(scale_breaks)){
  if(x!=length(scale_breaks)){
    scale_labels = c(scale_labels, paste0(as.character(scale_breaks[x]),"-",as.character(scale_breaks[x+1])))
  }
}
income_decile_plot <- ggplot() +
  geom_polygon(data=geo_data, aes(x=long, y=lat, group=group, fill=msoa_av_income_decile), alpha=0.9, colour="gray", size=0.1) +
  theme_void() +
  coord_map() +
  labs(title = "Average MSOA income decile", subtitle = "BNSSG middle layer super output areas", fill="Average income decile") +
  scale_fill_viridis_c() +
  theme(plot.title      = element_text(size= 16, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.40, l = 2, unit = "cm")),
        plot.subtitle   = element_text(size= 12, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.43, l = 2, unit = "cm")),
        legend.position = c(0.20, 0.65)
  )
income_decile_plot

# Plot average MSOA air quality indicator map
scale_breaks  = unique(c(min(trunc(geo_data$msoa_av_air_quality*100)/100, na.rm=T),
                         round(quantile(geo_data$msoa_av_air_quality, 0.05, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_air_quality, 0.10, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_air_quality, 0.25, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_air_quality, 0.50, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_air_quality, 0.75, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_air_quality, 0.90, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_air_quality, 0.95, na.rm=T)[[1]], digits=0),
                         max(ceiling(geo_data$msoa_av_air_quality*100)/100, na.rm=T)))
scale_labels = c()
for(x in seq_along(scale_breaks)){
  if(x!=length(scale_breaks)){
    scale_labels = c(scale_labels, paste0(as.character(scale_breaks[x]),"-",as.character(scale_breaks[x+1])))
  }
}
air_quality_plot <- ggplot() +
  geom_polygon(data=geo_data, aes(x=long, y=lat, group=group, fill=msoa_av_air_quality), alpha=0.9, colour="gray", size=0.1) +
  theme_void() +
  coord_map() +
  labs(title = "Average MSOA air quailty indicator", subtitle = "BNSSG middle layer super output areas", fill="Average air quailty indicator") +
  scale_fill_viridis_c() +
  theme(plot.title      = element_text(size= 16, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.40, l = 2, unit = "cm")),
        plot.subtitle   = element_text(size= 12, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.43, l = 2, unit = "cm")),
        legend.position = c(0.20, 0.65)
  )
air_quality_plot

# Plot average number of takeaways MSOA
scale_breaks  = unique(c(min(trunc(geo_data$msoa_av_num_takeaways*100)/100, na.rm=T),
                         round(quantile(geo_data$msoa_av_num_takeaways, 0.05, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_num_takeaways, 0.10, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_num_takeaways, 0.25, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_num_takeaways, 0.50, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_num_takeaways, 0.75, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_num_takeaways, 0.90, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_num_takeaways, 0.95, na.rm=T)[[1]], digits=0),
                         max(ceiling(geo_data$msoa_av_num_takeaways*100)/100, na.rm=T)))
scale_labels = c()
for(x in seq_along(scale_breaks)){
  if(x!=length(scale_breaks)){
    scale_labels = c(scale_labels, paste0(as.character(scale_breaks[x]),"-",as.character(scale_breaks[x+1])))
  }
}
num_takeaways_plot <- ggplot() +
  geom_polygon(data=geo_data, aes(x=long, y=lat, group=group, fill=msoa_av_num_takeaways), alpha=0.9, colour="gray", size=0.1) +
  theme_void() +
  coord_map() +
  labs(title = "Average number of local (LSOA) takeaways", subtitle = "BNSSG middle layer super output areas", fill="Number of local takeaways") +
  scale_fill_viridis_c() +
  theme(plot.title      = element_text(size= 16, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.40, l = 2, unit = "cm")),
        plot.subtitle   = element_text(size= 12, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.43, l = 2, unit = "cm")),
        legend.position = c(0.20, 0.65)
  )
num_takeaways_plot

# Plot average MSOA distance to GP
scale_breaks  = unique(c(min(trunc(geo_data$msoa_av_km_to_gp*100)/100, na.rm=T),
                         round(quantile(geo_data$msoa_av_km_to_gp, 0.05, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_km_to_gp, 0.10, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_km_to_gp, 0.25, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_km_to_gp, 0.50, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_km_to_gp, 0.75, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_km_to_gp, 0.90, na.rm=T)[[1]], digits=0),
                         round(quantile(geo_data$msoa_av_km_to_gp, 0.95, na.rm=T)[[1]], digits=0),
                         max(ceiling(geo_data$msoa_av_km_to_gp*100)/100, na.rm=T)))
scale_labels = c()
for(x in seq_along(scale_breaks)){
  if(x!=length(scale_breaks)){
    scale_labels = c(scale_labels, paste0(as.character(scale_breaks[x]),"-",as.character(scale_breaks[x+1])))
  }
}
gp_distance_plot <- ggplot() +
  geom_polygon(data=geo_data, aes(x=long, y=lat, group=group, fill=msoa_av_km_to_gp), alpha=0.9, colour="gray", size=0.1) +
  theme_void() +
  coord_map() +
  labs(title = "Average distance to GP practice", subtitle = "BNSSG middle layer super output areas", fill="Average distance (km)") +
  scale_fill_viridis_c() +
  theme(plot.title      = element_text(size= 16, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.40, l = 2, unit = "cm")),
        plot.subtitle   = element_text(size= 12, hjust=0.01, color = "#4e4d47", margin = margin(b = -0.1, t = 0.43, l = 2, unit = "cm")),
        legend.position = c(0.20, 0.65)
  )
gp_distance_plot


# Plot ethnicity percentage against ACS rate
ggplot(data=geo_data%>%dplyr::group_by(id)%>%dplyr::slice(1),
       aes(x=msoa_av_ethnic_population, y=msoa_incidence_per1000_peryear)) +
  geom_point() +
  geom_smooth(method=lm , color="red", fill="#69b3a2", se=TRUE) +
  labs(title = "Local ACS incidence by local ethnic minority percentage",
       subtitle = "BNSSG middle layer super output areas (MSOA)",
       x = "Local ethnic minority percentage (%)",
       y = "Local ACS incidence (per 1000 population per year)")

# Plot average MSOA IMD centile against ACS rate
ggplot(data=geo_data%>%dplyr::group_by(id)%>%dplyr::slice(1),
       aes(x=msoa_av_imd_centile, y=msoa_incidence_per1000_peryear)) +
  geom_point() +
  geom_smooth(method=lm , color="red", fill="#69b3a2", se=TRUE) +
  labs(title = "Local ACS incidence by local Index of Multiple Deprivation (IMD) centile",
       subtitle = "BNSSG middle layer super output areas (MSOA)",
       x = "Local Index of Multiple Deprivation (IMD) centile",
       y = "Local ACS incidence (per 1000 population per year)")

# Plot average MSOA GP distance against ACS rate
ggplot(data=geo_data%>%dplyr::group_by(id)%>%dplyr::slice(1),
       aes(x=msoa_av_km_to_gp, y=msoa_incidence_per1000_peryear)) +
  geom_point() +
  geom_smooth(method=lm , color="red", fill="#69b3a2", se=TRUE) +
  labs(title = "Local ACS incidence by distance to GP practice",
       subtitle = "BNSSG middle layer super output areas (MSOA)",
       x = "Average distance to GP practice (km)",
       y = "Local ACS incidence (per 1000 population per year)")

# Plot average MSOA air quailty against ACS rate
ggplot(data=geo_data%>%dplyr::group_by(id)%>%dplyr::slice(1),
       aes(x=msoa_av_air_quality, y=msoa_incidence_per1000_peryear)) +
  geom_point() +
  geom_smooth(method=lm , color="red", fill="#69b3a2", se=TRUE) +
  labs(title = "Local ACS incidence by local air quailty",
       subtitle = "BNSSG middle layer super output areas (MSOA)",
       x = "Average air quality indicator (higher=worse)",
       y = "Local ACS incidence (per 1000 population per year)")

