#' load_demographics
#'
#' @param db_conn_struct list of Database objects
#' @param patient_ids list of Patient objects
#'
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @return modifies the patient objects in-place
#' @export
#'
load_demographics_v2 <- function(db_conn_struct,
                                 patient_ids){

  # Timing checks
  ptm <- proc.time() # Start the clock

  # Checks and adjustments
  stopifnot(all(purrr::map_lgl(db_conn_struct, ~ checkmate::test_class(.x, "Database"))),
            all(purrr::map_lgl(patient_ids,    ~ is.double(.x))))

  # Starting variables
  . = NULL


  # Extract data from each database / datatable as appropriate
  if(db_conn_struct$sus_apc$is_connected){

    var_dict        <- create_data_table_variable_dictionary(db_conn_struct$sus_apc$table_name)            #create the named list to convert to standardised naming
    base_vars       <- c(pseudo_nhs_id        = var_dict[["pseudo_nhs_id"]],            #grab the basic variables we'll need
                         episode_start_date   = var_dict[["episode_start_date"]],
                         age                  = var_dict[["age"]],
                         sex                  = var_dict[["sex"]],
                         ethnicity            = var_dict[["ethnicity"]])

    # Auto-create the SQL query and execute
    # Had to use filter_at() instead of newer filter(dplyr::across()) as I think it struggles to convert to SQL code
    #     https://stackoverflow.com/questions/26497751/pass-a-vector-of-variable-names-to-arrange-in-dplyr
    #     https://dplyr.tidyverse.org/articles/programming.html#fnref1
    sus_apc_data <- db_conn_struct$sus_apc$data %>%                                       #a reference to the SQL table
      dplyr::select(dplyr::all_of(base_vars)) %>%                                 #select the variables to work with
      dplyr::distinct() %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &                                 #leave if no id, as can't do anything with these
                    !is.na(.data$episode_start_date) &
                    !is.na(.data$age) &
                    .data$pseudo_nhs_id %in% patient_ids) %>%
      dplyr::collect() %>%
      dplyr::group_by(.data$pseudo_nhs_id) %>%
      dplyr::mutate(min_dob = lubridate::as_datetime(.data$episode_start_date, tz = "GMT") - lubridate::duration(as.numeric(.data$age)+1.0, "years")
                                                                                           + lubridate::duration(1, "day"),
                    max_dob = lubridate::as_datetime(.data$episode_start_date, tz = "GMT") - lubridate::duration(as.numeric(.data$age), "years"),
                    pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id),
                    sex = as.character(.data$sex)) %>%
      dplyr::group_by(.data$pseudo_nhs_id) %>%
      dplyr::mutate(dob_interval_est = lubridate::interval(max(.data$min_dob), min(.data$max_dob)),
                    dob_estimate = lubridate::int_start(.data$dob_interval_est) + (lubridate::int_end(.data$dob_interval_est) - lubridate::int_start(.data$dob_interval_est))/2.0) %>%
      dplyr::ungroup() %>%
      dplyr::distinct(.data$pseudo_nhs_id,
                      .data$dob_estimate,
                      .data$sex,
                      .data$ethnicity)

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = patient_ids)
    sus_apc_data <- dplyr::left_join(ids, sus_apc_data, by = "pseudo_nhs_id")

  }

  # Extract data from each database / datatable as appropriate
  if(db_conn_struct$swd_att_hist$is_connected & db_conn_struct$swd_lsoa$is_connected){

    var_dict        <- create_data_table_variable_dictionary(db_conn_struct$swd_att_hist$table_name)       #create the named list to convert to standardised naming
    base_vars       <- c(pseudo_nhs_id                                 = var_dict[["pseudo_nhs_id"]],                               #grab the basic variables we'll need
                         attribute_period_date                         = var_dict[["attribute_period_date"]],
                         sex                                           = var_dict[["sex"]],
                         ethnicity                                     = var_dict[["ethnicity"]],
                         lower_layer_super_output_area_code            = var_dict[["lower_layer_super_output_area_code"]],
                         gp_practice_code                              = var_dict[["gp_practice_code"]])

    # This is a history of every month's data download; therefore need to take the most recent non-NA value for each variable
    swd_att_hist_data <- db_conn_struct$swd_att_hist$data %>%                     #a reference to the SQL table
      dplyr::select(dplyr::all_of(base_vars)) %>%                                 #select the variables to work with
      dplyr::distinct() %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &                                 #leave if no id, as can't do anything with these
                    .data$pseudo_nhs_id %in% patient_ids) %>%
      dplyr::collect() %>%
      dplyr::arrange(dplyr::desc(.data$attribute_period_date)) %>%
      dplyr::group_by(.data$pseudo_nhs_id) %>%
      dplyr::mutate(dplyr::across(c(-.data$attribute_period_date), ~ dplyr::first(stats::na.omit(.x)))) %>%
      dplyr::slice_head(n=1) %>%
      dplyr::select(-.data$attribute_period_date) %>%
      dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id),
                    sex = as.character(.data$sex))

    #-----------------------------------
    # Now merge the LSOA details in
    var_dict        <- create_data_table_variable_dictionary(db_conn_struct$swd_lsoa$table_name)       #create the named list to convert to standardised naming
    base_vars       <- c(lower_layer_super_output_area_code                = var_dict[["lower_layer_super_output_area_code"]],
                         lower_layer_super_output_area_name                = var_dict[["lower_layer_super_output_area_name"]],
                         lower_layer_super_output_area_16plus_population   = var_dict[["lower_layer_super_output_area_16plus_population"]],
                         lower_layer_super_output_area_pct_ethnic_minority = var_dict[["lower_layer_super_output_area_pct_ethnic_minority"]],
                         middle_layer_super_output_area_name               = var_dict[["middle_layer_super_output_area_name"]],
                         area_ward_name                                    = var_dict[["area_ward_name"]])

    swd_lsoa <- db_conn_struct$swd_lsoa$data %>%                                  #a reference to the SQL table
      dplyr::select(dplyr::all_of(base_vars)) %>%                                 #select the variables to work with
      dplyr::distinct() %>%
      dplyr::collect()

    swd_att_hist_data <- swd_att_hist_data %>%
      dplyr::left_join(swd_lsoa, by="lower_layer_super_output_area_code")

  }

  # Extract data from each database / datatable as appropriate
  if(db_conn_struct$swd_att$is_connected){

    var_dict        <- create_data_table_variable_dictionary(db_conn_struct$swd_att$table_name)       #create the named list to convert to standardised naming
    base_vars       <- c(pseudo_nhs_id                                 = var_dict[["pseudo_nhs_id"]],                               #grab the basic variables we'll need
                         index_of_multiple_deprivation_decile          = var_dict[["index_of_multiple_deprivation_decile"]],
                         income_decile                                 = var_dict[["income_decile"]],
                         employment_decile                             = var_dict[["employment_decile"]],
                         air_quality_indicator                         = var_dict[["air_quality_indicator"]],
                         urban_rural_classification                    = var_dict[["urban_rural_classification"]])

    swd_att_data <- db_conn_struct$swd_att$data %>%                               #a reference to the SQL table
      dplyr::select(dplyr::all_of(base_vars)) %>%                                 #select the variables to work with
      dplyr::distinct() %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &                                 #leave if no id, as can't do anything with these
                      .data$pseudo_nhs_id %in% patient_ids) %>%
      dplyr::collect() %>%
      dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id))

  }

  ids <- data.frame(pseudo_nhs_id = patient_ids)
  data <- purrr::reduce(list(ids, sus_apc_data, swd_att_data, swd_att_hist_data), dplyr::full_join, by = "pseudo_nhs_id")

  # Coalesce and Clean up the sex and gender coding
  sex_codes       <- get_codes(file_path = system.file("sex_codes/sex_codes.csv", package = "acsprojectns"))
  ethnicity_codes <- get_codes(file_path = system.file("ethnicity_codes/ethnicity_codes.csv", package = "acsprojectns"))
  mode            <- function(x){ ux   <- unique(x)[!is.na(unique(x))]
                                  r    <- ux[which.max(tabulate(match(x, ux)))]
                                  return(r)}

  data <- data %>%
    tidyr::pivot_longer(tidyr::starts_with("ethnicity"), names_to = "ethnicity_old",  values_to = "ethnicity_code") %>%
    tidyr::pivot_longer(tidyr::starts_with("sex"),       names_to = "sex_old",        values_to = "sex_code") %>%
    fuzzyjoin::regex_left_join(ethnicity_codes, by="ethnicity_code", ignore_case =TRUE) %>%
    fuzzyjoin::regex_left_join(sex_codes,       by="sex_code",       ignore_case =TRUE) %>%
    dplyr::group_by(.data$pseudo_nhs_id) %>%
    dplyr::mutate(ethnicity       = purrr::map_chr(.data$ethnicity,       ~ mode(.x)),         # here we take the most occurring (mode) value, in case there are different values
                  ethnicity_group = purrr::map_chr(.data$ethnicity_group, ~ mode(.x)),
                  sex             = purrr::map_chr(.data$sex,             ~ mode(.x))) %>%
    dplyr::slice_head() %>%
    dplyr::ungroup() %>%
    dplyr::relocate(.data$ethnicity, .data$ethnicity_group, .data$sex, .after = .data$dob_estimate) %>%
    dplyr::select(!dplyr::matches("\\.x$|\\.y$|_old$")) %>%
    dplyr::group_nest(.data$pseudo_nhs_id, .key = "demographics")


  # Stop the clock
  message(paste("Time taken to execute load_demographics() =",
                round((proc.time() - ptm)[3], digits=2)), " seconds")

  return(data)
}
