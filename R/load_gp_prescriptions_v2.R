#' load_gp_prescriptions
#'
#' @param db_conn_struct list of Database objects
#' @param patient_ids list of Patient objects
#'
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @return modifies the patient objects in-place
#'
#' @export
#'
load_gp_prescriptions_v2 <- function(db_conn_struct,
                                     patient_ids){

  # Timing checks
  ptm <- proc.time() # Start the clock

  # Checks and adjustments
  stopifnot(all(purrr::map_lgl(db_conn_struct, ~ checkmate::test_class(.x, "Database"))),
            all(purrr::map_lgl(patient_ids,    ~ is.double(.x))))

  # Starting variables
  . = NULL

  # Extract data from each database / datatable as appropriate
  if(db_conn_struct$swd_act$is_connected){

    var_dict  <- create_data_table_variable_dictionary(db_conn_struct$swd_act$table_name)    #create the named list to convert to standardised naming
    base_vars <- c(pseudo_nhs_id         = var_dict[["pseudo_nhs_id"]],              #grab the basic variables we'll need
                   episode_start_date    = var_dict[["episode_start_date"]],         #datetime_window looking at prescriptions start_date==end_date
                   activity_type         = var_dict[["pod_level_1"]],                #the column containing the 'primary_care_prescription' flag
                   prescription_quantity = var_dict[["details_level_1a"]],           #the column containing ????the number of tablets given????
                   prescription_info     = var_dict[["details_level_1b"]],           #the column containing the description of the drug prescribed
                   prescription_type     = var_dict[["pod_level_2a"]],               #the column containing whether 'repeat' or 'acute' prescription
                   prescription_cost     = var_dict[["episode_cost_1"]])             #the column containing the cost of the medication
    med_filter_string <- "(?i)([A-z ]+)(\\d+\\.?\\d*)?([A-z]+)?[ ]*(?:[A-z]*)"

    # Auto-create the SQL query and execute
    # Had to use filter_at() instead of newer filter(dplyr::across()) as I think it struggles to convert to SQL code
    #     https://stackoverflow.com/questions/26497751/pass-a-vector-of-variable-names-to-arrange-in-dplyr
    #     https://dplyr.tidyverse.org/articles/programming.html#fnref1
    data <- db_conn_struct$swd_act$data %>%                                                  #a reference to the SQL table
      dplyr::select(dplyr::all_of(base_vars)) %>%                                  #select the variables to work with
      dplyr::distinct() %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                    .data$pseudo_nhs_id %in% patient_ids &
                    .data$activity_type == 'primary_care_prescription') %>%
      #dplyr::show_query() %>%
      dplyr::collect() %>%                                                        #pull all the data to local
      dplyr::mutate(pseudo_nhs_id              = as.numeric(.data$pseudo_nhs_id),
                    episode_start_datetime     = lubridate::ymd_hms(paste(.data$episode_start_date, "00:00:00"), tz="GMT"),
                    medication_name            = trimws(stringr::str_match(.data$prescription_info, pattern = med_filter_string)[,2]),
                    medication_dose            = stringr::str_match(.data$prescription_info, pattern = med_filter_string)[,3],
                    medication_units           = stringr::str_match(.data$prescription_info, pattern = med_filter_string)[,4],
                    medication_source          = "GP") %>%
      dplyr::select(.data$pseudo_nhs_id,
                    .data$episode_start_datetime,
                    .data$medication_source,
                    .data$medication_name,
                    .data$medication_dose,
                    .data$medication_units,
                    .data$prescription_quantity,
                    .data$prescription_type,
                    .data$prescription_cost)

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = patient_ids)
    data <- dplyr::left_join(ids, data, by = "pseudo_nhs_id") %>% # creates NA entries for the IDs with missing medication data
      dplyr::group_nest(.data$pseudo_nhs_id, .key = "gp_prescriptions")
  }

  # Stop the clock
  message(paste("Time taken to execute load_gp_prescriptions() =",
                round((proc.time() - ptm)[3], digits=2)), " seconds")

  return(data)
}
