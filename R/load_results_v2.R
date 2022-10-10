#' load_swd_measurements_v2
#'
#' @param db_conn_struct list of Database objects
#' @param patient_ids list of Patient ids
#'
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @return the swd activity data frame
#'
#' @export
#'
load_swd_measurements_v2 <- function(db_conn_struct,
                            patient_ids){

  # Timing checks
  ptm <- proc.time() # Start the clock

  # Checks and adjustments
  stopifnot(all(purrr::map_lgl(db_conn_struct, ~ checkmate::test_class(.x, "Database"))),
            all(purrr::map_lgl(patient_ids,    ~ is.double(.x))))


  # fev1
  # albumin
  # mrc_dyspnoea
  # hba1c
  # creatinine
  # cholesterol
  # blood_pressure
  # bmi
  # egfr
  # efi_category
  # alcohol_cscore


  # Starting variables
  . = NULL

  # Extract data from each database / datatable as appropriate
  if(db_conn_struct$results$is_connected){


    var_dict  <- create_data_table_variable_dictionary(db_conn_struct$results$table_name)    #create the named list to convert to standardised naming
    base_vars <- c(pseudo_nhs_id         = var_dict[["pseudo_nhs_id"]],              #grab the basic variables we'll need
                   measurement_type      = var_dict[["measurement_type"]],
                   measurement_datetime  = var_dict[["measurement_datetime"]],
                   measurement_value     = var_dict[["measurement_value"]])

    data <- db_conn_struct$results$data %>%
      dplyr::select(dplyr::all_of(base_vars)) %>%                                  #select the variables to work with
      dplyr::distinct() %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                    .data$pseudo_nhs_id %in% patient_ids) %>%
      dplyr::collect() %>%
      dplyr::mutate(pseudo_nhs_id   = as.numeric(.data$pseudo_nhs_id))

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = patient_ids)
    data <- dplyr::left_join(ids, data, by = "pseudo_nhs_id") %>%
      dplyr::group_nest(.data$pseudo_nhs_id, .key = "swd_measurements")
  }

  # Stop the clock
  message(paste("Time taken to execute load_swd_measurements_v2() =",
                round((proc.time() - ptm)[3], digits=2)), " seconds")

  return(data)
}

