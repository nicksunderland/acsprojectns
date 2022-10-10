#' load_mortality
#'
#' @param db_conn_struct list of Database objects
#' @param patients list of Patient objects
#'
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @return modifies the patient objects in-place
#'
#' @export
#'
load_mortality <- function(db_conn_struct,
                             patients){

  # Timing checks
  ptm <- proc.time() # Start the clock

  # Checks and adjustments
  stopifnot(all(purrr::map_lgl(db_conn_struct, ~ checkmate::test_class(.x, "Database"))),
            all(purrr::map_lgl(patients,       ~ checkmate::test_class(.x, "Patient" ))))

  # Starting variables
  . = NULL
  ids = purrr::map_dbl(patients, ~ .x$id)

  # Extract data from each database / datatable as appropriate
  if(db_conn_struct$mortality$is_connected){

    var_dict        <- create_data_table_variable_dictionary(db_conn_struct$mortality$table_name)    #create the named list to convert to standardised naming
    base_vars       <- c(pseudo_nhs_id = var_dict[["pseudo_nhs_id"]],                                #grab the basic variables we'll need
                         date_of_death = var_dict[["date_of_death"]])

    # Generate the sql query and collect from the database
    data <- db_conn_struct$mortality$data %>%  #a reference to the SQL table
      dplyr::select(dplyr::all_of(base_vars)) %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                    .data$pseudo_nhs_id %in% ids) %>%
      dplyr::collect() %>%                                                        #pull all the data to local
      dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id),
                    date_of_death = as.POSIXct(.data$date_of_death, tz = "GMT")) %>%
      dplyr::group_by(.data$pseudo_nhs_id) %>%
      dplyr::slice_min(.data$date_of_death, with_ties = FALSE) %>%
      dplyr::ungroup() %>%
      dplyr::distinct(.data$pseudo_nhs_id,
                      .data$date_of_death)

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = ids)
    data <- dplyr::left_join(ids, data, by = "pseudo_nhs_id")

    # Invisibly add the data into the Patient objects
    #furrr::future_map2_dbl(patients, data$date_of_death, function(x, y) x$set_date_of_death(y))
    # dont know why furrr doesnt update the R6 object
    purrr::map2_dbl(patients, data$date_of_death, function(x, y) x$set_date_of_death(y))
  }

  # Stop the clock
  message(paste("Time taken to execute load_mortality() =",
                round((proc.time() - ptm)[3], digits=2)), " seconds")

  invisible(NULL)
}
