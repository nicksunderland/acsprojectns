#' load_demographics
#'
#' @param db_conn_struct list of Database objects
#' @param patients list of Patient objects
#'
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @return modifies the patient objects in-place
#' @export
#'
load_demographics <- function(db_conn_struct,
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
  if(db_conn_struct$sus_apc$is_connected){

    var_dict        <- create_data_table_variable_dictionary(db_conn_struct$sus_apc$table_name)            #create the named list to convert to standardised naming
    base_vars       <- c(pseudo_nhs_id        = var_dict[["pseudo_nhs_id"]],            #grab the basic variables we'll need
                         episode_start_date   = var_dict[["episode_start_date"]],
                         age                  = var_dict[["age"]])

    # Auto-create the SQL query and execute
    # Had to use filter_at() instead of newer filter(dplyr::across()) as I think it struggles to convert to SQL code
    #     https://stackoverflow.com/questions/26497751/pass-a-vector-of-variable-names-to-arrange-in-dplyr
    #     https://dplyr.tidyverse.org/articles/programming.html#fnref1


    data <- db_conn_struct$sus_apc$data %>%                                       #a reference to the SQL table
      dplyr::select(dplyr::all_of(base_vars)) %>%                                 #select the variables to work with
      dplyr::distinct() %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &                                 #leave if no id, as can't do anything with these
                    !is.na(.data$episode_start_date) &
                    !is.na(.data$age) &
                    .data$pseudo_nhs_id %in% ids) %>%
      dplyr::collect() %>%
      dplyr::group_by(.data$pseudo_nhs_id) %>%
      dplyr::mutate(min_dob = lubridate::as_datetime(.data$episode_start_date, tz = "GMT") - lubridate::duration(as.numeric(.data$age)+1.0, "years")
                                                                                           + lubridate::duration(1, "day"),
                    max_dob = lubridate::as_datetime(.data$episode_start_date, tz = "GMT") - lubridate::duration(as.numeric(.data$age), "years"),
                    pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id)) %>%
      dplyr::group_by(.data$pseudo_nhs_id) %>%
      dplyr::mutate(dob_interval_est = lubridate::interval(max(.data$min_dob), min(.data$max_dob)),
                    dob_estimate = lubridate::int_start(.data$dob_interval_est) + (lubridate::int_end(.data$dob_interval_est) - lubridate::int_start(.data$dob_interval_est))/2.0) %>%
      dplyr::ungroup() %>%
      dplyr::distinct(.data$pseudo_nhs_id,
                      .data$dob_estimate)

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = ids)
    data <- dplyr::left_join(ids, data, by = "pseudo_nhs_id")

    # Invisibly add the data into the Patient objects
    #purrr::future_map2_dbl(patients, data$dob_estimate, function(x, y) x$set_date_of_birth(y))
    #purrr doesn't seem to update the R6 object ...???
    purrr::map2_dbl(patients, data$dob_estimate, function(x, y) x$set_date_of_birth(y))
  }

  # Stop the clock
  message(paste("Time taken to execute load_demographics() =",
                round((proc.time() - ptm)[3], digits=2)), " seconds")

  invisible(NULL)
}
