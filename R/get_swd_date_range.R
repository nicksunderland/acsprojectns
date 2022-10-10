#' get_swd_date_range
#'
#' @param swd_att_hist an R6 Database object connected to the SWD attributes history database
#' @param patient_ids list of patient ids
#'
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#'
#' @return a df to append to the ids
#' @export
#'
get_swd_date_range <- function(swd_att_hist,
                               patient_ids){

  # Timing checks
  ptm <- proc.time() # Start the clock

  # Checks and adjustments
  stopifnot(R6::is.R6(swd_att_hist),
            swd_att_hist$is_connected,
            length(patient_ids) == length(unique(patient_ids)),
            all(purrr::map_lgl(patient_ids,    ~ is.double(.x))))

  # Starting variables
  . = NULL

  var_dict        <- create_data_table_variable_dictionary(swd_att_hist$table_name)       #create the named list to convert to standardised naming
  base_vars       <- c(pseudo_nhs_id         = var_dict[["pseudo_nhs_id"]],                               #grab the basic variables we'll need
                       attribute_period_date = var_dict[["attribute_period_date"]])

  swd_att_hist_data <- swd_att_hist$data %>%                                       #a reference to the SQL table
    dplyr::select(dplyr::all_of(base_vars)) %>%                                 #select the variables to work with
    dplyr::distinct() %>%
    dplyr::filter(!is.na(.data$pseudo_nhs_id) &                                 #leave if no id, as can't do anything with these
                  !is.na(.data$attribute_period_date) &
                  .data$pseudo_nhs_id %in% patient_ids) %>%
    dplyr::collect() %>%
    dplyr::mutate(pseudo_nhs_id = as.numeric(.data$pseudo_nhs_id),
                  attribute_period_date = lubridate::as_date(.data$attribute_period_date)) %>%
    dplyr::group_by(.data$pseudo_nhs_id) %>%
    dplyr::mutate(swd_date_range = lubridate::interval(min(.data$attribute_period_date, na.rm=T),
                                                       max(.data$attribute_period_date, na.rm=T), tzone = "GMT")) %>%
    dplyr::slice(n=1) %>%
    dplyr::select(.data$pseudo_nhs_id,
                  .data$swd_date_range)

  # Check the interval is positive


  ids <- data.frame(pseudo_nhs_id = patient_ids)
  swd_att_hist_data <- dplyr::left_join(ids, swd_att_hist_data, by = "pseudo_nhs_id")

  # Stop the clock
  message(paste("Time taken to execute load_swd_attributes() =",
                round((proc.time() - ptm)[3], digits=2)), " seconds")

  return(swd_att_hist_data)
}
