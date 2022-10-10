#' load_swd_activity_v2
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
load_swd_activity_v2 <- function(db_conn_struct,
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
                   spell_start_date      = var_dict[["episode_start_date"]],         #datetime_window looking at prescriptions start_date==end_date
                   spell_end_date        = var_dict[["episode_end_date"]],         #datetime_window looking at prescriptions start_date==end_date
                   activity_provider     = var_dict[["pod_level_1"]],                #the column containing the 'primary_care_contact' or ' secondary' or 'community' etc flag
                   activity_setting      = var_dict[["pod_level_2a"]],               #the column containing the descriptor of who the practitioner was e.g. 'gp' or 'anp'
                   activity_urgency      = var_dict[["pod_level_2b"]],               #the column containing the descriptor of the urgency
                   activity_speciality   = var_dict[["details_level_1b"]],           #the column containing the 'primary_care_contact' or ' secondary' or 'community' etc flag
                   activity_cost         = var_dict[["episode_cost_1"]])             #the column containing the cost of the medication


    # Auto-create the SQL query and execute
    # Had to use filter_at() instead of newer filter(dplyr::across()) as I think it struggles to convert to SQL code
    #     https://stackoverflow.com/questions/26497751/pass-a-vector-of-variable-names-to-arrange-in-dplyr
    #     https://dplyr.tidyverse.org/articles/programming.html#fnref1
    data <- db_conn_struct$swd_act$data %>%
      dplyr::select(dplyr::all_of(base_vars)) %>%                                  #select the variables to work with
      dplyr::distinct() %>%
      dplyr::filter(!is.na(.data$pseudo_nhs_id) &
                    !is.na(.data$spell_start_date) &
                    !is.na(.data$spell_end_date) &
                    .data$pseudo_nhs_id %in% patient_ids &
                    .data$activity_provider %in% c('primary_care_contact', 'secondary', 'community', "mental_health")) %>%
      #dplyr::show_query() %>%
      dplyr::collect() %>%
      dplyr::mutate(pseudo_nhs_id   = as.numeric(.data$pseudo_nhs_id),
                    spell_interval  = lubridate::interval(.data$spell_start_date, .data$spell_end_date, tz="GMT")) %>%
      dplyr::select(.data$pseudo_nhs_id,
                    .data$spell_interval,
                    .data$activity_provider,
                    .data$activity_setting,
                    .data$activity_urgency,
                    .data$activity_speciality,
                    .data$activity_cost)

    # Join on the ids; for some reason it doesn't like this in the above pipe....
    ids <- data.frame(pseudo_nhs_id = patient_ids)
    data <- dplyr::left_join(ids, data, by = "pseudo_nhs_id") %>%
      dplyr::group_nest(.data$pseudo_nhs_id, .key = "swd_activity")
  }

  # Stop the clock
  message(paste("Time taken to execute load_swd_activity() =",
                round((proc.time() - ptm)[3], digits=2)), " seconds")

  return(data)
}
