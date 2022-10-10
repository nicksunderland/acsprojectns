#' module_hospital_events
#'
#' @param id_list list of IDs
#' @param codes_list list of ICD 10 codes
#' @param datetime_window lubridate::interval object, either 1 or length(id_list) for patient specific windows
#'
#' @return the events df summary
#' @export
#'
module_hospital_events <- function(id_list,
                                   codes_list,
                                   datetime_window){

  # Checks and adjustments
  stopifnot((length(datetime_window) == length(id_list) | length(datetime_window) == 1),
            (lubridate::is.interval(datetime_window) & !is.na(datetime_window)),
            (is.atomic(codes_list) & !is.na(codes_list)))

  # Starting variables
  . = NULL
  db_conn_struct   <- create_db_struct()
  on.exit( disconnect_db_struct(db_conn_struct) )                               # Close all the connections in the db_struct
  min_date_str     <- as.character(min(lubridate::int_start(datetime_window)))
  max_date_str     <- as.character(max(lubridate::int_end(datetime_window)))
  if(length(datetime_window) != length(id_list)) datetime_window <- rep(datetime_window, length(id_list))
  output_df_struct <- list(ids       = data.frame(pseudo_nhs_id = id_list),
                           df_sus    = NA,
                           df_bloods = NA,
                           df_gp_act = NA,
                           df_gp_att = NA,
                           df_mort   = NA)

  # Extract data from each database / datatable as appropriate
  if(!all(is.na(db_conn_struct$hosp_sus))){

    output_df_struct$df_sus <- get_hospital_admissions(codes_list = codes_list,
                                                       return_all_codes = FALSE,
                                                       search_strategy = "primary_diagnosis",
                                                       datetime_window = lubridate::interval(min(lubridate::int_start(datetime_window)), max(lubridate::int_end(datetime_window)), tzone = "GMT"),
                                                       id_list = id_list,
                                                       ccg_provider_code_list = NA,
                                                       level = "spell",
                                                       verbose = FALSE) %>%
      {if(nrow(.) == 0){
        stats::setNames(data.frame(matrix(ncol = 7, nrow = 0)), c("pseudo_nhs_id", "num_spells", "all_spell_codes", "min_spell_interval", "min_spell_codes", "max_spell_interval", "max_spell_codes" ))
       }else{
        dplyr::left_join(., data.frame(pseudo_nhs_id = id_list, date_range_filter = datetime_window), by = "pseudo_nhs_id") %>%
        dplyr::filter(lubridate::int_start(.data$spell_interval) %within% .data$date_range_filter) %>%   # within function is inclusive of end points
        dplyr::group_by(.data$pseudo_nhs_id) %>%
        dplyr::mutate(min_spell_interval = .data$spell_interval[which.min(lubridate::int_start(.data$spell_interval))],
                      min_spell_codes    = .data$spell_codes   [which.min(lubridate::int_start(.data$spell_interval))],
                      max_spell_interval = .data$spell_interval[which.max(lubridate::int_start(.data$spell_interval))],
                      max_spell_codes    = .data$spell_codes   [which.max(lubridate::int_start(.data$spell_interval))],
                      num_spells         = dplyr::n(),
                      all_spell_codes    = list(unique(as.character(unlist(.data$spell_codes))))) %>%
        dplyr::ungroup() %>%
        dplyr::distinct(.data$pseudo_nhs_id, .keep_all = TRUE) %>%
        dplyr::select(.data$pseudo_nhs_id,
                      .data$num_spells,
                      .data$all_spell_codes,
                      .data$min_spell_interval,
                      .data$min_spell_codes,
                      .data$max_spell_interval,
                      .data$max_spell_codes)
       }
      }
  }


  if(!all(is.na(db_conn_struct$uk_mortality))){
    # nothing relevant from this database
  }


  return( plyr::join_all(output_df_struct[!is.na(output_df_struct)], by = "pseudo_nhs_id", type = "left") )

}
