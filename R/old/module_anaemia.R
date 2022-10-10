#' module_anaemia
#'
#' @param id_list character list of nhs / patient IDs
#' @param datetime_window either a list of lubridate::interval objects (one for each ID), or a single lubridate::interval object (i.e. a fixed date range)
#'
#' @importFrom lubridate `%within%`
#' @importFrom magrittr "%>%"
#' @importFrom rlang .data
#' @importFrom rlang :=
#'
#' @return the anaemia module data frame
#'
#' @export
#'
module_anaemia <- function(id_list,
                           datetime_window){

  # Checks and adjustments
  stopifnot((length(datetime_window) == length(id_list) | length(datetime_window) == 1),
            (lubridate::is.interval(datetime_window)))

  # Starting variables
  . = NULL
  db_conn_struct   <- create_db_struct()
  on.exit( disconnect_db_struct(db_conn_struct) )                               # Close all the connections in the db_conn_struct
  min_date_str     <- as.character(min(lubridate::int_start(datetime_window)))
  max_date_str     <- as.character(max(lubridate::int_end(datetime_window)))
  if(length(datetime_window) != length(id_list)) datetime_window <- rep(datetime_window, length(id_list))
  output_df_struct <- list(ids       = data.frame(pseudo_nhs_id = id_list),
                           df_sus    = NA,
                           df_bloods = NA,
                           df_gp_act = NA,
                           df_gp_att = NA)

  # Extract data from each database / datatable as appropriate
  if(!all(is.na(db_conn_struct$hosp_sus))){

    # Read in the anaemia codes
    anaemia_codes <- readr::read_csv(system.file("anaemia_codes.csv", package = "acsprojectns"), show_col_types = FALSE)

    # Search the SUS data in the maximum/biggest date range for anaemia codes appearing in either the primary or secondary diagnosis code columns
    output_df_struct$df_sus <- get_hospital_admissions(id_list         = id_list,
                                                       level           = "episode",
                                                       codes_list      = anaemia_codes$icd_codes,
                                                       search_strategy = "primary_and_secondary_diagnoses",
                                                       return_all_codes= FALSE,
                                                       datetime_window = lubridate::interval(min(lubridate::int_start(datetime_window)), max(lubridate::int_end(datetime_window))),
                                                       verbose         = FALSE) %>%
      dplyr::left_join(., data.frame(pseudo_nhs_id = id_list, date_range_filter = datetime_window), by = "pseudo_nhs_id") %>%
      dplyr::filter(lubridate::int_start(.data$episode_interval) %within% .data$date_range_filter) %>%   # within function is inclusive of end points
      dplyr::group_by(.data$pseudo_nhs_id) %>%
      dplyr::mutate(anaemia_codes_in_date_range = list(unique(as.character(.data$episode_codes)))) %>%
      dplyr::ungroup() %>%
      dplyr::distinct(.data$pseudo_nhs_id,
                      .data$anaemia_codes_in_date_range)
  }

  if(!all(is.na(db_conn_struct$hosp_bloods))){

    # Create name of the haemoglobin columns
    if(length(unique(datetime_window)) == 1){
      mean_haemoglobin_col_name <- glue::glue("mean_hb_{min_date_str}-to-{max_date_str}")
    }else{
      mean_haemoglobin_col_name <- glue::glue("mean_hb_pt_specific_date_range")
    }

    # Get the data
    output_df_struct$df_bloods <- get_test_results(id_list               = id_list,
                                                   result_type_code_list = c("HB"),
                                                   datetime_window       = lubridate::interval(min(lubridate::int_start(datetime_window)), max(lubridate::int_end(datetime_window))),
                                                   verbose               = FALSE) %>%
      dplyr::mutate(test_result_value = as.numeric(.data$test_result_value),
                    test_datetime     = as.POSIXct(.data$test_datetime)) %>%
      dplyr::left_join(., data.frame(pseudo_nhs_id = id_list, date_range_filter = datetime_window), by = "pseudo_nhs_id") %>%
      dplyr::filter(!is.na(.data$test_result_value) &
                    .data$test_datetime %within% .data$date_range_filter) %>%  # within function is inclusive of end points
      dplyr::group_by(.data$pseudo_nhs_id) %>%
      dplyr::mutate(!!rlang::sym(mean_haemoglobin_col_name) := mean(.data$test_result_value)) %>%
      dplyr::slice_max(.data$test_datetime, with_ties = FALSE) %>%
      dplyr::mutate(latest_haemoglobin = .data$test_result_value,
                    latest_haemoglobin_datetime = .data$test_datetime) %>%
      dplyr::ungroup() %>%
      dplyr::distinct(.data$pseudo_nhs_id,
                      !!rlang::sym(mean_haemoglobin_col_name),
                      .data$latest_haemoglobin,
                      .data$latest_haemoglobin_datetime,
                      haemoglobin_units = .data$test_result_units)

  }

  if(!all(is.na(db_conn_struct$gp_attributes))){

    output_df_struct$df_gp_att <- get_gp_attributes(id_list         = id_list,
                                                    cols_to_extract = c("anaemia_other", "anaemia_iron_deficiency")) %>%
      dplyr::group_by(.data$pseudo_nhs_id) %>%
      dplyr::mutate(n = dplyr::n()) %>%
      dplyr::slice_max(.data$n, with_ties = FALSE) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(anaemia_other                  = tidyr::replace_na(.data$anaemia_other, 0),
                    anaemia_iron_deficiency        = tidyr::replace_na(.data$anaemia_iron_deficiency, 0)) %>%
      dplyr::rename(gp_att_anaemia_other           = .data$anaemia_other,
                    gp_att_anaemia_iron_deficiency = .data$anaemia_iron_deficiency)

    if(max(output_df_struct$df_gp_att$n) > 1) warning("Duplicate ID rows in 'gp_attributes' have been removed")

    output_df_struct$df_gp_att <- output_df_struct$df_gp_att %>%
      dplyr::select(-c(.data$n))
  }


 return( plyr::join_all(output_df_struct[!is.na(output_df_struct)], by = "pseudo_nhs_id", type = "left") )
}
