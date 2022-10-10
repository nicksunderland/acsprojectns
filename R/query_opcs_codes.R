#' Title query_opcs_codes
#'
#' @param codes A vector OPCS codes to search for e.g. c("K72")
#' @param search_strat_date The date search strategy; return the most recent date with "max", or the most historical date with "min"
#' @param time_window The time window to search within - must be a lubridate::interval() object
#' @param return_vals A switch on what is being returned. If nothing found then NA is returned. Can be a vector, in which case multiple colums returned
#' "code" returns the OPCS code
#' "spell_interval" returns the spell interval in which the OPCS code was found
#' @param hospital_events_tbl table of hospital episodes and associated data
#' @param search_strat_adm_method the admission method to filter on, on of c(...)
#' "exists" returns a boolean as to whether a code was found within the time window
#' @return Depends on the return_val parameter
#'
#' @export
#'
query_opcs_codes <- function(hospital_events_tbl, codes, search_strat_date, search_strat_adm_method="all", time_window, return_vals){

  # Input checks
  stopifnot(
    is.character(codes),
    search_strat_date %in% c("min", "max"),
    "Admission method search strategy must be one of: 'all', 'emergency', 'elective'"
    = search_strat_adm_method %in% c("all", "emergency", "elective"),
    lubridate::is.interval(time_window),
    all(purrr::map_lgl(return_vals, ~ .x %in% c("code", "procedure_datetime", "admission_method", "spell_interval")))
  )

  # Return the requested "return_val" type, or an appropriately typed NA
  na_return = list("code"               = NA_character_,
                   "procedure_datetime" = as.POSIXct(NA_real_),
                   "spell_interval"     = lubridate::interval(as.POSIXct(NA_real_), as.POSIXct(NA_real_), tzone = "GMT"),
                   "admission_method"   = NA_character_)
  if(is.null(hospital_events_tbl)){
    return(na_return[return_vals])
  }

  # Starting variables
  . = NULL

  # Expand the OPCS codes
  df_opcs_code_datetimes <- hospital_events_tbl %>%
    tidyr::unchop(., cols = c(.data$procedure_opcs_codes)) %>%
    dplyr::select(code               = .data$procedure_opcs_codes,
                  procedure_datetime = .data$episode_start_datetime,
                  spell_interval     = .data$spell_interval,
                  admission_method   = .data$admission_method) %>%
    dplyr::filter(!is.na(.data$code)) %>%
    dplyr::mutate(code = as.character(.data$code))


  # See if our codes are present
  l <- df_opcs_code_datetimes %>%
    dplyr::filter(.data$code %in% codes &
                  lubridate::`%within%`(.data$procedure_datetime, time_window),
                  dplyr::case_when(search_strat_adm_method == "elective"  ~ base::grepl("elective" , .data$admission_method, ignore.case=TRUE),
                                   search_strat_adm_method == "emergency" ~ base::grepl("emergency", .data$admission_method, ignore.case=TRUE),
                                   search_strat_adm_method == "all"       ~ TRUE,
                                   TRUE ~ TRUE)) %>%
    dplyr::arrange(match(.data$code, codes)) %>% # arrange the codes by the input code vector (need to put the most important first)
    {if(search_strat_date == "min")                    dplyr::slice_min(., lubridate::int_start(.data$spell_interval), with_ties = FALSE) else
      if(search_strat_date == "max")                   dplyr::slice_max(., lubridate::int_start(.data$spell_interval), with_ties = FALSE)}


  # Return the requested "return_val" type, or an appropriately typed NA
  if(nrow(l)==0){
    return(na_return[return_vals])
  }else{
    return(l[return_vals])
  }
}
